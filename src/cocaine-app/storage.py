# -*- coding: utf-8 -*-
import functools
import logging
import math
import time
import types

import msgpack

from errors import CacheUpstreamError
import jobs.job
from jobs.job_types import JobTypes
from infrastructure import infrastructure
from infrastructure_cache import cache
from config import config
from mastermind_core.storage import Host as BaseHost
from mastermind_core.storage import NodeBackend as BaseNodeBackend
from mastermind_core.storage import Node
from mastermind_core.storage import Fs
from mastermind_core.storage import Status
from mastermind.query.couples import Couple as CoupleInfo
from mastermind.query.groups import Group as GroupInfo

logger = logging.getLogger('mm.storage')


VFS_RESERVED_SPACE = config.get('reserved_space', 112742891520)  # default is 105 Gb for one vfs
NODE_BACKEND_STAT_STALE_TIMEOUT = config.get('node_backend_stat_stale_timeout', 120)

FORBIDDEN_DHT_GROUPS = config.get('forbidden_dht_groups', False)
FORBIDDEN_DC_SHARING_AMONG_GROUPS = config.get('forbidden_dc_sharing_among_groups', False)
FORBIDDEN_NS_WITHOUT_SETTINGS = config.get('forbidden_ns_without_settings', False)
FORBIDDEN_UNMATCHED_GROUP_TOTAL_SPACE = config.get('forbidden_unmatched_group_total_space', False)

CACHE_GROUP_PATH_PREFIX = config.get('cache', {}).get('group_path_prefix')


GOOD_STATUSES = set([Status.OK, Status.FULL])
NOT_BAD_STATUSES = set([Status.OK, Status.FULL, Status.FROZEN])


class ResourceError(KeyError):
    def __str__(self):
        return str(self.args[0])


class Repositary(object):
    def __init__(self, constructor, resource_desc=None):
        self.elements = {}
        self.constructor = constructor
        self.resource_desc = resource_desc or self.constructor.__name__

    def add(self, *args, **kwargs):
        e = self.constructor(*args, **kwargs)
        self.elements[e] = e
        return e

    def get(self, key):
        try:
            return self.elements[key]
        except KeyError:
            raise ResourceError('{} {} is not found'.format(
                self.resource_desc, key))

    def remove(self, key):
        return self.elements.pop(key)

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        return key in self.elements

    def __iter__(self):
        return self.elements.__iter__()

    def __repr__(self):
        return '<Repositary object: [%s] >' % (', '.join((repr(e) for e in self.elements.itervalues())))

    def keys(self):
        return self.elements.keys()

    def __len__(self):
        return len(self.elements)


class Host(BaseHost):

    @property
    def hostname(self):
        return cache.get_hostname_by_addr(self.addr)

    @property
    def hostname_or_not(self):
        return cache.get_hostname_by_addr(self.addr, strict=False)

    @property
    def dc(self):
        return cache.get_dc_by_host(self.hostname)

    @property
    def dc_or_not(self):
        return cache.get_dc_by_host(self.hostname, strict=False)

    @property
    def parents(self):
        return cache.get_host_tree(self.hostname)

    @property
    def full_path(self):
        parent = self.parents
        parts = [parent['name']]
        while 'parent' in parent:
            parent = parent['parent']
            parts.append(parent['name'])
        return '|'.join(reversed(parts))


class NodeBackend(BaseNodeBackend):

    @property
    def effective_space(self):

        if self.stat is None:
            return 0

        share = float(self.stat.total_space) / self.stat.vfs_total_space
        free_space_req_share = math.ceil(VFS_RESERVED_SPACE * share)

        return int(max(0, self.stat.total_space - free_space_req_share))

    @effective_space.setter
    def effective_space(self, value):
        pass

    @property
    def effective_free_space(self):
        if self.stat.vfs_free_space <= VFS_RESERVED_SPACE:
            return 0
        return max(
            self.stat.free_space - (self.stat.total_space - self.effective_space),
            0
        )

    @effective_free_space.setter
    def effective_free_space(self, value):
        pass

    def update_statistics_status(self):
        self.stalled = self.stat.ts < (time.time() - NODE_BACKEND_STAT_STALE_TIMEOUT)


class Group(object):

    DEFAULT_NAMESPACE = 'default'
    CACHE_NAMESPACE = 'storage_cache'

    TYPE_DATA = 'data'
    TYPE_CACHE = 'cache'
    TYPE_UNMARKED = 'unmarked'

    def __init__(self, group_id, node_backends=None):
        self.group_id = group_id
        self.status = Status.INIT
        self.node_backends = []
        self.couple = None
        self.meta = None
        self.status_text = "Group %s is not inititalized yet" % (self)
        self.active_job = None

        for node_backend in node_backends or []:
            self.add_node_backend(node_backend)

    def add_node_backend(self, node_backend):
        self.node_backends.append(node_backend)
        if node_backend.group:
            node_backend.group.remove_node_backend(node_backend)
        node_backend.set_group(self)

    def remove_node_backend(self, node_backend):
        self.node_backends.remove(node_backend)
        if node_backend.group is self:
            node_backend.remove_group()

    @property
    def want_defrag(self):
        for nb in self.node_backends:
            if nb.stat and nb.stat.want_defrag > 1:
                return True
        return False

    def parse_meta(self, meta):
        if meta is None:
            self.meta = None
            return

        parsed = msgpack.unpackb(meta)
        if isinstance(parsed, (tuple, list)):
            self.meta = {'version': 1, 'couple': parsed, 'namespace': self.DEFAULT_NAMESPACE, 'frozen': False}
        elif isinstance(parsed, dict) and parsed['version'] == 2:
            self.meta = parsed
        else:
            raise Exception('Unable to parse meta')

    def equal_meta(self, other):
        if type(self.meta) != type(other.meta):
            return False
        if self.meta is None:
            return True

        negligeable_keys = ['service', 'version']
        for key in set(self.meta.keys() + other.meta.keys()):
            if key in negligeable_keys:
                continue
            if self.meta.get(key) != other.meta.get(key):
                return False

        return True

    def get_stat(self):
        return reduce(lambda res, x: res + x, [nb.stat for nb in self.node_backends if nb.stat])

    def update_status_recursive(self):
        if self.couple:
            self.couple.update_status()
        else:
            self.update_status()

    @property
    def effective_space(self):
        return sum(nb.effective_space for nb in self.node_backends)

    @property
    def type(self):
        if not self.meta:

            if CACHE_GROUP_PATH_PREFIX:

                def is_cache_group_backend(nb):
                    return nb.base_path.startswith(CACHE_GROUP_PATH_PREFIX)

                if any(is_cache_group_backend(nb) for nb in self.node_backends):
                    return self.TYPE_UNMARKED

            return self.TYPE_DATA
        if self.meta.get('type') == self.TYPE_CACHE:
            return self.TYPE_CACHE
        return self.TYPE_DATA

    def update_status(self):
        """Updates group's own status.
        WARNING: This method should not take into consideration any of the
        groups' state coupled with itself nor any of the couple attributes,
        properties, state, etc."""

        if not self.node_backends:
            logger.info('Group {0}: no node backends, status set to INIT'.format(self.group_id))
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                                'no node backends serving this group'.format(self))
            return self.status

        if FORBIDDEN_DHT_GROUPS and len(self.node_backends) > 1:
            self.status = Status.BROKEN
            self.status_text = ('Group {} is in BROKEN state because '
                                'is has {} node backends but only 1 is allowed'.format(
                                    self.group_id, len(self.node_backends)))
            return self.status

        # node statuses should be updated before group status is set
        # statuses = tuple(nb.update_status() for nb in self.node_backends)
        statuses = tuple(nb.status for nb in self.node_backends)

        logger.info('In group {0} meta = {1}'.format(self, str(self.meta)))
        if not self.meta:
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because meta key '
                                'was not read from it'.format(self))
            return self.status

        if Status.BROKEN in statuses:
            self.status = Status.BROKEN
            self.status_text = ('Group {0} has BROKEN status because '
                                'some node statuses are BROKEN'.format(self))
            return self.status

        if self.type == self.TYPE_DATA:
            # perform checks for common data group
            status = self.update_storage_group_status()
            if status:
                return status
        elif self.type == self.TYPE_CACHE:
            pass

        if Status.RO in statuses:
            self.status = Status.RO
            self.status_text = ('Group {0} is in Read-Only state because '
                                'there is RO node backends'.format(self))

            service_status = self.meta.get('service', {}).get('status')
            if service_status == Status.MIGRATING:
                if self.active_job and self.meta['service']['job_id'] == self.active_job['id']:
                    self.status = Status.MIGRATING
                    self.status_text = ('Group {0} is migrating, job id is {1}'.format(
                        self, self.meta['service']['job_id']))
                else:
                    self.status = Status.BAD
                    self.status_text = ('Group {0} has no active job, but marked as '
                                        'migrating by job id {1}'.format(
                                            self, self.meta['service']['job_id']))

            return self.status

        if not all(st == Status.OK for st in statuses):
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'some node statuses are not OK'.format(self))
            return self.status

        self.status = Status.COUPLED
        self.status_text = 'Group {0} is OK'.format(self)

        return self.status

    def update_storage_group_status(self):
        if not self.meta['couple']:
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                                'no coupling info'.format(self))
            return self.status

        if not self.couple:
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'couple was not created'.format(self))
            return self.status

        if not self.couple.check_groups(self.meta['couple']):
            self.status = Status.BAD
            self.status_text = ('Group {} is in Bad state because couple check fails'.format(self))
            return self.status

        if not self.meta.get('namespace'):
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'no namespace has been assigned to it'.format(self))
            return self.status

        if self.group_id not in self.meta['couple']:
            self.status = Status.BROKEN
            self.status_text = ('Group {0} is in BROKEN state because '
                                'its group id is missing from coupling info'.format(self))
            return self.status

    def info(self):
        g = GroupInfo(self.group_id)
        data = {'id': self.group_id,
                'status': self.status,
                'status_text': self.status_text,
                'node_backends': [nb.info() for nb in self.node_backends],
                'couple': str(self.couple) if self.couple else None}
        if self.meta:
            data['namespace'] = self.meta.get('namespace')
        if self.active_job:
            data['active_job'] = self.active_job

        g._set_raw_data(data)
        return g

    def set_active_job(self, job):
        if job is None:
            self.active_job = None
            return
        self.active_job = {
            'id': job.id,
            'type': job.type,
            'status': job.status,
        }

    def compose_cache_group_meta(self):
        return {
            'version': 2,
            'type': self.TYPE_CACHE,
            'namespace': self.CACHE_NAMESPACE,
            'couple': (self.group_id,)
        }

    @property
    def coupled_groups(self):
        if not self.couple:
            return []

        return [g for g in self.couple if g is not self]

    def __hash__(self):
        return hash(self.group_id)

    def __str__(self):
        return str(self.group_id)

    def __repr__(self):
        return '<Group object: group_id=%d, status=%s node backends=[%s], meta=%s, couple=%s>' % (self.group_id, self.status, ', '.join((repr(nb) for nb in self.node_backends)), str(self.meta), str(self.couple))

    def __eq__(self, other):
        return self.group_id == other


def status_change_log(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        status = self.status
        new_status = f(self, *args, **kwargs)
        if status != new_status:
            logger.info('Couple {0} status updated from {1} to {2} ({3})'.format(
                self, status, new_status, self.status_text))
        return new_status
    return wrapper


class Couple(object):
    def __init__(self, groups):
        self.status = Status.INIT
        self.namespace = None
        self.groups = sorted(groups, key=lambda group: group.group_id)
        self.meta = None
        for group in self.groups:
            if group.couple:
                raise Exception('Group %s is already in couple' % (repr(group)))

            group.couple = self
        self.status_text = 'Couple {0} is not inititalized yet'.format(str(self))
        self.active_job = None

    def get_stat(self):
        try:
            return reduce(lambda res, x: res * x, [group.get_stat() for group in self.groups])
        except TypeError:
            return None

    def account_job_in_status(self):
        if self.status == Status.BAD:
            if self.active_job and self.active_job['type'] in (JobTypes.TYPE_MOVE_JOB,
                                                               JobTypes.TYPE_RESTORE_GROUP_JOB):
                if self.active_job['status'] in (jobs.job.Job.STATUS_NEW,
                                                 jobs.job.Job.STATUS_EXECUTING):
                    self.status = Status.SERVICE_ACTIVE
                    self.status_text = 'Couple {} has active job {}'.format(
                        str(self), self.active_job['id'])
                else:
                    self.status = Status.SERVICE_STALLED
                    self.status_text = 'Couple {} has stalled job {}'.format(
                        str(self), self.active_job['id'])
        return self.status

    @status_change_log
    def update_status(self):
        statuses = [group.update_status() for group in self.groups]

        for group in self.groups:
            if not group.meta:
                self.status = Status.BAD
                self.status_text = "Couple's group {} has empty meta data".format(group)
                return self.account_job_in_status()
            if self.namespace != group.meta.get('namespace'):
                self.status = Status.BAD
                self.status_text = "Couple's namespace does not match namespace " \
                                   "in group's meta data ('{}' != '{}')".format(
                                       self.namespace, group.meta.get('namespace'))
                return self.status

        self.active_job = None
        for group in self.groups:
            if group.active_job:
                self.active_job = group.active_job
                break

        if any(not self.groups[0].equal_meta(group) for group in self.groups):
            self.status = Status.BAD
            self.status_text = 'Couple {0} groups has unequal meta data'.format(str(self))
            return self.account_job_in_status()

        if any(group.meta and group.meta.get('frozen') for group in self.groups):
            self.status = Status.FROZEN
            self.status_text = 'Couple {0} is frozen'.format(str(self))
            return self.status

        if FORBIDDEN_DC_SHARING_AMONG_GROUPS:
            # checking if any pair of groups has its node backends in same dc
            groups_dcs = []
            for group in self.groups:
                group_dcs = set()
                for nb in group.node_backends:
                    try:
                        group_dcs.add(nb.node.host.dc)
                    except CacheUpstreamError:
                        self.status_text = 'Failed to resolve dc for host {}'.format(nb.node.host)
                        self.status = Status.BAD
                        return self.status
                groups_dcs.append(group_dcs)

            dc_set = groups_dcs[0]
            for group_dcs in groups_dcs[1:]:
                if dc_set & group_dcs:
                    self.status = Status.BROKEN
                    self.status_text = 'Couple {0} has nodes sharing the same DC'.format(str(self))
                    return self.status
                dc_set = dc_set | group_dcs

        if FORBIDDEN_NS_WITHOUT_SETTINGS:
            is_cache_couple = self.namespace.id == Group.CACHE_NAMESPACE
            if not infrastructure.ns_settings.get(self.namespace.id) and not is_cache_couple:
                self.status = Status.BROKEN
                self.status_text = ('Couple {} is assigned to a namespace {}, which is '
                                    'not set up'.format(self, self.namespace))
                return self.status

        if all(st == Status.COUPLED for st in statuses):

            if FORBIDDEN_UNMATCHED_GROUP_TOTAL_SPACE:
                group_stats = [g.get_stat() for g in self.groups]
                total_spaces = [gs.total_space for gs in group_stats if gs]
                if any(ts != total_spaces[0] for ts in total_spaces):
                    self.status = Status.BROKEN
                    self.status_text = ('Couple {0} has unequal total space in groups'.format(
                        str(self)))
                    return self.status

            if self.is_full():
                self.status = Status.FULL
                self.status_text = 'Couple {0} is full'.format(str(self))
            else:
                self.status = Status.OK
                self.status_text = 'Couple {0} is OK'.format(str(self))

            return self.status

        if Status.INIT in statuses:
            self.status = Status.INIT
            self.status_text = 'Couple {0} has uninitialized groups'.format(str(self))

        elif Status.BROKEN in statuses:
            self.status = Status.BROKEN
            self.status_text = 'Couple {0} has broken groups'.format(str(self))

        elif Status.BAD in statuses:

            group_status_texts = []
            for group in filter(lambda g: g.status == Status.BAD, self.groups):
                group_status_texts.append(group.status_text)
            self.status = Status.BAD
            self.status_text = 'Couple {} has bad groups: {}'.format(
                str(self), ', '.join(group_status_texts))

        elif Status.RO in statuses or Status.MIGRATING in statuses:
            self.status = Status.BAD
            self.status_text = 'Couple {0} has read-only groups'.format(str(self))

        else:
            self.status = Status.BAD
            self.status_text = 'Couple {0} is bad for some reason'.format(str(self))

        return self.account_job_in_status()

    def check_groups(self, groups):

        for group in self.groups:
            if group.meta is None or 'couple' not in group.meta or not group.meta['couple']:
                return False

            if set(groups) != set(group.meta['couple']):
                return False

        if set(groups) != set((g.group_id for g in self.groups)):
            return False

        return True

    @property
    def frozen(self):
        return any(group.meta.get('frozen') for group in self.groups if group.meta)

    @property
    def closed(self):
        return self.status == Status.FULL

    def destroy(self):
        if self.namespace:
            self.namespace.remove_couple(self)
        for group in self.groups:
            group.couple = None
            group.meta = None
            group.update_status()

        couples.remove(self)
        self.groups = []
        self.status = Status.INIT

    def compose_group_meta(self, namespace, frozen):
        return {
            'version': 2,
            'couple': self.as_tuple(),
            'namespace': namespace,
            'frozen': bool(frozen),
        }

    RESERVED_SPACE_KEY = 'reserved-space-percentage'

    def is_full(self):

        ns_reserved_space = infrastructure.ns_settings.get(self.namespace.id, {}).get(self.RESERVED_SPACE_KEY, 0.0)

        # TODO: move this logic to effective_free_space property,
        #       it should handle all calculations by itself
        for group in self.groups:
            for nb in group.node_backends:
                if nb.is_full(ns_reserved_space):
                    return True

        if self.effective_free_space <= 0:
            return True

        return False

    @property
    def groups_effective_space(self):
        return min(g.effective_space for g in self.groups)

    @property
    def ns_reserved_space_percentage(self):
        return infrastructure.ns_settings.get(self.namespace.id, {}).get(self.RESERVED_SPACE_KEY, 0.0)

    @property
    def ns_reserved_space(self):
        return int(math.ceil(self.groups_effective_space * self.ns_reserved_space_percentage))

    @property
    def effective_space(self):
        return int(math.floor(
            self.groups_effective_space * (1.0 - self.ns_reserved_space_percentage)
        ))

    @property
    def effective_free_space(self):
        stat = self.get_stat()
        if not stat:
            return 0
        return int(max(stat.free_space -
                       (stat.total_space - self.effective_space), 0))

    @property
    def free_reserved_space(self):
        stat = self.get_stat()
        if not stat:
            return 0
        reserved_space = self.ns_reserved_space
        groups_free_eff_space = stat.free_space - (stat.total_space - self.groups_effective_space)
        if groups_free_eff_space <= 0:
            # when free space is less than what was reserved for service demands
            return 0
        if groups_free_eff_space > reserved_space:
            # when free effective space > 0
            return reserved_space
        return groups_free_eff_space

    def as_tuple(self):
        return tuple(group.group_id for group in self.groups)

    def info(self):
        c = CoupleInfo(str(self))
        data = {'id': str(self),
                'couple_status': self.status,
                'couple_status_text': self.status_text,
                'tuple': self.as_tuple()}
        try:
            data['namespace'] = self.namespace.id
        except ValueError:
            pass
        stat = self.get_stat()
        if stat:
            data['free_space'] = int(stat.free_space)
            data['used_space'] = int(stat.used_space)
            try:
                data['effective_space'] = self.effective_space
                data['free_effective_space'] = self.effective_free_space
                data['free_reserved_space'] = self.free_reserved_space
            except ValueError:
                # failed to determine couple's namespace
                data['effective_space'], data['free_effective_space'] = 0, 0
                data['free_reserved_space'] = 0

        data['groups'] = [g.info().serialize() for g in self.groups]
        data['hosts'] = {
            'primary': []
        }

        c._set_raw_data(data)
        return c

    FALLBACK_HOSTS_PER_DC = config.get('fallback_hosts_per_dc', 10)

    def couple_hosts(self):
        hosts = {'primary': [],
                 'fallback': []}
        used_hosts = set()
        used_dcs = set()

        def serialize_node(node):
            return {
                'host': node.host.hostname,
                'dc': node.host.dc,
            }

        for group in self.groups:
            for nb in group.node_backends:
                node = nb.node
                if node.host in used_hosts:
                    continue
                try:
                    hosts['primary'].append(serialize_node(node))
                except CacheUpstreamError:
                    continue
                used_hosts.add(node.host)
                used_dcs.add(node.host.dc)

        for dc in used_dcs:
            count = 0
            for node in dc_host_view[dc].by_la:
                if node.host in used_hosts:
                    continue
                try:
                    hosts['fallback'].append(serialize_node(node))
                except CacheUpstreamError:
                    continue
                used_hosts.add(node.host)
                count += 1
                if count >= self.FALLBACK_HOSTS_PER_DC:
                    break

        return hosts

    @property
    def keys_diff(self):
        group_keys = []
        for group in self.groups:
            if not len(group.node_backends):
                continue
            group_keys.append(group.get_stat().files)
        if not group_keys:
            return None
        max_keys = max(group_keys)
        return sum(max_keys - gk for gk in group_keys)

    def __contains__(self, group):
        return group in self.groups

    def __iter__(self):
        return self.groups.__iter__()

    def __len__(self):
        return len(self.groups)

    def __str__(self):
        return ':'.join(str(group) for group in self.groups)

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, Couple):
            return self.groups == other.groups

    def __repr__(self):
        return '<Couple object: status={status}, groups=[{groups}] >'.format(
            status=self.status,
            groups=', '.join(repr(g) for g in self.groups),
        )


class DcNodes(object):
    def __init__(self):
        self.nodes = []
        self.__by_la = None

    def append(self, node):
        self.nodes.append(node)

    @property
    def by_la(self):
        if self.__by_la is None:
            self.__by_la = sorted(self.nodes, key=lambda node: node.stat.load_average)
        return self.__by_la


class DcHostView(object):

    def __init__(self):
        self.dcs_hosts = {}

    def update(self):
        dcs_hosts = {}
        # TODO: iterate through hosts when host statistics will be moved
        # to a separate object
        hosts = set()
        for node in nodes:
            dc_hosts = dcs_hosts.setdefault(node.host.dc, DcNodes())
            if node.host in hosts:
                continue
            dc_hosts.append(node)
        self.dcs_hosts = dcs_hosts

    def __getitem__(self, key):
        return self.dcs_hosts[key]


class Namespace(object):
    def __init__(self, id):
        self.id = id
        self.couples = set()

    def add_couple(self, couple):
        if couple.namespace:
            raise ValueError('Couple {} already belongs to namespace {}, cannot be assigned to '
                             'namespace {}'.format(couple, self, couple.namespace))
        self.couples.add(couple)
        couple.namespace = self

    def remove_couple(self, couple):
        self.couples.remove(couple)
        couple.namespace = None

    def __str__(self):
        return self.id

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, types.StringTypes):
            return str(self) == other

        if isinstance(other, Namespace):
            return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '<Namespace: id={id} >'.format(id=self.id)


hosts = Repositary(Host)
groups = Repositary(Group)
nodes = Repositary(Node)
node_backends = Repositary(NodeBackend, 'Node backend')
couples = Repositary(Couple)
namespaces = Repositary(Namespace)
fs = Repositary(Fs)

dc_host_view = DcHostView()


cache_couples = Repositary(Couple, 'Cache couple')


'''
h = hosts.add('95.108.228.31')
g = groups.add(1)
n = nodes.add(hosts['95.108.228.31'], 1025, 2)
g.add_node(n)

g2 = groups.add(2)
n2 = nodes.add(h, 1026. 2)
g2.add_node(n2)

couple = couples.add([g, g2])

print '1:2' in couples
groups[1].parse_meta({'couple': (1,2)})
groups[2].parse_meta({'couple': (1,2)})
couple.update_status()

print repr(couples)
print 1 in groups
print [g for g in couple]
couple.destroy()
print repr(couples)
'''
