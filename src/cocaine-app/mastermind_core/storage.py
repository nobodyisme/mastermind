import datetime
import errno
import os.path
import threading
import time

from mastermind_core import helpers as h


class Status(object):
    INIT = 'INIT'
    OK = 'OK'
    FULL = 'FULL'
    COUPLED = 'COUPLED'
    BAD = 'BAD'
    BROKEN = 'BROKEN'
    RO = 'RO'
    STALLED = 'STALLED'
    FROZEN = 'FROZEN'

    MIGRATING = 'MIGRATING'

    SERVICE_ACTIVE = 'SERVICE_ACTIVE'
    SERVICE_STALLED = 'SERVICE_STALLED'


class Host(object):
    def __init__(self, addr):
        self.addr = addr
        self.nodes = []

    def __eq__(self, other):
        if isinstance(other, str):
            return self.addr == other

        if isinstance(other, Host):
            return self.addr == other.addr

        return False

    def __hash__(self):
        return hash(self.__str__())

    def __repr__(self):
        return ('<Host object: addr=%s, nodes=[%s] >' %
                (self.addr, ', '.join((repr(n) for n in self.nodes))))

    def __str__(self):
        return self.addr


class CommandsStat(object):
    def __init__(self):
        self.ts = None

        self.ell_disk_read_time_cnt, self.ell_disk_write_time_cnt = None, None
        self.ell_disk_read_size, self.ell_disk_write_size = None, None
        self.ell_net_read_size, self.ell_net_write_size = None, None

        self.ell_disk_read_time, self.ell_disk_write_time = 0, 0
        self.ell_disk_read_rate, self.ell_disk_write_rate = 0.0, 0.0
        self.ell_net_read_rate, self.ell_net_write_rate = 0.0, 0.0

    def update(self, raw_stat, collect_ts):

        disk_read_stats = self.commands_stats(
            raw_stat,
            read_ops=True,
            disk=True,
            internal=True,
            outside=True
        )
        new_ell_disk_read_time_cnt = self.sum(disk_read_stats, 'time')
        new_ell_disk_read_size = self.sum(disk_read_stats, 'size')
        disk_write_stats = self.commands_stats(
            raw_stat,
            write_ops=True,
            disk=True,
            internal=True,
            outside=True
        )
        new_ell_disk_write_time_cnt = self.sum(disk_write_stats, 'time')
        new_ell_disk_write_size = self.sum(disk_write_stats, 'size')

        net_read_stats = self.commands_stats(
            raw_stat,
            read_ops=True,
            disk=True,
            cache=True,
            internal=True,
            outside=True
        )
        new_ell_net_read_size = self.sum(net_read_stats, 'size')

        net_write_stats = self.commands_stats(
            raw_stat,
            write_ops=True,
            disk=True,
            cache=True,
            internal=True,
            outside=True
        )
        new_ell_net_write_size = self.sum(net_write_stats, 'size')

        if self.ts:
            diff_ts = collect_ts - self.ts
            if diff_ts <= 1:
                return
            self.ell_disk_read_time = h.unidirectional_value_map(
                self.ell_disk_read_time,
                self.ell_disk_read_time_cnt,
                new_ell_disk_read_time_cnt,
                func=lambda ov, nv: nv - ov
            )
            self.ell_disk_write_time = h.unidirectional_value_map(
                self.ell_disk_write_time,
                self.ell_disk_write_time_cnt,
                new_ell_disk_write_time_cnt,
                func=lambda ov, nv: nv - ov
            )

            self.ell_disk_read_rate = h.unidirectional_value_map(
                self.ell_disk_read_rate,
                self.ell_disk_read_size,
                new_ell_disk_read_size,
                func=lambda ov, nv: (nv - ov) / float(diff_ts)
            )
            self.ell_disk_write_rate = h.unidirectional_value_map(
                self.ell_disk_write_rate,
                self.ell_disk_write_size,
                new_ell_disk_write_size,
                func=lambda ov, nv: (nv - ov) / float(diff_ts)
            )

            self.ell_net_read_rate = h.unidirectional_value_map(
                self.ell_net_read_rate,
                self.ell_net_read_size,
                new_ell_net_read_size,
                func=lambda ov, nv: (nv - ov) / float(diff_ts)
            )
            self.ell_net_write_rate = h.unidirectional_value_map(
                self.ell_net_write_rate,
                self.ell_net_write_size,
                new_ell_net_write_size,
                func=lambda ov, nv: (nv - ov) / float(diff_ts)
            )

        self.ell_disk_read_time_cnt = new_ell_disk_read_time_cnt
        self.ell_disk_write_time_cnt = new_ell_disk_write_time_cnt
        self.ell_disk_read_size = new_ell_disk_read_size
        self.ell_disk_write_size = new_ell_disk_write_size

        self.ell_net_read_size = new_ell_net_read_size
        self.ell_net_write_size = new_ell_net_write_size

        self.ts = collect_ts

    @staticmethod
    def sum(stats, field):
        return sum(map(lambda s: s[field], stats), 0)

    @staticmethod
    def commands_stats(commands,
                       write_ops=False,
                       read_ops=False,
                       disk=False,
                       cache=False,
                       internal=False,
                       outside=False):

        def filter(cmd_type, src_type, dst_type):
            if not write_ops and cmd_type == 'WRITE':
                return False
            if not read_ops and cmd_type != 'WRITE':
                return False
            if not disk and src_type == 'disk':
                return False
            if not cache and src_type == 'cache':
                return False
            if not internal and dst_type == 'internal':
                return False
            if not outside and dst_type == 'outside':
                return False
            return True

        commands_stats = [stat
                          for cmd_type, cmd_stat in commands.iteritems()
                          for src_type, src_stat in cmd_stat.iteritems()
                          for dst_type, stat in src_stat.iteritems()
                          if filter(cmd_type, src_type, dst_type)]
        return commands_stats

    def __add__(self, other):
        new = CommandsStat()

        new.ell_disk_read_time = self.ell_disk_read_time + other.ell_disk_read_time
        new.ell_disk_write_time = self.ell_disk_write_time + other.ell_disk_write_time
        new.ell_disk_read_rate = self.ell_disk_read_rate + other.ell_disk_read_rate
        new.ell_disk_write_rate = self.ell_disk_write_rate + other.ell_disk_write_rate
        new.ell_net_read_rate = self.ell_net_read_rate + other.ell_net_read_rate
        new.ell_net_write_rate = self.ell_net_write_rate + other.ell_net_write_rate

        return new


class NodeStat(object):
    def __init__(self):
        self.ts = None
        self.load_average = 0.0
        self.tx_bytes, self.rx_bytes = 0, 0
        self.tx_rate, self.rx_rate = 0.0, 0.0

        self.commands_stat = CommandsStat()

    def update(self, raw_stat, collect_ts):
        self.load_average = float(raw_stat['procfs']['vm']['la'][0]) / 100
        interfaces = raw_stat['procfs'].get('net', {}).get('net_interfaces', {})

        new_rx_bytes = sum(map(
            lambda if_: if_[1].get('receive', {}).get('bytes', 0) if if_[0] != 'lo' else 0,
            interfaces.items()))
        new_tx_bytes = sum(map(
            lambda if_: if_[1].get('transmit', {}).get('bytes', 0) if if_[0] != 'lo' else 0,
            interfaces.items()))

        if self.ts is not None and collect_ts > self.ts:
            # conditions are checked for the case of *x_bytes counter overflow
            diff_ts = collect_ts - self.ts
            self.tx_rate = h.unidirectional_value_map(
                self.tx_rate,
                self.tx_bytes,
                new_tx_bytes,
                func=lambda ov, nv: (nv - ov) / float(diff_ts)
            )
            self.rx_rate = h.unidirectional_value_map(
                self.rx_rate,
                self.rx_bytes,
                new_rx_bytes,
                func=lambda ov, nv: (nv - ov) / float(diff_ts)
            )

        self.tx_bytes = new_tx_bytes
        self.rx_bytes = new_rx_bytes

        self.ts = collect_ts

    def __add__(self, other):
        res = NodeStat()

        res.ts = min(self.ts, other.ts)
        res.load_average = max(self.load_average, other.load_average)

        return res

    def __mul__(self, other):
        res = NodeStat()
        res.ts = min(self.ts, other.ts)
        res.load_average = max(self.load_average, other.load_average)

        return res

    def update_commands_stats(self, node_backends):
        self.commands_stat = sum((nb.stat.commands_stat for nb in node_backends), CommandsStat())


class Node(object):
    def __init__(self, host, port, family):
        self.host = host
        self.port = int(port)
        self.family = int(family)
        self.host.nodes.append(self)

        self.stat = NodeStat()

    def update_statistics(self, new_stat, collect_ts):
        self.stat.update(new_stat, collect_ts)

    def __repr__(self):
        return '<Node object: host={host}, port={port}>'.format(
            host=str(self.host), port=self.port)

    def __str__(self):
        return '{host}:{port}:{family}'.format(
            host=self.host.addr,
            port=self.port,
            family=self.family,
        )

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, Node):
            return self.host.addr == other.host.addr and self.port == other.port and self.family == other.family

    def update_commands_stats(self, node_backends):
        self.stat.update_commands_stats(node_backends)


class NodeBackendStat(object):
    def __init__(self, node_stat):
        # TODO: not required anymore, remove (?)
        self.node_stat = node_stat
        self.ts = None

        self.free_space, self.total_space, self.used_space = 0, 0, 0
        self.vfs_free_space, self.vfs_total_space, self.vfs_used_space = 0, 0, 0

        self.commands_stat = CommandsStat()

        self.last_read, self.last_write = 0, 0  # not used anymore?
        self.read_rps, self.write_rps = 0, 0  # not used anymore?

        # Tupical SATA HDD performance is 100 IOPS
        # It will be used as first estimation for maximum node performance
        self.max_read_rps, self.max_write_rps = 100, 100  # not used anymore?

        self.fragmentation = 0.0
        self.files = 0
        self.files_removed, self.files_removed_size = 0, 0

        self.fsid = None
        self.defrag_state = None
        self.want_defrag = 0

        self.blob_size_limit = 0
        self.max_blob_base_size = 0
        self.blob_size = 0

        self.start_stat_commit_err_count = 0
        self.cur_stat_commit_err_count = 0

        self.io_blocking_size = 0
        self.io_nonblocking_size = 0

        self.backend_start_ts = 0

    def update(self, raw_stat, collect_ts):

        if self.ts and collect_ts > self.ts:
            dt = collect_ts - self.ts

            if 'dstat' in raw_stat['backend'] and 'error' not in raw_stat['backend']['dstat']:
                last_read = raw_stat['backend']['dstat']['read_ios']
                last_write = raw_stat['backend']['dstat']['write_ios']

                self.read_rps = (last_read - self.last_read) / dt
                self.write_rps = (last_write - self.last_write) / dt

                # Disk usage should be used here instead of load average
                self.max_read_rps = self.max_rps(self.read_rps, self.node_stat.load_average)
                self.max_write_rps = self.max_rps(self.write_rps, self.node_stat.load_average)

                self.last_read = last_read
                self.last_write = last_write

        self.ts = collect_ts

        self.vfs_total_space = raw_stat['backend']['vfs']['blocks'] * \
            raw_stat['backend']['vfs']['bsize']
        self.vfs_free_space = raw_stat['backend']['vfs']['bavail'] * \
            raw_stat['backend']['vfs']['bsize']
        self.vfs_used_space = self.vfs_total_space - self.vfs_free_space

        self.files = raw_stat['backend']['summary_stats']['records_total'] - \
            raw_stat['backend']['summary_stats']['records_removed']
        self.files_removed = raw_stat['backend']['summary_stats']['records_removed']
        self.files_removed_size = raw_stat['backend']['summary_stats']['records_removed_size']
        self.fragmentation = float(self.files_removed) / ((self.files + self.files_removed) or 1)

        self.fsid = raw_stat['backend']['vfs']['fsid']
        self.defrag_state = raw_stat['status']['defrag_state']
        self.want_defrag = raw_stat['backend']['summary_stats']['want_defrag']

        self.blob_size_limit = raw_stat['backend']['config'].get('blob_size_limit', 0)
        if self.blob_size_limit > 0:
            # vfs_total_space can be less than blob_size_limit in case of misconfiguration
            self.total_space = min(self.blob_size_limit, self.vfs_total_space)
            self.used_space = raw_stat['backend']['summary_stats'].get('base_size', 0)
            self.free_space = min(max(0, self.total_space - self.used_space), self.vfs_free_space)
        else:
            self.total_space = self.vfs_total_space
            self.free_space = self.vfs_free_space
            self.used_space = self.vfs_used_space

        if len(raw_stat['backend'].get('base_stats', {})):
            self.max_blob_base_size = max(
                [blob_stat['base_size']
                    for blob_stat in raw_stat['backend']['base_stats'].values()])
        else:
            self.max_blob_base_size = 0

        self.blob_size = raw_stat['backend']['config']['blob_size']

        self.io_blocking_size = raw_stat['io']['blocking']['current_size']
        self.io_nonblocking_size = raw_stat['io']['nonblocking']['current_size']

        self.cur_stat_commit_err_count = raw_stat['stats'].get(
            'stat_commit', {}).get('errors', {}).get(errno.EROFS, 0)

        if self.backend_start_ts < raw_stat['status']['last_start']['tv_sec']:
            self.backend_start_ts = raw_stat['status']['last_start']['tv_sec']
            self._reset_stat_commit_errors()

        self.commands_stat.update(raw_stat['commands'], collect_ts)

    def _reset_stat_commit_errors(self):
        self.start_stat_commit_err_count = self.cur_stat_commit_err_count

    @property
    def stat_commit_errors(self):
        if self.cur_stat_commit_err_count < self.start_stat_commit_err_count:
            self._reset_stat_commit_errors()
        return self.cur_stat_commit_err_count - self.start_stat_commit_err_count

    def max_rps(self, rps, load_avg):
        return max(rps / max(load_avg, 0.01), 100)

    def __add__(self, other):
        node_stat = self.node_stat + other.node_stat
        res = NodeBackendStat(node_stat)

        res.ts = min(self.ts, other.ts)

        res.node_stat = node_stat

        res.total_space = self.total_space + other.total_space
        res.free_space = self.free_space + other.free_space
        res.used_space = self.used_space + other.used_space

        res.read_rps = self.read_rps + other.read_rps
        res.write_rps = self.write_rps + other.write_rps

        res.max_read_rps = self.max_read_rps + other.max_read_rps
        res.max_write_rps = self.max_write_rps + other.max_write_rps

        res.files = self.files + other.files
        res.files_removed = self.files_removed + other.files_removed
        res.files_removed_size = self.files_removed_size + other.files_removed_size
        res.fragmentation = float(res.files_removed) / (res.files_removed + res.files or 1)

        res.blob_size_limit = min(self.blob_size_limit, other.blob_size_limit)
        res.max_blob_base_size = max(self.max_blob_base_size, other.max_blob_base_size)
        res.blob_size = max(self.blob_size, other.blob_size)

        res.io_blocking_size = max(self.io_blocking_size, other.io_blocking_size)
        res.io_nonblocking_size = max(self.io_nonblocking_size, other.io_nonblocking_size)

        return res

    def __mul__(self, other):
        node_stat = self.node_stat * other.node_stat
        res = NodeBackendStat(node_stat)

        res.ts = min(self.ts, other.ts)

        res.node_stat = node_stat

        res.total_space = min(self.total_space, other.total_space)
        res.free_space = min(self.free_space, other.free_space)
        res.used_space = max(self.used_space, other.used_space)

        res.read_rps = max(self.read_rps, other.read_rps)
        res.write_rps = max(self.write_rps, other.write_rps)

        res.max_read_rps = min(self.max_read_rps, other.max_read_rps)
        res.max_write_rps = min(self.max_write_rps, other.max_write_rps)

        # files and files_removed are taken from the stat object with maximum
        # total number of keys. If total number of keys is equal,
        # the stat object with larger number of removed keys is more up-to-date
        files_stat = max(self, other, key=lambda stat: (stat.files + stat.files_removed, stat.files_removed))
        res.files = files_stat.files
        res.files_removed = files_stat.files_removed
        res.files_removed_size = max(self.files_removed_size, other.files_removed_size)

        # ATTENTION: fragmentation coefficient in this case would not necessary
        # be equal to [removed keys / total keys]
        res.fragmentation = max(self.fragmentation, other.fragmentation)

        res.blob_size_limit = min(self.blob_size_limit, other.blob_size_limit)
        res.max_blob_base_size = max(self.max_blob_base_size, other.max_blob_base_size)
        res.blob_size = max(self.blob_size, other.blob_size)

        res.io_blocking_size = max(self.io_blocking_size, other.io_blocking_size)
        res.io_nonblocking_size = max(self.io_nonblocking_size, other.io_nonblocking_size)

        return res

    def __repr__(self):
        return ('<NodeBackendStat object: ts=%s, write_rps=%d, max_write_rps=%d, read_rps=%d, '
                'max_read_rps=%d, total_space=%d, free_space=%d, files=%s, files_removed=%s, '
                'fragmentation=%s, node_load_average=%s>' % (
                    h.ts_to_str(self.ts), self.write_rps, self.max_write_rps, self.read_rps,
                    self.max_read_rps, self.total_space, self.free_space, self.files,
                    self.files_removed, self.fragmentation, self.node_stat.load_average))


class FsStat(object):

    SECTOR_BYTES = 512

    def __init__(self):
        self.ts = None
        self.total_space = 0

        self.dstat = {}
        self.vfs_stat = {}
        self.disk_util = 0.0
        self.disk_util_read = 0.0
        self.disk_util_write = 0.0

        self.disk_read_rate = 0.0
        self.disk_write_rate = 0.0

        self.commands_stat = CommandsStat()

    def apply_new_dstat(self, new_dstat):
        if not self.dstat.get('ts'):
            return
        diff_ts = new_dstat['ts'] - self.dstat['ts']
        if diff_ts <= 1.0:
            return

        disk_util = h.unidirectional_value_map(
            self.disk_util,
            self.dstat['io_ticks'],
            new_dstat['io_ticks'],
            func=lambda ov, nv: (nv - ov) / diff_ts / float(10 ** 3)
        )
        self.disk_util = disk_util

        read_ticks = new_dstat['read_ticks'] - self.dstat['read_ticks']
        write_ticks = new_dstat['write_ticks'] - self.dstat['write_ticks']
        total_rw_ticks = read_ticks + write_ticks
        self.disk_util_read = h.unidirectional_value_map(
            self.disk_util_read,
            self.dstat['read_ticks'],
            new_dstat['read_ticks'],
            func=lambda ov, nv: (total_rw_ticks and
                                 disk_util * (nv - ov) / float(total_rw_ticks) or
                                 0.0)
        )
        self.disk_util_write = h.unidirectional_value_map(
            self.disk_util_write,
            self.dstat['write_ticks'],
            new_dstat['write_ticks'],
            func=lambda ov, nv: (total_rw_ticks and
                                 disk_util * (nv - ov) / float(total_rw_ticks) or
                                 0.0)
        )
        self.disk_read_rate = h.unidirectional_value_map(
            self.disk_read_rate,
            self.dstat['read_sectors'],
            new_dstat['read_sectors'],
            func=lambda ov, nv: (nv - ov) * self.SECTOR_BYTES / float(diff_ts)
        )

    def apply_new_vfs_stat(self, new_vfs_stat):
        new_free_space = new_vfs_stat['bavail'] * new_vfs_stat['bsize']
        if self.vfs_stat.get('ts'):
            diff_ts = new_vfs_stat['ts'] - self.vfs_stat['ts']
            if diff_ts > 1.0:
                written_bytes = self.free_space - new_free_space
                self.disk_write_rate = written_bytes / diff_ts

        self.total_space = new_vfs_stat['blocks'] * new_vfs_stat['bsize']
        self.free_space = new_free_space

    def update(self, raw_stat, collect_ts):
        self.ts = collect_ts
        vfs_stat = raw_stat['vfs']
        dstat_stat = raw_stat['dstat']

        if 'error' in dstat_stat:
            new_dstat = {}
            self.apply_new_dstat(new_dstat)
        else:
            new_dstat = dstat_stat
            new_dstat['ts'] = (dstat_stat['timestamp']['tv_sec'] +
                               dstat_stat['timestamp']['tv_usec'] / float(10 ** 6))
            self.apply_new_dstat(new_dstat)

        self.dstat = new_dstat

        if 'error' in vfs_stat:
            new_vfs_stat = {}
            self.apply_new_vfs_stat(new_vfs_stat)
        else:
            new_vfs_stat = vfs_stat
            new_vfs_stat['ts'] = (vfs_stat['timestamp']['tv_sec'] +
                                  vfs_stat['timestamp']['tv_usec'] / float(10 ** 6))
            self.apply_new_vfs_stat(new_vfs_stat)

        self.vfs_stat = new_vfs_stat

    def update_commands_stats(self, node_backends):
        self.commands_stat = sum((nb.stat.commands_stat for nb in node_backends), CommandsStat())


class Fs(object):
    def __init__(self, host, fsid):
        self.host = host
        self.fsid = fsid
        self.status = Status.OK

        self.node_backends = {}

        self.stat = FsStat()

    def add_node_backend(self, nb):
        if nb.fs:
            nb.fs.remove_node_backend(nb)
        self.node_backends[nb] = nb
        nb.fs = self

    def remove_node_backend(self, nb):
        del self.node_backends[nb]

    def update_statistics(self, new_stat, collect_ts):
        self.stat.update(new_stat, collect_ts)

    def update_commands_stats(self):
        self.stat.update_commands_stats(self.node_backends)

    def update_status(self):
        nbs = self.node_backends.keys()
        prev_status = self.status

        total_space = 0
        for nb in nbs:
            if nb.status not in (Status.OK, Status.BROKEN):
                continue
            total_space += nb.stat.total_space

        if total_space > self.stat.total_space:
            self.status = Status.BROKEN
        else:
            self.status = Status.OK

        # TODO: unwind cycle dependency between node backend status and fs
        # status. E.g., check node backend status and file system status
        # separately on group status updating.
        if self.status != prev_status:
            for nb in nbs:
                nb.update_status()

    def __repr__(self):
        return '<Fs object: host={host}, fsid={fsid}>'.format(
            host=str(self.host), fsid=self.fsid)

    def __str__(self):
        return '{host}:{fsid}'.format(host=self.host.addr, fsid=self.fsid)

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, Fs):
            return self.host.addr == other.host.addr and self.fsid == other.fsid


class NodeBackend(object):

    ACTIVE_STATUSES = (Status.OK, Status.RO, Status.BROKEN)

    def __init__(self, node, backend_id):

        self.node = node
        self.backend_id = backend_id
        self.fs = None
        self.group = None

        self.stat = NodeBackendStat(self.node.stat)

        self.read_only = False
        self.disabled = False
        self.start_ts = 0
        self.status = Status.INIT
        self.status_text = "Node %s is not inititalized yet" % (self.__str__())

        self.stalled = False

        self.base_path = None

        self.effective_space = 0
        self.effective_free_space = 0

    def set_group(self, group):
        self.group = group

    def remove_group(self):
        self.group = None

    def disable(self):
        self.disabled = True

    def enable(self):
        self.disabled = False

    def make_read_only(self):
        self.read_only = True

    def make_writable(self):
        self.read_only = False

    def update_statistics(self, new_stat, collect_ts):
        self.base_path = os.path.dirname(new_stat['backend']['config'].get('data') or
                                         new_stat['backend']['config'].get('file')) + '/'
        self.stat.update(new_stat, collect_ts)

    def update_status(self):
        if not self.stat:
            self.status = Status.INIT
            self.status_text = 'No statistics gathered for node backend {0}'.format(self.__str__())

        elif self.disabled:
            self.status = Status.STALLED
            self.status_text = 'Node backend {0} has been disabled'.format(str(self))

        elif self.stalled:
            self.status = Status.STALLED
            self.status_text = ('Statistics for node backend {} is too old: '
                                'it was gathered {} seconds ago'.format(
                                    self.__str__(), int(time.time() - self.stat.ts)))

        elif self.fs.status == Status.BROKEN:
            self.status = Status.BROKEN
            self.status_text = ("Node backends' space limit is not properly "
                                "configured on fs {0}".format(self.fs.fsid))

        elif self.read_only:
            self.status = Status.RO
            self.status_text = 'Node backend {0} is in read-only state'.format(str(self))

        else:
            self.status = Status.OK
            self.status_text = 'Node {0} is OK'.format(str(self))

        return self.status

    def is_full(self, reserved_space=0.0):

        if self.stat is None:
            return False

        assert 0.0 <= reserved_space <= 1.0, 'Reserved space should have non-negative value lte 1.0'

        if self.stat.used_space >= self.effective_space * (1.0 - reserved_space):
            return True
        if self.effective_free_space <= 0:
            return True
        return False

    @property
    def stat_commit_errors(self):
        return (self.stat and
                self.stat.stat_commit_errors or
                0)

    def info(self):
        res = {}

        res['node'] = '{0}:{1}:{2}'.format(self.node.host, self.node.port, self.node.family)
        res['backend_id'] = self.backend_id
        res['addr'] = str(self)
        res['hostname'] = self.node.host.hostname_or_not
        res['status'] = self.status
        res['status_text'] = self.status_text
        res['dc'] = self.node.host.dc_or_not
        res['last_stat_update'] = (
            self.stat and
            datetime.datetime.fromtimestamp(self.stat.ts).strftime('%Y-%m-%d %H:%M:%S') or
            'unknown')
        if self.node.stat:
            res['tx_rate'] = self.node.stat.tx_rate
            res['rx_rate'] = self.node.stat.rx_rate
        if self.stat:
            res['free_space'] = int(self.stat.free_space)
            res['effective_space'] = self.effective_space
            res['free_effective_space'] = self.effective_free_space
            res['used_space'] = int(self.stat.used_space)
            res['total_space'] = int(self.stat.total_space)
            res['total_files'] = self.stat.files + self.stat.files_removed
            res['records_alive'] = self.stat.files
            res['records_removed'] = self.stat.files_removed
            res['records_removed_size'] = self.stat.files_removed_size
            res['fragmentation'] = self.stat.fragmentation
            res['defrag_state'] = self.stat.defrag_state
            res['want_defrag'] = self.stat.want_defrag
            res['io_blocking_size'] = self.stat.io_blocking_size
            res['io_nonblocking_size'] = self.stat.io_nonblocking_size

        if self.base_path:
            res['path'] = self.base_path

        return res

    def __repr__(self):
        return ('<Node backend object: node=%s, backend_id=%d, '
                'status=%s, read_only=%s, stat=%s>' % (
                    str(self.node), self.backend_id,
                    self.status, str(self.read_only), repr(self.stat)))

    def __str__(self):
        return '{ip}:{port}:{family}/{backend_id}'.format(
            ip=self.node.host.addr,
            port=self.node.port,
            family=self.node.family,
            backend_id=self.backend_id,
        )

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, NodeBackend):
            return (self.node.host.addr == other.node.host.addr and
                    self.node.port == other.node.port and
                    self.node.family == other.node.family and
                    self.backend_id == other.backend_id)


class ResourceError(KeyError):
    def __str__(self):
        return str(self.args[0])


class Repository(dict):
    def __init__(self, item_type, item_desc=None):
        super(Repository, self).__init__()
        self._item_type = item_type
        self._item_desc = item_desc or self._item_type.__name__

    def add(self, *args, **kwargs):
        item = self._item_type(*args, **kwargs)
        self[item] = item
        return item

    def remove(self, key):
        return self.elements.pop(key)

    def __getitem__(self, key):
        try:
            return super(Repository, self).__getitem__(key)
        except KeyError:
            raise ResourceError('{} {} is not found'.format(
                self._item_desc, key))


class Storage(object):

    _instance = None
    _instance_lock = threading.Lock()

    def __init__(self):
        self.hosts = Repository(Host)
        self.nodes = Repository(Node)
        self.node_backends = Repository(NodeBackend, 'Node backend')
        self.fs = Repository(Fs)

    @classmethod
    def current(cls):
        if cls._instance is None:
            with cls._instance_lock:
                # check once again under the lock
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def make_current(self):
        with Storage._instance_lock:
            Storage._instance = self
