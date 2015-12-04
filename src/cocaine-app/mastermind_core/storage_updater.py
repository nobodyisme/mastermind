import json
import logging

from mastermind.helpers import elliptics_time_to_ts

import storage


logger = logging.getLogger('mm.storage')


class FileStorageStateFetcher(object):
    def __init__(self, state_path):
        self.path = state_path

    def fetch(self):
        with open(self.path, 'rb') as f:
            return json.loads(f.read())


class StorageUpdater(object):
    def __init__(self, fetcher):
        self.fetcher = fetcher

    def update(self):
        state = self.fetcher.fetch()
        self._process_nodes(state['nodes'])

    def _process_nodes(self, nodes_state):
        for node_state in nodes_state:
            try:
                node_id = '{host_addr}:{port}:{family}'.format(
                    host_addr=node_state['host']['addr'],
                    port=node_state['port'],
                    family=node_state['family'],
                )
            except KeyError:
                error_msg = (
                    'Malformed response from collector, failed to parse node address: '
                    'host address {address}, port {port}, family {family}'.format(
                        address=node_state.get('host', {}).get('addr'),
                        port=node_state.get('port'),
                        family=node_state.get('family'),
                    )
                )
                logger.error(error_msg)
                continue
            try:
                self._process_node(node_id, node_state)
            except Exception:
                logger.error('Failed to process node {} state from collector'.format(node_id))
                continue

    def _process_node(self, node_id, node_state):
        host_id = node_state['host']['addr']
        if host_id not in storage.hosts:
            logger.info('Creating host {}'.format(host_id))
        host = storage.hosts.get_or_create(host_id)

        if node_id not in storage.nodes:
            logger.info('Creating node {}'.format(node_id))
            node = storage.nodes.add(host, node_state['port'], node_state['family'])
        else:
            node = storage.nodes[node_id]

        stat = node.stat

        stat.load_average = node_state['load_average']
        stat.ts = elliptics_time_to_ts(node_state['timestamp'])
        stat.tx_rate = node_state['tx_rate']
        stat.rx_rate = node_state['rx_rate']

        # TODO: commands stats

        self._process_filesystems(self, node.host, node_state['filesystems'])

        self._process_backends(self, node, node_state['backends'])

    def _process_filesystems(self, host, filesystems_state):
        for filesystem_state in filesystems_state:
            try:
                fs_id = '{host}:{fsid}'.format(host, filesystem_state['fsid'])
            except KeyError:
                error_msg = (
                    'Malformed response from collector, failed to parse filesystem id '
                    'on host {host}: fsid: {fsid}'.format(
                        host=host,
                        fsid=filesystem_state.get('fsid'),
                    )
                )
                logger.error(error_msg)
                continue
            try:
                self._process_filesystem(fs_id, host, filesystem_state)
            except Exception:
                logger.error(
                    'Failed to process filesystem {} state for host {} from collector'.format(
                        fs_id,
                        host
                    )
                )
                continue

    def _process_filesystem(self, fs_id, host, filesystem_state):
        if fs_id not in storage.fss:
            logger.info('Creating filesystem {} on host {}'.format(fs_id, host))
            fs = storage.fss.add(host, filesystem_state['fsid'])
        else:
            fs = storage.fss[fs_id]

        fs.status = filesystem_state['status']
        fs.status_text = filesystem_state['status_text']

        stat = fs.stat
        stat.ts = elliptics_time_to_ts(filesystem_state['timestamp'])  # Why is it elliptics-like? Does it have to do anything with elliptics?
        stat.total_space = filesystem_state['total_space']
        stat.free_space = filesystem_state['free_space']

        stat.disk_util = filesystem_state['disk_util']
        stat.disk_util_read = filesystem_state['disk_util_read']
        stat.disk_util_write = filesystem_state['disk_util_write']

        stat.disk_read_rate = filesystem_state['disk_read_rate']  # TODO: check that this is not the same as in commands_stats
        stat.disk_write_rate = filesystem_state['disk_write_rate']

        # TODO: commands stats

    def _process_backends(self, node, backends_state):
        for backend_state in backends_state:
            try:
                # TODO: backend id now includes family, check backend id
                # generation everywhere
                # TODO: rename 'addr' to 'id'
                backend_id = backend_state['addr']
            except KeyError:
                error_msg = (
                    'Malformed response from collector, failed to parse backend '
                    'on node {node}: addr: {addr}'.format(
                        node=node,
                        addr=backend_state.get('addr'),
                    )
                )
                logger.error(error_msg)
            try:
                self._process_backend(backend_id, node, backend_state)
            except Exception:
                logger.error(
                    'Failed to process backend {} state for node {} from collector'.format(
                        backend_id,
                        node
                    )
                )
                continue

    def _process_backend(self, backend_id, node, backend_state):
        if backend_id not in storage.node_backends:
            logger.info('Creating node backend {}'.format(backend_id))
            node_backend = storage.node_backends.add(node, backend_state['backend_id'])
        else:
            node_backend = storage.node_backends[backend_id]

        fs_id = '{host}:{fsid}'.format(
            host=node.host,
            fsid=storage.fss['fsid'],
        )
        fs = storage.fss[fs_id]

        if node_backend not in fs:
            fs.add_node_backend(node_backend)

        if node_backend.base_path != backend_state['base_path']:
            # TODO: update group history
            pass

        node_backend.base_path = backend_state['base_path']
        node_backend.read_only = backend_state['read_only']
        node_backend.status = backend_state['status']
        node_backend.status_text = backend_state['status_text']

        # TODO: commands stats

        stat = node_backend.stat
        stat.ts = elliptics_time_to_ts(backend_state['timestamp'])

        stat.free_space = backend_state['free_space']
        stat.total_space = backend_state['total_space']
        stat.used_space = backend_state['used_space']

        stat.vfs_free_space = backend_state['vfs_free_space']
        stat.vfs_total_space = backend_state['vfs_total_space']
        stat.vfs_used_space = backend_state['vfs_used_space']

        stat.fragmentation = backend_state['fragmentation']

        stat.files = backend_state['records']
        stat.files_removed = backend_state['records_removed']
        stat.files_removed_size = backend_state['records_removed_size']

        stat.defrag_state = backend_state['defrag_state']
        stat.want_defrag = backend_state['want_defrag']

        stat.blob_size = backend_state['blob_size']
        stat.blob_size_limit = backend_state['blob_size_limit']
        stat.max_blob_base_size = backend_state['max_blob_base_size']

        stat.io_blocking_size = backend_state['io_blocking_size']
        stat.io_nonblocking_size = backend_state['io_nonblocking_size']
