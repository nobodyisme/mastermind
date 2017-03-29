from contextlib import contextmanager
import logging
import os.path
import sys
import traceback

from kazoo.client import KazooClient
from kazoo.exceptions import (
    LockTimeout,
    NodeExistsError,
    NoNodeError,
    KazooException,
    ZookeeperError,
)
from kazoo.retry import KazooRetry, RetryFailedError
from mastermind.utils.queue import LockingQueue
from mastermind_core import helpers
from mastermind_core.config import config
import msgpack

# from errors import ConnectionError, InvalidDataError
from lock import Lock
from sync.error import LockError, LockFailedError, LockAlreadyAcquiredError, InconsistentLockError


logger = logging.getLogger('mm')

kazoo_logger = logging.getLogger('kazoo')
kazoo_logger.propagate = False
[kazoo_logger.addHandler(h) for h in logger.handlers]
kazoo_logger.setLevel(logging.INFO)


SYNC_CFG = config.get('sync', {})
CACHE_MANAGER_CFG = config.get('cache', {}).get('manager', {})
DEFAULT_TIMEOUT = 3


class ZkSyncManager(object):

    RETRIES = 2
    LOCK_TIMEOUT = 3

    def __init__(self, host='127.0.0.1:2181', lock_path_prefix='/mastermind/locks/', **kwargs):
        self.client = KazooClient(host, timeout=SYNC_CFG.get('timeout', DEFAULT_TIMEOUT))
        logger.info('Connecting to zookeeper host {}, lock_path_prefix: {}'.format(
            host, lock_path_prefix))
        try:
            self.client.start()
        except Exception as e:
            logger.error(e)
            raise

        self._retry = KazooRetry(max_tries=self.RETRIES)

        self.lock_path_prefix = helpers.encode(lock_path_prefix)

    @contextmanager
    def lock(self, lockid, blocking=True, timeout=LOCK_TIMEOUT):
        lock = Lock(self.client, self.lock_path_prefix + lockid)
        try:
            acquired = lock.acquire(blocking=blocking, timeout=timeout)
            logger.debug('Lock {0} acquired: {1}'.format(lockid, acquired))
            if not acquired:
                # TODO: Change exception time or set all required parameters for
                # this type of exception
                raise LockAlreadyAcquiredError(lock_id=lockid)
            yield
        except LockTimeout:
            logger.info('Failed to acquire lock {} due to timeout ({} seconds)'.format(
                lockid, timeout))
            raise LockFailedError(lock_id=lockid)
        except LockAlreadyAcquiredError:
            raise
        except LockError as e:
            logger.error('Failed to acquire lock {0}: {1}\n{2}'.format(
                lockid, e, traceback.format_exc()))
            raise
        finally:
            lock.release()

    @contextmanager
    def ephemeral_locks(self, locks, data=''):
        self.persistent_locks_acquire(locks, data=data, ephemeral=True)
        exc_info = None
        try:
            yield
        except:
            # will be raised in the 'finally' block
            exc_info = sys.exc_info()
        finally:
            try:
                self.persistent_locks_release(locks, check=data)
            except Exception as e:
                logger.error('Failed to release ephemeral locks {}: {}'.format(locks, e))
                pass
            if exc_info:
                # raising original exception if any
                raise exc_info[0], exc_info[1], exc_info[2]

    def persistent_locks_acquire(self, locks, data='', ephemeral=False):
        try:
            retry = self._retry.copy()
            result = retry(self._inner_persistent_locks_acquire, locks=locks, data=data, ephemeral=ephemeral)
        except RetryFailedError:
            raise LockError('Failed to acquire persistent locks {} after several retries'.format(
                locks))
        except KazooException as e:
            logger.error('Failed to fetch persistent locks {0}: {1}\n{2}'.format(
                locks, e, traceback.format_exc()))
            raise LockError
        return result

    def _inner_persistent_locks_acquire(self, locks, data, ephemeral=False):

        ensured_paths = set()

        tr = self.client.transaction()
        for lockid in locks:
            path = self.lock_path_prefix + lockid
            parts = path.rsplit('/', 1)
            if len(parts) == 2 and parts[0] not in ensured_paths:
                self.client.ensure_path(parts[0])
                ensured_paths.add(parts[0])
            tr.create(path, data, ephemeral=ephemeral)

        failed = False
        failed_locks = []
        result = tr.commit()
        for lockid, res in zip(locks, result):
            if isinstance(res, ZookeeperError):
                failed = True
            if isinstance(res, NodeExistsError):
                failed_locks.append(lockid)

        if failed_locks:
            holders = []
            for f in failed_locks:
                # TODO: fetch all holders with 1 transaction request
                holders.append((f, self.client.get(self.lock_path_prefix + f)))
            foreign_holders = [(l, h) for l, h in holders if h[0] != data]
            failed_lock, holder_resp = foreign_holders and foreign_holders[0] or holders[0]
            holder = holder_resp[0]
            holders_ids = list(set(h[0] for _, h in holders))
            logger.warn('Persistent lock {0} is already set by {1}'.format(failed_lock, holder))
            raise LockAlreadyAcquiredError(
                'Lock for {0} is already acquired by job {1}'.format(failed_lock, holder),
                lock_id=failed_lock, holder_id=holder,
                lock_ids=failed_locks, holders_ids=holders_ids)
        elif failed:
            logger.error('Failed to set persistent locks {0}, result: {1}'.format(
                locks, result))
            raise LockError

        return True

    def get_children_locks(self, lock_prefix):
        try:
            retry = self._retry.copy()
            result = retry(self.__inner_get_children_locks, lock_prefix)
        except RetryFailedError:
            raise LockError('Failed to get fetch children locks for {}'.format(
                lock_prefix))
        return result

    def __inner_get_children_locks(self, lock_prefix):
        full_path = self.lock_path_prefix + lock_prefix
        self.client.ensure_path(os.path.normpath(full_path))
        result = self.client.get_children(full_path)
        return ['{0}{1}'.format(lock_prefix, lock) for lock in result]

    def persistent_locks_release(self, locks, check=''):
        try:
            retry = self._retry.copy()
            result = retry(self.__inner_persistent_locks_release, locks=locks, check=check)
        except RetryFailedError:
            raise LockError(
                'Failed to release persistent locks {} after several retries'.format(locks)
            )
        except KazooException as e:
            logger.error('Failed to remove persistent locks {0}: {1}\n{2}'.format(
                locks, e, traceback.format_exc()))
            raise LockError
        return result

    def __inner_persistent_locks_release(self, locks, check):
        for lockid in locks:
            try:
                if check:
                    data = self.client.get(self.lock_path_prefix + lockid)
                    if data[0] != check:
                        logger.error(
                            'Lock {lock_id} has inconsistent data: {current_data}, '
                            'expected {expected_data}'.format(
                                lock_id=lockid,
                                current_data=data[0],
                                expected_data=check,
                            )
                        )
                        raise InconsistentLockError(lock_id=lockid, holder_id=data[0])
                self.client.delete(self.lock_path_prefix + lockid)
            except NoNodeError:
                logger.warn('Persistent lock {0} is already removed'.format(lockid))
                pass
        return True


class ZkCacheTaskManager(object):

    RETRIES = 2

    def __init__(self, host='127.0.0.1:2181', lock_path_prefix='/mastermind/cache/', **kwargs):
        self.client = KazooClient(host, timeout=CACHE_MANAGER_CFG.get('timeout', DEFAULT_TIMEOUT))
        logger.info('Connecting to zookeeper host {}, lock_path_prefix: {}'.format(
            host, lock_path_prefix))
        try:
            self.client.start()
        except Exception as e:
            logger.error(e)
            raise

        self.lock_path_prefix = helpers.encode(lock_path_prefix)

    def put_task(self, task):
        group_id = task['group']
        q = LockingQueue(self.client, self.lock_path_prefix, group_id)
        return q.put(self._serialize(task))

    def put_all(self, tasks):
        for task in tasks:
            self.put_task(task)

    def list(self):
        for group_id in self.client.retry(self.client.get_children, self.lock_path_prefix):
            for item in LockingQueue(self.client, self.lock_path_prefix, group_id).list():
                yield self._unserialize(item)

    @staticmethod
    def _serialize(task):
        return msgpack.packb(task)

    @staticmethod
    def _unserialize(task):
        return msgpack.unpackb(task)
