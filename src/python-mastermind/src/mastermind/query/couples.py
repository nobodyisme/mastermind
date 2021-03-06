import copy

from mastermind import query
from mastermind.query import Query, LazyDataObject
from mastermind.query.groups import Group


class CouplesQuery(Query):
    def __init__(self, client, filter=None):
        super(CouplesQuery, self).__init__(client)
        self._filter = filter or {}

    def __getitem__(self, key):
        return Couple(key, self.client)

    def __iter__(self):
        couples = self.client.request('get_couples_list', [self._filter])
        for c_data in couples:
            cq = Couple(CoupleDataObject._raw_id(c_data), self.client)
            cq._set_raw_data(c_data)
            yield cq

    def __len__(self):
        return len(list(self.__iter__()))

    def __contains__(self, key):
        ns = Couple(key, self.client)
        try:
            # TODO: explicitely perform upstream query
            return bool(ns.status)
        except RuntimeError:
            return False

    def filter(self, **kwargs):
        """Filter couples list.

        Keyword args:
          namespace: get couples belonging to a certain namespace.
          state: mostly the same as couple status, but one state can actually
            combine several statuses. Represents couple state from admin's point of view.
            States to couple statuses:
            good: OK
            full: FULL
            frozen: FROZEN
            bad: INIT, BAD
            broken: BROKEN
            service-active: SERVICE_ACTIVE
            service-stalled: SERVICE_STALLED

        Returns:
          New couples query object with selected filter parameters.
        """
        updated_filter = copy.copy(self._filter)
        if 'namespace' in kwargs:
            updated_filter['namespace'] = query.namespaces.Namespace._object(
                kwargs['namespace'], self.client).id
        if 'state' in kwargs:
            updated_filter['state'] = kwargs['state']
        return CouplesQuery(self.client, filter=updated_filter)

    def __delitem__(self, key):
        return Couple._object(key, self.client).remove()


class CoupleDataObject(LazyDataObject):
    def _fetch_data(self):
        return self.client.request('get_couple_info_by_coupleid', self.id)

    @staticmethod
    def _raw_id(raw_data):
        return raw_data['id']

    @property
    @LazyDataObject._lazy_load
    def status(self):
        """Current status of couple.

        Possible values:
          'INIT' - newly created couple or any of the groups has INIT status;
          'OK' - couple is up and ready for write requests;
          'FULL' - couple is up but has no available space for write requests;
          'FROZEN' - couple was frozen and should not participate in write requests;
          'RO' - any of couple's groups are in read-only state or migrating;
          'SERVICE_ACTIVE' - some of couple's groups are being processed in move or restore job,
            job is executing;
          'SERVICE_STALLED' - some of couple's groups are being processed in move or restore job,
            but job is in pending state and requires attention;
          'BROKEN' - couple's configuration is invalid, text description is available through
            status_text attribute;
          'BAD' - represents error state, text description is available through
            status_text attribute;
        """
        return self._data['couple_status']

    @property
    @LazyDataObject._lazy_load
    def status_text(self):
        """Human-readable and clarifying version of status.
        """
        return self._data['couple_status_text']

    @property
    @LazyDataObject._lazy_load
    def as_tuple(self):
        """Tuple of coupled groups' ids.
        """
        return self._data['tuple']

    @property
    @LazyDataObject._lazy_load
    def groups(self):
        """Coupled groups.
        """
        return self._data['groups']

    def _preprocess_raw_data(self, data):
        groups = []
        for g_data in data['groups'][:]:
            groups.append(Group.from_data(g_data, self.client))
        data['groups'] = groups
        return data

    def serialize(self):
        data = super(CoupleDataObject, self).serialize()
        groups = [group.serialize() for group in data['groups']]
        data['groups'] = groups
        return data


GOOD_STATUSES = set(['OK', 'FULL', 'FROZEN'])


class CoupleQuery(Query):
    def remove(self):
        confirm_phrase = 'Yes, I want to break {} couple {}'.format(
            'good' if self.status in GOOD_STATUSES else 'bad',
            self.id)
        return self.client.request('break_couple', [self.as_tuple, confirm_phrase])


class Couple(CoupleQuery, CoupleDataObject):
    def __init__(self, id, client=None):
        super(Couple, self).__init__(client)
        self.id = id

    def __repr__(self):
        return '<Couple {}: status {} ({})>'.format(self.id, self.status, self.status_text)
