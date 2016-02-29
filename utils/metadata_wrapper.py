class _UpdatingMetadataDictWrapper(object):
    def __init__(self, parent, attr_name):
        self._parent = parent
        self._attr_name = attr_name
        self._refresh()

    def _refresh(self):
        self._data = getattr(self._parent._wrapped, self._attr_name)

    @property
    def _wrapped(self):
        self._refresh()
        return self._data

    def __getitem__(self, idx):
        return self._wrapped[idx]

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __iter__(self):
        for k in self._wrapped:
            yield k

    def __repr__(self):
        return '{cls_name}(parent={parent}, attr_name={attr_name}'.format(
            cls_name=self.__class__.__name__,
            parent=repr(self._parent),
            attr_name=self._attr_name)

    def __str__(self):
        return str(self._wrapped)


class _UpdatingTableMetadataWrapper(object):
    def __init__(self, cluster, ks_name, table_name):
        self._cluster = cluster
        self._ks_name = ks_name
        self._table_name = table_name

    def _refresh(self):
        self._cluster.refresh_table_metadata(self._ks_name, self._table_name)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    @property
    def _wrapped(self):
        self._refresh()
        return self._cluster.metadata.keyspaces[self._ks_name].tables[self._table_name]

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, ks_name={ks_name}, table_name={table_name})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            ks_name=self._ks_name,
            table_name=self._table_name)


class _UpdatingKeyspaceMetadataWrapper(object):
    def __init__(self, cluster, ks_name):
        self._cluster = cluster
        self._ks_name = ks_name

    @property
    def _wrapped(self):
        self._refresh()
        return self._cluster.metadata.keyspaces[self._ks_name]

    def _refresh(self):
        self._cluster.refresh_keyspace_metadata(self._ks_name)

    @property
    def tables(self):
        return {k: _UpdatingTableMetadataWrapper(self._cluster, self._ks_name, k)
                for k in self._wrapped.tables}

    @property
    def user_types(self):
        return _UpdatingMetadataDictWrapper(parent=self, attr_name='user_types')

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __repr__(self):
        return '{cls_name}(cluster={cluster}, ks_name={ks_name})'.format(
            cls_name=self.__class__.__name__,
            cluster=repr(self._cluster),
            ks_name=self._ks_name)


class UpdatingClusterMetadataWrapper(object):
    """
    A class that provides an interface to a cluster's metadata that is
    refreshed on access. Currently only does so for the keyspaces attribute.
    """
    def __init__(self, cluster):
        """
        @param cluster The cassandra.cluster.Cluster object to wrap.
        """
        self._cluster = cluster

    @property
    def keyspaces(self):
        self._cluster.refresh_schema_metadata()
        return {k: _UpdatingKeyspaceMetadataWrapper(self._cluster, k)
                for k in self._cluster.metadata.keyspaces}

    def __repr__(self):
        return '{cls_name}(cluster={cluster})'.format(
            cls_name=self.__class__.__name__, cluster=repr(self._cluster))
