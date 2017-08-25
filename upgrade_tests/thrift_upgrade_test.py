# coding: utf-8

import itertools
from unittest import skipUnless

from cassandra.query import dict_factory
from nose.tools import assert_not_in, assert_equal
from dtest import RUN_STATIC_UPGRADE_MATRIX, debug
from thrift_tests import get_thrift_client, _i64
from tools.decorators import since
from upgrade_base import UpgradeTester
from upgrade_manifest import build_upgrade_pairs

from thrift_bindings.v22 import Cassandra
from thrift_bindings.v22.Cassandra import (CfDef, Column, ColumnDef,
                                           ColumnParent,
                                           ConsistencyLevel,
                                           SlicePredicate, SliceRange)


@since('2.1', max_version='4.0.x')
class TestThrift(UpgradeTester):
    """
    Verify dense and sparse supercolumn functionality with and without renamed columns
    in 3.X after upgrading from 2.x.

    @jira_ticket CASSANDRA-12373
    """
    def dense_supercolumn_test(self):
        cursor = self.prepare(nodes=2, rf=2, row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cf = Cassandra.CfDef('ks', 'dense_super_1', column_type='Super', subcomparator_type='LongType')
        client.system_add_column_family(cf)

        for i in xrange(1, 3):
            client.insert('k1', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(100), 'value1', 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(200), 'value2', 0), ConsistencyLevel.ONE)

        self._validate_dense_cql(cursor)
        self._validate_dense_thrift(client)

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            self._validate_dense_cql(cursor)
            self._validate_dense_thrift(client)

    def dense_supercolumn_test_with_renames(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cf = Cassandra.CfDef('ks', 'dense_super_2', column_type='Super', subcomparator_type='LongType')
        client.system_add_column_family(cf)

        for i in xrange(1, 3):
            client.insert('k1', ColumnParent('dense_super_2', 'key{}'.format(i)), Column(_i64(100), 'value1', 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('dense_super_2', 'key{}'.format(i)), Column(_i64(200), 'value2', 0), ConsistencyLevel.ONE)

        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME key TO renamed_key")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME column1 TO renamed_column1")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME column2 TO renamed_column2")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME value TO renamed_value")

        self._validate_dense_cql(cursor, cf='dense_super_2', key=u'renamed_key', column1=u'renamed_column1', column2=u'renamed_column2', value=u'renamed_value')
        self._validate_dense_thrift(client, cf='dense_super_2')

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            self._validate_dense_cql(cursor, cf='dense_super_2', key=u'renamed_key', column1=u'renamed_column1', column2=u'renamed_column2', value=u'renamed_value')
            self._validate_dense_thrift(client, cf='dense_super_2')

    def _validate_dense_cql(self, cursor, cf='dense_super_1', key=u'key', column1=u'column1', column2=u'column2', value=u'value'):
        cursor.execute('use ks')
        # print(list(cursor.execute("SELECT * FROM {}".format(cf))))

        assert_equal(list(cursor.execute("SELECT * FROM {}".format(cf))),
                     [{ key: 'k1', column1: 'key1', column2: 100, value: 'value1'},
                      { key: 'k1', column1: 'key2', column2: 100, value: 'value1'},
                      { key: 'k2', column1: 'key1', column2: 200, value: 'value2'},
                      { key: 'k2', column1: 'key2', column2: 200, value: 'value2'}])

    def _validate_dense_thrift(self, client, cf='dense_super_1'):
        client.transport.open()
        client.set_keyspace('ks')
        result = client.get_slice('k1', ColumnParent(cf), SlicePredicate(slice_range=SliceRange('', '', False, 5)), ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].super_column.name == 'key1'
        assert result[1].super_column.name == 'key2'

        print(result[0])
        print(result[1])
        for cosc in result:
            assert cosc.super_column.columns[0].name == _i64(100)
            assert cosc.super_column.columns[0].value == 'value1'

    def sparse_supercolumn_test_with_renames(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cd1 = ColumnDef('col1', 'LongType', None, None)
        cd2 = ColumnDef('col2', 'LongType', None, None)
        cf = Cassandra.CfDef('ks', 'sparse_super_1', column_type='Super', column_metadata=[cd1, cd2], subcomparator_type='AsciiType')
        client.system_add_column_family(cf)

        cursor.execute("ALTER TABLE ks.sparse_super_1 RENAME key TO renamed_key")
        cursor.execute("ALTER TABLE ks.sparse_super_1 RENAME column1 TO renamed_column1")

        for i in xrange(1, 3):
            client.insert('k1', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("value1", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("value2", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

        self._validate_sparse_thrift(client)
        self._validate_sparse_cql(cursor, column1=u'renamed_column1', key=u'renamed_key')

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            self._validate_sparse_cql(cursor, column1=u'renamed_column1', key=u'renamed_key')
            self._validate_sparse_thrift(client)

    def supercolumn_test(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cd1 = ColumnDef('col1', 'LongType', None, None)
        cd2 = ColumnDef('col2', 'LongType', None, None)
        cf = Cassandra.CfDef('ks', 'sparse_super_2', column_type='Super', column_metadata=[cd1, cd2], subcomparator_type='AsciiType')
        client.system_add_column_family(cf)

        for i in xrange(1, 3):
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("value1", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("value2", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

        self._validate_sparse_thrift(client, cf='sparse_super_2')
        self._validate_sparse_cql(cursor, cf='sparse_super_2')

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            self._validate_sparse_thrift(client, cf='sparse_super_2')
            self._validate_sparse_cql(cursor, cf='sparse_super_2')

    def _validate_sparse_cql(self, cursor, cf='sparse_super_1', column1=u'column1', col1=u'col1', col2=u'col2', key='key'):
        cursor.execute('use ks')

        assert_equal(list(cursor.execute("SELECT * FROM {}".format(cf))),
                     [{ key: 'k1', column1: 'key1', col1: 200, col2: 300 },
                      { key: 'k1', column1: 'key2', col1: 200, col2: 300 },
                      { key: 'k2', column1: 'key1', col1: 200, col2: 300 },
                      { key: 'k2', column1: 'key2', col1: 200, col2: 300 }])

    def _validate_sparse_thrift(self, client, cf='sparse_super_1'):
        client.transport.open()
        client.set_keyspace('ks')
        result = client.get_slice('k1', ColumnParent(cf), SlicePredicate(slice_range=SliceRange('', '', False, 5)), ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].super_column.name == 'key1'
        assert result[1].super_column.name == 'key2'

        for cosc in result:
            assert cosc.super_column.columns[0].name == 'col1'
            assert cosc.super_column.columns[0].value == _i64(200)
            assert cosc.super_column.columns[1].name == 'col2'
            assert cosc.super_column.columns[1].value == _i64(300)
            assert cosc.super_column.columns[2].name == 'value1'
            assert cosc.super_column.columns[2].value == _i64(100)

topology_specs = [
    {'NODES': 3,
     'RF': 3,
     'CL': ConsistencyLevel.ALL},
    {'NODES': 2,
     'RF': 1},
]
specs = [dict(s, UPGRADE_PATH=p, __test__=True)
         for s, p in itertools.product(topology_specs, build_upgrade_pairs())]

for spec in specs:
    suffix = 'Nodes{num_nodes}RF{rf}_{pathname}'.format(num_nodes=spec['NODES'],
                                                        rf=spec['RF'],
                                                        pathname=spec['UPGRADE_PATH'].name)
    gen_class_name = TestThrift.__name__ + suffix
    assert_not_in(gen_class_name, globals())

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or spec['UPGRADE_PATH'].upgrade_meta.matches_current_env_version_family
    globals()[gen_class_name] = skipUnless(upgrade_applies_to_env, 'test not applicable to env.')(type(gen_class_name, (TestThrift,), spec))
