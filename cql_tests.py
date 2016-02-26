# coding: utf-8

import itertools
import struct
import time

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.metadata import NetworkTopologyStrategy, SimpleStrategy
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ProtocolException
from cassandra.query import SimpleStatement

from assertions import assert_invalid, assert_one, assert_unavailable
from dtest import Tester, canReuseCluster, freshCluster
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        Mutation)
from thrift_tests import get_thrift_client
from tools import debug, get_keyspace_metadata, get_schema_metadata, rows_to_list, since


class CQLTester(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False,
                nodes=1, rf=1, protocol_version=None, user=None, password=None,
                start_rpc=False, **kwargs):
        cluster = self.cluster

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if user:
            config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms': 0}
            cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session


@canReuseCluster
class StorageProxyCQLTester(CQLTester):
    """
    Each CQL statement is exercised at least once in order to
    ensure we execute the code path in StorageProxy.
    # TODO This probably isn't true anymore?
    Note that in depth CQL validation is done in Java unit tests,
    see CASSANDRA-9160.

    # TODO I'm not convinced we need these. Seems like all the functionality
    #      is covered in greater detail in other test classes.
    """

    def keyspace_test(self):
        """
        Smoke test that basic keyspace operations work:

        - create a keyspace
        - assert keyspace exists and is configured as expected with the driver metadata API
        - ALTER it
        - assert keyspace was correctly altered with the driver metadata API
        - DROP it
        - assert keyspace is no longer in keyspace metadata
        """
        session = self.prepare(create_keyspace=False)

        self.assertNotIn('ks', get_schema_metadata(session).keyspaces)
        session.execute("CREATE KEYSPACE ks WITH replication = "
                        "{ 'class':'SimpleStrategy', 'replication_factor':1} "
                        "AND DURABLE_WRITES = true")
        self.assertIn('ks', get_schema_metadata(session).keyspaces)
        ks_meta = get_keyspace_metadata(session, 'ks')
        self.assertTrue(ks_meta.durable_writes)
        self.assertIsInstance(ks_meta.replication_strategy, SimpleStrategy)

        session.execute("ALTER KEYSPACE ks WITH replication = "
                        "{ 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } "
                        "AND DURABLE_WRITES = false")
        ks_meta = get_keyspace_metadata(session, 'ks')
        self.assertFalse(ks_meta.durable_writes)
        self.assertIsInstance(ks_meta.replication_strategy, NetworkTopologyStrategy)

        session.execute("DROP KEYSPACE ks")
        self.assertNotIn('ks', get_schema_metadata(session).keyspaces)

    def table_test(self):
        """
        Smoke test that basic table operations work:

        - create 2 tables, one with and one without COMPACT STORAGE
        - ALTER the table without COMPACT STORAGE, adding a column

        For each of those tables:

        - insert 10 values
        - SELECT * and assert the values are there
        - TRUNCATE the table
        - SELECT * and assert there are no values
        - DROP the table
        - SELECT * and assert the statement raises an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")

        session.execute("ALTER TABLE test1 ADD v2 int")

        for i in range(0, 10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES ({i}, {i}, {i})".format(i=i))
            session.execute("INSERT INTO test2 (k, c1, v1) VALUES ({i}, {i}, {i})".format(i=i))

        res = sorted(session.execute("SELECT * FROM test1"))
        self.assertEqual(rows_to_list(res), [[i, i, i] for i in range(0, 10)])

        res = sorted(session.execute("SELECT * FROM test2"))
        self.assertEqual(rows_to_list(res), [[i, i, i] for i in range(0, 10)])

        session.execute("TRUNCATE test1")
        session.execute("TRUNCATE test2")

        res = session.execute("SELECT * FROM test1")
        self.assertEqual(rows_to_list(res), [])

        res = session.execute("SELECT * FROM test2")
        self.assertEqual(rows_to_list(res), [])

        session.execute("DROP TABLE test1")
        session.execute("DROP TABLE test2")

        assert_invalid(session, "SELECT * FROM test1", expected=InvalidRequest)
        assert_invalid(session, "SELECT * FROM test2", expected=InvalidRequest)

    def index_test(self):
        """
        Smoke test CQL statements related to indexes:

        - CREATE a table
        - CREATE an index on that table
        - INSERT 10 values into the table
        - SELECT from the table over the indexed value and assert the expected values come back
        - drop the index
        - assert SELECTing over the indexed value raises an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        session.execute("CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int)")
        session.execute("CREATE INDEX testidx ON test3 (v1)")

        for i in range(0, 10):
            session.execute("INSERT INTO test3 (k, v1, v2) VALUES ({i}, {i}, {i})".format(i=i))

        res = session.execute("SELECT * FROM test3 WHERE v1 = 0")
        self.assertEqual(rows_to_list(res), [[0, 0, 0]])

        session.execute("DROP INDEX testidx")

        assert_invalid(session, "SELECT * FROM test3 where v1 = 0", expected=InvalidRequest)

    def type_test(self):
        """
        Smoke test basic TYPE operations:

        - CREATE a type
        - CREATE a table using that type
        - ALTER the type and CREATE another table
        - DROP the tables and type
        - CREATE another table using the DROPped type and assert it fails with an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        # TODO is this even necessary given the existence of the auth_tests?
        """
        session = self.prepare()

        session.execute("CREATE TYPE address_t (street text, city text, zip_code int)")
        session.execute("CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("ALTER TYPE address_t ADD phones set<text>")
        session.execute("CREATE TABLE test5 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("DROP TABLE test4")
        session.execute("DROP TABLE test5")
        session.execute("DROP TYPE address_t")
        assert_invalid(session,
                       "CREATE TABLE test6 (id int PRIMARY KEY, address frozen<address_t>)",
                       expected=InvalidRequest)

    def user_test(self):
        """
        Smoke test for basic USER queries:

        - get a session as the default superuser
        - CREATE a user
        - ALTER that user by giving it a different password
        - DROP that user
        # TODO list users after each to make sure each statement works
        """
        session = self.prepare(user='cassandra', password='cassandra')

        session.execute("CREATE USER user1 WITH PASSWORD 'secret'")

        session.execute("ALTER USER user1 WITH PASSWORD 'secret^2'")

        session.execute("DROP USER user1")

    def statements_test(self):
        """
        Smoke test SELECT and UPDATE statements:

        - create a table
        - insert 20 rows into the table
        - run SELECT COUNT queries and assert they return the correct values
            - bare and with IN and equality conditions
        - run SELECT * queries with = conditions
        - run UPDATE queries
        - SELECT * and assert the UPDATEd values are there
        - DELETE with a = condition
        - SELECT the deleted values and make sure nothing is returned
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        session.execute("CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in range(0, 10):
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', {i}, {i}, {i})".format(i=i))
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', {i}, {i}, {i})".format(i=i))

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        self.assertEqual(rows_to_list(res), [[10]])

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2')")
        self.assertEqual(rows_to_list(res), [[20]])

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0")
        self.assertEqual(rows_to_list(res), [[2]])

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        self.assertEqual(rows_to_list(res), [['ev1', i, i, i] for i in range(0, 10)])

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev2'")
        self.assertEqual(rows_to_list(res), [['ev2', i, i, i] for i in range(0, 10)])

        for i in range(0, 10):
            session.execute("UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time={i}".format(i=i))

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        self.assertEqual(rows_to_list(res), [['ev1', i, 0, 0] for i in range(0, 10)])

        session.execute("DELETE FROM test7 WHERE kind = 'ev1'")
        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        self.assertEqual(rows_to_list(res), [])

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        self.assertEqual(rows_to_list(res), [[0]])

    def batch_test(self):
        """
        Smoke test for BATCH statements:

        - CREATE a table
        - create a BATCH statement and execute it at QUORUM
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test8 (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        query = SimpleStatement("""
            BEGIN BATCH
                INSERT INTO test8 (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE test8 SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO test8 (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM test8 WHERE userid = 'user1';
            APPLY BATCH;
        """, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(query)


@canReuseCluster
class MiscellaneousCQLTester(CQLTester):
    """
    CQL tests that cannot be performed as Java unit tests, see CASSANDRA-9160.
    If you're considering adding a test here, consider writing Java unit tests
    for CQL validation instead. Add a new test here only if there is a reason
    for it, e.g. the test is related to the client protocol or thrift, requires
    examining the log files, or must run on multiple nodes.
    """

    @since('2.1', max_version='3.0')
    def large_collection_errors_test(self):
        """
        Assert C* logs warnings when selecting too large a collection over
        protocol v2:

        - prepare the cluster and connect using protocol v2
        - CREATE a table containing a map column
        - insert over 65535 elements into the map
        - select all the elements of the map
        - assert that the correct error was logged
        """

        # We only warn with protocol 2
        session = self.prepare(protocol_version=2)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        self.ignore_log_patterns = ["Detected collection for table"]

        session.execute("""
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

        # Insert more than the max, which is 65535
        for i in range(70000):
            session.execute("UPDATE maps SET properties[{}] = 'x' WHERE userid = 'user'".format(i))

        # Query for the data and throw exception
        session.execute("SELECT properties FROM maps WHERE userid = 'user'")
        node1.watch_log_for("Detected collection for table ks.maps with 70000 elements, more than the 65535 limit. "
                            "Only the first 65535 elements will be returned to the client. Please see "
                            "http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.")

    def cql3_insert_thrift_test(self):
        """
        Check that we can insert from thrift into a CQL3 table:

        - CREATE a table via CQL
        - insert values via thrift
        - SELECT the inserted values and assert they are there as expected

        @jira_ticket CASSANDRA-4377
        """
        session = self.prepare(start_rpc=True)

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')
        key = struct.pack('>i', 2)
        column_name_component = struct.pack('>i', 4)
        # component length + component + EOC + component length + component + EOC
        column_name = '\x00\x04' + column_name_component + '\x00' + '\x00\x01' + 'v' + '\x00'
        value = struct.pack('>i', 8)
        client.batch_mutate(
            {key: {'test': [Mutation(ColumnOrSuperColumn(column=Column(name=column_name, value=value, timestamp=100)))]}},
            ThriftConsistencyLevel.ONE)

        res = session.execute("SELECT * FROM test")
        self.assertEqual(rows_to_list(res), [[2, 4, 8]])

    def rename_test(self):
        """
        Check that a thrift-created table can be renamed via CQL:

        - create a table via the thrift interface
        - INSERT a row via CQL
        - ALTER the name of the table via CQL
        - SELECT from the table and assert the values inserted are there
        # TODO why doesn't this check that the column names were actually changed?
        """
        session = self.prepare(start_rpc=True)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        cfdef = CfDef()
        cfdef.keyspace = 'ks'
        cfdef.name = 'test'
        cfdef.column_type = 'Standard'
        cfdef.comparator_type = 'CompositeType(Int32Type, Int32Type, Int32Type)'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'

        client.set_keyspace('ks')
        client.system_add_column_family(cfdef)

        session.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")

        time.sleep(1)

        session.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(session, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    def invalid_string_literals_test(self):
        """
        @jira_ticket CASSANDRA-8101

        - assert INSERTing into a nonexistent table fails normally, with an InvalidRequest exception
        - create a table with ascii and text columns
        - assert that trying to execute an insert statement with non-UTF8 contents raises a ProtocolException
            - tries to insert into a nonexistent column to make sure the ProtocolException is raised over other errors
        """
        session = self.prepare()
        # this should fail as normal, not with a ProtocolException
        assert_invalid(session, u"insert into invalid_string_literals (k, a) VALUES (0, '\u038E\u0394\u03B4\u03E0')")

        session = self.cql_connection(self.cluster.nodelist()[0], keyspace='ks')
        session.execute("create table invalid_string_literals (k int primary key, a ascii, b text)")

        # this should still fail with an InvalidRequest
        assert_invalid(session, u"insert into invalid_string_literals (k, c) VALUES (0, '\u038E\u0394\u03B4\u03E0')")
        # but since the protocol requires strings to be valid UTF-8, the error
        # response to this is a ProtocolException, not an error about the
        # nonexistent column
        with self.assertRaisesRegexp(ProtocolException, 'Cannot decode string as UTF8'):
            session.execute("insert into invalid_string_literals (k, c) VALUES (0, '\xc2\x01')")

    def prepared_statement_invalidation_test(self):
        """
        @jira_ticket CASSANDRA-7910

        - CREATE a table and INSERT a row
        - prepare 2 prepared SELECT statements
        - SELECT the row with a bound prepared statement and assert it returns the expected row
        - ALTER the table, dropping a column
        - assert prepared statement without that column in it still works
        - assert prepared statement containing that column fails
        - ALTER the table, adding a column
        - assert prepared statement without that column in it still works
        - assert prepared statement containing that column also still works
        - ALTER the table, changing the type of a column
        - assert that both prepared statements still work

        # TODO this basically tests driver behavior if I read it correctly.
        # Should this be in dtests at all?
        # TODO should these assert that the ALTERs happened correctly?
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int, b int, c int)")
        session.execute("INSERT INTO test (k, a, b, c) VALUES (0, 0, 0, 0)")

        wildcard_prepared = session.prepare("SELECT * FROM test")
        explicit_prepared = session.prepare("SELECT k, a, b, c FROM test")
        result = session.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, 0)])

        session.execute("ALTER TABLE test DROP c")
        result = session.execute(wildcard_prepared.bind(None))
        # wildcard select can be automatically re-prepared by the driver
        self.assertEqual(result, [(0, 0, 0)])
        # but re-preparing the statement with explicit columns should fail
        # (see PYTHON-207 for why we expect InvalidRequestException instead of the normal exc)
        assert_invalid(session, explicit_prepared.bind(None), expected=InvalidRequest)

        session.execute("ALTER TABLE test ADD d int")
        result = session.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

        explicit_prepared = session.prepare("SELECT k, a, b, d FROM test")

        # when the type is altered, both statements will need to be re-prepared
        # by the driver, but the re-preparation should succeed
        session.execute("ALTER TABLE test ALTER d TYPE blob")
        result = session.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

        result = session.execute(explicit_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

    @freshCluster()
    def range_slice_test(self):
        """
        Regression test for CASSANDRA-1337:

        - CREATE a table
        - INSERT 2 rows
        - SELECT * from the table
        - assert 2 rows were returned

        @jira_ticket CASSANDRA-1337
        # TODO I don't see how this is an interesting test or how it tests 1337.
        """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)
        time.sleep(1)

        session.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        session.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        res = list(session.execute("SELECT * FROM test"))
        self.assertEqual(len(res), 2, msg=res)


@since('3.2')
class AbortedQueriesTester(CQLTester):
    """
    @jira_ticket CASSANDRA-7392

    Test that read-queries that take longer than read_request_timeout_in_ms
    time out.

    # TODO The important part of these is "set up a combination of
    #      configuration options that will make all reads time out, then
    #      try to read and assert it times out". This can probably be made much
    #      simpler -- most of the logic can be factored out. In many cases it
    #      probably isn't even necessary to define a custom table or to insert
    #      more than one value.
    """

    def local_query_test(self):
        """
        Check that a query running on the local coordinator node times out:

        - set a 1-second read timeout
        - start the cluster with read_iteration_delay set to 1.5 seconds
            - (this will cause read queries to take longer than the read timeout)
        - CREATE and INSERT into a table
        - SELECT * from the table using a retry policy that never retries, and assert it times out

        @jira_ticket CASSANDRA-7392
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds during each
        # iteration of non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        cluster.populate(1).start(wait_for_binary_proto=True,
                                  jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                                            "-Dcassandra.test.read_iteration_delay_ms=1500"])
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test1 (
                id int PRIMARY KEY,
                val text
            );
        """)

        for i in range(500):
            session.execute("INSERT INTO test1 (id, val) VALUES ({}, 'foo')".format(i))

        mark = node.mark_log()
        statement = SimpleStatement("SELECT * from test1",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)
        node.watch_log_for("operations timed out", from_mark=mark, timeout=60)

    def remote_query_test(self):
        """
        Check that a query running on a node other than the coordinator times out:

        - populate the cluster with 2 nodes
        - set a 1-second read timeout
        - start one node without having it join the ring
        - start the other node with read_iteration_delay set to 1.5 seconds
            - (this will cause read queries to take longer than the read timeout)
        - CREATE a table
        - INSERT 5000 rows on a session on the node that is not a member of the ring
        - run SELECT statements and assert they fail
        # TODO refactor SELECT statements:
        #        - run the statements in a loop to reduce duplication
        #        - watch the log after each query
        #        - assert we raise the right error
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True,
                    jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                              "-Dcassandra.test.read_iteration_delay_ms=1500"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test2 (
                id int,
                col int,
                val text,
                PRIMARY KEY(id, col)
            );
        """)

        for i, j in itertools.product(range(500), range(10)):
            session.execute("INSERT INTO test2 (id, col, val) VALUES ({}, {}, 'foo')".format(i, j))

        mark = node2.mark_log()

        statement = SimpleStatement("SELECT * from test2",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where id = 1",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where id IN (1, 10,  20) AND col < 10",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where col > 5 ALLOW FILTERING",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)

        node2.watch_log_for("operations timed out", from_mark=mark, timeout=60)

    def index_query_test(self):
        """
        Check that a secondary index query times out:

        - populate a 1-node cluster
        - set a 1-second read timeout
        - start one node without having it join the ring
        - start the other node with read_iteration_delay set to 1.5 seconds
            - (this will cause read queries to take longer than the read timeout)
        - CREATE a table
        - CREATE an index on the table
        - INSERT 500 values into the table
        - SELECT over the table and assert it times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(1).start(wait_for_binary_proto=True,
                                  jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                                            "-Dcassandra.test.read_iteration_delay_ms=1500"])  # see above for explanation
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test3 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute("CREATE INDEX ON test3 (col)")

        for i in range(500):
            session.execute("INSERT INTO test3 (id, col, val) VALUES ({}, {}, 'foo')".format(i, i // 10))

        mark = node.mark_log()
        statement = session.prepare("SELECT * from test3 WHERE col < ? ALLOW FILTERING")
        statement.consistency_level = ConsistencyLevel.ONE
        statement.retry_policy = FallthroughRetryPolicy()
        assert_unavailable(lambda c: debug(c.execute(statement, [50])), session)
        node.watch_log_for("operations timed out", from_mark=mark, timeout=60)

    def materialized_view_test(self):
        """
        Check that a materialized view query times out:

        - populate a 2-node cluster
        - set a 1-second read timeout
        - start one node without having it join the ring
        - start the other node with read_iteration_delay set to 1.5 seconds
            - (this will cause read queries to take longer than the read timeout)
        - CREATE a table
        - CREATE a materialized view over that table
        - INSERT 50 values into that table
        - assert querying that table results in an unavailable exception
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True,
                    jvm_args=["-Dcassandra.monitoring_check_interval_ms=50",
                              "-Dcassandra.test.read_iteration_delay_ms=1500"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test4 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM test4 "
                         "WHERE col IS NOT NULL AND id IS NOT NULL PRIMARY KEY (col, id)"))

        for i in range(50):
            session.execute("INSERT INTO test4 (id, col, val) VALUES ({}, {}, 'foo')".format(i, i // 10))

        mark = node2.mark_log()
        statement = SimpleStatement("SELECT * FROM mv WHERE col = 50",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: debug(c.execute(statement)), session)
        node2.watch_log_for("operations timed out", from_mark=mark, timeout=60)
