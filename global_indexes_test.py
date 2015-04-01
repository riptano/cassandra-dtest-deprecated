import random, re, time, uuid
from dtest import Tester, debug
from tools import since
from assertions import assert_one, assert_invalid, assert_none
from cassandra import InvalidRequest, ConsistencyLevel
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.protocol import ConfigurationException
from tools import since, new_node

@since('3.0')
class TestGlobalIndexes(Tester):

    def prepare(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        return session

    def prepare_user_table(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(session, 'users', columns=columns)

        # create index
        session.execute("CREATE GLOBAL INDEX ON ks.users (state) INCLUDE (password, session_token);")

        return session

    def add_dc_after_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        node4 = new_node(self.cluster, data_center='dc2')
        node4.start()
        node5 = new_node(self.cluster, remote_debug_port='2500', data_center='dc2')
        node5.start()

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t WHERE v = %d" % i, [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    def add_node_after_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        node4 = new_node(self.cluster)
        node4.start()

        session2 = self.patient_exclusive_cql_connection(node4)

        for i in xrange(1000):
            assert_one(session2, "SELECT * FROM ks.t WHERE v = %d" % i, [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    def allow_filtering_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 text, v3 decimal)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")
        session.execute("CREATE GLOBAL INDEX ON t (v2) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES (%d, %d, 'a', 3.0)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE id = %d AND v = %d" % (i, i), [i, i, 'a', 3.0])

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i, 'a', 3.0])

        rows = session.execute("SELECT * FROM t WHERE v2 = 'a'")
        assert len(rows) == 1000, "Expected 1000 rows but got %d" % len(rows)

        assert_invalid(session, "SELECT * FROM t WHERE v = %d AND v2 = 'a'" % i)
        assert_invalid(session, "SELECT * FROM t WHERE v = %d AND v2 = 'a' ALLOW FILTERING" % i)

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d AND v3 = 3.0 ALLOW FILTERING" % i, [i, i, 'a', 3.0])

    def clustering_column_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY (id, v))")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    def drop_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        session.execute("DROP GLOBAL INDEX ks.t_v_idx")

        for i in xrange(1000):
            assert_invalid(session, "SELECT * FROM t WHERE v = %d" % i)

    def drop_node_after_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

        node3 = self.cluster.nodelist()[2]
        node3.nodetool("decommission")
        self.cluster.remove(node3)

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM ks.t WHERE v = %d" % i, [i, i])

        for i in xrange(1000, 1100):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000, 1100):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    def global_index_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])

    def indexed_collections_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, numbers set<int>)")
        session.execute("CREATE GLOBAL INDEX ON t (numbers) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, numbers) VALUES (%d, %d, {%d})" % (i, i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE numbers CONTAINS %d" % (i), [i, i, i])

    def indexed_ttl_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int, v2 int, v3 int)")
        session.execute("CREATE GLOBAL INDEX ON t (v2) INCLUDE (*)")

        for i in xrange(1000):
            session.execute("INSERT INTO t (id, v, v2, v3) VALUES (%d, %d, %d, %d) USING TTL 30" % (i, i, i, i))

        for i in xrange(1000):
            assert_one(session, "SELECT * FROM t WHERE v2 = %d" % i, [i, i, i, i])

        time.sleep(30)

        for i in xrange(1000):
            assert_none(session, "SELECT * FROM t WHERE v2 = %d" % i)

    def populate_index_after_insert_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")

        for i in xrange(10000):
            session.execute("INSERT INTO t (id, v) VALUES (%d, %d)" % (i, i))

        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        #Should not be able to query an index until it is built.
        for i in xrange(10000):
            try:
                assert_one(session, "SELECT * FROM t WHERE v = %d" % i, [i, i])
            except InvalidRequest as e:
                assert re.search("Cannot query index until it is built.", str(e))

    def test_6924_dropping_cf(self):
        session = self.prepare()

        for i in xrange(10):
            try:
                session.execute("DROP TABLE t")
            except InvalidRequest:
                pass

            session.execute("CREATE TABLE t (id int PRIMARY KEY, v int)")
            session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

            for i in xrange(10):
                session.execute("INSERT INTO t (id, v) VALUES (%d, 0)" % i)

            rows = session.execute("select count(*) from t WHERE v=0")
            count = rows[0][0]
            self.assertEqual(count, 10)

    def test_8272(self):
        session = self.prepare()
        session.execute("CREATE TABLE t (id int PRIMARY KEY, v text)")
        session.execute("CREATE GLOBAL INDEX ON t (v) INCLUDE (*)")

        for i in xrange(1000):
            insert_query = SimpleStatement("INSERT INTO t (id, v) VALUES (%d, 'foo')" % i, ConsistencyLevel.ALL)
            session.execute(insert_query)

            update_query = SimpleStatement("UPDATE t SET v = 'bar' WHERE id = %d" % i, ConsistencyLevel.QUORUM)

            session.execute(update_query)
            assert_none(session, "SELECT * from t WHERE v = 'foo'", ConsistencyLevel.QUORUM)


    def test_create_index(self):
        session = self.prepare_user_table()

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

    def test_double_indexing_column(self):
        session = self.prepare_user_table()

        assert_invalid(session, "CREATE INDEX ON ks.users (state)")
        assert_invalid(session, "CREATE GLOBAL INDEX ON ks.users (state) INCLUDE (password)")

        session.execute("CREATE INDEX ON ks.users (gender)")
        assert_invalid(session, "CREATE GLOBAL INDEX ON ks.users (gender) INCLUDE (birth_year)")

    def test_drop_index(self):
        session = self.prepare_user_table()

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

        session.execute("DROP GLOBAL INDEX ks.users_state_idx")

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 0, "Expecting 0 global indexes, got" + str(result)

    def test_drop_indexed_column(self):
        session = self.prepare_user_table()

        assert_invalid(session, "ALTER TABLE ks.users DROP state")
        assert_invalid(session, "ALTER TABLE ks.users ALTER state TYPE blob")

    def test_drop_indexed_table(self):
        session = self.prepare_user_table()

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 1, "Expecting 1 global index, got" + str(result)

        session.execute("DROP TABLE ks.users")

        result = session.execute("SELECT * FROM system.schema_globalindexes WHERE keyspace_name='ks' AND columnfamily_name='users'")
        assert len(result) == 0, "Expecting 0 global indexes, got" + str(result)


    def test_index_prepared_statement(self):
        session = self.prepare_user_table()

        insertPrepared = session.prepare("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES (?, ?, ?, ?, ?);")
        selectPrepared = session.prepare("SELECT state, password, session_token FROM users WHERE state=?;")

        # insert data
        session.execute(insertPrepared.bind(('user1', 'ch@ngem3a', 'f', 'TX', 1968)))
        session.execute(insertPrepared.bind(('user2', 'ch@ngem3b', 'm', 'CA', 1971)))
        session.execute(insertPrepared.bind(('user3', 'ch@ngem3c', 'f', 'FL', 1978)))
        session.execute(insertPrepared.bind(('user4', 'ch@ngem3d', 'm', 'TX', 1974)))

        result = session.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)
        result = session.execute(selectPrepared.bind(['TX']))
        assert len(result) == 2, "Expecting 2 users, got" + str(result)
        result = session.execute(selectPrepared.bind(['FL']))
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
        result = session.execute(selectPrepared.bind(['MA']))
        assert len(result) == 0, "Expecting 0 users, got" + str(result)

    def test_index_query(self):
        session = self.prepare_user_table()

        # insert data
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        result = session.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)
        result = session.execute("SELECT state, password, session_token FROM users WHERE state='TX';")
        assert len(result) == 2, "Expecting 2 users, got" + str(result)
        result = session.execute("SELECT state, password, session_token FROM users WHERE state='CA';")
        assert len(result) == 1, "Expecting 1 users, got" + str(result)
        result = session.execute("SELECT state, password, session_token FROM users WHERE state='MA';")
        assert len(result) == 0, "Expecting 0 users, got" + str(result)
