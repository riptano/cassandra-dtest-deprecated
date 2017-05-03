import re
import time

from cassandra import AuthenticationFailed, Unauthorized
from cassandra.cluster import NoHostAvailable

from tools.assertions import assert_invalid
from dtest import Tester, debug
from tools.decorators import since

class TestAuthJoinRingFalse(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def login_test(self):
        # also tests default user creation (cassandra/cassandra)
        self.prepare()

        node1, node2, node3 = self.cluster.nodelist()
        node3.stop(wait_other_notice=True)
        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='cassandra', timeout=300)
        try:
            self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
        try:
            self.patient_exclusive_cql_connection(node=node3, user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

    def login_tokens_empty_test(self):
        # also tests default user creation (cassandra/cassandra)
        # prepare (without starting node3)
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': 0}

        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(3)
        node1, node2, node3 = self.cluster.nodelist()
        node1.start(wait_for_binary_proto=True)
        node2.start()

        # default user setup is delayed by 10 seconds to reduce log spam
        time.sleep(10)
        n = self.cluster.wait_for_any_log('Created default superuser', 10)
        debug("Default role created by " + n.name)
        # end-prepare

        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='cassandra', timeout=300)
        try:
            self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
        try:
            self.patient_exclusive_cql_connection(node=node3, user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

    def modify_and_select_auth_test(self):
        self.prepare()

        node1, node2, node3 = self.cluster.nodelist()
        cassandra = self.patient_exclusive_cql_connection(node=node1, user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        node3.stop(wait_other_notice=True)
        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        cathy = self.patient_exclusive_cql_connection(node=node3, user='cathy', password='12345', timeout=300)

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        node3.stop()
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)

        cassandra = self.patient_exclusive_cql_connection(node=node1, user='cassandra', password='cassandra')
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")

        node3.stop(wait_other_notice=True)
        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)
        cathy = self.patient_exclusive_cql_connection(node=node3, user='cathy', password='12345')

        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(0, len(rows))

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "DELETE FROM ks.cf WHERE id = 1")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "TRUNCATE ks.cf")

        node3.stop()
        node3.start(wait_other_notice=True)

        cassandra = self.patient_exclusive_cql_connection(node=node1, user='cassandra', password='cassandra')
        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")

        node3.stop(wait_other_notice=True)
        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)
        cathy = self.patient_exclusive_cql_connection(node=node3, user='cathy', password='12345')

        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(2, len(rows))

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(1, len(rows))

        node3.stop()
        node3.start(wait_other_notice=True)
        cathy = self.patient_exclusive_cql_connection(node=node3, user='cathy', password='12345')

        rows = list(cathy.execute("TRUNCATE ks.cf"))
        assert len(rows) == 0

    def prepare(self):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': 0}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(3).start()

        # default user setup is delayed by 10 seconds to reduce log spam
        time.sleep(10)
        n = self.cluster.wait_for_any_log('Created default superuser', 10)
        debug("Default role created by " + n.name)

    def assertUnauthorized(self, message, session, query):
        with self.assertRaises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, cm.exception.message), "Expected '%s', but got '%s'" % (message, cm.exception.message)

