import time

from cql import ProgrammingError
from cql.cassandra.ttypes import AuthenticationException
from dtest import Tester, debug
from tools import *
from nose.tools import nottest

class TestAuthRoles(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def create_drop_role_test(self):
        self.prepare()
        cursor = self.get_cursor(user='cassandra', password='cassandra')

        cursor.execute("LIST ROLES")
        self.assertEqual(0, cursor.rowcount)

        cursor.execute("CREATE ROLE role1")

        cursor.execute("LIST ROLES")
        self.assertEqual(1, cursor.rowcount) # role1

        cursor.execute("DROP ROLE role1")

        cursor.execute("LIST ROLES")
        self.assertEqual(0, cursor.rowcount)

    def conditional_create_drop_role_test(self):
        self.prepare()
        cursor = self.get_cursor(user='cassandra', password='cassandra')

        cursor.execute("LIST ROLES")
        self.assertEqual(0, cursor.rowcount)

        cursor.execute("CREATE ROLE IF NOT EXISTS role1")
        cursor.execute("CREATE ROLE IF NOT EXISTS role1")

        cursor.execute("LIST ROLES")
        self.assertEqual(1, cursor.rowcount) # role1

        cursor.execute("DROP ROLE IF EXISTS role1")
        cursor.execute("DROP ROLE IF EXISTS role1")

        cursor.execute("LIST ROLES")
        self.assertEqual(0, cursor.rowcount)

    def create_drop_role_validation_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        mike = self.get_cursor(user='mike', password='12345')

        self.assertUnauthorized("Only superusers are allowed to perform CREATE ROLE queries", mike, 'CREATE ROLE role2')

        cassandra.execute("CREATE ROLE role1")

        self.assertUnauthorized("Only superusers are allowed to perform DROP ROLE queries", mike, 'DROP ROLE role1')

        self.assertUnauthorized("Role role1 already exists", cassandra, 'CREATE ROLE role1')

        cassandra.execute("DROP ROLE role1")

        self.assertUnauthorized("Role role1 doesn't exist", cassandra, 'DROP ROLE role1')

    def grant_revoke_roles_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        cassandra.execute("GRANT ROLE role1 TO ROLE role2")
        cassandra.execute("GRANT ROLE role2 TO USER mike")

        self.assertRoles(['role1'], cassandra, 'LIST ROLES OF ROLE role2')
        self.assertRoles(['role1', 'role2'], cassandra, 'LIST ROLES OF USER mike')
        self.assertRoles(['role2'], cassandra, 'LIST ROLES OF USER mike NORECURSIVE')

        cassandra.execute("REVOKE ROLE role2 FROM USER mike")

        self.assertRoles([], cassandra, 'LIST ROLES OF USER mike')

        cassandra.execute("REVOKE ROLE role1 FROM ROLE role2")

        self.assertRoles([], cassandra, 'LIST ROLES OF ROLE role2')


    def grant_revoke_role_validation_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        mike = self.get_cursor(user='mike', password='12345')

        self.assertUnauthorized("Role role1 doesn't exist", cassandra, 'GRANT ROLE role1 TO USER mike')

        cassandra.execute("CREATE ROLE role1")
        
        self.assertUnauthorized("User john doesn't exist", cassandra, 'GRANT ROLE role1 TO USER john')
        self.assertUnauthorized("Role role2 doesn't exist", cassandra, 'GRANT ROLE role1 TO ROLE role2')

        cassandra.execute("CREATE USER john WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE ROLE role2")

        self.assertUnauthorized('Only superusers are allowed to perform role management queries', mike, 'GRANT ROLE role1 TO USER john')
        self.assertUnauthorized('Only superusers are allowed to perform role management queries', mike, 'GRANT ROLE role1 TO ROLE role2')

        cassandra.execute("GRANT ROLE role1 TO USER john")
        cassandra.execute("GRANT ROLE role1 TO ROLE role2")

        self.assertUnauthorized('Only superusers are allowed to perform role management queries', mike, 'REVOKE ROLE role1 FROM USER john')
        self.assertUnauthorized('Only superusers are allowed to perform role management queries', mike, 'REVOKE ROLE role1 FROM ROLE role2')

    def list_roles_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        mike = self.get_cursor(user='mike', password='12345')

        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        self.assertRoles(['role1', 'role2'], cassandra, 'LIST ROLES')

        cassandra.execute("GRANT ROLE role1 TO ROLE role2")
        cassandra.execute("GRANT ROLE role2 TO USER mike")

        self.assertRoles(['role1'], cassandra, 'LIST ROLES OF ROLE role2')
        self.assertRoles(['role1', 'role2'], cassandra, 'LIST ROLES OF USER mike')
        self.assertRoles(['role2'], cassandra, 'LIST ROLES OF USER mike NORECURSIVE')

    def list_roles_validation_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        mike = self.get_cursor(user='mike', password='12345')

        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        cassandra.execute("GRANT ROLE role1 TO ROLE role2")
        cassandra.execute("GRANT ROLE role2 TO USER mike")
        
        self.assertUnauthorized('Only superusers are allowed to LIST ROLES for another user or role', mike, 'LIST ROLES OF USER mike')
        self.assertUnauthorized('Only superusers are allowed to LIST ROLES for another user or role', mike, 'LIST ROLES OF ROLE role2')

    def grant_revoke_permissions_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE ROLE role1")

        cassandra.execute("GRANT ALL ON table ks.cf TO ROLE role1")
        cassandra.execute("GRANT ROLE role1 TO USER mike")

        mike = self.get_cursor(user='mike', password='12345')
        
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        mike.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, mike.rowcount)

        cassandra.execute("REVOKE ROLE role1 FROM USER mike")

        self.assertUnauthorized("User mike has no MODIFY permission on <table ks.cf> or any of its parents", mike, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        
        cassandra.execute("GRANT ROLE role1 TO USER mike")
        cassandra.execute("REVOKE ALL ON ks.cf FROM ROLE role1")
        
        self.assertUnauthorized("User mike has no MODIFY permission on <table ks.cf> or any of its parents", mike, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

    def list_permissions_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        cassandra.execute("GRANT SELECT ON table ks.cf TO ROLE role1")
        cassandra.execute("GRANT ALTER ON table ks.cf TO ROLE role2")
        cassandra.execute("GRANT MODIFY ON table ks.cf TO USER mike")

        cassandra.execute("GRANT ROLE role1 TO ROLE role2")
        cassandra.execute("GRANT ROLE role2 TO USER mike")
        
        self.assertPermissionsListed([('mike', 'None', '<table ks.cf>', 'MODIFY'),
                                      ('None', 'role1', '<table ks.cf>', 'SELECT'),
                                      ('None', 'role2', '<table ks.cf>', 'ALTER')],
                                     cassandra, "LIST ALL PERMISSIONS")

        self.assertPermissionsListed([('None', 'role1', '<table ks.cf>', 'SELECT')],
                                     cassandra, "LIST ALL PERMISSIONS OF ROLE role1")

        self.assertPermissionsListed([('None', 'role1', '<table ks.cf>', 'SELECT'),
                                      ('None', 'role2', '<table ks.cf>', 'ALTER')],
                                     cassandra, "LIST ALL PERMISSIONS OF ROLE role2")

        mike = self.get_cursor(user='mike', password='12345')
        
        self.assertPermissionsListed([('mike', 'None', '<table ks.cf>', 'MODIFY'),
                                      ('None', 'role1', '<table ks.cf>', 'SELECT'),
                                      ('None', 'role2', '<table ks.cf>', 'ALTER')],
                                     mike, "LIST ALL PERMISSIONS OF mike")
        
    def list_permissions_validation_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE USER john WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        cassandra.execute("GRANT SELECT ON table ks.cf TO ROLE role1")
        cassandra.execute("GRANT ALTER ON table ks.cf TO ROLE role2")
        cassandra.execute("GRANT MODIFY ON table ks.cf TO USER john")

        cassandra.execute("GRANT ROLE role1 TO ROLE role2")
        cassandra.execute("GRANT ROLE role2 TO USER mike")

        mike = self.get_cursor(user='mike', password='12345')

        self.assertUnauthorized("You are not authorized to view all roles and users permissions", mike, "LIST ALL PERMISSIONS")
        self.assertUnauthorized("You are not authorized to view Role-role1 permissions", mike, "LIST ALL PERMISSIONS OF ROLE role1")
        self.assertUnauthorized("You are not authorized to view User-john permissions", mike, "LIST ALL PERMISSIONS OF USER john")

    def role_caching_authenticated_user_test(self):
        # This test is to show that the RoleCachingAuthenticatedUser works correctly
        # and revokes the roles from a logged in user
        self.prepare(permissions_expiry=2000)

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE ROLE role1")

        cassandra.execute("GRANT ALL ON table ks.cf TO ROLE role1")
        cassandra.execute("GRANT ROLE role1 TO USER mike")

        mike = self.get_cursor(user='mike', password='12345')

        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        mike.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, mike.rowcount)

        cassandra.execute("REVOKE ROLE role1 FROM USER mike")
        # should still have permissions after 1s
        time.sleep(1.0)
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        mike.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, mike.rowcount)
        # after 2.5s the cache should have timed out and the roles refreshed
        time.sleep(1.5)

        self.assertUnauthorized("User mike has no MODIFY permission on <table ks.cf> or any of its parents", mike, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

    def prepare(self, nodes=1, permissions_expiry=0):
        config = {'authenticator' : 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer' : 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'role_mapper' : 'org.apache.cassandra.auth.CassandraRoleMapper',
                  'permissions_validity_in_ms' : permissions_expiry}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start(no_wait=True)
        # default user setup is delayed by 10 seconds to reduce log spam
        
        if nodes == 1:
            self.cluster.nodelist()[0].watch_log_for('Created default superuser')
        else:
            # can' just watch for log - the line will appear in just one of the nodes' logs
            # only one test uses more than 1 node, though, so some sleep is fine.
            time.sleep(15)

    def get_cursor(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.cql_connection(node, version="3.1.7", user=user, password=password)
        return conn.cursor()

    def assertPermissionsListed(self, expected, cursor, query):
        cursor.execute(query)
        perms = [(str(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in cursor.fetchall()]
        self.assertEqual(sorted(expected), sorted(perms))

    def assertRoles(self, expected, cursor, query):
        cursor.execute(query)
        roles = [(r[0]) for r in cursor.fetchall()]
        self.assertEqual(sorted(expected), sorted(roles))

    def assertUnauthorized(self, message, cursor, query):
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute(query)
        self.assertEqual("Bad Request: " + message, cm.exception.message)
