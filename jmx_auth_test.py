from ccmlib.node import NodetoolError
from dtest import Tester
from jmxutils import apply_jmx_authentication


class TestAuth(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def login_test(self):
        self.prepare()
        [node] = self.cluster.nodelist()
        node.nodetool('-u cassandra -pw cassandra status')

        session = self.patient_cql_connection(node, user='cassandra', password='cassandra')
        # the jmx_user role has no login privilege but give it a password anyway
        # to demonstrate that LOGIN is required for JMX authentication
        session.execute("CREATE ROLE jmx_user WITH LOGIN=false AND PASSWORD='321cba'")
        session.execute("GRANT SELECT ON MBEAN 'org.apache.cassandra.net:type=FailureDetector' TO jmx_user")
        session.execute("GRANT DESCRIBE ON ALL MBEANS TO jmx_user")
        session.execute("CREATE ROLE test WITH LOGIN=true and PASSWORD='abc123'")

        with self.assertRaisesRegexp(NodetoolError, 'Username and/or password are incorrect'):
            node.nodetool('-u baduser -pw abc123 gossipinfo')

        with self.assertRaisesRegexp(NodetoolError, 'Username and/or password are incorrect'):
            node.nodetool('-u test -pw badpassword gossipinfo')

        # role must have LOGIN attribute
        with self.assertRaisesRegexp(NodetoolError, 'jmx_user is not permitted to log in'):
            node.nodetool('-u jmx_user -pw 321cba gossipinfo')

        # test doesn't yet have any privileges on the necessary JMX resources
        with self.assertRaisesRegexp(NodetoolError, 'Access Denied'):
            node.nodetool('-u test -pw abc123 gossipinfo')

        session.execute("GRANT jmx_user TO test")
        node.nodetool('-u test -pw abc123 gossipinfo')

        # superuser status applies to JMX authz too
        node.nodetool('-u cassandra -pw cassandra gossipinfo')

    def prepare(self, nodes=1, permissions_validity=0):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': permissions_validity}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        [node] = self.cluster.nodelist()
        apply_jmx_authentication(node)
        node.start()
        node.watch_log_for('Created default superuser')
