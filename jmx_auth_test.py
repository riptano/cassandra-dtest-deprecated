import os
import re
import time

from assertions import assert_all, assert_invalid, assert_one
from cassandra import AuthenticationFailed, InvalidRequest, Unauthorized
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import SyntaxException
from ccmlib.common import get_version_from_build
from ccmlib.node import NodetoolError
from dtest import Tester, debug
from tools import since
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

        try:
            node.nodetool('-u cassandra -pw badpassword info')
        except NodetoolError as e:
             self.assertIn('Username and/or password are incorrect', str(e))

        try:
            node.nodetool('-u baduser -pw cassandra gossipinfo')
        except NodetoolError as e:
            self.assertIn('Username and/or password are incorrect', str(e))

    def prepare(self, nodes=1, permissions_validity=0):
        config = {'authenticator' : 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer' : 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms' : permissions_validity}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        [node] = self.cluster.nodelist()
        apply_jmx_authentication(node)
        node.start()
        node.watch_log_for('Created default superuser')