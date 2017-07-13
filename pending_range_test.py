from cassandra.query import SimpleStatement
from nose.plugins.attrib import attr

from dtest import TRACE, Tester, debug, create_ks
from tools.decorators import no_vnodes


@no_vnodes()
class TestPendingRangeMovements(Tester):

    @attr('resource-intensive')
    def pending_range_test(self):
        """
        @jira_ticket CASSANDRA-10887
        """
        cluster = self.cluster
        # If we are on 2.1, we need to set the log level to debug or higher, as debug.log does not exist.
        if cluster.version() < '2.2' and not TRACE:
            cluster.set_log_level('DEBUG')

        # Create 5 node cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2 = cluster.nodelist()[0:2]

        # Set up RF=3 keyspace
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)

        session.execute("CREATE TABLE users (login text PRIMARY KEY, email text, name text, login_count int)")

        # We use the partition key 'jdoe3' because it belongs to node1.
        # The key MUST belong to node1 to repro the bug.
        session.execute("INSERT INTO users (login, email, name, login_count) VALUES ('jdoe3', 'jdoe@abc.com', 'Jane Doe', 1) IF NOT EXISTS;")

        lwt_query = SimpleStatement("UPDATE users SET email = 'janedoe@abc.com' WHERE login = 'jdoe3' IF email = 'jdoe@abc.com'")

        # Show we can execute LWT no problem
        for i in xrange(1000):
            session.execute(lwt_query)

        token = '-634023222112864484'

        mark = node1.mark_log()

        # Move a node
        node1.nodetool('move {}'.format(token))

        # Watch the log so we know when the node is moving
        node1.watch_log_for('Moving .* to {}'.format(token), timeout=10, from_mark=mark)
        node1.watch_log_for('Sleeping 30000 ms before start streaming/fetching ranges', timeout=10, from_mark=mark)

        if cluster.version() >= '2.2':
            node2.watch_log_for('127.0.0.1 state moving', timeout=10, filename='debug.log')
        else:
            # 2.1 doesn't have debug.log, so we are logging at trace, and look
            # in the system.log file
            node2.watch_log_for('127.0.0.1 state moving', timeout=10, filename='system.log')

        # Once the node is MOVING, kill it immediately, let the other nodes notice
        node1.stop(gently=False, wait_other_notice=True)

        # Verify other nodes believe this is Down/Moving
        out, _, _ = node2.nodetool('ring')
        debug("Nodetool Ring output: {}".format(out))
        self.assertRegexpMatches(out, '127\.0\.0\.1.*?Down.*?Moving')

        # Check we can still execute LWT
        for i in xrange(1000):
            session.execute(lwt_query)


class TestNonRedundantRangeCalculation(Tester):

    def non_redundant_calculation_test(self):
        """
        @jira_ticket CASSANDRA-12281
        """
        cluster = self.cluster
        # If we are on 2.1, we need to set the log level to debug or higher, as debug.log does not exist.
        if cluster.version() < '2.2' and not TRACE:
            cluster.set_log_level('DEBUG')

        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, _ = cluster.nodelist()

        # set up multiple keyspaces with matching configuration
        session = self.patient_cql_connection(node1)

        kss1 = {'ks1', 'ks2', 'ks3'}
        for ks in kss1:
            create_ks(session, ks, 2)

        kss2 = {'ks4', 'ks5'}
        for ks in kss2:
            create_ks(session, ks, 3)

        kss3 = {'ks6'}
        for ks in kss3:
            create_ks(session, ks, 1)

        kss4 = {'ks7', 'ks8'}
        for ks in kss4:
            create_ks(session, ks, {'dc1': 1, 'dc2': 2})

        # decommission node2 to trigger range calculation
        node2.decommission()
        node2.stop(wait_other_notice=True)

        # check if keyspaces have been grouped for calculation as expected
        kss1Found = False
        kss2Found = False
        kss3Found = False
        kss4Found = False

        rs = 'Starting pending range calculation for \\[([^]]+)\\]'
        if cluster.version() >= '2.2':
            matches = node1.grep_log(rs, filename='debug.log')
        else:
            matches = node1.grep_log(rs, filename='system.log')

        self.assertTrue(matches and len(matches) > 0)
        for _, m in matches:
            nodes = [s.strip() for s in m.group(1).split(',')]
            debug("Nodes grouped for pending range calculation: {}".format(nodes))

            kss1Found = kss1Found or kss1.issubset(nodes) and not (kss2.union(kss3.union(kss4))).intersection(nodes)
            kss2Found = kss2Found or kss2.issubset(nodes) and not (kss1.union(kss3.union(kss4))).intersection(nodes)
            kss3Found = kss3Found or kss3.issubset(nodes) and not (kss1.union(kss2.union(kss4))).intersection(nodes)
            kss4Found = kss4Found or kss4.issubset(nodes) and not (kss1.union(kss2.union(kss3))).intersection(nodes)

        self.assertTrue(kss1Found and kss2Found and kss3Found and kss4Found)
