from collections import defaultdict
import os
import re
import time

from dtest import Tester, debug, PRINT_DEBUG
from tools import no_vnodes, since

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

TRACE_DETERMINE_REPLICAS = re.compile('Determining replicas for mutation')
TRACE_SEND_MESSAGE = re.compile('Sending message to /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
TRACE_RESPOND_MESSAGE = re.compile('Message received from /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')
TRACE_COMMIT_LOG = re.compile('Appending to commitlog')
TRACE_FORWARD_WRITE = re.compile('Enqueuing forwarded write to /([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)')

# Some pre-computed murmur 3 hashes; there are no good python murmur3
# hashing libraries :(
murmur3_hashes = {
    5: -7509452495886106294,
    10: -6715243485458697746,
    16: -5477287129830487822,
    13: -5034495173465742853,
    11: -4156302194539278891,
    1: -4069959284402364209,
    19: -3974532302236993209,
    8: -3799847372828181882,
    2: -3248873570005575792,
    4: -2729420104000364805,
    18: -2695747960476065067,
    15: -1191135763843456182,
    20: 1388667306199997068,
    7: 1634052884888577606,
    6: 2705480034054113608,
    9: 3728482343045213994,
    14: 4279681877540623768,
    17: 5467144456125416399,
    12: 8582886034424406875,
    3: 9010454139840013625
}


@no_vnodes()
class ReplicationTest(Tester):
    """This test suite looks at how data is replicated across a cluster
    and who the coordinator, replicas and forwarders involved are.
    """

    def get_replicas_from_trace(self, trace):
        """Look at trace and return a list of the replicas contacted"""
        coordinator = None
        nodes_sent_write = set([])  # Nodes sent a write request
        nodes_responded_write = set([])  # Nodes that acknowledges a write
        replicas_written = set([])  # Nodes that wrote to their commitlog
        forwarders = set([])  # Nodes that forwarded a write to another node
        nodes_contacted = defaultdict(set)  # node -> list of nodes that were contacted

        for trace_event in trace.events:
            # Step 1, find coordinator node:
            activity = trace_event.description
            source = trace_event.source
            if activity.startswith('Determining replicas for mutation'):
                if not coordinator:
                    coordinator = source
                    break
            if not coordinator:
                continue

        for trace_event in trace.events:
            session = trace._session
            activity = trace_event.description
            source = trace_event.source
            source_elapsed = trace_event.source_elapsed
            thread = trace_event.thread_name

            # Step 2, find all the nodes that each node talked to:
            send_match = TRACE_SEND_MESSAGE.search(activity)
            recv_match = TRACE_RESPOND_MESSAGE.search(activity)
            if send_match:
                node_contacted = send_match.groups()[0]
                if source == coordinator:
                    nodes_sent_write.add(node_contacted)
                nodes_contacted[source].add(node_contacted)
            elif recv_match:
                node_contacted = recv_match.groups()[0]
                if source == coordinator:
                    nodes_responded_write.add(recv_match.groups()[0])

            # Step 3, find nodes that forwarded to other nodes:
            # (Happens in multi-datacenter clusters)
            if source != coordinator:
                forward_match = TRACE_FORWARD_WRITE.search(activity)
                if forward_match:
                    forwarding_node = forward_match.groups()[0]
                    nodes_sent_write.add(forwarding_node)
                    forwarders.add(forwarding_node)

            # Step 4, find nodes who actually wrote data:
            if TRACE_COMMIT_LOG.search(activity):
                replicas_written.add(source)

        return {"coordinator": coordinator,
                "forwarders": forwarders,
                "replicas": replicas_written,
                "nodes_sent_write": nodes_sent_write,
                "nodes_responded_write": nodes_responded_write,
                "nodes_contacted": nodes_contacted
                }

    def get_replicas_for_token(self, token, replication_factor,
                               strategy='SimpleStrategy', nodes=None):
        """Figure out which node(s) should receive data for a given token and
        replication factor"""
        if not nodes:
            nodes = self.cluster.nodelist()
        token_ranges = sorted(zip([n.initial_token for n in nodes], nodes))
        replicas = []

        # Find first replica:
        for i, (r, node) in enumerate(token_ranges):
            if token <= r:
                replicas.append(node.address())
                first_ring_position = i
                break
        else:
            replicas.append(token_ranges[0][1].address())
            first_ring_position = 0

        # Find other replicas:
        if strategy == 'SimpleStrategy':
            for node in nodes[first_ring_position + 1:]:
                replicas.append(node.address())
                if len(replicas) == replication_factor:
                    break
            if len(replicas) != replication_factor:
                # Replication token range looped:
                for node in nodes:
                    replicas.append(node.address())
                    if len(replicas) == replication_factor:
                        break
        elif strategy == 'NetworkTopologyStrategy':
            # NetworkTopologyStrategy can be broken down into multiple
            # SimpleStrategies, just once per datacenter:
            for dc, rf in replication_factor.items():
                dc_nodes = [n for n in nodes if n.data_center == dc]
                replicas.extend(self.get_replicas_for_token(
                    token, rf, nodes=dc_nodes))
        else:
            raise NotImplemented('replication strategy not implemented: %s'
                                 % strategy)

        return replicas

    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if PRINT_DEBUG:
            print("-" * 40)
            for t in trace.events:
                print("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name))
            print("-" * 40)

    def simple_test(self):
        """Test the SimpleStrategy on a 3 node cluster"""
        self.cluster.populate(3).start()
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        self.conn = self.patient_exclusive_cql_connection(node1)

        # Install a tracing session so we can get info about who the
        # coordinator is contacting:
        session = self.conn
        session.max_trace_wait = 120

        replication_factor = 3
        self.create_ks(session, 'test', replication_factor)
        session.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        for key, token in murmur3_hashes.items():
            query = SimpleStatement("INSERT INTO test (id, value) VALUES (%s, 'asdf')" % key, consistency_level=ConsistencyLevel.ALL)
            future = session.execute_async(query, trace=True)
            future.result()
            trace = future.get_query_trace(max_wait=120)
            self.pprint_trace(trace)
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor))
            debug('\nreplicas should be: %s' % replicas_should_be)
            debug('replicas were: %s' % stats['replicas'])

            # Make sure the correct nodes are replicas:
            self.assertEqual(stats['replicas'], replicas_should_be)
            # Make sure that each replica node was contacted and
            # acknowledged the write:
            self.assertEqual(stats['nodes_sent_write'], stats['nodes_responded_write'])

    def network_topology_test(self):
        """Test the NetworkTopologyStrategy on a 2DC 3:3 node cluster"""
        self.cluster.populate([3, 3]).start()
        time.sleep(5)
        node1 = self.cluster.nodelist()[0]
        ip_nodes = dict((node.address(), node) for node in self.cluster.nodelist())
        self.conn = self.patient_exclusive_cql_connection(node1)

        # Install a tracing session so we can get info about who the
        # coordinator is contacting:
        session = self.conn

        replication_factor = {'dc1': 2, 'dc2': 2}
        self.create_ks(session, 'test', replication_factor)
        session.execute('CREATE TABLE test.test (id int PRIMARY KEY, value text)', trace=False)
        # Wait for table creation, otherwise trace times out -
        # CASSANDRA-5658
        time.sleep(5)

        forwarders_used = set()

        for key, token in murmur3_hashes.items():
            query = SimpleStatement("INSERT INTO test (id, value) VALUES (%s, 'asdf')" % key, consistency_level=ConsistencyLevel.ALL)
            future = session.execute_async(query, trace=True)
            future.result()
            trace = future.get_query_trace(max_wait=120)
            self.pprint_trace(trace)
            stats = self.get_replicas_from_trace(trace)
            replicas_should_be = set(self.get_replicas_for_token(
                token, replication_factor, strategy='NetworkTopologyStrategy'))
            debug('Current token is %s' % token)
            debug('\nreplicas should be: %s' % replicas_should_be)
            debug('replicas were: %s' % stats['replicas'])

            # Make sure the coordinator only talked to a single node in
            # the second datacenter - CASSANDRA-5632:
            num_in_other_dcs_contacted = 0
            for node_contacted in stats['nodes_contacted'][node1.address()]:
                if ip_nodes[node_contacted].data_center != node1.data_center:
                    num_in_other_dcs_contacted += 1
            self.assertEqual(num_in_other_dcs_contacted, 1)

            # Record the forwarder used for each INSERT:
            forwarders_used = forwarders_used.union(stats['forwarders'])

            try:
                # Make sure the correct nodes are replicas:
                self.assertEqual(stats['replicas'], replicas_should_be)
                # Make sure that each replica node was contacted and
                # acknowledged the write:
                self.assertEqual(stats['nodes_sent_write'], stats['nodes_responded_write'])
            except AssertionError as e:
                debug("Failed on key %s and token %s." % (key, token))
                raise e

        # Given a diverse enough keyset, each node in the second
        # datacenter should get a chance to be a forwarder:
        self.assertEqual(len(forwarders_used), 3)


class SnitchConfigurationUpdateTest(Tester):
    """
    Test to repro CASSANDRA-10238, wherein changing snitch properties to change racks without a restart could violate RF contract.
    """
    def check_endpoint_count(self, ks, table, nodes, rf):
        """
        Check a dummy key expecting it to have replication factor as the sum of rf on all dcs.
        """
        expected_count = sum([int(r) for d, r in rf.iteritems() if d != 'class'])
        for node in nodes:
            cmd = "getendpoints {} {} dummy".format(ks, table)
            out, err = node.nodetool(cmd)

            if len(err.strip()) > 0:
                debug(err)
                raise RuntimeError("Error running 'nodetool {}'".format(cmd))

            debug("Endpoints for node {}, expected count is {}".format(node.address(), expected_count))
            debug(out)
            ips_found = re.findall('(\d+\.\d+\.\d+\.\d+)', out)

            self.assertEqual(len(ips_found), expected_count, "wrong number of endpoints found ({}), should be: {}".format(len(ips_found), expected_count))

    def wait_for_nodes_on_racks(self, nodes, expected_racks):
        """
        Waits for nodes to match the expected racks.
        """
        regex = re.compile("^UN(?:\s*)127\.0\.0(?:.*)\s(.*)$", re.IGNORECASE)
        for i, node in enumerate(nodes):
            wait_expire = time.time() + 120
            while time.time() < wait_expire:
                out, err = node.nodetool("status")

                debug(out)
                if len(err.strip()) > 0:
                    raise RuntimeError("Error trying to run nodetool status")

                racks = []
                for line in out.split(os.linesep):
                    m = regex.match(line)
                    if m:
                        racks.append(m.group(1))

                if racks == expected_racks:
                    # great, the topology change is propogated
                    debug("Topology change detected on node {}".format(i))
                    break
                else:
                    debug("Waiting for topology change on node {}".format(i))
                    time.sleep(5)
            else:
                raise RuntimeError("Ran out of time waiting for topology to change on node {}".format(i))

    def test_rf_collapse_gossiping_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are collapsed using a gossiping property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc=dc1", "rack=rack{}".format(i)],
                                       snitch_lines_after=lambda i, node: ["dc=dc1", "rack=rack1"],
                                       final_racks=["rack1", "rack1", "rack1"])

    def test_rf_expand_gossiping_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are expanded using a gossiping property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc=dc1", "rack=rack1"],
                                       snitch_lines_after=lambda i, node: ["dc=dc1", "rack=rack{}".format(i)],
                                       final_racks=["rack0", "rack1", "rack2"])

    def test_rf_collapse_gossiping_property_file_snitch_multi_dc(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are collapsed using a gossiping property file snitch the RF is not impacted, in a multi-dc environment.
        """
        self._test_rf_on_snitch_update(nodes=[3, 3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3, 'dc2': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc={}".format(node.data_center), "rack=rack{}".format(i % 3)],
                                       snitch_lines_after=lambda i, node: ["dc={}".format(node.data_center), "rack=rack1"],
                                       final_racks=["rack1", "rack1", "rack1", "rack1", "rack1", "rack1"])

    def test_rf_expand_gossiping_property_file_snitch_multi_dc(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are expanded using a gossiping property file snitch the RF is not impacted, in a multi-dc environment.
        """
        self._test_rf_on_snitch_update(nodes=[3, 3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3, 'dc2': 3},
                                       snitch_class_name='GossipingPropertyFileSnitch',
                                       snitch_config_file='cassandra-rackdc.properties',
                                       snitch_lines_before=lambda i, node: ["dc={}".format(node.data_center), "rack=rack1"],
                                       snitch_lines_after=lambda i, node: ["dc={}".format(node.data_center), "rack=rack{}".format(i % 3)],
                                       final_racks=["rack0", "rack1", "rack2", "rack0", "rack1", "rack2"])

    def test_rf_collapse_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are collapsed using a property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='PropertyFileSnitch',
                                       snitch_config_file='cassandra-topology.properties',
                                       snitch_lines_before=lambda i, node: ["127.0.0.1=dc1:rack0", "127.0.0.2=dc1:rack1", "127.0.0.3=dc1:rack2"],
                                       snitch_lines_after=lambda i, node: ["default=dc1:rack0"],
                                       final_racks=["rack0", "rack0", "rack0"])

    def test_rf_expand_property_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are expanded using a property file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='PropertyFileSnitch',
                                       snitch_config_file='cassandra-topology.properties',
                                       snitch_lines_before=lambda i, node: ["default=dc1:rack0"],
                                       snitch_lines_after=lambda i, node: ["127.0.0.1=dc1:rack0", "127.0.0.2=dc1:rack1", "127.0.0.3=dc1:rack2"],
                                       final_racks=["rack0", "rack1", "rack2"])

    @since('2.0', max_version='2.1.x')
    def test_rf_collapse_yaml_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are collapsed using a yaml file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='YamlFileNetworkTopologySnitch',
                                       snitch_config_file='cassandra-topology.yaml',
                                       snitch_lines_before=lambda i, node: ["topology:",
                                                                            "  - dc_name: dc1",
                                                                            "    racks:",
                                                                            "    - rack_name: rack0",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.1",
                                                                            "    - rack_name: rack1",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.2",
                                                                            "    - rack_name: rack2",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.3"],
                                       snitch_lines_after=lambda i, node: ["topology:",
                                                                           "  - dc_name: dc1",
                                                                           "    racks:",
                                                                           "    - rack_name: rack0",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.1",
                                                                           "      - broadcast_address: 127.0.0.2",
                                                                           "      - broadcast_address: 127.0.0.3"],
                                       final_racks=["rack0", "rack0", "rack0"])

    @since('2.0', max_version='2.1.x')
    def test_rf_expand_yaml_file_snitch(self):
        """
        @jira_ticket CASSANDRA-10238

        Confirm that when racks are expanded using a yaml file snitch the RF is not impacted.
        """
        self._test_rf_on_snitch_update(nodes=[3], rf={'class': '\'NetworkTopologyStrategy\'', 'dc1': 3},
                                       snitch_class_name='YamlFileNetworkTopologySnitch',
                                       snitch_config_file='cassandra-topology.yaml',
                                       snitch_lines_before=lambda i, node: ["topology:",
                                                                            "  - dc_name: dc1",
                                                                            "    racks:",
                                                                            "    - rack_name: rack0",
                                                                            "      nodes:",
                                                                            "      - broadcast_address: 127.0.0.1",
                                                                            "      - broadcast_address: 127.0.0.2",
                                                                            "      - broadcast_address: 127.0.0.3"],
                                       snitch_lines_after=lambda i, node: ["topology:",
                                                                           "  - dc_name: dc1",
                                                                           "    racks:",
                                                                           "    - rack_name: rack0",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.1",
                                                                           "    - rack_name: rack1",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.2",
                                                                           "    - rack_name: rack2",
                                                                           "      nodes:",
                                                                           "      - broadcast_address: 127.0.0.3"],
                                       final_racks=["rack0", "rack1", "rack2"])

    def _test_rf_on_snitch_update(self, nodes, rf, snitch_class_name, snitch_config_file, snitch_lines_before, snitch_lines_after, final_racks):
        cluster = self.cluster
        cluster.populate(nodes)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.{}'.format(snitch_class_name)})

        # start with separate racks
        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
                for line in snitch_lines_before(i, node):
                    topo_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(cluster.nodelist()[0])

        options = (', ').join(['\'{}\': {}'.format(d, r) for d, r in rf.iteritems()])
        session.execute("CREATE KEYSPACE testing WITH replication = {{{}}}".format(options))
        session.execute("CREATE TABLE testing.rf_test (key text PRIMARY KEY, value text)")

        # make sure endpoint count is correct before continuing with the rest of the test
        self.check_endpoint_count('testing', 'rf_test', cluster.nodelist(), rf)

        # update snitch file on all nodes
        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
                for line in snitch_lines_after(i, node):
                    topo_file.write(line + os.linesep)

        self.wait_for_nodes_on_racks(cluster.nodelist(), final_racks)

        # nodes have joined racks, check endpoint counts again
        self.check_endpoint_count('testing', 'rf_test', cluster.nodelist(), rf)

    def test_switch_data_center_startup_fails(self):
        """
        @jira_ticket CASSANDRA-9474

        Confirm that switching data centers fails to bring up the node.
        """
        expected_error = r"Cannot start node if snitch's data center (.*) differs from previous data center (.*)\." \
                         " Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_dc=true."
        self.ignore_log_patterns = [expected_error]

        cluster = self.cluster
        cluster.populate(1)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        node = cluster.nodelist()[0]
        with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
            topo_file.write("dc=dc9" + os.linesep)
            topo_file.write("rack=rack1" + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        node.stop()

        with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as topo_file:
            topo_file.write("dc=dc0" + os.linesep)
            topo_file.write("rack=rack1" + os.linesep)

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark, timeout=10)
