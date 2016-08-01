from dtest import Tester, debug
from ccmlib.node import Node
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from tools import since
from time import time, sleep
from assertions import assert_none, assert_invalid
from jmxutils import make_mbean, JolokiaAgent, remove_perf_disable_shared_mem

@since('3.0')
class TestHintedHandoff(Tester):

    def check_delivery(self, node, destination_nodes, numhints, wait_for_delivery=False):
        destinations = []
        for dest in destination_nodes:
            name = 'Hints_created-' + dest
            destinations.append(make_mbean('metrics', type='HintedHandOffManager', name=dest))
        hints_created = False
        hints_delivered = False
        timeout = time() + 90.00
        while not handedoff and time() < timeout:
            with JolokiaAgent(node) as jmx:
                try:
                    results = []
                    for mbean in destinations:
                        results.append(jmx.read_attribute(mbean, 'Count'))
                    #hints can sometimes be accumulated from system tables too, allow a little bit of skew
                    if all([numhints <= node_hints <= (numhints + 10) for node_hints in results]):
                        hints_created = True
                        if wait_for_delivery:
                            waiting = True
                            while waiting:
                                res = []
                                for mbean in destinations:
                                    res.append(jmx.read_attribute(mbean, 'Count'))
                                if all([hint_count==0 for hint_count in res]):
                                    waiting = False
                                    hints_delivered = True
                    else:
                        debug(node3)
                        debug(node2)
                        return hints_created, hints_delivered
                except Exception,e:
                    debug(str(e))
        return hints_created, hints_delivered

    def simple_functionality_test(self):
        """
        Simple Test - check hints are delivered
        - bring up 3 node cluster with rf = 3
        - take down nodes 2, 3
        - write data to node 1, data should include updates and deletes (so to test mutations on same key)
        - bring up nodes 2, 3
        - allow hint delivery
        - take down node 1 and verify data using cl=one for each node
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)
        node3.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node2.start()
        node3.start()

        hints, delivered = self.check_delivery(node1, [str(node2.address()), str(node3.address())], 10200, True)
        self.assertTrue(hints)
        self.assertTrue(delivered)

        node1.stop(gently = False)

        for node in [node2,node3]:
            othernode = [x for x in [node2, node3] if not node == x][0]
            othernode.stop(gently=False)
            cursor = self.patient_cql_connection(node)
            for x in range(200, numhints):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], x)
            for x in range(100, 200):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], 100000)
            for x in range(0, 100):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                assert_none(cursor, query)
            othernode.start()

    def upgrade_versions_test(self):
        """
        tests upgrading node with existing hints
        - bring up 3 node cluster with rf=3 - cassandra-2.2
        - take down node2, node3
        - write data to node 1, allowing hints to build up (check using system.hints)
        - take down node 1
        - upgrade node 1 to cassandra-3.0
        - bring up node 2 & 3, wait for hint delivery
        - bring down node 1, query node 2 at cl=one to verify all data is present
        - similarly query node 3
        """

        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)
        node3.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node1.stop(gently=False)
        node1.set_install_dir(version = '3.0')
        node1.start()

        #check system.hints truncated
        assert_invalid(cursor, "SELECT * FROM system.hints LIMIT 10")

        node2.set_install_dir(version = '3.0')
        node3.set_install_dir(version = '3.0')

        node2.start()
        node3.start()

        hints, delivered = self.check_delivery(node1, [str(node2.address()), str(node3.address())], 10200, True)
        self.assertTrue(hints)
        self.assertTrue(delivered)

        node1.stop(gently=False)

        for node in [node2,node3]:
            othernode = [x for x in [node2, node3] if not node == x][0]
            othernode.stop(gently)
            cursor = self.patient_cql_connection(node)
            for x in range(200, numhints):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], x)
            for x in range(100, 200):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                results = cursor.execute(query)
                self.assertEqual(results[0][0], 100000)
            for x in range(0, 100):
                query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
                assert_none(cursor, query)
            othernode.start(wait=True)

    def nodetool_commands_test(self):
        """
        Check nodetool pausehandoff, resumehandoff, sethintedhandoffthrottlekb, statushandoff
        - bring up 3 node cluster with rf = 3
        - sethintedhandoffthrottlekb to low kb
        - take down node, write data to others
        - bring up node
        - check statushandoff is reporting accurate stats
        - try pausing handoff, check status again to ensure, resume handoff
        - verify data
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node1.nodetool("sethintedhandoffthrottlekb 10")

        node3.stop(gently=False)

        numhints = 1000     
        for x in range(0, numhints):
            insq = SimpleStatement("INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x)), consistency_level=ConsistencyLevel.TWO)
            cursor.execute(insq)
        
        node3.start()

        notstarted = True
        while notstarted:
            output = node1.nodetool("statushandoff", capture_output=True)
            if "running" in output:
                notstarted = False

        node1.nodetool("pausehandoff")

        output = node1.nodetool("statushandoff", capture_output=True)
        self.assertTrue("paused" in output)
        
        node1.nodetool("resumehandoff")

        output = node1.nodetool("statushandoff", capture_output=True)
        self.assertTrue("running" in output)

        hints, delivered = self.check_delivery(node1, [str(node2.address()), str(node3.address())], 10200, True)
        self.assertTrue(hints)
        self.assertTrue(delivered)


    def interrupted_delivery_test(self):
        """
        Check hints are delivered after receiving node is restarted during delivery
        - bring up 3 node cluster with rf = 3
        - take down node 2
        - write data to node 1, data should include updates and deletes (so to test mutations on same key)
        - bring up node 2
        - force/allow hint delivery
        - take down node 1 and verify data using cl=one
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        #create ks and table with rf 3
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        node2.stop(wait_other_notice=True, gently=False)

        numhints=10000
        for x in range(0, numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        for x in range(0, 100):
            delq = "DELETE FROM hhtest.hhtab WHERE key = {key}".format(key=str(x))
            cursor.execute(delq)

        for x in range(100, 200):
            updateq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, 100000)".format(key=str(x))
            cursor.execute(updateq)

        node2.start()

        hints, delivered = self.check_delivery(node1, [str(node2.address())], 10200)
        self.assertTrue(hints)
        
        node2.stop(gently=False)

        node2.start(wait=False)

        hints, delivered = self.check_delivery(node1, [str(node2.address())], 10200)
        self.assertTrue(delivered)

        node1.stop(gently=False)

        cursor = self.patient_cql_connection(node)
        for x in range(200, numhints):
            query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
            results = cursor.execute(query)
            self.assertEqual(results[0][0], x)
        for x in range(100, 200):
            query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
            results = cursor.execute(query)
            self.assertEqual(results[0][0], 100000)
        for x in range(0, 100):
            query = "SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x))
            assert_none(cursor, query)
