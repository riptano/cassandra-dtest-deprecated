from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since
import subprocess
import os
import time
import shutil

@since('3.0')
class TestJBOD(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r"doesn't exist",
        ]
        Tester.__init__(self, *args, **kwargs)
        self.allow_log_errors = True

    def verify_keys_test(self):
        """
        Test to see that all keys are on the same disk:
        Start up 3 node cluster, with 2 data disks.
        Write a number of partitions to cluster, remembering keys.
        Use getsstables to see which sstables each key is on. 
        Check that all keys are on same disk.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        cluster.set_configuration_options({'disk_failure_policy':'best_effort'})

        cluster.set_data_dirs(['data1', 'data2'])
        cluster.start()

        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE test.jtest(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        #insert a bunch of keys
        numkeys=100
        for x in range(0, numkeys):
            insq = "INSERT INTO test.jtest(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)
        node1.flush()

        initial_sstables = [None]*numkeys
        #figure out which dir each key is in
        for x in range(0, numkeys):
            sstable_path = node1.nodetool('getsstables test jtest ' + str(x), capture_output=True)
            if "data1" in sstable_path[0]:
                initial_sstables[x] = "data1"
            elif "data2" in sstable_path[0]:
                initial_sstables[x] = "data2"
            else:
                self.fail(sstable_path)

        #update added keys to ensure that keys remain on the same disk
        for x in range(0, numkeys):
            insq = "INSERT INTO test.jtest(key,val) VALUES ({key}, 66)".format(key=str(x))
            cursor.execute(insq)
        node1.flush()

        #check that keys are on same disks as prior
        final_sstables = [None]*numkeys
        for x in range(0, numkeys):
            sstable_paths = node1.nodetool('getsstables test jtest ' + str(x), capture_output=True)
            for path in sstable_paths:
                debug(initial_sstables[x])
                debug(path)
                if path != "":
                    self.assertTrue(initial_sstables[x] in path)
    
    def add_disk_test(self):
        """
        Test adding a disk works properly:
        Start up 3 node cluster, with 2 data disks, but only first specified in yaml.
        Note disk_failure_policy should be set to best-effort.
        Write decent amount of data then stop cluster.
        Add additional disk to yaml and remove one existing disk (ie remove directory) and start cluster.
        Ensure that data files properly distributed across disks.      
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        cluster.set_data_dirs(['data1', 'data2'])
        cluster.set_configuration_options({'disk_failure_policy':'best_effort'})

        cluster.start()

        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}"
        cfq = "CREATE TABLE test.jtest(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)
        
        numkeys=100
        for x in range(0, numkeys):
            insq = SimpleStatement("INSERT INTO test.jtest(key,val) VALUES ({key}, {key})".format(key=str(x)), consistency_level=ConsistencyLevel.ALL)
            cursor.execute(insq)
        node1.flush()

        cluster.drain()
        cluster.stop(gently=False)
        cluster.set_data_dirs(['data1', 'data2', 'data3'])
        shutil.rmtree(os.path.join(node1.get_path(), 'data1'))
        cluster.start()
        cursor = self.patient_cql_connection(node2)


        numkeys=100
        for x in range(0, numkeys):
            insq = SimpleStatement("SELECT val FROM test.jtest WHERE key={key};".format(key=str(x)), consistency_level=ConsistencyLevel.ALL)
            try:
                res = cursor.execute(insq)
                debug(res)
                self.assertEqual(res[0][0], x)
            except Exception, e:
                self.fail("Failed during key verification: " + str(e))