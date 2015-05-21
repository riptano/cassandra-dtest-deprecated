from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since

class TestOfflineTools(Tester):

    def sstablelevelreset_test(self):
    """
    Insert data and call sstablelevelreset on a series of 
    tables. Confirm level is reset and that compaction works
    normally using sstable2json to read tables.
    @since 2.1.5
    """
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()

        #test by trying to run on nonexistent keyspace
        cluster.stop()
        (output, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertTrue("ColumnFamily not found: keyspace1/standard1" in output, msg="Did not throw CF not found error")
        self.assertEqual(rc, 0, msg="Return code was not 0 where should have failed")

        #now test by generating keyspace but not flushing sstables
        cluster.start()
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=3)'])
        cursor = self.patient_cql_connection(node1)
        cursor.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy'};")
        cluster.stop()

        (output, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertTrue("Found no sstables, did you give the correct keyspace/columnfamily?" in output, msg="Improper error msg with no sstables")
        self.assertEqual(rc, 0, msg="improper return code when no sstables")

        #test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start()
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop()

        (output, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertTrue("since it is already on level 0" in output, msg="Did not skip sstables where level = 0")
        self.assertEqual(rc, 0, msg="improper return code where level = 0")

        #test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start()
        node1.stress(['write', 'n=5000000', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop()

        (output, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)


    def sstableofflinerelevel_test(self):
    """ 
    Generate sstables and allow compaction to run.
    Stop node and run sstableofflinerelevel. 
    """
        pass