import os
import random
import time
import shutil
import subprocess
import tempfile
import re
import time
from threading import Thread
from dtest import Tester, debug
from tools import new_node, query_c1c2, since, KillOnBootstrap, InterruptBootstrap
from assertions import assert_almost_equal
from ccmlib.node import NodeError
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
import re

class TestStreaming(Tester):

    def prepare_row_cache_tests(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={
            'row_cache_size_in_mb': 10,
            'row_cache_save_period': 5
        })
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()

        # write one key with stress only to create schema
        node1.stress(['write', 'n=1', 'cl=TWO', 'no-warmup', '-schema',
                     'replication(factor=2)',
                     'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)',
                     '-pop', 'seq=1..1'])

        # enable row caching on standard1 table
        session = self.exclusive_cql_connection(node1)
        if cluster.version() >= '2.2':
            session.execute("ALTER TABLE keyspace1.standard1 WITH caching = { 'keys' : 'ALL', 'rows_per_partition' : 'ALL' };")
        else:
            session.execute("ALTER TABLE keyspace1.standard1 WITH caching=ALL;")
        session.shutdown()

    def extract_invalidated_keys_and_ranges(self, node):
        invalidated_logs = node.grep_log('ranges of row cache on table')
        invalidated_keys = 0
        invalidated_ranges = 0
        for log in invalidated_logs:
            count = re.search(r'Invalidating (\d+) keys', str(log), re.IGNORECASE)
            if count:
                invalidated_keys += int(count.group(1))
            count = re.search(r'and (\d+) ranges', str(log), re.IGNORECASE)
            if count:
                invalidated_ranges += int(count.group(1))
        debug("invalidated_keys: {0}".format(invalidated_keys))
        debug("invalidated_ranges: {0}".format(invalidated_ranges))
        return (invalidated_keys, invalidated_ranges)

    @since('2.1')
    def test_stream_cache_invalidation_by_key(self):
        """Invalidate keys individually if less than 10K keys per sstable"""
        self.prepare_row_cache_tests()

        cluster = self.cluster
        node1, node2 = cluster.nodelist()

        # create one sstable with 10k keys on node1 and node2
        node1.stress(['write', 'n=10K', 'cl=TWO', 'no-warmup', '-schema',
                     'replication(factor=2)',
                     'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)',
                     '-pop', 'seq=1..10K'])
        cluster.flush()
        cluster.compact() #make sure we have one sstable

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.start()

        # wait for node3 ready to finish bootstrap
        node3.watch_log_for("Starting listening for CQL clients")
        session = self.exclusive_cql_connection(node3)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert rows[0][0] == 'COMPLETED', rows[0][0]
        session.shutdown()

        # Make sure the correct number of keys was invalidated
        keys, ranges = self.extract_invalidated_keys_and_ranges(node3)
        assert_almost_equal(6666, keys, error=0.05)
        self.assertEqual(0, ranges)


    @since('2.1')
    def test_stream_cache_invalidation_by_range(self):
        """Invalidate keys by range if more than 10K keys per sstable"""
        self.prepare_row_cache_tests()

        cluster = self.cluster
        node1, node2 = cluster.nodelist()

        # create one sstable with 40k keys on node1 and node2
        node1.stress(['write', 'n=40K', 'cl=TWO', 'no-warmup', '-schema',
                     'replication(factor=2)',
                     'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)',
                     '-pop', 'seq=1..40K'])
        cluster.flush()
        cluster.compact() #make sure we have one sstable

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.start()

        # wait for node3 ready to finish bootstrap
        node3.watch_log_for("Starting listening for CQL clients")
        session = self.exclusive_cql_connection(node3)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert rows[0][0] == 'COMPLETED', rows[0][0]
        session.shutdown()

        # Make sure the correct number of ranges was invalidated
        keys, ranges = self.extract_invalidated_keys_and_ranges(node3)
        self.assertEqual(0, keys)
        self.assertEqual(2, ranges)


    @since('2.1')
    def test_stream_cache_invalidation_by_key_and_range(self):
        """Invalidate keys by range if more than 100K keys per sstable"""
        self.prepare_row_cache_tests()

        cluster = self.cluster
        node1, node2 = cluster.nodelist()

        # create one sstable with 40k keys on node1 and node2
        node1.stress(['write', 'n=40K', 'cl=TWO', 'no-warmup', '-schema',
                     'replication(factor=2)',
                     'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)',
                     '-pop', 'seq=1..40K'])
        cluster.flush()
        cluster.compact() #make sure we have one sstable
        # create another sstable with 10k keys
        node1.stress(['write', 'n=10K', 'cl=TWO', 'no-warmup', '-schema',
                     'replication(factor=2)',
                     'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)',
                     '-pop', 'seq=40000..50000'])
        cluster.flush()

        # start bootstrapping node3 and wait for streaming
        node3 = new_node(cluster)
        node3.start()

        # wait for node3 ready to finish bootstrap
        node3.watch_log_for("Starting listening for CQL clients")
        session = self.exclusive_cql_connection(node3)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert rows[0][0] == 'COMPLETED', rows[0][0]
        session.shutdown()

        # Make sure the correct number of ranges was invalidated
        keys, ranges = self.extract_invalidated_keys_and_ranges(node3)
        assert_almost_equal(6666, keys, error=0.05)
        self.assertEqual(2, ranges)
