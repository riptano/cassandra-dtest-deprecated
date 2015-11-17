from dtest import Tester, debug
from tools import since, InterruptCompaction
from ccmlib import common
from ccmlib.node import NodetoolError

import subprocess
import glob
import os

# These must match the stress schema names
KeyspaceName = 'keyspace1'
TableName = 'standard1'


def _remove_prefix(string, prefix):
    assert string.startswith(prefix), '{p} not a prefix of {s}'.format(p=prefix, s=string)
    suffix = string[len(prefix):]
    assert string.endswith(suffix), '{u} not a prefix of {t}'.format(u=suffix, t=string)
    assert len(suffix) + len(prefix) == len(string)
    return suffix


def _strip_common_prefix(strings):
    strings = list(map(os.path.normcase, strings))
    common_prefix = os.path.commonprefix(strings)

    return [_remove_prefix(string, common_prefix) for string in strings]


@since('3.0')
class SSTableUtilTest(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def compaction_test(self):
        """
        @jira_ticket CASSANDRA-7066
        Check we can list the sstable files after successfull compaction (no temporary sstable files)
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        self._create_data(node, KeyspaceName, TableName, 100000)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        node.compact()
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

    def abortedcompaction_test(self):
        """
        @jira_ticket CASSANDRA-7066
        Check we can list the sstable files after aborted compaction (temporary sstable files)
        Then perform a cleanup and verify the temporary files are gone
        """
        log_file_name = 'debug.log'
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        numrecords = 400000

        self._create_data(node, KeyspaceName, TableName, numrecords)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        t = InterruptCompaction(node, TableName, filename=log_file_name)
        t.start()

        try:
            node.compact()
            assert False, "Compaction should have failed"
        except NodetoolError:
            pass  # expected to fail

        t.join()

        # should compaction finish before the node is killed, this test would fail,
        # in which case try increasing numrecords
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName, finalfiles)
        self.assertTrue(len(tmpfiles) > 0)

        self._invoke_sstableutil(KeyspaceName, TableName, cleanup=True)

        self.assertEqual([], self._invoke_sstableutil(KeyspaceName, TableName, type='tmp'))
        self.assertEqual(finalfiles, self._invoke_sstableutil(KeyspaceName, TableName, type='final'))

        # restart to make sure not data is lost
        node.start(wait_for_binary_proto=True)
        node.watch_log_for("Compacted(.*)%s" % (TableName, ), filename=log_file_name)

        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        debug("Run stress to ensure data is readable")
        self._read_data(node, numrecords)

    def _create_data(self, node, ks, table, numrecords):
        """
         This is just to create the schema so we can disable compaction
        """
        node.stress(['write', 'n=1', '-rate', 'threads=1'])
        node.nodetool('disableautocompaction %s %s' % (ks, table))

        node.stress(['write', 'n=%d' % (numrecords), '-rate', 'threads=50'])
        node.flush()

    def _read_data(self, node, numrecords):
        node.stress(['read', 'n=%d' % (numrecords,), '-rate', 'threads=25'])

    def _check_files(self, node, ks, table, expected_finalfiles=[], expected_tmpfiles=[]):
        if common.is_win():
            if expected_finalfiles:
                expected_finalfiles = _strip_common_prefix(expected_finalfiles)
            if expected_tmpfiles:
                expected_tmpfiles = _strip_common_prefix(expected_tmpfiles)

        sstablefiles = self._get_sstable_files(node, ks, table)

        if common.is_win():
            sstablefiles = _strip_common_prefix(sstablefiles)

        debug("Comparing all files...")
        allfiles = self._invoke_sstableutil(ks, table, type='all')

        if common.is_win():
            allfiles = _strip_common_prefix(allfiles)

        self.assertEqual(sstablefiles, allfiles)

        if len(expected_finalfiles) == 0:
            expected_finalfiles = allfiles

        debug("Comparing final files...")
        finalfiles = self._invoke_sstableutil(ks, table, type='final')

        if common.is_win():
            finalfiles = _strip_common_prefix(finalfiles)

        self.assertEqual(expected_finalfiles, finalfiles)

        debug("Comparing tmp files...")
        tmpfiles = self._invoke_sstableutil(ks, table, type='tmp')

        common_prefix = os.path.commonprefix(list(tmpfiles) + list(expected_tmpfiles))

        self.assertEqual([_remove_prefix(s, common_prefix) for s in tmpfiles],
                         [_remove_prefix(s, common_prefix) for s in expected_tmpfiles])

        debug("Comparing op logs...")
        expectedoplogs = sorted(self._get_sstable_transaction_logs(node, ks, table))
        oplogs = sorted(list(set(self._invoke_sstableutil(ks, table, type='tmp', oplogs=True)) - set(tmpfiles)))
        self.assertEqual(expectedoplogs, oplogs)

        return finalfiles, tmpfiles

    def _invoke_sstableutil(self, ks, table, type='all', oplogs=False, cleanup=False):
        """
        Invoke sstableutil and return the list of files, if any
        """
        debug("About to invoke sstableutil...")
        node1 = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node1.get_install_cassandra_root(), node1.get_node_cassandra_root())
        tool_bin = node1.get_tool('sstableutil')

        args = [tool_bin, '--type', type]

        if oplogs:
            args.extend(['--oplog'])
        if cleanup:
            args.extend(['--cleanup'])

        args.extend([ks, table])

        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (stdout, stderr) = p.communicate()

        if p.returncode != 0:
            debug(stderr)
            assert False, "Error invoking sstableutil; returned {code}".format(code=p.returncode)

        debug(stdout)
        match = ks + os.sep + table + '-'
        ret = sorted(filter(lambda s: match in s, stdout.splitlines()))
        debug("Got %d files" % (len(ret),))
        return ret

    def _get_sstable_files(self, node, ks, table):
        """
        Read sstable files directly from disk
        """
        keyspace_dir = os.path.join(node.get_path(), 'data', ks)

        ret = []
        for ext in ('*.db', '*.txt', '*.adler32', '*.crc32'):
            ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', ext)))

        return sorted(ret)

    def _get_sstable_transaction_logs(self, node, ks, table):
        keyspace_dir = os.path.join(node.get_path(), 'data', ks)
        ret = glob.glob(os.path.join(keyspace_dir, table + '-*', "*.log"))

        return sorted(ret)
