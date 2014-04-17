from dtest import Tester, debug
from tools import replace_in_file
import tempfile
import shutil
import glob
import os
import time

class SnapshotTester(Tester):
    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def insert_rows(self, cursor, start, end):
        for r in range(start, end):
            cursor.execute("INSERT INTO ks.cf (key, val) VALUES ({r}, 'asdf');".format(r=r))

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(**locals())
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir,ks))
        os.mkdir(os.path.join(tmpdir,ks,cf))
        node_dir = node.get_path()
        
        # Find the snapshot dir, it's different in various C* versions:
        snapshot_dir = "{node_dir}/data/{ks}/{cf}/snapshots/{name}".format(**locals())
        if not os.path.isdir(snapshot_dir):
            snapshot_dir = glob.glob("{node_dir}/flush/{ks}/{cf}-*/snapshots/{name}".format(**locals()))[0]
        debug("snapshot_dir is : " + snapshot_dir)
        debug("snapshot copy is : " + tmpdir)

        os.system('cp -a {snapshot_dir}/* {tmpdir}/{ks}/{cf}/'.format(**locals()))
        return tmpdir

    def restore_snapshot(self, snapshot_dir, node, ks, cf):
        debug("Restoring snapshot....")
        node_dir = node.get_path()
        snapshot_dir = os.path.join(snapshot_dir, ks, cf)
        ip = node.address()
        os.system('{node_dir}/bin/sstableloader -d {ip} {snapshot_dir}'.format(**locals()))

class TestSnapshot(SnapshotTester):

    def __init__(self, *args, **kwargs):
        SnapshotTester.__init__(self, *args, **kwargs)

    def test_basic_snapshot_and_restore(self):
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')

        self.insert_rows(cursor, 0, 100)
        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(cursor, 100, 200)
        cursor.execute('SELECT count(*) from ks.cf')
        self.assertEqual(cursor.fetchone()[0], 200)

        # Drop the keyspace, make sure we have no data:
        cursor.execute('DROP KEYSPACE ks')
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')
        cursor.execute('SELECT count(*) from ks.cf')
        self.assertEqual(cursor.fetchone()[0], 0)

        # Restore data from snapshot:
        self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf')
        node1.nodetool('refresh ks cf')
        cursor.execute('SELECT count(*) from ks.cf')

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

        self.assertEqual(cursor.fetchone()[0], 100)

class TestArchiveCommitlog(SnapshotTester):
    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'commitlog_segment_size_in_mb':1}
        SnapshotTester.__init__(self, *args, **kwargs)

    def test_archive_commitlog(self):
        self.run_archive_commitlog(restore_point_in_time = False)

    def test_archive_commitlog_with_active_commitlog(self):
        """Copy the active commitlogs to the archive directory before restoration"""
        self.run_archive_commitlog(restore_point_in_time = False, archive_active_commitlogs=True)

    def dont_test_archive_commitlog(self):
        """Run the archive commitlog test, but forget to add the restore commands:"""
        self.run_archive_commitlog(restore_point_in_time = False, restore_archived_commitlog=False)

    def test_archive_commitlog_point_in_time(self):
        """Test archive commit log with restore_point_in_time setting"""
        self.run_archive_commitlog(restore_point_in_time = True)

    def test_archive_commitlog_point_in_time_with_active_commitlog(self):
        """Test archive commit log with restore_point_in_time setting"""
        self.run_archive_commitlog(restore_point_in_time = True, archive_active_commitlogs=True)

    def run_archive_commitlog(self, restore_point_in_time=False, restore_archived_commitlog=True, archive_active_commitlogs=False):
        """Run archive commit log restoration test"""

        cluster = self.cluster
        cluster.populate(1)
        (node1,) = cluster.nodelist()

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = tempfile.mkdtemp()
        debug("tmp_commitlog: " + tmp_commitlog)

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(),'conf','commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command=/bin/cp %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog))])

        cluster.start()

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        debug("Writing first 30,000 rows...")
        self.insert_rows(cursor, 0, 30000)
        # Record when this first set of inserts finished:
        insert_cutoff_times = [time.gmtime()]

        # Delete all commitlog backups so far:
        os.system('rm {tmp_commitlog}/*'.format(tmp_commitlog=tmp_commitlog))

        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # Write more data:
        debug("Writing second 30,000 rows...")
        self.insert_rows(cursor, 30000, 60000)
        node1.flush()
        time.sleep(10)
        # Record when this second set of inserts finished:
        insert_cutoff_times.append(time.gmtime())
        
        debug("Writing final 5,000 rows...")
        self.insert_rows(cursor,60000, 65000)
        # Record when the third set of inserts finished:
        insert_cutoff_times.append(time.gmtime())

        cursor.execute('SELECT count(*) from ks.cf')
        # Make sure we have the same amount of rows as when we snapshotted:
        self.assertEqual(cursor.fetchone()[0], 65000)

        # Check that there are at least one commit log backed up that
        # is not one of the active commit logs:
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        debug("node1 commitlog dir: " + commitlog_dir)
        self.assertTrue(len(set(os.listdir(tmp_commitlog)) - set(os.listdir(commitlog_dir))) > 0)

        cluster.flush()
        cluster.compact()
        node1.drain()
        if archive_active_commitlogs:
            # Copy the active commitlogs to the backup directory:
            for f in glob.glob(commitlog_dir+"/*"):
                shutil.copy2(f, tmp_commitlog)

        # Destroy the cluster
        cluster.stop()
        self.copy_logs(name=self.id().split(".")[0]+"_pre-restore")
        cluster = self.cluster = self._get_cluster()
        cluster.populate(1)
        (node1,) = cluster.nodelist()
        cluster.start()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')

        # Restore from snapshot:
        self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf')
        cursor.execute('SELECT count(*) from ks.cf')
        # Make sure we have the same amount of rows as when we snapshotted:
        self.assertEqual(cursor.fetchone()[0], 30000)
        
        # Edit commitlog_archiving.properties. Remove the archive
        # command  and set a restore command and restore_directories:
        if restore_archived_commitlog:
            replace_in_file(os.path.join(node1.get_path(),'conf','commitlog_archiving.properties'),
                            [(r'^archive_command=.*$', 'archive_command='),
                             (r'^restore_command=.*$', 'restore_command=cp -f %from %to'),
                             (r'^restore_directories=.*$', 'restore_directories={tmp_commitlog}'.format(
                                 tmp_commitlog=tmp_commitlog))])
            
            if restore_point_in_time:
                restore_time = time.strftime("%Y:%m:%d %H:%M:%S", insert_cutoff_times[1])
                replace_in_file(os.path.join(node1.get_path(),'conf','commitlog_archiving.properties'),
                                [(r'^restore_point_in_time=.*$', 'restore_point_in_time={restore_time}'.format(**locals()))])
        
        debug("Restarting node1..")
        node1.stop()
        node1.start()

        node1.nodetool('flush')
        node1.nodetool('compact')

        cursor = self.patient_cql_connection(node1).cursor()
        cursor.execute('SELECT count(*) from ks.cf')

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)
        debug("removing tmp_commitlog: " + tmp_commitlog)
        shutil.rmtree(tmp_commitlog)

        # Now we should have 30000 rows from the snapshot + 30000 rows
        # from the commitlog backups:
        if not restore_archived_commitlog:
            self.assertEqual(cursor.fetchone()[0], 30000)
        elif restore_point_in_time:
            self.assertEqual(cursor.fetchone()[0], 60000)
        else:
            self.assertEqual(cursor.fetchone()[0], 65000)
