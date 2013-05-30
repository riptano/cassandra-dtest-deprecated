from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import random
import os

from config_loader import ConfigLoader

from tools import ThriftConnection

versions = (
    '1.1.9', 'git:cassandra-1.2'
)

class ConfigTest(Tester):

    def __init__(self, *args, **kwargs):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = versions[0]
        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner':'org.apache.cassandra.dht.RandomPartitioner'}
        Tester.__init__(self, *args, **kwargs)

    def upgrade_test(self):
        print 'ConfigTest :: upgrade_test()'
        config_loader = ConfigLoader() ## setup to True/ False if you want to enable / disable debug
        config_loader.enableDebug()
        config_loader.load_config_dist()

    def upgrade_test_mixed(self):
        print 'ConfigTest :: upgrade_test_mixed()'
                         
                
