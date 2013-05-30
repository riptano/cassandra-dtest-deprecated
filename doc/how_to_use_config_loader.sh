#! /bin/bash
set -x

nosetests -s -x ./nose_test_with_config_loader.py --tc-file cassandra_dtest_config.json --tc-format json --tc=dtest.tests_to_log:"test_update, test_delete" --tc=dtest.log_level:TRACE --tc=dtest.class:com.datastax.qa.dtest.sample.class
