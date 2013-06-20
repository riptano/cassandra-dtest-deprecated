class ConfigLoader:

    # 
    # Standard init function that might enable Debug
    #
    def __init__(self, DEBUG=False):
        self.__DEBUG = DEBUG

    def enableDebug(self):
        self.__DEBUG = True

    def disableDebug(self):
        self.__DEBUG = False


    #
    # Load configuration distionary with config file and args key - value entries.
    # Modify this based on the "rules" specified by the arguments passed
    #
    def load_config_dist(self):

        from testconfig import config

        if self.__DEBUG:
            print 'load_config_dist() :: load original config dictionary BEFORE modifying it'
            self.__print_conf_dist(config)

        print '\n########################################################\n' if self.__DEBUG else None
        log_class = None
        log_level = None
        tests_list = None
        if (config.has_key('dtest')):

            # 
            # Process keys: dtest.class and dtest.log_level. There are generic rules
            #   a) if dtest.class is not set, rootLogger is considered
            #   b) if dtest.log_level is specified, but dtest.class is not - log level is applied to rootLogger
            #
            
            if (config['dtest'].has_key('class')):
                log_class = config['dtest']['class']
            else:
                log_class = 'rootLogger'

            if (config['dtest'].has_key('log_level')):
                log_level = config['dtest']['log_level']

            # 
            # Process dtest.tests_to_log. It should be a list of the tests, that overwrite the information 
            # in config, eluminate DEFAULT section of config as well as other test cases not specified in 
            # dtest.tests_to_log., and makes appropriate changes to the other section of test cases
            #
            if (config['dtest'].has_key('tests_to_log')):
                tests_list = (config['dtest']['tests_to_log']).split(",")

            # Iterate given config over the list of the tests specified in dtest.tests_to_log
            for config_test_case_key in config.keys():
                print 'config_test_case_key => ' + config_test_case_key if self.__DEBUG else None 
                found_test_case = False
                for arg_test_case in tests_list:
                    print '\targ_test_case => ' + arg_test_case if self.__DEBUG else None
                    arg_test_case = arg_test_case.strip()
                    if config_test_case_key == arg_test_case:
                        found_test_case = True
                        if (log_class != None and log_level != None):
                            if self.__DEBUG:
                                print 'Processing ' + config_test_case_key + ' and setting [' + log_class + '] = ' + log_level
                            config[config_test_case_key][log_class] = log_level
                if False == found_test_case:
                    del config[config_test_case_key]
                    print '\t\tdeleting: ' + config_test_case_key if self.__DEBUG else None
        
        else:
            print 'Do nothing. No command line arguments passed. Do standard nose execution' if self.__DEBUG else None
            self.__usage() 


        if self.__DEBUG:
            print 'load_config_dist() :: load original config dictionary AFTER modifying it'
            self.__print_conf_dist(config)

        return config

        
    #
    # Print the content of config dictionary
    #
    def __print_conf_dist(self, dict):
        if self.__DEBUG:
            print '#################################################################'
            print '######################### MAP ###################################'
            for cases_key in dict.keys():
                print 'Processing key = ' + cases_key + ' ....'
                test_case_dist = dict[cases_key]
                for entry_key in test_case_dist.keys():
                    print '\t' + entry_key + ' => ' + test_case_dist[entry_key]
            print '#################################################################'


    def __usage(self):
        print '                                                                                                                     '
        print ' Describe the way of how Cassandra cluster can be configured to enable specific logger levels for the given class,   '
        print '    classes, or package. Configuration may be done on per nosetest basis or fo all tests, similar for all tests, or  '
        print '    different set for each tests, as well as certain overwrite options are provided                                  '
        print '                                                                                                                     '  
        print ' Usage: [options]                                                                                                    ' 
        print '    --tc-file <Cassandra DTest Config> - specify configuration file. Default format is JSON                          '
        print '    --tc-format json - specify configuration file format. Default format is JSON                                     '
        print '    --tc=dtest.tests_to_log:"<List of test cases>" - specify list of nose test cases to apply logging.               '
        print '          If provided and configuration file is specified - overwrites list of the test cases in config file.        ' 
        print '          If provided and no configuration file specified - specify the list of test cases to apply logging          ' 
        print '          If it is not provided -  logging is applied to all test cases                                              '
        print '    --tc=dtest.log_level:<LEVEL> - specify log level                                                                 '
        print '          if --tc=dtest.tests_to_log is specified - it is applied to the tests mentioned there                       '
        print '          if --tc=dtest.class is not specified - it is applied to rootLogger                                         '
        print '    --tc=dtest.class:<CLASS> - class to log or rootLogger. Must have to specify --tc=dtest.log_level                 '
        print '          if is not specified or specified to rootLogger - set --tc=dtest.log_level to rootLogger                    '
        print '                                                                                                                     '
        print ' Examples:                                                                                                           '   
        print '    --tc-file config.ini --tc-format ini --tc=dtest.tests_to_log:"update_test, delete_test"                          ' 
        print '    --tc=dtest.log_level:DEBUG --tc=dtest.class:com.myclass  --tc=dtest.tests_to_log:"update_test"                   '
        print '                                                                                                                     '
   
