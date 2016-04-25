import os

#import socket
from socket import gethostname, gethostbyaddr, gaierror

from ConfigParser import SafeConfigParser

MANDATORY = 'm'

supported_stations = ['kumpula', 'helsinki', 'norrkoping', 'nkp']

valid_config_variables = [
    'aapp_prefix',
    'aapp_workdir',
    'aapp_run_noaa_script',
    'aapp_run_metop_script',
    'tle_indir',
    'tle_outdir',
    'tle_script',
    'alleph_script',
    'use_dyn_work_dir',
    'subscribe_topics',
    'publish_pps_format',
    'publish_l1_format',
    'pps_out_dir',
    'metop_data_out_dir',
    'noaa_data_out_dir',
    'aapp_log_files_dir',
    'aapp_log_files_backup',
    'servername',
    'dataserver',
    'locktime_before_rerun',
    'passlength_threshold'
]

#
# Variables that are directories
# ('variable_name', 'permission: r, rw', 'depends on variable_name')
#
valid_dir_permissions = [
    ('noaa_data_out_dir', 'rw', 'publish_l1_format'),
    ('metop_data_out_dir', 'rw', 'publish_l1_format'),
    ('pps_out_dir', 'rw', 'publish_pps_format'),
    ('aapp_prefix', 'r', MANDATORY),
    ('aapp_workdir', 'rw', MANDATORY),
    ('tle_indir', 'r', MANDATORY),
    ('tle_outdir', 'rw', MANDATORY),
    ('aapp_log_files_dir', 'rw', MANDATORY)
]

valid_readable_files = ['aapp_run_noaa_script',
                        'aapp_run_metop_script',
                        'alleph_script',
                        'tle_script']

valid_servers = [
    ('servername', 'host'),
    ('dataserver', 'server')
]

# Config variable will be replaced by following config variable
# if the variable (first one) is empty in config file

optional_config = {'dataserver': 'servername'}

VALID_CONFIGURATION = {
    'supported_stations': supported_stations,
    'valid_config_variables': valid_config_variables,
    'valid_dir_permissions': valid_dir_permissions,
    'valid_readable_files': valid_readable_files,
    'valid_servers': valid_servers,
    'optional_config': optional_config
}


def check_station(config, valid_stations):
    """
    Check if station can be used without modifications.
    """
    if config['station'] in valid_stations:
        return True
    return False


def check_hostserver(host):
    """
    Check the host servername
    """
    current_host = gethostname()
    if current_host == host:
        return True
    return False


def check_dataserver(server):
    """
    Check the dataserver by address
    """
    try:
        name, dummy, addresslist = gethostbyaddr(server)
        if server == name or server == addresslist[0]:
            return True
    except gaierror:
        return False
    return False


def check_bool(value):
    """
    Check if value is boolean
    """
    return type(value) is bool


def check_dir(directory, test):
    """ First check if directory exists and has access
    Second test if directory is writable
    Print error message if fails
    """
#    print "check_dir: test is ", test
    if test == 'r' or test == 'rw':
        if not (os.path.exists(directory) or
                os.access(directory, os.R_OK)):
            print ("ERROR: Directory doesn't exist or " +
                   "it is not readable!:" +
                   directory)
            return False
        if test == 'rw':
            # print "RW test"
            test_file = "tmp_write_test.tmp"
            filename = os.path.join(directory, test_file)
            try:
                test = open(filename, "w")
                test.close()
                os.remove(filename)
            except IOError:
                print ("ERROR: Cannot write to directory! " +
                       directory)
                return False
    else:
        print "ERROR: Unknown test."
    return True


def check_dir_permissions(config, dir_permissions):
    """
    Check if directories are as defined in dir_permissions[]
    """

    test_results = []

    for dirname, perm, required in dir_permissions:
        #        print dirname, config[dirname]

        if required == MANDATORY:
            check = check_dir(config[dirname], perm)
        else:
            check = True
            if config[required]:
                check = check_dir(config[dirname], perm)

            # else:
            #     print "%s %s %s %s %s" % ("ERROR: ", dirname,
            #                               "requires", required,
            #                               "but it's NOT defined!")
            #     check = False
        test_results.append(check)

    if all(test_results):
        return True
    else:
        print "Number of failures: ", len(test_results) - sum(test_results)
        return False


def check_file(filename):
    '''Checking is file exisiting and readable'''
    return os.path.isfile(filename) and os.access(filename, os.R_OK)


def check_readable_files(config, files_to_check):
    """
    Check files_to_check[] are readable
    """
#    print "------------------------------"
    test_results = []
    for filename in files_to_check:
        check = check_file(config[filename])
        test_results.append(check)
#        print filename, " is ", check

    if all(test_results):
        return True
    else:
        print "Number of failures: ", len(test_results) - sum(test_results)
        return False


def check_config_file_options(config, valid_config=None):
    """
    Check input config dictionary
    """

    dir_permissions = valid_config['valid_dir_permissions']
    readable_files = valid_config['valid_readable_files']
    servers = valid_config['valid_servers']

    print "Checking directories..."
    if not check_dir_permissions(config, dir_permissions):
        print "Checking directories failed."
        return False

    print "Checking files..."
    if not check_readable_files(config, readable_files):
        return False

    if servers:
        return True
        # print "Checking servers..."
        # for server, server_type in servers:
        #     #           print "SERVERS:", server, server_type
        #  #           print "Check:", config[server]
        #     if config[server] and server_type == 'host':
        #         if not check_hostserver(config[server]):
        #             print "Unknown host server: ", config[server]
        #             return False
        #     if config[server] and server_type == 'server':
        #         if not check_dataserver(config[server]):
        #             print "Unknown server: ", config[server]
        #             return False

    return True


def read_config_file_options(filename, station, env, valid_config=None):
    """
    Read and checks config file
    If ok, return configuration dictionary
    """
    config = SafeConfigParser()

    if valid_config == None:
        valid_config = VALID_CONFIGURATION

    # Config variable will be replaced by following config
    optional_config_variables = valid_config['optional_config']
    mandatory_config_variables = valid_config['valid_config_variables']

    configuration = {}
    configuration['station'] = station
    configuration['environment'] = env
    config.read(filename)
    try:
        config_opts = dict(config.items(env, raw=False))
    except Exception as err:
        print " Section %s %s" % (env,
                                  "is not defined in your" +
                                  "aapp_runner config file!")
        return None
    # Read config file
    for item in mandatory_config_variables:
        try:
            configuration[item] = config_opts[item]
#            print "Required variable: ", item
            if item in optional_config_variables and config_opts[item] == '':
                #                print "This will be replaced:", item
                new_item = optional_config_variables.get(item, item)
                print (item, "was not defined. Value from",
                       new_item, "will be used.")
                configuration[item] = config_opts[new_item]
        except KeyError as err:
         #           print "---------"
         #           if item in optional_config_variables:
         #               #print "This will be replaced:", item
         #               new_item = optional_config_variables.get(item, item)
         #               print (item, "was not defined. Value from",
         #                      new_item, "will be used.")
         #               configuration[item] = config_opts[new_item]
         #           else:
            print "%s %s %s" % (err.args,
                                "is missing."
                                "Please, check your config file",
                                filename)
            return None

    # Fix the list of subscribe topics:
    topics = configuration['subscribe_topics'].split(',')
    topiclist = []
    for topic in topics:
        topiclist.append(topic.strip(' '))
    configuration['subscribe_topics'] = topiclist

    # print "DATASERVER is", configuration['dataserver']
    if not check_config_file_options(configuration, valid_config):
        return None

    return configuration
