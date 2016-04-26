# About this plugin:
#   This plugin uses collectd's Python plugin to record PuppetDB informations.
#
# Authors:
#   Laurent Apollis
#
# collectd:
#   http://collectd.org
# PuppetDB:
#   http://docs.puppetlabs.com/puppetdb/
# collectd-python:
#   http://collectd.org/documentation/manpages/collectd-python.5.shtml


import collectd

from pypuppetdb import connect

# Host to connect to. Override in config by specifying 'Host'.
PUPPETDB_HOST    = 'localhost'
# Port to connect to. Override in config by specifying 'Port'.
PUPPETDB_PORT    = 8080
# Use ssl. Override in config by specifying 'SSL_VERIFY'.
PUPPETDB_SSL     = None
# Key used to connect to ('/path/to/private.pem'). Override in config by specifying 'Key'.
PUPPETDB_KEY     = None
# CERT used to connect to ('/path/to/public.pem'). Override in config by specifying 'CERT'.
PUPPETDB_CERT    = None
#Connect timeout. Override in config by specifying 'Timeout'.
PUPPETDB_TIMEOUT = 20
#Time to consider unreported nodes. Override in config by specifying 'UnreportTime'.
UNREPORTED_TIME = 25
# Verbose logging on/off. Override in config by specifying 'Verbose'.
VERBOSE_LOGGING  = False

def dispatch_value(value, key, type, type_instance=None):
    if not type_instance:
        type_instance = key

    log_verbose('Sending value: %s=%s' % (type_instance, value))

    val = collectd.Values(plugin='puppetdb')
    val.type = type
    val.type_instance = type_instance
    val.values = [value]
    val.dispatch()

def read_callback():
    puppetdb = connect(
        api_version= 3,
        host=PUPPETDB_HOST,
        port=PUPPETDB_PORT,
        ssl_verify=PUPPETDB_SSL,
        ssl_key=PUPPETDB_KEY,
        ssl_cert=PUPPETDB_CERT,
        timeout=PUPPETDB_TIMEOUT,
    )

    prefix = 'com.puppetlabs.puppetdb.query.population'
    num_nodes = puppetdb.metric(
        "{0}{1}".format(prefix, ':type=default,name=num-nodes'))
    num_resources = puppetdb.metric(
        "{0}{1}".format(prefix, ':type=default,name=num-resources'))
    avg_resources_node = puppetdb.metric(
        "{0}{1}".format(prefix, ':type=default,name=avg-resources-per-node'))

    # Ftech nodes
    nodes = puppetdb.nodes(
        unreported=UNREPORTED_TIME,
        with_status=True)

    #Init stats
    stats = {
        'changed': 0,
        'unchanged': 0,
        'failed': 0,
        'unreported': 0,
        'noop': 0
        }

    for node in nodes:
        if node.status == 'unreported':
            stats['unreported'] += 1
        elif node.status == 'changed':
            stats['changed'] += 1
        elif node.status == 'failed':
            stats['failed'] += 1
        elif node.status == 'noop':
            stats['noop'] += 1
        else:
            stats['unchanged'] += 1

    log_verbose('population: %s\n' % num_nodes['Value'])
    dispatch_value(num_nodes['Value'], 'population','gauge')

    log_verbose('unreported: %s\n' % stats['unreported'])
    dispatch_value(stats['unreported'], 'unreported','gauge')
    
    log_verbose('changed: %s\n' % stats['changed'])
    dispatch_value(stats['changed'], 'changed','gauge')
    
    log_verbose('failed: %s\n' % stats['failed'])
    dispatch_value(stats['failed'], 'failed','gauge')
    
    log_verbose('noop: %s\n' % stats['noop'])
    dispatch_value(stats['noop'], 'noop','gauge')
    
    log_verbose('unchanged: %s\n' % stats['unchanged'])
    dispatch_value(stats['unchanged'], 'unchanged','gauge')


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('puppetdb plugin [verbose]: %s' % msg)

def configure_callback(conf):
    """Receive configuration block"""
    global PUPPETDB_HOST, PUPPETDB_PORT, PUPPETDB_SSL, PUPPETDB_KEY, PUPPETDB_CERT, PUPPETDB_TIMEOUT, UNREPORTED_TIME, VERBOSE_LOGGING
    for node in conf.children:
        if node.key == 'Host':
            PUPPETDB_HOST = node.values[0]
        elif node.key == 'Port':
            PUPPETDB_PORT = int(node.values[0])
        elif node.key == 'SSL_VERIFY':
            PUPPETDB_SSL = node.values[0]
        elif node.key == 'Key':
            PUPPETDB_KEY = node.values[0]
        elif node.key == 'CERT':
            PUPPETDB_CERT = node.values[0]
        elif node.key == 'Timeout':
            PUPPETDB_TIMEOUT = int(node.values[0])
        elif node.key == 'UnreportTime':
            UNREPORTED_TIME = int(node.values[0])
        elif node.key == 'Verbose':
            VERBOSE_LOGGING = bool(node.values[0])
        else:
            collectd.warning('puppetdb plugin: Unknown config key: %s.'
                             % node.key)
    log_verbose('Configured with host=%s, port=%s, ssl=%s, key=%s, cert=%s, timeout=%s' % (PUPPETDB_HOST, PUPPETDB_PORT, PUPPETDB_SSL, PUPPETDB_KEY, PUPPETDB_CERT, PUPPETDB_TIMEOUT))


collectd.register_config(configure_callback)
collectd.register_read(read_callback)