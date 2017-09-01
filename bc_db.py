'''
Manage cassandra db init and upgrades
'''
import os
import sys
import logging

from cassandra.cluster import Cluster
import click

DB_SCHEMA_VERSION = 1

@click.group()
def run():
    pass

@run.command()
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
def init(host, cluster):
    '''
    Merge events in larger window, send message to rabbitmq
    '''
    if len(host) == 0:
        host_list = ['127.0.0.1']
    else:
        host_list = list(host)

    if 'CASSANDRA_HOST' in os.environ:
        host_list = [os.environ['CASSANDRA_HOST']]
    if 'CASSANDRA_CLUSTER' in os.environ:
        cluster = os.environ['CASSANDRA_CLUSTER']

    session = None
    cassandra_cluster = None
    try:
        cassandra_cluster = Cluster(host_list)
        session = cassandra_cluster.connect()
        # create cluster if necessary
        session.execute('''CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};''' % (cluster))
        # connect to cluster
        session = cassandra_cluster.connect(cluster)
        session.default_timeout = 30.0
    except Exception as e:
        logging.exception("Cassandra connection error: " + str(e))
        sys.exit(1)

    rows = None
    try:
        rows = session.execute('''SELECT * FROM dbschema''')
    except Exception:
        pass
    version = None
    if rows:
        version = rows[0].version
    if version:
        logging.error("DB already created")
        sys.exit(1)


    session.execute('''CREATE TABLE IF NOT EXISTS dbschema(id int, version int, PRIMARY KEY(id))''')
    session.execute('''CREATE TABLE IF NOT EXISTS api(id varchar, owner varchar, PRIMARY KEY(id))''')
    session.execute('''CREATE TABLE IF NOT EXISTS retention(container varchar, id int, ts timestamp, PRIMARY KEY((container,id)))''')

    session.execute('''CREATE TABLE container(id varchar, status int, primary key(id))''')
    session.execute('''CREATE INDEX on container(status)''')

    session.execute('''CREATE TABLE IF NOT EXISTS io (container varchar, io_in counter, io_out counter, ts timestamp, proc_id int, file_name varchar, PRIMARY KEY (container, proc_id, ts, file_name))''')
    session.execute('''CREATE TABLE IF NOT EXISTS io_all (container varchar, io_in counter, io_out counter, proc_id int, file_name varchar, PRIMARY KEY (container, proc_id, file_name))''')
    session.execute('''CREATE TABLE IF NOT EXISTS mem (container varchar,vm_size bigint, vm_rss bigint, vm_swap bigint, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts))''')
    session.execute('''CREATE TABLE IF NOT EXISTS cpu (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu))''')
    session.execute('''CREATE TABLE IF NOT EXISTS cpu_all (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts))''')
    session.execute('''CREATE TABLE IF NOT EXISTS proc (container varchar, start timestamp, end timestamp, proc_name varchar, exe varchar, args varchar, proc_id int, parent_id int, is_root int, PRIMARY KEY (container, proc_id))''')
    session.execute('''CREATE TABLE IF NOT EXISTS proc_cpu (container varchar, cpu counter, proc_id int, PRIMARY KEY (container, proc_id))''')
    for r in['m', 'h', 'd']:
        session.execute('''CREATE TABLE IF NOT EXISTS cpu_per_''' + r + ''' (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu))''')
        session.execute('''CREATE TABLE IF NOT EXISTS cpu_all_per_''' + r + ''' (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts))''')
        session.execute('''CREATE TABLE IF NOT EXISTS mem_per_''' + r + ''' (container varchar,vm_size bigint, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts))''')
    session.execute('''UPDATE dbschema SET version=1 WHERE id=1''')

    if cassandra_cluster:
        cassandra_cluster.shutdown()

@run.command()
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
def upgrade(host, cluster):
    if len(host) == 0:
        host_list = ['127.0.0.1']
    else:
        host_list = list(host)

    if 'CASSANDRA_HOST' in os.environ:
        host_list = [os.environ['CASSANDRA_HOST']]
    if 'CASSANDRA_CLUSTER' in os.environ:
        cluster = os.environ['CASSANDRA_CLUSTER']

    session = None
    cassandra_cluster = None
    try:
        cassandra_cluster = Cluster(host_list)
        session = cassandra_cluster.connect(cluster)
        session.default_timeout = 30.0
    except Exception as e:
        logging.exception("Cassandra connection error: " + str(e))
        sys.exit(1)
    rows = session.execute('''SELECT * FROM dbschema''')
    version = None
    if rows:
        version = rows[0].version
    logging.info("Schema version: %s" % [version])
    logging.warn("Schema up-to-date, nothing to do")

    if cassandra_cluster:
        cassandra_cluster.shutdown()


if __name__ == '__main__':
    run()
