import click
import os
import logging
import string
import random

from cassandra.cluster import Cluster

def __cassandra_update_api(session, key, owner):
        session.execute(
        """
        UPDATE api
        SET owner = %s
        WHERE id=%s
        """,
        (owner, key)
        )

def __cassandra_delete_api(session, key):
        session.execute(
        """
        DELETE FROM api
        WHERE id=%s
        """,
        [key]
        )

@click.group()
def run():
    pass

@run.command()
@click.option('--api', help='API key')
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
def delete(api, host, cluster):
    session =  None
    if not api:
        print("Api key missing")
        sys.exit(1)

    if len(host) == 0:
        host_list = ['127.0.0.1']
    else:
        host_list = list(host)

    try:
        cassandra_cluster = Cluster(host_list)
        session = cassandra_cluster.connect(cluster)
        session.default_timeout = 30.0
    except Exception as e:
        logging.error("Cassandra connection error: " + str(e))
        sys.exit(1)
    __cassandra_delete_api(session ,api)

@run.command()
@click.option('--owner', help='API key user owner')
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
def create(owner, host, cluster):
    session = None
    if not owner:
        print("Owner missing")
        sys.exit(1)

    if len(host) == 0:
        host_list = ['127.0.0.1']
    else:
        host_list = list(host)

    try:
        cassandra_cluster = Cluster(host_list)
        session = cassandra_cluster.connect(cluster)
        session.default_timeout = 30.0
    except Exception as e:
        logging.error("Cassandra connection error: " + str(e))
        sys.exit(1)
    # generate api key
    key = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
    __cassandra_update_api(session, key, owner)
    print("Api key: %s" % (key))
    return key


if __name__ == '__main__':
    run()
