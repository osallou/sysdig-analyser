import os
import json
import logging
import sys
import uuid
import re
import copy
import datetime
import time

from bson import json_util

import redis
import pika
from cassandra.cluster import Cluster
from progressbar import Percentage, ProgressBar, Bar
import click

@click.group()
def run():
    pass


class RetentionHandler(object):

    def __init__(self, cassandra_session):
        self.session = cassandra_session
        redis_host = 'localhost'
        redis_port = 6379
        if 'REDIS_HOST' in os.environ:
                redis_host = os.environ['REDIS_HOST']
        if 'REDIS_PORT' in os.environ:
                redis_host = int(os.environ['REDIS_PORT'])
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)

    def __get_retention_interval(self, retention):
        '''
        Return timestamp (ms) interval before which events should be deleted
        '''
        up_to = None
        retention_seconds = None
        if retention == 's':
            # 1h
            retention_seconds = 3600
            if 'BC_RETENTION_SECONDS' in os.environ:
                retention_seconds = os.environ['BC_RETENTION_SECONDS']
        elif retention == 'h':
            # 2d
            retention_seconds = 3600 * 24 * 2
            if 'BC_RETENTION_MINUTES' in os.environ:
                retention_seconds = os.environ['BC_RETENTION_MINUTES']
        elif retention == 'm':
            # 120d
            retention_seconds = 3600 * 24 * 120
            if 'BC_RETENTION_HOURS' in os.environ:
                retention_seconds = os.environ['BC_RETENTION_HOURS']
        if retention_seconds is None:
            return None
        up_to = datetime.datetime.now() - datetime.timedelta(seconds=retention_seconds)
        return int(time.mktime(up_to.timetuple())*1000)

    def __get_cont_proc(self, container):
        '''
        Get proc ids with last write time for container
        '''
        contproc = {}
        rows = self.session.execute("SELECT container, proc_id FROM proc_cpu WHERE container='" + container + "'")
        last_w = None
        for row in rows:
            contproc[row.container+':'+str(row.proc_id)] = 1
        return contproc

    def __cassandra_delete(self, table, container, proc_id, up_to=None):
        if up_to is None:
            return;
        logging.debug("DELETE FROM %s container=%s, proc_id=%s, ts<%s" % (table, container, str(proc_id), str(up_to)))
        self.session.execute(
        """
        DELETE FROM """ + table + """ WHERE container=%s AND proc_id=%s AND ts<%s;
        """,
        (container, proc_id, up_to)
        )

    def __cassandra_delete_cpu_all(self, container):
        contproc = self.__get_cont_proc(container)
        for table in ['cpu_all', 'cpu_all_per_m', 'cpu_all_per_h']:
            up_to = None
            if table == 'cpu_all':
                up_to = self.__get_retention_interval('s')
            elif table ==  'cpu_all_per_m':
                up_to = self.__get_retention_interval('m')
            elif table ==  'cpu_all_per_h':
                up_to = self.__get_retention_interval('h')
            if up_to is None:
                logging.debug('nothing in cpu_all to delete for %s' % (container))
                return
            for elt in list(contproc.keys()):
                (container, proc_id) = elt.split(':')
                proc_id = int(proc_id)
                self.__cassandra_delete(table, container, proc_id, up_to)

    def __cassandra_delete_cpu(self, container):
        contproc = self.__get_cont_proc(container)
        for table in ['cpu', 'cpu_per_m', 'cpu_per_h']:
            up_to = None
            if table == 'cpu':
                up_to = self.__get_retention_interval('s')
            elif table ==  'cpu_per_m':
                up_to = self.__get_retention_interval('m')
            elif table ==  'cpu_per_h':
                up_to = self.__get_retention_interval('h')
            if up_to is None:
                logging.debug('nothing to in cpu delete for %s' % (container))
            for elt in list(contproc.keys()):
                (container, proc_id) = elt.split(':')
                proc_id = int(proc_id)
                self.__cassandra_delete(table, container, proc_id, up_to)

    def __cassandra_delete_mem(self, container):
        contproc = self.__get_cont_proc(container)
        for table in ['mem', 'mem_per_m', 'mem_per_h']:
            up_to = None
            if table == 'mem':
                up_to = self.__get_retention_interval('s')
            elif table ==  'mem_per_m':
                up_to = self.__get_retention_interval('m')
            elif table ==  'mem_per_h':
                up_to = self.__get_retention_interval('h')
            if up_to is None:
                logging.debug('nothing to delete in mem for %s' % (container))
            for elt in list(contproc.keys()):
                (container, proc_id) = elt.split(':')
                proc_id = int(proc_id)
                self.__cassandra_delete(table, container, proc_id, up_to)

    def __delete_old(self, container):
        self.__cassandra_delete_cpu(container)
        self.__cassandra_delete_cpu_all(container)
        self.__cassandra_delete_mem(container)

    def callback_clean(self, ch, method, properties, body):
        try:
            rt = json.loads(body)
            content = rt['event']
            logging.debug('Message: %s' % (content))
            self.__delete_old(content['container'])
        except Exception as e:
            logging.exception("Failed to handle retention query: " + str(e))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)


@run.command()
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
@click.option('--rabbit', help="rabbitmq host")
@click.option('--debug', help="set log level to debug", is_flag=True)
def listen(host, cluster, rabbit, debug):
    '''
    For rabbitmq credentials MUST use env variables RABBITMQ_USER and RABBITMQ_PASSWORD
    '''
    if len(host) == 0:
        host_list = ['127.0.0.1']
    else:
        host_list = list(host)

    if debug:
        logging.basicConfig(level=logging.DEBUG)

    if 'CASSANDRA_HOST' in os.environ:
        host_list = [os.environ['CASSANDRA_HOST']]
    if 'CASSANDRA_CLUSTER' in os.environ:
        cluster = os.environ['CASSANDRA_CLUSTER']
    if 'RABBITMQ_HOST' in os.environ:
        rabbit = os.environ['RABBITMQ_HOST']

    rabbitmq_user = None
    rabbitmq_password = None
    if 'RABBITMQ_USER' in os.environ and 'RABBITMQ_PASSWORD' in os.environ:
            rabbitmq_user = os.environ['RABBITMQ_USER']
            rabbitmq_password = os.environ['RABBITMQ_PASSWORD']

    try:
        cassandra_cluster = Cluster(host_list)
        session = cassandra_cluster.connect(cluster)
        session.default_timeout = 30.0
    except Exception as e:
        logging.error("Cassandra connection error: " + str(e))
        sys.exit(1)

    rtHandler = RetentionHandler(session)

    connection = None
    if rabbitmq_user:
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, credentials=credentials, heartbeat_interval=0))
    else:
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, heartbeat_interval=0))
    channel = connection.channel()
    channel.queue_declare(queue='bc_clean', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
            rtHandler.callback_clean,
            queue='bc_clean')
    channel.start_consuming()


if __name__ == '__main__':
    run()
