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
        self.channel = None

    def __get_retention_interval(self, retention):
        '''
        Return timestamp (ms) interval before which events should be deleted
        '''
        up_to = None
        retention_seconds = None
        if retention == 's':
            # 1h
            retention_seconds = 3600
        elif retention == 'h':
            # 2d
            retention_seconds = 3600 * 24 * 2
        elif retention == 'm':
            # 120d
            retention_seconds = 3600 * 24 * 120
        if retention_seconds is None:
            return None
        up_to = datetime.datetime.now() - datetime.timedelta(seconds=retention_seconds)
        return int(time.mktime(up_to.timetuple())*1000)

    def __cassandra_update_procs(self, event):
        '''
        Record process info
        '''
        if not event:
            return
        if 'is_root' not in event:
            event['is_root'] = 0

        self.session.execute(
            """
            UPDATE proc
            SET parent_id = %s,
                proc_name = %s,
                exe=%s,
                args=%s,
                is_root=%s
            WHERE proc_id=%s and container=%s
            """,
            (event['parent_id'], event['proc_name'], event['exe'], event['args'],  event['is_root'], event['proc'], event['container'])
        )

    def __cassandra_update_io(self, event):
        self.session.execute(
            """
            UPDATE io_all
            SET io_in = io_in + %s,
                io_out = io_out + %s
            WHERE proc_id=%s AND file_name=%s AND container=%s
            """,
            (event['in'], event['out'], event['proc'], event['name'], event['container'])
        )


    def __get_ts(self, event_date):
        return int(time.mktime(event_date.timetuple())*1000)

    def __cassandra_update_per_cpu(self, event):
        if not event:
            return
        start = datetime.datetime.fromtimestamp(event['start']/1000)
        start_m = start.replace(second=0)
        start_h = start_m.replace(minute=0)
        start_d = start_h.replace(hour=0)
        self.session.execute(
            """
            UPDATE cpu
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
            """,
            (event['duration'], self.__get_ts(start), int(event['proc']), event['cpu'], event['container'])
        )
        self.session.execute(
            """
            UPDATE cpu_per_m
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
            """,
            (event['duration'], self.__get_ts(start_m), int(event['proc']), event['cpu'], event['container'])
        )
        self.session.execute(
            """
            UPDATE cpu_per_h
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
            """,
            (event['duration'], self.__get_ts(start_h), int(event['proc']), event['cpu'], event['container'])
        )
        self.session.execute(
            """
            UPDATE cpu_per_d
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
            """,
            (event['duration'], self.__get_ts(start_d), int(event['proc']), event['cpu'], event['container'])
        )

    def __cassandra_update_cpu_all(self, event):
        if not event:
            return
        start = datetime.datetime.fromtimestamp(event['start']/1000)
        start_m = start.replace(second=0)
        start_h = start_m.replace(minute=0)
        start_d = start_h.replace(hour=0)
        self.session.execute(
            """
            UPDATE proc_cpu
            SET cpu = cpu + %s
            WHERE proc_id=%s and container=%s
            """,
            (event['duration'], int(event['proc']), event['container'])
        )

        self.session.execute(
            """
            UPDATE cpu_all
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s and container=%s
            """,
            (event['duration'], self.__get_ts(start), int(event['proc']), event['container'])
        )
        self.session.execute(
            """
            UPDATE cpu_all
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s and container=%s
            """,
            (event['duration'], self.__get_ts(start_m), int(event['proc']), event['container'])
        )
        self.session.execute(
            """
            UPDATE cpu_all
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s and container=%s
            """,
            (event['duration'], self.__get_ts(start_h), int(event['proc']), event['container'])
        )
        self.session.execute(
            """
            UPDATE cpu_all
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s and container=%s
            """,
            (event['duration'], self.__get_ts(start_d), int(event['proc']), event['container'])
        )

    def __cassandra_update_cpu(self, event):
        self.__cassandra_update_per_cpu(event)
        self.__cassandra_update_cpu_all(event)

    def __cassandra_update_mem(self, event):
        if not event:
            return
        start = datetime.datetime.fromtimestamp(event['start']/1000)
        start_m = start.replace(second=0)
        start_h = start_m.replace(minute=0)
        start_d = start_h.replace(hour=0)
        self.session.execute(
            """
            UPDATE mem
            SET vm_size = %s,
                vm_rss = %s,
                vm_swap = %s
            WHERE ts=%s AND proc_id=%s AND container=%s
            """,
            (event['vm_size'], event['vm_rss'], event['vm_swap'], self.__get_ts(start), event['proc'], event['container'])
        )
        self.session.execute(
            """
            UPDATE mem_per_m
            SET vm_size = %s,
                vm_rss = %s,
                vm_swap = %s
            WHERE ts=%s AND proc_id=%s AND container=%s
            """,
            (event['vm_size'], event['vm_rss'], event['vm_swap'], self.__get_ts(start_m), event['proc'], event['container'])
        )
        self.session.execute(
            """
            UPDATE mem_per_h
            SET vm_size = %s,
                vm_rss = %s,
                vm_swap = %s
            WHERE ts=%s AND proc_id=%s AND container=%s
            """,
            (event['vm_size'], event['vm_rss'], event['vm_swap'], self.__get_ts(start_h), event['proc'], event['container'])
        )
        self.session.execute(
            """
            UPDATE mem_per_d
            SET vm_size = %s,
                vm_rss = %s,
                vm_swap = %s
            WHERE ts=%s AND proc_id=%s AND container=%s
            """,
            (event['vm_size'], event['vm_rss'], event['vm_swap'], self.__get_ts(start_d), event['proc'], event['container'])
        )

    def __delete_old(self, container):
        logging.debug('Request cleanup of %s' % (container))
        now = datetime.datetime.now()
        self.redis_client.set('bc:' + container + 'last_delete', time.mktime(now.timetuple()))
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key='bc_clean',
                body=json.dumps({'event': {'container': container}}),
                properties=pika.BasicProperties(
                    # make message persistent
                    delivery_mode=2
                ))
        except Exception as e:
            logging.exception('Failed to send clean event: ' + str(e))

    def __cleanup(self, container):
        last_delete = self.redis_client.get('bc:' + container + 'last_delete')
        if last_delete is None:
            self.__delete_old(container)
        else:
            now = datetime.datetime.now()
            last_delete_date = datetime.datetime.fromtimestamp(float(last_delete))
            if last_delete_date < now - datetime.timedelta(seconds=60*10):
                self.__delete_old(container)

    def callback_record(self, ch, method, properties, body):
        try:
            rt = json.loads(body)
            content = rt['event']
            logging.debug('Message: %s' % (content))
            if content['evt_type'] == 'fd':
                for data in content['data']:
                    # event['in'], event['out'], event['proc'], event['name'], event['container']
                    event = {
                        'proc': int(data[0]),
                        'container': data[2],
                        'name': data[3],
                        'in': 0,
                        'out': 0,
                        'start':  long(content['ts'])/1000000
                    }
                    if data[4] == 'in':
                        event['in'] = data[5]
                    else:
                        event['out'] = data[5]
                    self.__cassandra_update_io(event)
                    self.__cleanup(event['container'])
            elif content['evt_type'] == 'cpu':
                for data in content['data']:
                    is_root = 0
                    if int(data[3]) == 1:
                        is_root = 1
                    event = {
                        'cpu': int(data[0]),
                        'proc_name': data[1],
                        'proc': int(data[2]),
                        'vm_size': int(data[4]),
                        'vm_rss': 0,
                        'vm_swap': 0,
                        'container': data[5],
                        'exe': data[6],
                        'args': data[7],
                        'parent_id': int(data[8]),
                        'duration': data[9],
                        'start':  long(content['ts'])/1000000,
                        'is_root': is_root
                    }
                    logging.debug('Record event, container=%s, ts=%s' % (event['container'], str(event['start'])))
                    self.__cassandra_update_procs(event)
                    self.__cassandra_update_cpu(event)
                    self.__cassandra_update_mem(event)
                    self.__cleanup(event['container'])
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
    rtHandler.channel = channel
    channel.queue_declare(queue='bc_record', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
            rtHandler.callback_record,
            queue='bc_record')
    channel.start_consuming()


if __name__ == '__main__':
    run()
