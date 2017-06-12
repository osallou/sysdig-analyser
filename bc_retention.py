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

    def __cassandra_update_container_retention(self, session, container, ts, retention):
        retention_type = 0
        if retention == 'm':
            retention_type = 0
        elif retention == 'h':
            retention_type = 1
        elif retention == 'd':
            retention_type = 2
        session.execute(
            """
            UPDATE retention
            SET ts =  %s
            WHERE id=%s and container=%s
            """,
            (ts, retention_type, container)
        )


    def __cassandra_select_container_retention(self, session, container, retention):
        retention_type = 0
        if retention == 'm':
            retention_type = 0
        elif retention == 'h':
            retention_type = 1
        elif retention == 'd':
            retention_type = 2
        rows = session.execute('''SELECT * from retention where container=%s and id= %s''', (container, retention_type))
        return rows



    def __cassandra_update_mem(self, session, retention, event):
        table = 'mem_per_m'
        if retention == 'h':
            table = 'mem_per_h'

        if retention == 'd':
            table = 'mem_per_d'

        session.execute(
            """
            UPDATE """ + table + """
            SET vm_size =  %s
            WHERE ts=%s AND proc_id=%s and container=%s
            """,
            (event['vm_size'], event['start'], event['proc'], event['container'])
        )


    def __cassandra_update_cpu(self, session, retention, event):
        table = 'cpu_per_m'
        if retention == 'h':
            table = 'cpu_per_h'

        if retention == 'd':
            table = 'cpu_per_d'

        session.execute(
            """
            UPDATE """ + table + """
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s and container=%s and cpu=%s
            """,
            (event['duration'], event['start'], event['proc'], event['container'], event['cpu'])
        )

    def __cassandra_update_cpu_all(self, session, retention, event):
        table = 'cpu_all_per_m'
        if retention == 'h':
            table = 'cpu_all_per_h'

        if retention == 'd':
            table = 'cpu_all_per_d'

        session.execute(
            """
            UPDATE """ + table + """
            SET duration = duration + %s
            WHERE ts=%s AND proc_id=%s and container=%s
            """,
            (event['duration'], event['start'], event['proc'], event['container'])
        )

    def __cassandra_query_containers(self, session, retention=None):
        table = 'cpu_all'
        if retention == 'm':
            table = 'cpu_all'
        if retention == 'h':
            table = 'cpu_all_per_m'
        if retention == 'd':
            table = 'cpu_all_per_h'
        rows = session.execute("SELECT DISTINCT container FROM " + table)
        return rows

    def __cassandra_query_cpu(self, session, container, retention):
        # logging.warn("DEBUG query: SELECT * FROM cpu WHERE container='"+container+"'")
        table = 'cpu'
        if retention == 'h':
            table = 'cpu_per_m'

        if retention == 'd':
            table = 'cpu_per_h'

        rows = session.execute("SELECT * FROM " + table + " WHERE container='"+container+"'")
        return rows

    def __cassandra_query_mem(self, session, container, retention):
        # logging.warn("DEBUG query: SELECT * FROM mem WHERE container='"+container+"'")
        table = 'mem'
        if retention == 'h':
            table = 'mem_per_m'

        if retention == 'd':
            table = 'mem_per_h'

        rows = session.execute("SELECT * FROM " + table + " WHERE container='"+container+"'")
        return rows


    def __cassandra_query_proc(self, session, container):
        rows = session.execute("SELECT * FROM proc WHERE container='"+container+"'")
        return rows


    def __cassandra_query_cpu_all(self, session, container, retention):
        # logging.warn("DEBUG query: SELECT * FROM cpu WHERE container='"+container+"'")
        table_all = 'cpu_all'
        if retention == 'h':
            table_all = 'cpu_all_per_m'

        if retention == 'd':
            table_all = 'cpu_all_per_h'

        rows = session.execute("SELECT * FROM " + table_all + " WHERE container='"+container+"'")
        return rows

    def __cassandra_delete(self, session, table, container, proc_id, up_to=None):
        '''
        TODO pb if we delete based on current inserts, we may not match some old proc_id
        we need to get all container/proc_id to add a filter on ts
        Cannot delete *with timestamp* when there is a counter (cpu, cpu_all)
        '''
        if up_to is None:
            return;
        logging.debug("DELETE FROM %s container=%s, proc_id=%s,ts<%s" % (table, container, str(proc_id), str(up_to)))
        session.execute(
        """
        DELETE FROM """ + table + """ WHERE container=%s AND proc_id=%s AND ts<%s;
        """,
        (container, proc_id, up_to)
        )

    def __cassandra_delete_cpu_all(self, session, retention):
        (retention_seconds, retention_interval) = self.__get_retention_interval(retention)
        now = datetime.datetime.now()
        up_to = now - datetime.timedelta(seconds=retention_interval)
        table = 'cpu_all'
        if retention == 'm':
            table = 'cpu_all'
        if retention == 'h':
            table = 'cpu_all_per_m'
        if retention == 'd':
            table = 'cpu_all_per_h'
        contproc = {}
        rows = session.execute('SELECT container, proc_id FROM ' + table)
        for row in rows:
            contproc[row.container+':'+str(row.proc_id)] = 1
        for elt in list(contproc.keys()):
            (container, proc_id) = elt.split(':')
            proc_id = int(proc_id)
            self.__cassandra_delete(session, table, container, proc_id, up_to)



    def __cassandra_delete_cpu(self, session, retention):
        (retention_seconds, retention_interval) = self.__get_retention_interval(retention)
        now = datetime.datetime.now()
        up_to = now - datetime.timedelta(seconds=retention_interval)

        table = 'cpu'
        if retention == 'm':
            table = 'cpu'
        if retention == 'h':
            table = 'cpu_per_m'
        if retention == 'd':
            table = 'cpu_per_h'

        contproc = {}
        rows = session.execute('SELECT container, proc_id FROM ' + table)
        for row in rows:
            contproc[row.container+':'+str(row.proc_id)] = 1
        for elt in list(contproc.keys()):
            (container, proc_id) = elt.split(':')
            proc_id = int(proc_id)
            self.__cassandra_delete(session, table, container, proc_id, up_to)

    def __cassandra_delete_mem(self, session, retention):
        (retention_seconds, retention_interval) = self.__get_retention_interval(retention)
        now = datetime.datetime.now()
        up_to = now - datetime.timedelta(seconds=retention_interval)

        table = 'mem'
        if retention == 'm':
            table = 'mem'
        if retention == 'h':
            table = 'mem_per_m'
        if retention == 'd':
            table = 'mem_per_h'

        contproc = {}
        rows = session.execute('SELECT container, proc_id FROM ' + table)
        for row in rows:
            contproc[row.container+':'+str(row.proc_id)] = 1
        for elt in list(contproc.keys()):
            (container, proc_id) = elt.split(':')
            proc_id = int(proc_id)
            self.__cassandra_delete(session, table, container, proc_id, up_to)


    def add_event(self, events, event):
        if event.container not in events:
            events[event.container] = {}
        cpu = 0
        if hasattr(event, 'cpu'):
            cpu = event.cpu
        if cpu not in events[event.container]:
            events[event.container][cpu] = {}
        if event.proc_id not in events[event.container][cpu]:
            events[event.container][cpu][event.proc_id] = 0
        events[event.container][cpu][event.proc_id] += event.duration


    def save_events(self, session, events, timestamp=None, retention='m', cpu_all=False):
        for container_name, container in events.iteritems():
            for cpu_id, cpu in container.iteritems():
                for proc_id, duration in cpu.iteritems():
                    event = {
                        'container': container_name,
                        'start': timestamp,
                        'proc': proc_id,
                        'duration': duration,
                        'cpu': cpu_id
                    }
                    if cpu_all:
                        self.__cassandra_update_cpu_all(session, retention, event)
                    else:
                        self.__cassandra_update_cpu(session, retention, event)
        events = {}


    def __cassandra_compute_cpu(self, session, container_events, retention='m', cpu_all=False):
        events = {}
        (retention_seconds, retention_interval) = self.__get_retention_interval(retention)
        # events[event['container']][event['cpu']][event['proc']]['duration']
        begin_ts = container_events[0].ts
        for event in container_events:
            if event.ts < begin_ts + datetime.timedelta(seconds=retention_interval):
                self.add_event(events, event)
            else:
                self.save_events(session, events, timestamp=begin_ts, retention=retention,cpu_all=cpu_all)
                events = {}
                self.add_event(events, event)
                while event.ts > begin_ts:
                    begin_ts += datetime.timedelta(seconds=retention_interval)
        # Save remaining events
        self.save_events(session, events, timestamp=begin_ts, retention=retention,cpu_all=cpu_all)
        events = {}

    def add_mem_event(self, events, event):
        if event.container not in events:
            events[event.container] = {}
        if event.proc_id not in events[event.container]:
            events[event.container][event.proc_id] = 0
        # Keep max
        if event.vm_size > events[event.container][event.proc_id]:
            events[event.container][event.proc_id] = event.vm_size

    def save_mem_events(self, session, events, timestamp=None, retention='m'):
        for container_name, container in events.iteritems():
            for proc_id, vm_size in container.iteritems():
                event = {
                    'container': container_name,
                    'start': timestamp,
                    'proc': proc_id,
                    'vm_size': vm_size
                }
                self.__cassandra_update_mem(session, retention, event)
        events = {}


    def __cassandra_compute_mem(self, session, container_events, retention='m'):
        events = {}
        (retention_seconds, retention_interval) = self.__get_retention_interval(retention)
        # events[event['container']][event['cpu']][event['proc']]['duration']
        begin_ts = container_events[0].ts
        for event in container_events:
            if event.ts < begin_ts + datetime.timedelta(seconds=retention_interval):
                self.add_mem_event(events, event)
            else:
                self.save_mem_events(session, events, timestamp=begin_ts, retention=retention)
                events = {}
                self.add_mem_event(events, event)
                while event.ts > begin_ts:
                    begin_ts += datetime.timedelta(seconds=retention_interval)
        # Save remaining events
        self.save_mem_events(session, events, timestamp=begin_ts, retention=retention)
        events = {}

    def __get_retention_interval(self, retention):
        retention_seconds = 3600 * 10
        retention_interval = 60
        if retention == 'm':
            # 10h
            retention_seconds = 3600 * 10
            retention_interval = 60
        elif retention == 'h':
            # 30j
            retention_seconds = 3600 * 24 * 30
            retention_interval = 3600
        elif retention == 'd':
            # 120d
            retention_seconds = 3600 * 24 * 120
            retention_interval = 3600 * 24
        return (retention_seconds, retention_interval)

    def retain(self, container, retention, last_ts, up_to):
        rows = self.__cassandra_query_cpu(self.session, container, retention)
        filtered_rows = []
        if last_ts is None:
            filtered_rows = rows
        else:
            for row in rows:
                if row.ts >= last_ts and row.ts <= up_to:
                    filtered_rows.append(row)
        if filtered_rows:
            sorted_rows = sorted(filtered_rows, key=lambda x: x.ts)
            self.__cassandra_compute_cpu(self.session, sorted_rows, retention=retention, cpu_all=False)

        rows = self.__cassandra_query_cpu_all(self.session, container, retention)
        filtered_rows = []
        if last_ts is None:
            filtered_rows = rows
        else:
            for row in rows:
                if row.ts > last_ts and row.ts <= up_to:
                    filtered_rows.append(row)
        if filtered_rows:
            sorted_rows = sorted(filtered_rows, key=lambda x: x.ts)
            self.__cassandra_compute_cpu(self.session, sorted_rows, retention=retention, cpu_all=True)

        rows = self.__cassandra_query_mem(self.session, container, retention)
        filtered_rows = []
        if last_ts is None:
            filtered_rows = rows
        else:
            for row in rows:
                if row.ts > last_ts and row.ts <= up_to:
                    filtered_rows.append(row)
        if filtered_rows:
            sorted_rows = sorted(filtered_rows, key=lambda x: x.ts)
            self.__cassandra_compute_mem(self.session, sorted_rows, retention=retention)

    def callback_retain(self, ch, method, properties, body):
        try:
            rt = json.loads(body)
            logging.debug('Message: %s' % (body))
            retention = rt['retention']
            container = rt['container']
            (retention_seconds, retention_interval) = self.__get_retention_interval(retention)
            rows = self.__cassandra_select_container_retention(self.session, container, retention)
            last_ts = None
            if rows:
                last_ts = rows[0].ts
            now = datetime.datetime.now()
            up_to = now - datetime.timedelta(seconds=retention_interval)
            # up_to_ts = int(time.mktime(up_to.timetuple())) * 1000
            if last_ts is not None and up_to <= last_ts:
                return
            self.retain(container, retention, last_ts, up_to)
            self.__cassandra_update_container_retention(self.session, container, up_to, retention)
            self.__cassandra_delete_cpu(self.session, retention)
            self.__cassandra_delete_cpu_all(self.session, retention)
            self.__cassandra_delete_mem(self.session, retention)
        except Exception as e:
            logging.exception("Failed to handle retention query: " + str(e))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)


@run.command()
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
@click.option('--rabbit', help="rabbitmq host")
def listen(host, cluster, rabbit):
    '''
    For rabbitmq credentials MUST use env variables RABBITMQ_USER and RABBITMQ_PASSWORD
    '''
    if len(host) == 0:
        host_list = ['127.0.0.1']
    else:
        host_list = list(host)

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
    channel.queue_declare(queue='bc_retain', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(
            rtHandler.callback_retain,
            queue='bc_retain')
    channel.start_consuming()



@run.command()
@click.option('--retention', default='m', help='retention m(minutes), h(hours), d(days)')
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
@click.option('--rabbit', help="rabbitmq host")
def retain(retention, host, cluster, rabbit):
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
    containers = rtHandler.__cassandra_query_containers(session, retention)

    connection = None
    if rabbitmq_user:
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, credentials=credentials, heartbeat_interval=0))
    else:
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, heartbeat_interval=0))

    channel = connection.channel()

    for container in containers:
        logging.info("Retain:%s:%s" % (container.container, retention))
        rtcontainer = {
            'container': container.container,
            'retention': retention
        }

        channel.basic_publish(
            exchange='',
            routing_key='bc_retain',
            body=json.dumps(rtcontainer),
            properties=pika.BasicProperties(
                # make message persistent
                delivery_mode=2
            ))


if __name__ == '__main__':
    run()
