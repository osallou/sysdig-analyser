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

from cassandra.cluster import Cluster
from progressbar import Percentage, ProgressBar, Bar
import click

def __cassandra_update_retention(session, ts, retention):
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
        WHERE id=%s
        """,
        (ts, retention_type)
    )

def __cassandra_select_retention(session, retention):
    retention_type = 0
    if retention == 'm':
        retention_type = 0
    elif retention == 'h':
        retention_type = 1
    elif retention == 'd':
        retention_type = 2
    rows = session.execute(
        """
        SELECT * FROM retention
        WHERE id=%s
        """,
        [retention_type]
    )
    if rows:
        return rows[0].ts
    else:
        return None

def __cassandra_select_retention(session, retention):
    retention_type = 0
    if retention == 'm':
        retention_type = 0
    elif retention == 'h':
        retention_type = 1
    elif retention == 'd':
        retention_type = 2
    rows = session.execute('SELECT * from retention where id=' + str(retention_type))
    return rows



def __cassandra_update_mem(session, retention, event):
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


def __cassandra_update_cpu(session, retention, event):
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

def __cassandra_update_cpu_all(session, retention, event):
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

def __cassandra_query_containers(session, retention=None):
    table = 'cpu_all'
    if retention == 'm':
        table = 'cpu_all'
    if retention == 'h':
        table = 'cpu_all_per_m'
    if retention == 'd':
        table = 'cpu_all_per_h'
    rows = session.execute("SELECT DISTINCT container FROM " + table)
    return rows

def __cassandra_query_cpu(session, container, retention):
    # logging.warn("DEBUG query: SELECT * FROM cpu WHERE container='"+container+"'")
    table = 'cpu'
    if retention == 'h':
        table = 'cpu_per_m'

    if retention == 'd':
        table = 'cpu_per_h'

    rows = session.execute("SELECT * FROM " + table + " WHERE container='"+container+"'")
    return rows

def __cassandra_query_mem(session, container, retention):
    # logging.warn("DEBUG query: SELECT * FROM mem WHERE container='"+container+"'")
    table = 'mem'
    if retention == 'h':
        table = 'mem_per_m'

    if retention == 'd':
        table = 'mem_per_h'

    rows = session.execute("SELECT * FROM " + table + " WHERE container='"+container+"'")
    return rows


def __cassandra_query_proc(session, container):
    rows = session.execute("SELECT * FROM proc WHERE container='"+container+"'")
    return rows


def __cassandra_query_cpu_all(session, container, retention):
    # logging.warn("DEBUG query: SELECT * FROM cpu WHERE container='"+container+"'")
    table_all = 'cpu_all'
    if retention == 'h':
        table_all = 'cpu_all_per_m'

    if retention == 'd':
        table_all = 'cpu_all_per_h'

    rows = session.execute("SELECT * FROM " + table_all + " WHERE container='"+container+"'")
    return rows

def __cassandra_delete(session, table, container, proc_id, up_to=None):
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

def __cassandra_delete_cpu_all(session, retention):
    (retention_seconds, retention_interval) = __get_retention_interval(retention)
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
        __cassandra_delete(session, table, container, proc_id, up_to)



def __cassandra_delete_cpu(session, retention):
    (retention_seconds, retention_interval) = __get_retention_interval(retention)
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
        __cassandra_delete(session, table, container, proc_id, up_to)

def __cassandra_delete_mem(session, retention):
    (retention_seconds, retention_interval) = __get_retention_interval(retention)
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
        __cassandra_delete(session, table, container, proc_id, up_to)


def add_event(events, event):
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


def save_events(session, events, timestamp=None, retention='m', cpu_all=False):
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
                    __cassandra_update_cpu_all(session, retention, event)
                else:
                    __cassandra_update_cpu(session, retention, event)
    events = {}


def __cassandra_compute_cpu(session, container_events, retention='m', cpu_all=False):
    events = {}
    (retention_seconds, retention_interval) = __get_retention_interval(retention)
    # events[event['container']][event['cpu']][event['proc']]['duration']
    begin_ts = container_events[0].ts
    for event in container_events:
        if event.ts < begin_ts + datetime.timedelta(seconds=retention_interval):
            add_event(events, event)
        else:
            save_events(session, events, timestamp=begin_ts, retention=retention,cpu_all=cpu_all)
            events = {}
            add_event(events, event)
            while event.ts > begin_ts:
                begin_ts += datetime.timedelta(seconds=retention_interval)
    # Save remaining events
    save_events(session, events, timestamp=begin_ts, retention=retention,cpu_all=cpu_all)
    events = {}

def add_mem_event(events, event):
    if event.container not in events:
        events[event.container] = {}
    if event.proc_id not in events[event.container]:
        events[event.container][event.proc_id] = 0
    # Keep max
    if event.vm_size > events[event.container][event.proc_id]:
        events[event.container][event.proc_id] = event.vm_size

def save_mem_events(session, events, timestamp=None, retention='m'):
    for container_name, container in events.iteritems():
        for proc_id, vm_size in container.iteritems():
            event = {
                'container': container_name,
                'start': timestamp,
                'proc': proc_id,
                'vm_size': vm_size
            }
            __cassandra_update_mem(session, retention, event)
    events = {}


def __cassandra_compute_mem(session, container_events, retention='m'):
    events = {}
    (retention_seconds, retention_interval) = __get_retention_interval(retention)
    # events[event['container']][event['cpu']][event['proc']]['duration']
    begin_ts = container_events[0].ts
    for event in container_events:
        if event.ts < begin_ts + datetime.timedelta(seconds=retention_interval):
            add_mem_event(events, event)
        else:
            save_mem_events(session, events, timestamp=begin_ts, retention=retention)
            events = {}
            add_mem_event(events, event)
            while event.ts > begin_ts:
                begin_ts += datetime.timedelta(seconds=retention_interval)
    # Save remaining events
    save_mem_events(session, events, timestamp=begin_ts, retention=retention)
    events = {}

def __get_retention_interval(retention):
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


@click.group()
def run():
    pass

@run.command()
@click.option('--retention', default='m', help='retention m(minutes), h(hours), d(days)')
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
def retain(retention, host, cluster):
    '''
    Merge events in larger window
    '''
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
    containers = __cassandra_query_containers(session, retention)
    (retention_seconds, retention_interval) = __get_retention_interval(retention)
    rows = __cassandra_select_retention(session, retention)
    last_ts = None
    if rows:
        last_ts = rows[0].ts
    now = datetime.datetime.now()
    up_to = now - datetime.timedelta(seconds=retention_interval)
    # up_to_ts = int(time.mktime(up_to.timetuple())) * 1000
    if last_ts is not None and up_to <= last_ts:
        return

    data_to_remove = {}
    for container in containers:
        # procs = __cassandra_query_proc(session, container.container)
        # if proc.start not set, set it to min timestamp of cpu data
        # set proc.end to last event timestamp of cpu data
        rows = __cassandra_query_cpu(session, container.container, retention)
        filtered_rows = []
        if last_ts is None:
            filtered_rows = rows
        else:
            for row in rows:
                if row.ts >= last_ts and row.ts <= up_to:
                    filtered_rows.append(row)
        if filtered_rows:
            sorted_rows = sorted(filtered_rows, key=lambda x: x.ts)
            __cassandra_compute_cpu(session, sorted_rows, retention=retention, cpu_all=False)

        rows = __cassandra_query_cpu_all(session, container.container, retention)
        filtered_rows = []
        if last_ts is None:
            filtered_rows = rows
        else:
            for row in rows:
                if row.ts > last_ts and row.ts <= up_to:
                    filtered_rows.append(row)
        if filtered_rows:
            sorted_rows = sorted(filtered_rows, key=lambda x: x.ts)
            __cassandra_compute_cpu(session, sorted_rows, retention=retention, cpu_all=True)

        rows = __cassandra_query_mem(session, container.container, retention)
        filtered_rows = []
        if last_ts is None:
            filtered_rows = rows
        else:
            for row in rows:
                if row.ts > last_ts and row.ts <= up_to:
                    filtered_rows.append(row)
        if filtered_rows:
            sorted_rows = sorted(filtered_rows, key=lambda x: x.ts)
            __cassandra_compute_mem(session, sorted_rows, retention=retention)

    __cassandra_update_retention(session, up_to, retention)
    __cassandra_delete_cpu(session, retention)
    __cassandra_delete_cpu_all(session, retention)
    __cassandra_delete_mem(session, retention)


if __name__ == '__main__':
    run()
