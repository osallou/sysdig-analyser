import os
import json
import logging
import sys
import uuid
import re
import copy
import datetime

from bson import json_util

from cassandra.cluster import Cluster
from influxdb import InfluxDBClient


logging.basicConfig(filename='sysdig_analysis.log',level=logging.DEBUG, format='%(asctime)s %(message)s')

'''
cluster = Cluster()
session = cluster.connect('sysdig')
'''

containers = {}

def __cassandra_insert(event):
    if event['thread.vtid'] is None:
        event['thread.vtid'] = -1
    session.execute(
    """
    INSERT INTO events (id, ts, container_id, container_name, evt_cpu, evt_cputime, evt_vtid, proc_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (uuid.uuid1().hex, event['evt.outputtime'], event['container.id'], event['container.name'], event['evt.cpu'], 0, event['thread.vtid'], event['proc.name'])
    )

def __cassandra_select():
    rows = session.execute('SELECT proc_name, SUM(evt_cputime) FROM events where container_name=\'competent_thompson\'')
    for row in rows:
        logging.warn(str(row))


def __vm_re(pattern, evt_info):
    matches = re.search(pattern, evt_info)
    if matches:
        return int(matches.group(1))
    else:
        return 0

def __re(pattern, evt_info):
    matches = re.search(pattern, evt_info)
    if matches:
        return matches.group(1)
    else:
        return None

def __vm_info(evt_info):
    vm_size = 0
    vm_rss = 0
    vm_swap = 0

    vm_size = __vm_re('vm_size=(\d+)', evt_info)
    vm_rss = __vm_re('vm_rss=(\d+)', evt_info)
    vm_swap = __vm_re('vm_swap=(\d+)', evt_info)

    return (vm_size, vm_rss, vm_swap)

def __fd_info(evt_info):
    fd = __vm_re('fd=(\d+)', evt_info)
    name =  __re('name=(.*)', evt_info)
    if not name:
        name =  __re('\(<f>(.*)\)', evt_info)
    return (fd, name)

def __io_info(evt_info):
    fd = __vm_re('fd=(\d+)', evt_info)
    length =  __vm_re('length=(\d+)', evt_info)
    if fd and length:
        return (fd, length)
    else:
        return (None, None)

def sysdig_event(event):
    if event['proc.name'] == 'sysdig':
        return
    if event['container.id'] in [None, 'host']:
        last_ts = event['evt.outputtime']
        return
    # logging.debug('Container event: %s' % (event['container.name']))
    logging.debug(str(event))
    if event['container.name'] not in containers:
        containers[event['container.name']] = {
            'cpus': {},
            'memory': {},
            'fd': {},
            'container_id': event['container.id']
        }
    if event['evt.cpu'] not in containers[event['container.name']]['cpus']:
        containers[event['container.name']]['cpus'][event['evt.cpu']] = {}

    new_thread  = False
    if event['thread.vtid'] not in containers[event['container.name']]['cpus'][event['evt.cpu']]:
        containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']] = {
            'proc_name': event['proc.name'],
            'usage': [],
            'last_cpu': None,
            'fd': {}
        }
        new_thread = True
    containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['proc_name'] = event['proc.name']  # possible fork, keeping same vtid
    if event['evt.type'] == 'switch':
        if not containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu']:
            containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'] = begin_ts
        # thread is paused in favor of an other thread
        (vm_size, vm_rss, vm_swap) = __vm_info(event['evt.info'])
        containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['usage'].append({
            'start': containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'],
            'start_date': datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'] // 1000000000),
            'debug_date': str(datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'] // 1000000000)),
            'duration': event['evt.outputtime'] - containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'],
            'memory': (vm_size, vm_rss, vm_swap)
        })
        containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'] = None

    else:
        (vm_size, vm_rss, vm_swap) = __vm_info(event['evt.info'])

        if vm_size > 0 or vm_rss > 0 or vm_swap > 0:
            # Update memory info
            if event['thread.vtid'] not in containers[event['container.name']]['memory']:
                containers[event['container.name']]['memory'][event['thread.vtid']] = []
            containers[event['container.name']]['memory'][event['thread.vtid']].append({
                'ts': event['evt.outputtime'],
                'value': (vm_size, vm_rss, vm_swap),
                'proc_name': event['proc.name']
            })

        (fd, name) = __fd_info(event['evt.info'])
        if fd and name:
            if fd not in containers[event['container.name']]['fd']:
                containers[event['container.name']]['fd'][fd] = {}
            containers[event['container.name']]['fd'][fd][event['proc.name']] = name

        (fd, length) = __io_info(event['evt.info'])
        if fd and length:
            name = None
            logging.debug('#FD: ' + str(fd))
            logging.debug(str(containers[event['container.name']]['fd']))
            if fd in containers[event['container.name']]['fd'] and event['proc.name'] in containers[event['container.name']]['fd'][fd]:
                logging.debug("#FD FOUND " + str(containers[event['container.name']]['fd'][fd][event['proc.name']]))
                name = containers[event['container.name']]['fd'][fd][event['proc.name']]
            if fd not in containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['fd']:
                containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['fd'][fd] = {}
            if not name:
                name = 'fd_unknown'
            if name:
                if name not in containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['fd'][fd]:
                    containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['fd'][fd][name] =0
                containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['fd'][fd][name] += length

        if not containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu']:
            # startup of this thread
            containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]['last_cpu'] = event['evt.outputtime']
            logging.error("##UPDATE " + str(containers[event['container.name']]['cpus'][event['evt.cpu']][event['thread.vtid']]))
        else:
            # continue processing
            pass

    # cassandra_insert(event)
    return

def close_sysdig_event(name, container):
    for cpu in list(container['cpus'].keys()):
        for thread in list(container['cpus'][cpu].keys()):
            event = container['cpus'][cpu][thread]
            if event['last_cpu']:
                # Close it!
                event['usage'].append({
                    'start': event['last_cpu'],
                    'start_date': datetime.datetime.fromtimestamp(event['last_cpu'] // 1000000000),
                    'debug_date': str(datetime.datetime.fromtimestamp(event['last_cpu'] // 1000000000)),
                    'duration': end_ts - event['last_cpu'],
                    'memory': (0, 0, 0)
                })
                event['last_cpu'] = None

def close_sysdig_events(sysdig_containers):
    for container in list(sysdig_containers.keys()):
        close_sysdig_event(container, sysdig_containers[container])


def group_by_cpu(containers):
    for name, container in containers.iteritems():
        logging.debug("group_by_cpu container: " + name)
        container['cpus']['all'] = {}
        events = {}
        events_per_process = {}
        for cpu in list(container['cpus'].keys()):
            for process in list(container['cpus'][cpu].keys()):
                for usage in container['cpus'][cpu][process]['usage']:
                    if process not in events:
                        events_per_process[process] = {}
                        events[process] = {
                            'proc_name': process,
                            'usage': [],
                            'fd': {}
                        }
                    if usage['start'] not in events_per_process[process]:
                        events_per_process[process][usage['start']] = usage
                    else:
                        events_per_process[process][usage['start']]['duration'] += usage['duration']
                        if not events_per_process[process][usage['start']]['memory'] and usage['memory']:
                            events_per_process[process][usage['start']]['memory'] = usage['memory']
        for process, ts in events_per_process.iteritems():
            events[process]['usage'].append(ts)
        container['cpus']['all'] = events

def group_by_seconds(sysdig_containers, merge=1):
    '''
    Merge metrics within same *merge* seconds

    :param merge: number of seconds events should be merged within
    :type merge: int
    :return: list containers with merged values
    '''
    logging.debug("## group_by_seconds")
    merged_containers = {}
    for name, container in sysdig_containers.iteritems():
        logging.debug("container: " + name)
        merged_containers[name] = copy.deepcopy(container)
        for cpu in list(container['cpus'].keys()):
            logging.debug("cpu: " + str(cpu))
            for tid, thread in container['cpus'][cpu].iteritems():
                # logging.error(thread)
                logging.debug("tid: " + str(tid))
                if len(thread['usage']) == 0:
                    continue
                prev_usages = thread['usage']
                prev_usage = None
                max_memory = 0
                thread['usage'] = []
                current_ts = prev_usages[0]['start_date']
                next_ts = current_ts + datetime.timedelta(seconds=merge)
                for usage in prev_usages:
                    if usage['start_date'] >= next_ts:
                        if prev_usage:
                            logging.debug("Date > next, add prev_usage")
                            logging.debug(prev_usage)
                            thread['usage'].append(prev_usage)
                            prev_usage = None
                            max_memory = 0
                        while usage['start_date'] >= next_ts:
                            current_ts += datetime.timedelta(seconds=merge)
                            next_ts = current_ts + datetime.timedelta(seconds=merge)
                        logging.debug("go to next ts " + str(current_ts))
                    logging.debug('Check if split needed')
                    if usage['start_date'] + datetime.timedelta(usage['duration'] / 1000000000) > next_ts:
                        logging.debug('Split event')
                        # Slip event in multiple ones
                        splitted_usages = []
                        nb_elts = ((usage['duration']/ 1000000000) / merge) + 1
                        remaining_duration = usage['duration']
                        for index in range(nb_elts):
                            splitted_usage = copy.deepcopy(usage)
                            splitted_usage['duration'] = min(merge * 1000000000, remaining_duration)
                            splitted_usage['start'] += splitted_usage['duration'] + (merge * 1000000000 * index)
                            splitted_usage['start_date'] = datetime.datetime.fromtimestamp(splitted_usage['start'] // 1000000000)
                            splitted_usage['debug_date'] = str(splitted_usage['start_date'])
                            splitted_usages.append(splitted_usage)
                            remaining_duration -= splitted_usage['duration']
                    else:
                        splitted_usages = [usage]
                    logging.debug('splitted events: ' + str(splitted_usages))
                    for splitted_usage in splitted_usages:
                        logging.debug("Start: " + str(splitted_usage['start_date']) + " vs " + str(current_ts))
                        logging.debug("Next: " + str(splitted_usage['start_date']) + " vs " + str(next_ts))
                        logging.debug(str(splitted_usage))
                        if splitted_usage['start_date'] >= next_ts and prev_usage:
                            logging.debug("add prev usage")
                            thread['usage'].append(prev_usage)
                            prev_usage = None
                        while splitted_usage['start_date'] >= next_ts:
                            current_ts += datetime.timedelta(seconds=merge)
                            next_ts = current_ts + datetime.timedelta(seconds=merge)
                        if splitted_usage['start_date'] >= current_ts and splitted_usage['start_date'] < next_ts:
                            # concat events
                            if prev_usage:
                                logging.debug("concat with previous event")
                                logging.debug(str(prev_usage))
                                prev_usage['duration'] += splitted_usage['duration']

                                if splitted_usage['memory'][0] > max_memory:
                                    prev_memory = splitted_usage['memory']
                                    max_memory = splitted_usage['memory'][0]
                            prev_usage = splitted_usage

                if prev_usage:
                    logging.debug("Remaining, add prev_usage")
                    logging.debug(prev_usage)
                    thread['usage'].append(prev_usage)


    return sysdig_containers

def insert_influxdb(containers):
    host = 'localhost'
    port = 8086
    user = 'root'
    password = 'root'
    dbname = 'sysdig'
    json_body = []

    client = InfluxDBClient(host, port, user, password, dbname)
    for name, container in containers.iteritems():
        for cpu in list(container['cpus'].keys()):
            for tid, thread in container['cpus'][cpu].iteritems():
                '''
                        {
                            "measurement": "cpu_load_short",
                            "tags": {
                                "host": "server01",
                                "region": "us-west"
                            },
                            "time": "2009-11-10T23:00:00Z",
                            "fields": {
                                "value": 0.64
                            }
                        }
                '''
                process = thread['proc_name']
                memory = None
                for usage in thread['usage']:
                    measure = {
                        'measurement': 'cpu_' + str(container['container_id']),
                        'tags': {
                                'cpu': str(cpu),
                                'process': str(process),
                                'pid': str(tid)
                        },
                        'time': usage['start'] / 1000000000,
                        "fields": {
                            "value": usage['duration']
                        }
                    }
                    json_body.append(measure)

                    if usage['memory']:
                        memory = usage['memory']
                    if memory:
                        measure = {
                            'measurement': 'memory_' + str(container['container_id']),
                            'tags': {
                                'cpu': str(cpu),
                                'process': str(process),
                                'tid': str(tid)
                            },
                            'time': usage['start'] / 1000000000,
                            "fields": {
                                "value": memory[0]
                            }
                        }
                        json_body.append(measure)


    client.write_points(json_body, time_precision='s')
    logging.debug("##INFLUX" + str(json_body))

last_ts = 0
events = []
with open('test.all.json') as sysdig_output:
    events = json.load(sysdig_output)
if not events:
    sys.exit(0)

begin_ts = events[0]['evt.outputtime']
end_ts = events[len(events) - 1]['evt.outputtime']
logging.warn("Start ts: " + str(begin_ts))
logging.warn("End ts: " + str(end_ts))

for event in events:
    sysdig_event(event)

close_sysdig_events(containers)

logging.info(json.dumps(containers, default=json_util.default))


merged_containers = group_by_seconds(containers, 1)
#group_by_cpu(merged_containers)
insert_influxdb(merged_containers)
analyse = {
    'merge': 1,
    'containers': merged_containers,
    'start': begin_ts,
    'end': end_ts,
    'duration': (end_ts - begin_ts) // 1000000000
}

logging.info(json.dumps(analyse, default=json_util.default))

with open('output.json', 'w') as out:
    json.dump(analyse, out, default=json_util.default)
