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


logging.basicConfig(filename='sysdig_analysis.log',level=logging.INFO, format='%(asctime)s %(message)s')


cluster = Cluster()
session = cluster.connect('sysdig')


containers = {}


def __cassandra_io(event):
    session.execute(
    """
    UPDATE io
    SET io_in = io_in + %s,
        io_out = io_out + %s
    WHERE ts=%s AND proc_name=%s AND file_name=%s AND container=%s
    """,
    (event['in'], event['out'], event['start'] // 1000000000 * 1000, event['proc'], event['name'], event['container'])
    )
    session.execute(
    """
    UPDATE io_all
    SET io_in = io_in + %s,
        io_out = io_out + %s
    WHERE proc_name=%s AND file_name=%s AND container=%s
    """,
    (event['in'], event['out'], event['proc'], event['name'], event['container'])
    )


def __cassandra_mem(event):
    session.execute(
    """
    UPDATE mem
    SET vm_size = %s,
        vm_rss = %s,
        vm_swap = %s
    WHERE ts=%s AND proc_name=%s AND container=%s
    """,
    (event['vm_size'], event['vm_rss'], event['vm_swap'], event['start'] // 1000000000 * 1000, event['proc'], event['container'])
    )

def __cassandra_cpu(event):
    session.execute(
    """
    UPDATE cpu
    SET duration = duration + %s
    WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
    """,
    (event['duration'], event['start'] // 1000000000 * 1000, int(event['proc_id']), event['cpu'], event['container'])
    )

def __cassandra_cpu_all(event):
    session.execute(
    """
    UPDATE cpu_all
    SET duration = duration + %s
    WHERE ts=%s AND proc_id=%s and container=%s
    """,
    (event['duration'], event['start'] // 1000000000 * 1000, int(event['proc_id']), event['container'])
    )
    session.execute(
    """
    UPDATE proc_cpu
    SET cpu = cpu + %s
    WHERE proc_id=%s and container=%s
    """,
    (event['duration'], int(event['proc_id']), event['container'])
    )

# CREATE TABLE proc (ts timestamp, proc_name varchar, proc_id int, parent_id int, PRIMARY KEY (ts, proc_id));
def __cassandra_proc(event):
    session.execute(
    """
    UPDATE proc
    SET parent_id = %s,
        proc_name = %s,
        exe=%s,
        args=%s
    WHERE proc_id=%s and container=%s
    """,
    (event['parent_id'], event['proc_name'], event['exe'], event['args'], event['proc_id'], event['container'])
    )

def __cassandra_proc_start(event, child=None):
    proc_id = event['thread.vtid']
    if child:
        proc_id = child
    session.execute(
    """
    UPDATE proc
    SET start = %s
    WHERE proc_id=%s and container=%s
    """,
    (event['evt.outputtime'] // 1000000000 * 1000, proc_id, event['container.id'])
    )

def __cassandra_proc_end(event):
    session.execute(
    """
    UPDATE proc
    SET end = %s
    WHERE proc_id=%s and container=%s
    """,
    (event['evt.outputtime'] // 1000000000 * 1000, event['thread.vtid'], event['container.id'])
    )


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
    name =  __re('fd=\d+\((<\w+>.*?)\)', evt_info)
    if name and __re('^(<\w+>)$', name):
        return (None, None)
    if not name:
        name =  __re('name=(.*?)\s+', evt_info)
    if not name:
        name =  __re('\(<\w+>(.*?)\)', evt_info)
    return (fd, name)

def __io_info(evt_info):
    fd = __vm_re('fd=(\d+)', evt_info)
    length =  __vm_re('length=(\d+)', evt_info)
    if not length:
        length =  __vm_re('size=(\d+)', evt_info)
    if fd and length:
        return (fd, length)
    else:
        return (None, None)

def __clone_info(evt_info):
    res = __vm_re('res=(\d+)', evt_info)
    return res


def __exec_info(evt_info):
    exe = __re('exe=(\w+)', evt_info)
    args = None
    if exe:
        args = __re('args=(.*) tid=', evt_info)
    return (exe, args)

def sysdig_event(event):

    if event['proc.name'] == 'sysdig':
        return
    if event['container.id'] in [None, 'host']:
        last_ts = event['evt.outputtime']
        return
    logging.debug(str(event))

    utid = str(event['thread.vtid'])

    if event['container.name'] not in containers:
        containers[event['container.name']] = {
            'hierarchy': {},
            'procs': {},
            'commands': {},
            'cpus': {},
            'memory': {},
            'fd': {},
            'io': {},
            'container_id': event['container.id'],
            'last_cpus': {},
            'pids': {}
        }

    containers[event['container.name']]['pids'][event['thread.tid']] = event['thread.vtid']

    if event['thread.vtid'] == 1 and 1 not in containers[event['container.name']]['hierarchy']:
        containers[event['container.name']]['hierarchy'][1] = None

    is_clone = False
    if event['evt.type'] == 'clone':
        child = __clone_info(event['evt.info'])
        parent = event['thread.vtid']
        if child > 0:
            containers[event['container.name']]['hierarchy'][child] = parent
            __cassandra_proc_start(event, child=child)
            is_clone = True
            logging.info("StartEvent:"+str(child)+':'+str(event['evt.num']))
            containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+str(child)] = event['evt.outputtime']
            # containers[event['container.name']]['cpus'][event['evt.cpu']][child]['last_cpu'] = event['evt.outputtime']


    if event['evt.cpu'] not in containers[event['container.name']]['cpus']:
        containers[event['container.name']]['cpus'][event['evt.cpu']] = {}

    new_thread  = False
    containers[event['container.name']]['procs'][utid] = {'name': event['proc.name']}

    if event['evt.type'] == 'execve':
        containers[event['container.name']]['procs'][utid]['start'] = event['evt.outputtime']
        __cassandra_proc_start(event)
        containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+utid] = event['evt.outputtime']
        # containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = event['evt.outputtime']


    if event['evt.type'] == 'procexit':
        containers[event['container.name']]['procs'][utid]['end'] = event['evt.outputtime']
        __cassandra_proc_end(event)


    if utid not in containers[event['container.name']]['cpus'][event['evt.cpu']]:
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid] = {
            'proc_name': event['proc.name'],
            'usage': [],
            'last_cpu': None,
            'fd': {},
            'io': [],
            'first': True
        }
        new_thread = True
    if event["evt.type"] == "execve":
        # Execute a process, takes proc id
        (exe, args) = __exec_info(event['evt.info'])
        if exe:
            containers[event['container.name']]['commands'][utid]= {
                'exe': exe,
                'args': args
            }
        logging.info("StartEvent:"+str(utid)+':'+str(event['evt.num']))
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['proc_name'] = event['proc.name']  # possible fork, keeping same vtid
        containers[event['container.name']]['procs'][utid]['name'] = event['proc.name']


    '''
    if event['evt.type'] == 'switch':
        next_pid = __vm_re('next=(\d+)', event['evt.info'])
        if next_pid:
            # next_pid is tid, n ot vtid.....
            # containers[event['container.name']]['cpus'][event['evt.cpu']][str(next_pid)]['last_cpu'] = event['evt.outputtime']
            logging.info("switch to "+str(next_pid))
            if next_pid in containers[event['container.name']]['pids']:
                next_vtid = containers[event['container.name']]['pids'][next_pid]
                logging.info("switch to "+str(next_vtid))
                containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+str(next_vtid)] = event['evt.outputtime']
            else:
                logging.info("switch to unknown")
                # TODO we receive a switch to a process we have not seen the start, we do not know its internal pid
                # this should be stored in db and started at load to get known processes
                pass
    '''

    if (str(event['evt.cpu'])+'-'+utid) in containers[event['container.name']]['last_cpus']:
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+utid]
    else:
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = begin_ts

    if event['evt.type'] == 'switch' or event['evt.type'] == 'procexit':
        # intermediate switch, nothing occured meanwhile
        # if we have seen no other event for this cpu/proc before, then this is (may) be continuation from a previous record
        if str(event['evt.cpu'])+'-'+utid not in containers[event['container.name']]['last_cpus']:
            return

        '''
        if not containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['first'] and not containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu']:
            return
        '''
        if not containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu']:
            return
        logging.info("StopEvent:"+str(utid)+':'+str(event['evt.num']))
        # if not containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu']:
        #if not containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+utid]:
        #    containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+utid] = begin_ts

        # thread is paused in favor of an other thread
        (vm_size, vm_rss, vm_swap) = __vm_info(event['evt.info'])
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['usage'].append({
            'start': containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
            'start_date': datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000),
            'debug_date': str(datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000)),
            'duration': event['evt.outputtime'] - containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
            'memory': (vm_size, vm_rss, vm_swap)
        })
        logging.info(str({
            'proc': utid,
            'cpu': event['evt.cpu'],
            'last_cpu': containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
            'start': containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
            'start_date': datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000),
            'debug_date': str(datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000)),
            'duration': event['evt.outputtime'] - containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
            'memory': (vm_size, vm_rss, vm_swap)
        }))
        containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+utid] = None
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = None
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'] = {}

    else:
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['first'] = False
        (vm_size, vm_rss, vm_swap) = __vm_info(event['evt.info'])

        if vm_size > 0 or vm_rss > 0 or vm_swap > 0:
            # Update memory info
            __cassandra_mem({
                'start': event['evt.outputtime'],
                'vm_size': vm_size,
                'vm_rss': vm_rss,
                'vm_swap': vm_swap,
                'proc':  utid,
                'container': event['container.id']
            })

        (fd, name) = __fd_info(event['evt.info'])
        if fd and name:
            if fd not in containers[event['container.name']]['fd']:
                containers[event['container.name']]['fd'][fd] = {}
            containers[event['container.name']]['fd'][fd][event['proc.name']] = name

            __cassandra_io({
                'start': event['evt.outputtime'],
                'start_date': datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000),
                'debug_date': str(datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000)),
                'name': name,
                'length': 0,
                'in': 0,
                'out': 0,
                'proc': utid,
                'container': event['container.id']
            })

        (fd, length) = __io_info(event['evt.info'])

        if fd and length:
            name = None
            logging.debug('#FD: ' + str(fd))
            logging.debug(str(containers[event['container.name']]['fd']))
            if fd in containers[event['container.name']]['fd'] and event['proc.name'] in containers[event['container.name']]['fd'][fd]:
                logging.debug("#FD FOUND " + str(containers[event['container.name']]['fd'][fd][event['proc.name']]))
                name = containers[event['container.name']]['fd'][fd][event['proc.name']]
            if fd not in containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd']:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][fd] = {}
            if not name:
                name = 'fd_unknown'
            if name:
                if name not in containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][fd]:
                    containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][fd][name] =0
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][fd][name] += length
                io_event = {
                    'start': event['evt.outputtime'],
                    'start_date': datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000),
                    'debug_date': str(datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000)),
                    'name': name,
                    'length': length,
                    'in': 0,
                    'out': 0,
                    'proc': utid,
                    'container': event['container.id']
                }

                if event['evt.type'] == 'write':
                    io_event['out'] = length
                else:
                    io_event['in'] = length

                __cassandra_io(io_event)

            if name not in containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd']:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][name] = length
            else:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][name] += length

        if str(event['evt.cpu'])+'-'+str(utid) not in containers[event['container.name']]['last_cpus']:
            containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+str(utid)] = event['evt.outputtime']
        if not containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+str(utid)] and not is_clone:
            containers[event['container.name']]['last_cpus'][str(event['evt.cpu'])+'-'+str(utid)] = event['evt.outputtime']
        '''
        if not containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu']:
            # startup of this thread
            containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = event['evt.outputtime']
        else:
            # continue processing
            pass
        '''

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
        # io
        for proc in container['io'].keys():
            proc_io = container['io'][proc]
            if len(proc_io) > 0:
                    prev_thread_io = proc_io
                    proc_io = []
                    first_io = True
                    prev_io = {}
                    for thread_io in prev_thread_io:
                        if first_io:
                            current_ts = thread_io['start_date']
                            first_io = False
                        if thread_io['start_date'] >= current_ts and thread_io['start_date'] < current_ts + datetime.timedelta(seconds=merge):
                            # Merge with previous
                            if thread_io['name'] not in prev_io:
                                prev_io[thread_io['name']] = {
                                    'in': thread_io['in'],
                                    'out': thread_io['out'],
                                    'start': thread_io['start'],
                                    'start_date': thread_io['start_date'],
                                    'debug_date': thread_io['debug_date']
                                }
                                # prev_io[thread_io['name']] = thread_io['length']
                            else:
                                prev_io[thread_io['name']]['in'] += thread_io['in']
                                prev_io[thread_io['name']]['out'] += thread_io['out']
                        else:
                            proc_io.append(prev_io)
                            current_ts = thread_io['start_date']
                            prev_io = {}
                    if len(prev_io.keys()) > 0:
                        proc_io.append(prev_io)
        for cpu in list(container['cpus'].keys()):
            logging.debug("cpu: " + str(cpu))
            for tid, thread in container['cpus'][cpu].iteritems():
                # logging.error(thread)
                logging.debug("tid: " + str(tid))
                if len(thread['io']) > 0:
                    prev_thread_io = thread['io']
                    thread['io'] = []
                    first_io = True
                    prev_io = {}
                    for thread_io in prev_thread_io:
                        if first_io:
                            current_ts = thread_io['start_date']
                            first_io = False
                        if thread_io['start_date'] >= current_ts and thread_io['start_date'] < current_ts + datetime.timedelta(seconds=merge):
                            # Merge with previous
                            if thread_io['name'] not in prev_io:
                                prev_io[thread_io['name']] = {
                                    'in': thread_io['in'],
                                    'out': thread_io['out'],
                                    'start': thread_io['start'],
                                    'start_date': thread_io['start_date'],
                                    'debug_date': thread_io['debug_date']
                                }
                                # prev_io[thread_io['name']] = thread_io['length']
                            else:
                                prev_io[thread_io['name']]['in'] += thread_io['in']
                                prev_io[thread_io['name']]['out'] += thread_io['out']
                        else:
                            thread['io'].append(prev_io)
                            current_ts = thread_io['start_date']
                            prev_io = {}
                    if len(prev_io.keys()) > 0:
                        thread['io'].append(prev_io)


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
                            splitted_usage['start'] += (merge * 1000000000 * index)
                            splitted_usage['start_date'] = datetime.datetime.fromtimestamp(splitted_usage['start'] // 1000000000)
                            splitted_usage['debug_date'] = str(splitted_usage['start_date'])
                            splitted_usages.append(splitted_usage)
                            remaining_duration -= splitted_usage['duration']
                    else:
                        splitted_usages = [usage]
                    logging.debug('splitted events: ' + str(splitted_usages))
                    for splitted_usage in splitted_usages:
                        splitted_usage['cpu'] = cpu
                        # splitted_usage['proc'] = tid
                        splitted_usage['proc_id'] = tid
                        splitted_usage['proc'] =  container['procs'][tid]['name']+'(' + tid + ')'
                        # __cassandra_cpu(splitted_usage)
                        splitted_usage['total'] = merge * 1000000000
                        logging.debug("Start: " + str(splitted_usage['start_date']) + " vs " + str(current_ts))
                        logging.debug("Next: " + str(splitted_usage['start_date']) + " vs " + str(next_ts))
                        logging.debug(str(splitted_usage))
                        if splitted_usage['start_date'] >= next_ts and prev_usage:
                            logging.debug("add prev usage")
                            thread['usage'].append(prev_usage)
                            prev_usage['container'] = container['container_id']
                            __cassandra_cpu(prev_usage)
                            __cassandra_cpu_all(prev_usage)
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
                    prev_usage['container'] = container['container_id']
                    __cassandra_cpu(prev_usage)
                    __cassandra_cpu_all(prev_usage)


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

                    measure = {
                        'measurement': 'total_cpu_' + str(container['container_id']),
                        'tags': {
                                'cpu': str(cpu),
                        },
                        'time': usage['start'] / 1000000000,
                        "fields": {
                            "value": usage['total']
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

last_ts = 0
events = []

if len(sys.argv) < 2:
    print("Missing file argument")
    sys.exit(1)

with open(sys.argv[1]) as sysdig_output:
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

# hierarchy and proc info
for name, container in containers.iteritems():
    for proc_id, proc_info in container['hierarchy'].iteritems():
        exe = container['procs'][str(proc_id)]['name']
        args = ''
        if str(proc_id) in container['commands']:
            exe = container['commands'][str(proc_id)]['exe']
            args = container['commands'][str(proc_id)]['args']
        __cassandra_proc({
            'proc_id': int(proc_id),
            'proc_name': container['procs'][str(proc_id)]['name'],
            'parent_id': container['hierarchy'][proc_id],
            'exe': exe,
            'args': args,
            'container': container['container_id']
        })


# insert_influxdb(merged_containers)
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
