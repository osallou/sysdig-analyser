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

logging.basicConfig(filename='sysdig_analysis.log',level=logging.INFO, format='%(asctime)s %(message)s')


cluster = Cluster()
session = cluster.connect('sysdig')


containers = {}
last_call = {}


class CassandraHandler(object):

    def __init__(self, cassandra_session):
        self.session = cassandra_session
        self.last_time = None
        self.events = {}

    def record(self, event):
        return False


class CassandraMemHandler(CassandraHandler):

    def __init__(self, cassandra_session):
        CassandraHandler.__init__(self, cassandra_session)

    def __cassandra_mem(self, event):
        if not event:
            return
        session.execute(
        """
        UPDATE mem
        SET vm_size = %s,
            vm_rss = %s,
            vm_swap = %s
        WHERE ts=%s AND proc_name=%s AND container=%s
        """,
        (event['vm_size'], event['vm_rss'], event['vm_swap'], event['start'], event['proc'], event['container'])
        )

    def flush(self):
        for container_name, container in self.events.iteritems():
            for proc_id, proc in container.iteritems():
                last_event = {
                    'container': container_name,
                    'proc': proc_id,
                    'start': self.last_time,
                    'vm_size': proc['vm_size'],
                    'vm_rss': proc['vm_rss'],
                    'vm_swap': proc['vm_swap']
                }
                self.__cassandra_mem(last_event)
        self.events = {}

    def record(self, event):
        if self.last_time is None:
            self.last_time =  event['start'] // 1000000000 * 1000
        if self.last_time is None or self.last_time ==  event['start'] // 1000000000 * 1000:
            if event['container'] not in self.events:
                self.events[event['container']] = {}
            if event['proc'] not in self.events[event['container']]:
                self.events[event['container']][event['proc']] = {'vm_size': 0, 'vm_rss': 0, 'vm_swap': 0}
            self.events[event['container']][event['proc']]['vm_size'] = event['vm_size']
            self.events[event['container']][event['proc']]['vm_rss'] = event['vm_rss']
            self.events[event['container']][event['proc']]['vm_swap'] = event['vm_swap']
        else:
            self.flush()
            self.last_time =  event['start'] // 1000000000 * 1000
        return True


class CassandraCpuHandler(CassandraHandler):

    def __init__(self, cassandra_session):
        CassandraHandler.__init__(self, cassandra_session)

    def __cassandra_per_cpu(self, event):
        if not event:
            return
        session.execute(
        """
        UPDATE cpu
        SET duration = duration + %s
        WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
        """,
        (event['duration'], event['start'], int(event['proc']), event['cpu'], event['container'])
        )

    def __cassandra_cpu_all(self, event):
        if not event:
            return
        session.execute(
        """
        UPDATE cpu_all
        SET duration = duration + %s
        WHERE ts=%s AND proc_id=%s and container=%s
        """,
        (event['duration'], event['start'], int(event['proc']), event['container'])
        )
        session.execute(
        """
        UPDATE proc_cpu
        SET cpu = cpu + %s
        WHERE proc_id=%s and container=%s
        """,
        (event['duration'], int(event['proc']), event['container'])
        )

    def __cassandra_cpu(self, event):
        self.__cassandra_per_cpu(event)
        self.__cassandra_cpu_all(event)

    def flush(self):
        for container_name, container in self.events.iteritems():
            for cpu_id, cpu in container.iteritems():
                for proc_id, proc in cpu.iteritems():
                    last_event = {
                        'container': container_name,
                        'proc': proc_id,
                        'cpu': cpu_id,
                        'start': self.last_time,
                        'duration': proc['duration']
                    }
                    self.__cassandra_cpu(last_event)
        self.events = {}

    def record(self, event):
        if self.last_time is None:
            self.last_time =  event['start'] // 1000000000 * 1000
        if self.last_time is None or self.last_time ==  event['start'] // 1000000000 * 1000:
            if event['container'] not in self.events:
                self.events[event['container']] = {}
            if event['cpu'] not in self.events[event['container']]:
                self.events[event['container']][event['cpu']] = {}
            if event['proc'] not in self.events[event['container']][event['cpu']]:
                self.events[event['container']][event['cpu']][event['proc']] = {'duration': 0}
            self.events[event['container']][event['cpu']][event['proc']]['duration'] += event['duration']
        else:
            self.flush()
            self.last_time =  event['start'] // 1000000000 * 1000
        return True

class CassandraIoHandler(CassandraHandler):

    def __init__(self, cassandra_session):
        CassandraHandler.__init__(self, cassandra_session)

    def flush(self):
        for container_name, container in self.events.iteritems():
            for proc_id, proc in container.iteritems():
                for fd_name, fd in proc.iteritems():
                    last_event = {
                        'container': container_name,
                        'proc': proc_id,
                        'name': fd_name,
                        'start': self.last_time,
                        'in': fd['in'],
                        'out': fd['out']
                    }
                    self.__cassandra_io(last_event)
        self.events = {}

    def record(self, event):
        if self.last_time is None:
            self.last_time =  event['start'] // 1000000000 * 1000
        if self.last_time is None or self.last_time ==  event['start'] // 1000000000 * 1000:
            if event['container'] not in self.events:
                self.events[event['container']] = {}
            if event['proc'] not in self.events[event['container']]:
                self.events[event['container']][event['proc']] = {}
            if event['name'] not in self.events[event['container']][event['proc']]:
                self.events[event['container']][event['proc']][event['name']] = {'in': 0, 'out': 0}
            self.events[event['container']][event['proc']][event['name']]['in'] += event['in']
            self.events[event['container']][event['proc']][event['name']]['out'] += event['out']
        else:
            self.flush()
            self.last_time =  event['start'] // 1000000000 * 1000
        return True

    def __cassandra_io(self, event):
        if not event:
            return
        self.session.execute(
        """
        UPDATE io
        SET io_in = io_in + %s,
            io_out = io_out + %s
        WHERE ts=%s AND proc_name=%s AND file_name=%s AND container=%s
        """,
        (event['in'], event['out'], event['start'], event['proc'], event['name'], event['container'])
        )
        self.session.execute(
        """
        UPDATE io_all
        SET io_in = io_in + %s,
            io_out = io_out + %s
        WHERE proc_name=%s AND file_name=%s AND container=%s
        """,
        (event['in'], event['out'], event['proc'], event['name'], event['container'])
        )
        return True





def __cassandra_io(event):
    if not event:
        return
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
    if not event:
        return
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
    __cassandra_per_cpu(event)
    __cassandra_cpu_all(event)


def __cassandra_per_cpu(event):
    if not event:
        return
    session.execute(
    """
    UPDATE cpu
    SET duration = duration + %s
    WHERE ts=%s AND proc_id=%s AND cpu=%s AND container=%s
    """,
    (event['duration'], event['start'] // 1000000000 * 1000, int(event['proc_id']), event['cpu'], event['container'])
    )

def __cassandra_cpu_all(event):
    if not event:
        return
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
    if not event:
        return
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
    length =  __vm_re('size=(\d+)', evt_info)
    #length =  __vm_re('length=(\d+)', evt_info)
    #if not length:
    #    length =  __vm_re('size=(\d+)', evt_info)
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
        last_call[event['evt.cpu']] = event['evt.outputtime']
        return
    if event['container.id'] in [None, 'host']:
        last_call[event['evt.cpu']] = event['evt.outputtime']
        return
    logging.debug(str(event))

    utid = str(event['thread.vtid'])

    if 'container.name' not in event:
        if 'mesos.task.name' in event:
            event['container.name'] = event['mesos.task.name']
        else:
            event['container.name'] = event['container.id']

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


    if event['evt.cpu'] not in containers[event['container.name']]['cpus']:
        containers[event['container.name']]['cpus'][event['evt.cpu']] = {}

    new_thread  = False
    containers[event['container.name']]['procs'][utid] = {'name': event['proc.name']}

    if event['evt.type'] == 'execve':
        containers[event['container.name']]['procs'][utid]['start'] = event['evt.outputtime']
        __cassandra_proc_start(event)



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
            'first': True,
            'cpu_time': 0
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
        #logging.info("StartEvent:"+str(utid)+':'+str(event['evt.num']))
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['proc_name'] = event['proc.name']  # possible fork, keeping same vtid
        containers[event['container.name']]['procs'][utid]['name'] = event['proc.name']

    if 1 == 1:
        containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['first'] = False
        (vm_size, vm_rss, vm_swap) = __vm_info(event['evt.info'])

        if vm_size > 0 or vm_rss > 0 or vm_swap > 0:
            # Update memory info
            prev_mem = {
                'start': event['evt.outputtime'],
                'vm_size': vm_size,
                'vm_rss': vm_rss,
                'vm_swap': vm_swap,
                'proc':  utid,
                'container': event['container.id']
            }
            # __cassandra_mem(prev_mem)
            cmh.record(prev_mem)

        (fd, name) = __fd_info(event['evt.info'])
        if fd and name:
            if fd not in containers[event['container.name']]['fd']:
                containers[event['container.name']]['fd'][fd] = {}
            containers[event['container.name']]['fd'][fd][event['proc.name']] = name
            prev_io = {
                'start': event['evt.outputtime'],
                'start_date': datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000),
                'debug_date': str(datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000)),
                'name': name,
                'length': 0,
                'in': 0,
                'out': 0,
                'proc': utid,
                'container': event['container.id']
            }
            # __cassandra_io(prev_io)
            cih.record(prev_io)

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

                # __cassandra_io(io_event)
                cih.record(io_event)

            if name not in containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd']:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][name] = length
            else:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['fd'][name] += length

        if 1 == 1:
            if event['evt.cpu'] in last_call:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = last_call[event['evt.cpu']]
            else:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = begin_ts
                return
            logging.debug("StartEvent:"+str(utid)+':'+str(event['evt.num'])+':'+str(event['evt.cpu']))
            (vm_size, vm_rss, vm_swap) = __vm_info(event['evt.info'])
            prev_usage = {
                'start': containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
                'start_date': datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000),
                'debug_date': str(datetime.datetime.fromtimestamp(containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000)),
                'duration': event['evt.outputtime'] - containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'],
                'memory': (vm_size, vm_rss, vm_swap),
                'proc_id': utid,
                'proc': utid,
                'container': event['container.id'],
                'cpu': event['evt.cpu']
            }
            containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['usage'].append(prev_usage)
            logging.debug(str(prev_usage))
            # __cassandra_cpu(prev_usage)
            cch.record(prev_usage)

            if (event['evt.type'] == 'switch' or event['evt.type'] == 'procexit'):
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = None
                logging.debug("ResetEvent:"+str(utid)+':'+str(event['evt.num'])+':'+str(event['evt.cpu']))
            else:
                containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] = event['evt.outputtime']

    last_call[event['evt.cpu']] = event['evt.outputtime']
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
                    'memory': (0, 0, 0),
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
                        while usage['start_date'] >= next_ts:
                            current_ts += datetime.timedelta(seconds=merge)
                            next_ts = current_ts + datetime.timedelta(seconds=merge)
                        logging.debug("go to next ts " + str(current_ts))
                    logging.debug('Check if split needed')
                    up_to = datetime.datetime.fromtimestamp((usage['start'] + usage['duration']) // 1000000000)

                    if up_to > next_ts:
                        logging.info('Split event' + str(usage))
                        # Slip event in multiple ones
                        splitted_usages = []
                        nb_elts = ((usage['duration']/ 1000000000) / merge) + 1
                        remaining_duration = usage['duration']
                        ts_ns = time.mktime(next_ts.timetuple()) * 1000000000
                        remaining_duration -= (ts_ns - usage['start'])
                        first_splitted_usage = copy.deepcopy(usage)
                        first_splitted_usage['duration'] = (ts_ns - usage['start'])
                        splitted_usages.append(first_splitted_usage)
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
                        splitted_usage['container'] = container['container_id']
                        splitted_usage['proc'] =  container['procs'][tid]['name']+'(' + tid + ')'
                        # __cassandra_cpu(splitted_usage)
                        splitted_usage['total'] = merge * 1000000000
                        logging.debug("Start: " + str(splitted_usage['start_date']) + " vs " + str(current_ts))
                        logging.debug("Next: " + str(splitted_usage['start_date']) + " vs " + str(next_ts))
                        logging.debug(str(splitted_usage))


                        while splitted_usage['start_date'] >= next_ts:
                            current_ts += datetime.timedelta(seconds=merge)
                            next_ts = current_ts + datetime.timedelta(seconds=merge)
                        if splitted_usage['start_date'] >= current_ts and splitted_usage['start_date'] < next_ts:
                            __cassandra_cpu(splitted_usage)
                            __cassandra_cpu_all(splitted_usage)

                if 1==0 and prev_usage:
                    logging.debug("Remaining, add prev_usage")
                    logging.debug(prev_usage)
                    thread['usage'].append(prev_usage)
                    prev_usage['container'] = container['container_id']
                    __cassandra_cpu(prev_usage)
                    __cassandra_cpu_all(prev_usage)


    return sysdig_containers

last_ts = 0
events = []

if len(sys.argv) < 2:
    print("Missing file argument")
    sys.exit(1)


cmh = CassandraMemHandler(session)
cih = CassandraIoHandler(session)
cch = CassandraCpuHandler(session)

is_js = True
if is_js:
    with open(sys.argv[1]) as sysdig_output:
        events = json.load(sysdig_output)
    if not events:
        sys.exit(0)
    nb_events = len(events)
    begin_ts = events[0]['evt.outputtime']
    end_ts = events[len(events) - 1]['evt.outputtime']
    logging.warn("Start ts: " + str(begin_ts))
    logging.warn("End ts: " + str(end_ts))

    from progressbar import Percentage, ProgressBar, Bar
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=nb_events).start()
    i = 0
    for event in events:
        i += 1
        pbar.update(i)
        sysdig_event(event)
    pbar.finish()

else:
    nb_line = 0
    with open(sys.argv[1]) as sysdig_output:
        for line in sysdig_output:
            nb_line += 1
    from progressbar import Percentage, ProgressBar, Bar
    pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=nb_line).start()
    i = 0
    with open(sys.argv[1]) as sysdig_output:
        first_line = True
        for line in sysdig_output:
            i += 1
            pbar.update(i)
            matches = re.search('^(\d+)\s(\d+\.\d+)\s(\d+)\s([a-zA-Z0-9<>_-]+)\s\(([a-zA-Z0-9<>]+)\)\s([a-zA-Z0-9<>]+)\s\(([a-zA-Z0-9<>]+):([a-zA-Z0-9<>]+)\)\s([<>])\s(\w+)(.*)', line)
            if matches:
                event = {
                    'evt.num': int(matches.group(1)),
                    'evt.outputtime': int(float(matches.group(2)) * 1000000000),
                    'evt.cpu': int(matches.group(3)),
                    'container.name': matches.group(4),
                    'container.id': matches.group(5),
                    'proc.name': matches.group(6),
                    'thread.tid': matches.group(7),
                    'thread.vtid': matches.group(8),
                    'evt.dir': matches.group(9),
                    'evt.type': matches.group(10),
                    'evt.info': matches.group(11)
                }
                if event['container.id'] == '<NA>':
                    event['container.id'] = None
                if event['container.name'] == '<NA>':
                    event['container.name'] = None

                if event['thread.tid'] != '<NA>':
                    event['thread.tid'] = int(event['thread.tid'])
                else:
                    event['thread.tid'] = 0
                if event['thread.vtid'] != '<NA>':
                    event['thread.vtid'] = int(event['thread.vtid'])
                else:
                    event['thread.vtid'] = 0
                if first_line:
                    first_line = False
                    begin_ts = event['evt.outputtime']
                sysdig_event(event)
    pbar.finish()

cmh.flush()
cih.flush()
cch.flush()

'''
close_sysdig_events(containers)

merged_containers = group_by_seconds(containers, 1)
'''
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
