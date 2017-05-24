'''
Script to analyse sysdig output file (text or json formatted).
Text formatted files should have timestamp in absolute time format (-t a option of sysdig)
'''

import os
import json
import logging
import sys
import uuid
import re
import copy
import datetime
import cProfile

from bson import json_util

from cassandra.cluster import Cluster
from progressbar import Percentage, ProgressBar, Bar
import click

logging.basicConfig(filename='sysdig_analysis.log',level=logging.INFO, format='%(asctime)s %(message)s')

# TODO containers will grow over time with each new container
# should store procs info in db and record on change ? (first time and on modification)
# container info, should delete if no event received since XXX seconds ? or on message received when container ends ?

class CassandraHandler(object):
    '''
    Base handler used by specialized handlers
    '''

    def __init__(self, cassandra_session, simu=False):
        self.session = cassandra_session
        self.last_time = None
        self.events = {}
        self.simu = simu

    def record(self, event):
        return False


class CassandraMemHandler(CassandraHandler):
    '''
    Manage memory information of events
    '''

    def __init__(self, cassandra_session, simu=False):
        CassandraHandler.__init__(self, cassandra_session, simu)

    def __cassandra_mem(self, event):
        if not event:
            return
        if self.simu:
            # logging.debug("Record:__cassandra_mem"+str(event))
            return
        self.session.execute(
        """
        UPDATE mem
        SET vm_size = %s,
            vm_rss = %s,
            vm_swap = %s
        WHERE ts=%s AND proc_id=%s AND container=%s
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
            container_ref = None
            if event['container'] not in self.events:
                container_ref = self.events[event['container']] = {}
            else:
                container_ref = self.events[event['container']]
            if event['proc'] not in container_ref:
                container_ref[event['proc']] = {'vm_size': 0, 'vm_rss': 0, 'vm_swap': 0}
            container_ref[event['proc']]['vm_size'] = event['vm_size']
            container_ref[event['proc']]['vm_rss'] = event['vm_rss']
            container_ref[event['proc']]['vm_swap'] = event['vm_swap']
        else:
            self.flush()
            self.last_time =  event['start'] // 1000000000 * 1000
        return True


class CassandraCpuHandler(CassandraHandler):
    '''
    Manage cpu usage of events
    '''

    def __init__(self, cassandra_session, simu=False):
        CassandraHandler.__init__(self, cassandra_session, simu)

    def __cassandra_per_cpu(self, event):
        if not event:
            return
        if self.simu:
            # logging.debug("Record:__cassandra_per_cpu"+str(event))
            return
        self.session.execute(
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
        if self.simu:
            # logging.debug("Record:__cassandra_cpu_all"+str(event))
            return
        self.session.execute(
        """
        UPDATE cpu_all
        SET duration = duration + %s
        WHERE ts=%s AND proc_id=%s and container=%s
        """,
        (event['duration'], event['start'], int(event['proc']), event['container'])
        )
        self.session.execute(
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
            container_ref = None
            if event['container'] not in self.events:
                container_ref = self.events[event['container']] = {}
            else:
                container_ref = self.events[event['container']]
            if event['cpu'] not in container_ref:
                container_ref[event['cpu']] = {}
            if event['proc'] not in self.events[event['container']][event['cpu']]:
                container_ref[event['cpu']][event['proc']] = {'duration': 0}
            container_ref[event['cpu']][event['proc']]['duration'] += event['duration']
        else:
            self.flush()
            self.last_time =  event['start'] // 1000000000 * 1000
        return True

class CassandraIoHandler(CassandraHandler):
    '''
    Manages fd/io information
    '''

    def __init__(self, cassandra_session, simu=False):
        CassandraHandler.__init__(self, cassandra_session, simu)

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
            container_ref = self.events[event['container']]
            if event['proc'] not in container_ref:
                container_ref[event['proc']] = {}
            if event['name'] not in container_ref[event['proc']]:
                container_ref[event['proc']][event['name']] = {'in': 0, 'out': 0}
            container_ref[event['proc']][event['name']]['in'] += event['in']
            container_ref[event['proc']][event['name']]['out'] += event['out']
        else:
            self.flush()
            self.last_time =  event['start'] // 1000000000 * 1000
        return True

    def __cassandra_io(self, event):
        if not event:
            return
        if self.simu:
            # logging.debug("Record:__cassandra_io"+str(event))
            return
        self.session.execute(
        """
        UPDATE io
        SET io_in = io_in + %s,
            io_out = io_out + %s
        WHERE ts=%s AND proc_id=%s AND file_name=%s AND container=%s
        """,
        (event['in'], event['out'], event['start'], event['proc'], event['name'], event['container'])
        )
        self.session.execute(
        """
        UPDATE io_all
        SET io_in = io_in + %s,
            io_out = io_out + %s
        WHERE proc_id=%s AND file_name=%s AND container=%s
        """,
        (event['in'], event['out'], event['proc'], event['name'], event['container'])
        )
        return True


class CassandraProcHandler(CassandraHandler):
    '''
    Manages information about processes
    '''

    def __init__(self, cassandra_session, simu=False):
        CassandraHandler.__init__(self, cassandra_session, simu)


    def record(self, event):
        '''
        Record process info
        '''
        if not event:
            return
        if self.simu:
            logging.debug("Record:record"+str(event))
            return
        self.session.execute(
        """
        UPDATE proc
        SET parent_id = %s,
            proc_name = %s,
            exe=%s,
            args=%s
        WHERE proc_id=%s and container=%s
        """,
        (event['parent_id'], event['proc_name'], event['exe'], event['args'], event['proc'], event['container'])
        )

    def start(self, event, child=None):
        '''
        Record start time of process
        '''
        if self.simu:
            logging.debug("Record:Start"+str(event))
            return
        proc_id = event['thread.vtid']
        if child:
            proc_id = child
        self.session.execute(
        """
        UPDATE proc
        SET proc_name = %s, start = %s
        WHERE proc_id=%s and container=%s
        """,
        (event['proc.name'], event['evt.outputtime'] // 1000000000 * 1000, proc_id, event['container.id'])
        )

    def end(self, event):
        '''
        Record end time of process
        '''
        if self.simu:
            logging.debug("Record:End"+str(event))
            return
        self.session.execute(
        """
        UPDATE proc
        SET proc_name = %s, end = %s
        WHERE proc_id=%s and container=%s
        """,
        (event['proc.name'], event['evt.outputtime'] // 1000000000 * 1000, event['thread.vtid'], event['container.id'])
        )

class SysDigEventManager():
    '''
    Manager for sysdig events
    '''

    def __init__(self, session, begin_ts=None, simu=False):
        self.cmh = CassandraMemHandler(session, simu=simu)
        self.cih = CassandraIoHandler(session, simu=simu)
        self.cch = CassandraCpuHandler(session, simu=simu)
        self.cph = CassandraProcHandler(session, simu=simu)
        self.begin_ts = begin_ts
        self.last_call = {}
        self.containers = {}

        self.pattern_vm = re.compile('(\d+)\svm_rss=(\d+)\svm_swap=(\d+)')
        self.pattern_fd = re.compile('fd=(\d+)\((<\w+>.*?)\)')
        self.pattern_io = re.compile('fd=(\d+)\(.*\)\ssize=(\d+)')

    def json_event(self, line):
        self.process(line)

    def time_info(self, evt_time):
        timeinfo = evt_time.split('.', 1)
        timestamp = int(timeinfo[0]) * 1000000000 + int(timeinfo[1])
        return timestamp

    def get_event(self, matches):
        timestamp = self.time_info(matches[1])
        threads = matches[6][1:][:-1].split(':', 1)
        container_id = matches[4][1:][:-1]
        if container_id == '<NA>':
            container_id = None
        container_name = matches[3]
        if container_name == '<NA>':
            container_name = None
        thread_tid = threads[0]
        if thread_tid != '<NA>':
            thread_tid = int(thread_tid)
        else:
            thread_tid = 0
        thread_vtid = threads[1]
        if thread_vtid != '<NA>':
            thread_vtid = int(thread_vtid)
        else:
            thread_vtid = 0

        event = {
            'evt.num': int(matches[0]),
            'evt.outputtime': timestamp,
            'evt.cpu': int(matches[2]),
            'container.name': container_name,
            'container.id': container_id,
            'proc.name': matches[5],
            'thread.tid': thread_tid,
            'thread.vtid': thread_vtid,
            'evt.dir': matches[7],
            'evt.type': matches[8],
            'evt.info': ' '.join(matches[9:])
        }
        return event


    def text_event(self, line, first_line=False):
        # matches = re.search('^(\d+)\s(\d+\.\d+)\s(\d+)\s([a-zA-Z0-9<>_-]+)\s\(([a-zA-Z0-9<>]+)\)\s([a-zA-Z0-9<>\/\-_:~\.]+)\s\(([a-zA-Z0-9<>]+):([a-zA-Z0-9<>]+)\)\s([<>])\s([a-zA-Z0-9<>]+)(.*)', line)
        matches = line.split(' ')
        if len(matches) >= 9:
            # timeinfo = matches.group(2).split('.')
            # timeinfo = matches[1].split('.', 1)
            # timestamp = int(timeinfo[0]) * 1000000000 + int(timeinfo[1])

            '''
            event = {
                'evt.num': int(matches.group(1)),
                'evt.outputtime': timestamp,
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
            '''
            event = self.get_event(matches)

            if first_line:
                begin_ts = event['evt.outputtime']
                self.begin_ts = begin_ts

            self.process(event)
        else:
            logging.error("No regexp match: "+line)


    def get_memory_handler(self):
        return self.cmh

    def get_io_handler(self):
        return self.cih

    def get_cpu_handler(self):
        return self.cch

    def get_process_handler(self):
        return self.cph

    def flush(self):
        '''
        Flush current second info in database
        '''
        self.cmh.flush()
        self.cih.flush()
        self.cch.flush()

    def __vm_re(self, pattern, evt_info):
        matches = re.search(pattern, evt_info)
        if matches:
            return int(matches.group(1))
        else:
            return 0

    def __re(self, pattern, evt_info):
        matches = re.search(pattern, evt_info)
        if matches:
            return matches.group(1)
        else:
            return None

    def __vm_info(self, evt_info):
        vm_size = 0
        vm_rss = 0
        vm_swap = 0
        '''
        vm_size = self.__vm_re('vm_size=(\d+)', evt_info)
        vm_rss = self.__vm_re('vm_rss=(\d+)', evt_info)
        vm_swap = self.__vm_re('vm_swap=(\d+)', evt_info)
        '''
        is_vm = evt_info.split("vm_size=")
        matches = None
        if len(is_vm) == 2:
            matches = self.pattern_vm.match(is_vm[1])
        if matches:
            vm_size = int(matches.group(1))
            vm_rss = int(matches.group(2))
            vm_swap = int(matches.group(3))


        return (vm_size, vm_rss, vm_swap)

    def __fd_info(self, evt_info):
        # fd = self.__vm_re('fd=(\d+)', evt_info)
        # name =  self.__re('fd=\d+\((<\w+>.*?)\)', evt_info)
        fd = None
        name = None
        matches = self.pattern_fd.match(evt_info)
        if matches:
            res = matches.groups()
            fd = int(res[0])
            if len(res) > 1:
                name = res[1]
        '''
        if name and re.match('^<\w+>$', name):
            return (None, None)
        '''
        '''
        if not name:
            name =  self.__re('name=(.*?)\s+', evt_info)
        if not name:
            name =  self.__re('\(<\w+>(.*?)\)', evt_info)
        '''
        return (fd, name)

    def __io_info(self, evt_info):
        #fd = self.__vm_re('fd=(\d+)', evt_info)
        #length =  self.__vm_re('size=(\d+)', evt_info)
        matches = self.pattern_io.match(evt_info)
        fd = None
        length = None
        if matches:
            fd = int(matches.group(1))
            length = int(matches.group(2))

        if fd and length:
            return (fd, length)
        else:
            return (None, None)

    def __clone_info(self, evt_info):
        res = self.__vm_re('res=(\d+)', evt_info)
        return res

    def __exec_info(self, evt_info):
        exe = self.__re('exe=(\w+)', evt_info)
        args = None
        if exe:
            args = self.__re('args=(.*) tid=', evt_info)
        return (exe, args)

    def proc_update(self):
        try:
            for name, container in self.containers.iteritems():
                for proc_id, proc_info in container['hierarchy'].iteritems():
                    exe = ''
                    if proc_id in container['procs']:
                        exe = container['procs'][proc_id]['name']
                    args = ''
                    if proc_id in container['commands']:
                        exe = container['commands'][proc_id]['exe']
                        args = container['commands'][proc_id]['args']
                    proc_event = {
                        'proc': proc_id,
                        'proc_name': container['procs'][proc_id]['name'],
                        'parent_id': container['hierarchy'][proc_id],
                        'exe': exe,
                        'args': args,
                        'container': container['container_id']
                    }
                    self.cph.record(proc_event)
        except Exception as e:
            logging.error('proc_update error: ' + str(e))


    def process(self, event):
        '''
        Process a sysdig event
        '''
        last_call_ref = self.last_call

        if event['proc.name'] == 'sysdig':
            last_call_ref[event['evt.cpu']] = event['evt.outputtime']
            return
        if event['container.id'] in [None, 'host']:
            last_call_ref[event['evt.cpu']] = event['evt.outputtime']
            return
        # logging.debug(str(event))

        if event['thread.vtid'] is None:
            event['thread.vtid'] = 0

        utid = event['thread.vtid']

        if 'container.name' not in event:
            if 'mesos.task.name' in event:
                event['container.name'] = event['mesos.task.name']
            else:
                event['container.name'] = event['container.id']

        containers_ref = None
        if event['container.name'] not in self.containers:
            containers_ref = self.containers[event['container.name']] = {
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
        else:
            containers_ref = self.containers[event['container.name']]

        containers_ref['pids'][event['thread.tid']] = event['thread.vtid']

        if event['thread.vtid'] == 1 and 1 not in self.containers[event['container.name']]['hierarchy']:
            containers_ref['hierarchy'][1] = None

        is_clone = False
        if event['evt.type'] == 'clone':
            child = self.__clone_info(event['evt.info'])
            parent = event['thread.vtid']
            if child > 0:
                # TODO, if different, record?
                containers_ref['hierarchy'][child] = parent
                # __cassandra_proc_start(event, child=child)
                self.cph.start(event, child=child)


        if event['evt.cpu'] not in containers_ref['cpus']:
            containers_ref['cpus'][event['evt.cpu']] = {}

        containers_ref_cpu = containers_ref['cpus'][event['evt.cpu']]

        new_thread  = False
        containers_ref_proc = containers_ref['procs'][utid] = {'name': event['proc.name']}

        if event['evt.type'] == 'execve':
            containers_ref_proc['start'] = event['evt.outputtime']
            # __cassandra_proc_start(event)
            self.cph.start(event)



        if event['evt.type'] == 'procexit':
            containers_ref_proc['end'] = event['evt.outputtime']
            # __cassandra_proc_end(event)
            self.cph.end(event)


        containers_ref_cpu_utid = None
        if utid not in containers_ref_cpu:
            containers_ref_cpu_utid = containers_ref_cpu[utid] = {
                'proc_name': event['proc.name'],
                'usage': [],
                'last_cpu': None,
                'fd': {},
                'io': [],
                'first': True,
                'cpu_time': 0
            }
            new_thread = True
        else:
            containers_ref_cpu_utid = containers_ref_cpu[utid]

        if event["evt.type"] == "execve":
            # Execute a process, takes proc id
            # TODO could record event with exe/args now....
            # if utid not in ...
            #     record
            # else
            #     if ..[utid] != new exe and args
            #         record
            #     ..[utid] = new exe and args
            (exe, args) = self.__exec_info(event['evt.info'])
            if exe:
                containers_ref['commands'][utid]= {
                    'exe': exe,
                    'args': args
                }
            #logging.info("StartEvent:"+str(utid)+':'+str(event['evt.num']))
            containers_ref_cpu_utid['proc_name'] = event['proc.name']  # possible fork, keeping same vtid
            # TODO if different, record?
            containers_ref['procs'][utid]['name'] = event['proc.name']

        if 1 == 1:
            containers_ref_cpu_utid['first'] = False
            vm_size = 0
            vm_rss = 0
            vm_swap = 0
            if event['evt.type'] == 'switch':
                (vm_size, vm_rss, vm_swap) = self.__vm_info(event['evt.info'])

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
                self.cmh.record(prev_mem)

            (fd, name) = self.__fd_info(event['evt.info'])
            if fd and name:
                if fd not in containers_ref['fd']:
                    containers_ref['fd'][fd] = {}
                containers_ref['fd'][fd][event['proc.name']] = name
                prev_io = {
                    'start': event['evt.outputtime'],
                    # 'start_date': datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000),
                    # 'debug_date': str(datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000)),
                    'name': name,
                    'length': 0,
                    'in': 0,
                    'out': 0,
                    'proc': utid,
                    'container': event['container.id']
                }
                # __cassandra_io(prev_io)
                self.cih.record(prev_io)

            (fd, length) = self.__io_info(event['evt.info'])

            if fd and length:
                name = None
                # logging.debug('#FD: ' + str(fd))
                # logging.debug(str(self.containers[event['container.name']]['fd']))
                if fd in containers_ref['fd'] and event['proc.name'] in containers_ref['fd'][fd]:
                    # logging.debug("#FD FOUND " + str(self.containers[event['container.name']]['fd'][fd][event['proc.name']]))
                    name = containers_ref['fd'][fd][event['proc.name']]
                if fd not in containers_ref_cpu_utid['fd']:
                    containers_ref_cpu_utid['fd'][fd] = {}
                if not name:
                    name = 'fd_unknown'
                if name:
                    if name not in containers_ref_cpu_utid['fd'][fd]:
                        containers_ref_cpu_utid['fd'][fd][name] =0
                    containers_ref_cpu_utid['fd'][fd][name] += length
                    io_event = {
                        'start': event['evt.outputtime'],
                        # 'start_date': datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000),
                        # 'debug_date': str(datetime.datetime.fromtimestamp(event['evt.outputtime'] // 1000000000)),
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
                    self.cih.record(io_event)

                if name not in containers_ref_cpu_utid['fd']:
                    containers_ref_cpu_utid['fd'][name] = length
                else:
                    containers_ref_cpu_utid['fd'][name] += length

            if 1 == 1:
                if event['evt.cpu'] in last_call_ref:
                    containers_ref_cpu_utid['last_cpu'] = last_call_ref[event['evt.cpu']]
                else:
                    containers_ref_cpu_utid['last_cpu'] = self.begin_ts
                    return
                # logging.debug("StartEvent:"+str(utid)+':'+str(event['evt.num'])+':'+str(event['evt.cpu']))
                # (vm_size, vm_rss, vm_swap) = self.__vm_info(event['evt.info'])

                prev_usage = {
                    'start': containers_ref_cpu_utid['last_cpu'],
                    # 'start_date': datetime.datetime.fromtimestamp(self.containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000),
                    # 'debug_date': str(datetime.datetime.fromtimestamp(self.containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['last_cpu'] // 1000000000)),
                    'duration': event['evt.outputtime'] - containers_ref_cpu_utid['last_cpu'],
                    # 'memory': (vm_size, vm_rss, vm_swap),
                    'proc': utid,
                    'container': event['container.id'],
                    'cpu': event['evt.cpu']
                }
                # self.containers[event['container.name']]['cpus'][event['evt.cpu']][utid]['usage'].append(prev_usage)
                # logging.debug(str(prev_usage))
                # __cassandra_cpu(prev_usage)
                if event['evt.type'] != 'switch':
                    # Do not record cpu spent switching
                    self.cch.record(prev_usage)

                if (event['evt.type'] == 'switch' or event['evt.type'] == 'procexit'):
                    containers_ref_cpu_utid['last_cpu'] = None
                    # logging.debug("ResetEvent:"+str(utid)+':'+str(event['evt.num'])+':'+str(event['evt.cpu']))
                else:
                    containers_ref_cpu_utid['last_cpu'] = event['evt.outputtime']

        last_call_ref[event['evt.cpu']] = event['evt.outputtime']
        return





@click.command()
@click.option('--name', help='file name to analyse')
@click.option('--host', help='cassandra host, can specify multiple host', multiple=True)
@click.option('--cluster', default='sysdig', help='cassandra cluster name')
@click.option('--use_json', '-j', is_flag=True, help='Load json file')
@click.option('--use_text', '-t', is_flag=True, help='Load text file')
@click.option('--stdin', '-i', is_flag=True, help='Load stdin (text format only)')
@click.option('--progress', '-p', is_flag=True, help='Show progress')
@click.option('--simu', is_flag=True, help='Do not store in db, just log')
def analyse_events(name, host, cluster, use_json, use_text, stdin, progress, simu):

    if not name and not stdin:
        print("File name missing")
        sys.exit(1)
    if not use_json and not use_text:
        print("Please specify file format (text or json)")
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

    if use_json:
        with open(name) as sysdig_output:
            events = json.load(sysdig_output)
        if not events:
            sys.exit(0)
        nb_events = len(events)
        begin_ts = events[0]['evt.outputtime']
        end_ts = events[len(events) - 1]['evt.outputtime']
        logging.warn("Start ts: " + str(begin_ts))
        logging.warn("End ts: " + str(end_ts))

        pbar = None
        if progress:
            print("Number of events: %d" % nb_events)
            pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=nb_events).start()
        i = 0
        eventManager = SysDigEventManager(session, begin_ts=begin_ts, simu=simu)
        for event in events:
            i += 1
            if pbar is not None:
                pbar.update(i)
            eventManager.json_event(event)
            if i % 100000 == 0:
                eventManager.proc_update()
        if pbar is not None:
            pbar.finish()

    else:
        eventManager = SysDigEventManager(session, simu=simu)
        if stdin:
            first_line = True
            for line in sys.stdin:
                eventManager.text_event(line, first_line=first_line)
                if first_line:
                    first_line = False
        else:
            pbar = None
            if progress:
                nb_line = 0
                print("Loading info....")
                with open(name) as sysdig_output:
                    for line in sysdig_output:
                        nb_line += 1
                print("Number of events: %d" % nb_line)
                pbar = ProgressBar(widgets=[Percentage(), Bar()], maxval=nb_line).start()
            i = 0
            print("Loading events")
            with open(name) as sysdig_output:
                first_line = True
                for line in sysdig_output:
                    i += 1
                    if pbar is not None:
                        pbar.update(i)
                    eventManager.text_event(line, first_line=first_line)
                    if i % 100000 == 0:
                        eventManager.proc_update()
                    if first_line:
                        first_line = False
            if pbar is not None:
                pbar.finish()

    eventManager.flush()

    # hierarchy and proc info
    '''
    logging.debug('Finalize...')
    for name, container in eventManager.containers.iteritems():
        for proc_id, proc_info in container['hierarchy'].iteritems():
            exe = container['procs'][proc_id]['name']
            args = ''
            if proc_id in container['commands']:
                exe = container['commands'][proc_id]['exe']
                args = container['commands'][proc_id]['args']
            proc_event = {
                'proc': proc_id,
                'proc_name': container['procs'][proc_id]['name'],
                'parent_id': container['hierarchy'][proc_id],
                'exe': exe,
                'args': args,
                'container': container['container_id']
            }
            eventManager.get_process_handler().record(proc_event)
    '''


if __name__ == '__main__':
    cProfile.run('analyse_events()')
