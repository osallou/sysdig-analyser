import os
import json
import logging
import sys
import uuid
import re
import copy
import datetime
import time
import yaml

from bson import json_util

import pika
from progressbar import Percentage, ProgressBar, Bar
import click

import influxdb

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bubblechamber.model import File as BCFile
from bubblechamber.model import Process as BCProcess
from bubblechamber.model import Container as BCContainer

@click.group()
def run():
    pass


class RetentionHandler(object):

    def __init__(self, cfg):
        self.cfg = cfg
        self.engine = create_engine(self.cfg['mysql']['url'], pool_recycle=3600, echo=self.cfg['mysql'].get('debug', False))
        self.SqlSession = sessionmaker(bind=self.engine)
        self.db_influx = None
        if self.cfg['influxdb']['host']:
            host = self.cfg['influxdb']['host']
            port = self.cfg['influxdb'].get('port', 8086)
            username = self.cfg['influxdb']['user']
            password = self.cfg['influxdb']['password']
            database = self.cfg['influxdb']['db']
            self.db_influx = influxdb.InfluxDBClient(host, port, username, password, database)

    def __add_influx(self, data):
        try:
            self.db_influx.write_points(data)
        except Exception as e:
            # Do not fail on stat writing
            logging.exception('Stat:Error:' + str(e))

    def __add_cpu_mem(self, event):
        session = self.SqlSession()
        '''
            container = Column(String(64), primary_key=True)
            process_id = Column(Integer, primary_key=True)
            name = Column(String(100))
            arguments = Column(String(256))
            parent_id = Column(Integer)
        '''
        bc_proc = BCProcess(
            container=event['container'],
            process_id=event['proc'],
        )

        bc_proc = session.merge(bc_proc)
        bc_proc.name = event['proc_name']
        bc_proc.exe = event['exe']
        bc_proc.arguments = event['args']
        bc_proc.parent_id = event['parent_id']
        bc_proc.is_root = event['is_root']
        bc_proc.last_updated = datetime.datetime.now()

        bc_container = BCContainer(container=event['container'])
        bc_container = session.merge(bc_container)
        bc_container.last_updated = datetime.datetime.now()

        session.commit()
        session.close()
        # add influx cpu and mem
        points = [
            {
                "measurement": "bc:container:" + event['container']+ ":mem:vm_size",
                "tags": {
                    "proc": event['proc']
                },
                "time": event['ts'],
                "fields": {
                    "bytes": event['vm_size'],
                }
            },
            {
                "measurement": "bc:container:" + event['container']+ ":cpu:duration",
                "tags": {
                    "proc": event['proc']
                },
                "time": event['ts'],
                "fields": {
                    "duration": event['duration'],
                }
            }
        ]
        self.__add_influx(points)

    def __add_fd(self, event):
        session = self.SqlSession()
        bc_file = BCFile(
            container=event['container'],
            process_id=event['proc'],
            name=event['name']
        )
        '''
        event = {
            'proc': int(data[0]),
            'container': data[2],
            'name': data[3],
            'in': 0,
            'out': 0,
            'in_out': 0
            'start':  long(content['ts'])/1000000
        '''

        bc_file = session.merge(bc_file)
        bc_file.io_in = BCFile.io_in + event['in']
        bc_file.io_out = BCFile.io_out + event['out']
        bc_file.io_total = BCFile.io_total + event['in_out']
        is_system = 0
        if bc_file.name.startswith('/etc') or bc_file.name.startswith('/usr') or bc_file.name.startswith('/lib'):
            is_system = 1

        session.commit()
        session.close()

        # Then add to influx io global streams
        points = [
            {
                "measurement": "bc:container:" + event['container']+ ":fd:io:in",
                "tags": {
                    "proc": event['proc'],
                    "system": is_system
                },
                "time": event['ts'],
                "fields": {
                    "bytes": event['in'],
                }
            },
            {
                "measurement": "bc:container:" + event['container']+ ":fd:io:out",
                "tags": {
                    "proc": event['proc']
                },
                "time": event['ts'],
                "fields": {
                    "bytes": event['out'],
                }
            },
            {
                "measurement": "bc:container:" + event['container']+ ":fd:io:total",
                "tags": {
                    "proc": event['proc']
                },
                "time": event['ts'],
                "fields": {
                    "bytes": event['in'] + event['out'],
                }
            }
        ]
        self.__add_influx(points)

    def __get_retention_interval(self, retention):
        '''
        Return timestamp (ms) interval before which events should be deleted
        '''
        up_to = None
        retention_seconds = None
        if retention == 's':
            # 1h
            retention_seconds = 3600
        elif retention == 'm':
            # 2d
            retention_seconds = 3600 * 24 * 2
        elif retention == 'h':
            # 120d
            retention_seconds = 3600 * 24 * 120
        if retention_seconds is None:
            return None
        up_to = datetime.datetime.now() - datetime.timedelta(seconds=retention_seconds)
        return int(time.mktime(up_to.timetuple())*1000)


    def __get_ts(self, event_date):
        return int(time.mktime(event_date.timetuple())*1000)

    def is_cgroup(self, data_cgroup):
        if data_cgroup.startswith('/'):
            return True
        else:
            return False

    def is_docker(self, data_cpuset):
        if '/docker' in data_cpuset:
            return True
        return False

    def is_slurm(self, data_cpuset):
        if '/slurm' in data_cpuset:
            cpuset = data_cpuset.split('/')
            for fsg in cpuset:
                if fsg.startswith('job_'):
                    return (True, fsg)
        return (False, None)

    def callback_record(self, ch, method, properties, body):
        try:
            rt = json.loads(body)
            content = rt['event']
            logging.debug('Message: %s' % (content))
            if content is None or 'evt_type' not in content:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            if content['evt_type'] == 'fd':
                for data in content['data']:
                    is_cgroup = self.is_cgroup(data[2])
                    if is_cgroup:
                        if self.is_docker(data[2]):
                            continue
                        (is_slurm, cgroup) = self.is_slurm(data[2])
                        if is_slurm:
                            data[2] = cgroup
                    # event['in'], event['out'], event['proc'], event['name'], event['container']
                    event = {
                        'proc': int(data[0]),
                        'container': data[2],
                        'name': data[3],
                        'in': 0,
                        'out': 0,
                        'in_out': 0,
                        'start':  long(content['ts'])/1000000,
                        'ts': long(content['ts'])
                    }
                    if data[4] == 'in':
                        event['in'] = data[5]
                    else:
                        event['out'] = data[5]
                    event['in_out'] += data[5]
                    self.__add_fd(event)

            elif content['evt_type'] == 'cpu':
                for data in content['data']:
                    is_cgroup = self.is_cgroup(data[5])
                    if is_cgroup:
                        if self.is_docker(data[5]):
                            continue
                        (is_slurm, cgroup) = self.is_slurm(data[5])
                        if is_slurm:
                            data[5] = cgroup

                    is_root = 0
                    if is_cgroup and is_slurm and 'slurm_script' in data[7]:
                        is_root = 1

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
                        'is_root': is_root,
                        'ts': long(content['ts'])
                    }
                    logging.debug('Record event, container=%s, ts=%s' % (event['container'], str(event['start'])))
                    self.__add_cpu_mem(event)

        except Exception as e:
            logging.exception("Failed to handle retention query: " + str(e))
        finally:
            ch.basic_ack(delivery_tag=method.delivery_tag)


@run.command()
@click.option('--debug', help="set log level to debug", is_flag=True)
def listen(debug):
    '''
    For rabbitmq credentials MUST use env variables RABBITMQ_USER and RABBITMQ_PASSWORD
    '''
    config_file = 'config.yml'
    if 'BC_CONFIG' in os.environ:
            config_file = os.environ['BC_CONFIG']

    config = {}
    if os.path.exists(config_file):
        with open(config_file, 'r') as ymlfile:
            config = yaml.load(ymlfile)

    if debug:
        config['debug'] = True
        logging.basicConfig(level=logging.DEBUG)

    if os.environ.get('BC_MYSQL_URL', None):
        config['mysql']['url'] = os.environ['BC_MYSQL_URL']

    if 'RABBITMQ_HOST' in os.environ:
        config['rabbitmq']['host'] = os.environ['RABBITMQ_HOST']

    if 'RABBITMQ_USER' in os.environ and 'RABBITMQ_PASSWORD' in os.environ:
            config['rabbitmq']['user'] = os.environ['RABBITMQ_USER']
            config['rabbitmq']['password'] = os.environ['RABBITMQ_PASSWORD']

    if 'INFLUXDB_HOST' in os.environ:
        config['influxdb']['host'] = os.environ['INFLUXBD_HOST']
    if 'INFLUXDB_PORT' in os.environ:
        config['influxdb']['port'] = int(os.environ['INFLUXBD_PORT'])
    if 'INFLUXDB_DB' in os.environ:
        config['influxdb']['db'] = os.environ['INFLUXBD_DB']
    if 'INFLUXDB_USER' in os.environ:
        config['influxdb']['user'] = os.environ['INFLUXBD_HOST']
    if 'INFLUXDB_PASSWORD' in os.environ:
        config['influxdb']['password'] = os.environ['INFLUXBD_PASSWORD']

    rtHandler = RetentionHandler(config)

    connection = None
    if config['rabbitmq']['user']:
        credentials = pika.PlainCredentials(config['rabbitmq']['user'], config['rabbitmq']['password'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['rabbitmq']['host'], credentials=credentials, heartbeat_interval=0))
    else:
        connection = pika.BlockingConnection(pika.ConnectionParameters(config['rabbitmq']['host'], heartbeat_interval=0))
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
