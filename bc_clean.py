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
import sys

from bson import json_util

import redis
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

        redis_host = self.cfg['redis']['host']
        redis_port = self.cfg['redis'].get('port', 6379)
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)

    def __delete_container_stats(self, sql_session, container):
        sql_session.delete(container)
        #procs = sql_session.query(BCProcess).filter_by(container=container.container).all()
        #for proc in procs:
        #    logging.error(proc)
        sql_session.query(BCProcess).filter_by(container=container.container).delete(synchronize_session='fetch')
        sql_session.query(BCFile).filter_by(container=container.container).delete(synchronize_session='fetch')
        sql_session.commit()
        measurements = [
            "bc:container:" + container.container+ ":cpu:duration",
            "bc:container:" + container.container+ ":mem:vm_size",
            "bc:container:" + container.container+ ":fd:io:in",
            "bc:container:" + container.container+ ":fd:io:out",
            "bc:container:" + container.container+ ":fd:io:total"
        ]
        for measure in measurements:
            try:
                self.db_influx.delete_series(self.cfg['influxdb']['db'], measurement=measure)
            except Exception  as e:
                logging.exception("Failed to delete %s: %s" % (measure, str(e)))

    def delete_old(self, days):
        logging.warn('Delete all container stats older than %d days' % (days))
        now = datetime.datetime.now() - datetime.timedelta(days=days)
        sql_session = self.SqlSession()
        #sql_session.query(BCFile).filter_by(container=container).order_by(BCFile.last_updated).all()
        containers = sql_session.query(BCContainer).filter(BCContainer.last_updated < now).all()
        count = 0
        for container in containers:
            logging.info("Delete container stats: "+str(container.container))
            self.__delete_container_stats(sql_session, container)
            count += 1
        sql_session.close()
        logging.info("Deleted %d containers" % (count))

    def delete_container(self, cid):
        logging.warn('Delete container %s stats ' % (cid))
        sql_session = self.SqlSession()
        #sql_session.query(BCFile).filter_by(container=container).order_by(BCFile.last_updated).all()
        container = sql_session.query(BCContainer).filter_by(container=cid).first()
        if container:
            self.__delete_container_stats(sql_session, container)
        else:
            logging.error('did not found container %s' % (cid))
        sql_session.close()

@run.command()
@click.option('--days', help='delete container stats older then X days')
@click.option('--daemon', help="run indefinitely", is_flag=True)
@click.option('--container', help='id of container to delete')
@click.option('--debug', help="set log level to debug", is_flag=True)
def clean(days, daemon, container, debug):
    config_file = 'config.yml'
    if 'BC_CONFIG' in os.environ:
            config_file = os.environ['BC_CONFIG']

    config = {}
    if os.path.exists(config_file):
        with open(config_file, 'r') as ymlfile:
            config = yaml.load(ymlfile)

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if os.environ.get('BC_MYSQL_URL', None):
        config['mysql']['url'] = os.environ['BC_MYSQL_URL']

    if 'RABBITMQ_HOST' in os.environ:
        config['rabbitmq']['host'] = os.environ['RABBITMQ_HOST']

    if 'RABBITMQ_USER' in os.environ and 'RABBITMQ_PASSWORD' in os.environ:
            config['rabbitmq']['user'] = os.environ['RABBITMQ_USER']
            config['rabbitmq']['password'] = os.environ['RABBITMQ_PASSWORD']
    if 'REDIS_HOST' in os.environ:
            config['redis']['host'] = os.environ['REDIS_HOST']
    if 'REDIS_PORT' in os.environ:
            config['redis']['host'] = int(os.environ['REDIS_PORT'])

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
    if days and container:
        print("select days OR container options")
        sys.exit(1)

    if days:
        rtHandler.delete_old(int(days))
        if daemon:
            print("Run in daemon mode, check every 24h")
            while True:
                # sleep 24h
                time.sleep(3600*24)
                rtHandler.delete_old(int(days))

    if container:
        rtHandler.delete_container(container)


if __name__ == '__main__':
    run()
