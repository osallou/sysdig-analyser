'''
Manage db init and upgrades
'''
import os
import sys
import logging
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bubblechamber.model import Base
from bubblechamber.model import File as BCFile
from bubblechamber.model import Process as BCProcess
from bubblechamber.model import Container as BCContainer

import click

DB_SCHEMA_VERSION = 1

@click.group()
def run():
    pass

def __load_config(debug):
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

    return config

@run.command()
@click.option('--debug', help="set log level to debug", is_flag=True)
def init(debug):
    cfg =  __load_config(debug)
    engine = create_engine(cfg['mysql']['url'], pool_recycle=3600, echo=cfg['mysql'].get('debug', False))
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    run()
