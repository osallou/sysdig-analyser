import click
import os
import logging
import string
import random
import sys
import yaml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bubblechamber.model import Base
from bubblechamber.model import File as BCFile
from bubblechamber.model import Process as BCProcess
from bubblechamber.model import Container as BCContainer
from bubblechamber.model import ApiKey as BCApiKey

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


@click.group()
def run():
    pass

@run.command()
@click.option('--api', help='API key')
@click.option('--debug', help="set log level to debug", is_flag=True)
def delete(api, debug):
    if not api:
        print("Api key missing")
        sys.exit(1)

    cfg =  __load_config(debug)
    engine = create_engine(cfg['mysql']['url'], pool_recycle=3600, echo=cfg['mysql'].get('debug', False))
    maker = sessionmaker(bind=engine)
    sqlSession = maker()
    # generate api key
    bc_apikey = sqlSession.query(BCApiKey).filter_by(key=api).first()
    if bc_apikey:
        sqlSession.delete(bc_apikey)
        sqlSession.commit()
    else:
        print("No matching apikey found")
    sqlSession.close()

@run.command()
@click.option('--owner', help='API key user owner')
@click.option('--debug', help="set log level to debug", is_flag=True)
def create(owner, debug):
    if not owner:
        print("Owner missing")
        sys.exit(1)

    cfg =  __load_config(debug)
    engine = create_engine(cfg['mysql']['url'], pool_recycle=3600, echo=cfg['mysql'].get('debug', False))
    maker = sessionmaker(bind=engine)
    sqlSession = maker()
    # generate api key
    key = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
    bc_apikey = BCApiKey(
        key=key,
        email=owner
    )
    sqlSession.add(bc_apikey)
    sqlSession.commit()
    sqlSession.close()
    print("Api key: %s" % (key))
    return key


if __name__ == '__main__':
    run()
