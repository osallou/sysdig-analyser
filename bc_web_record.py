from flask import Flask
from flask import request
from flask import Blueprint
from flask.json import jsonify
from flask import g
import os
import json
import logging
import sys
import datetime
import time


import yaml
from bson import json_util
import jwt
from prometheus_client import Counter, Histogram
from prometheus_client.exposition import generate_latest
from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry
# from cassandra.cluster import Cluster
import consul
import pika
import influxdb
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bubblechamber.model import ApiKey as BCApiKey
from bubblechamber.model import Process as BCProcess
from bubblechamber.model import File as BCFile
from bubblechamber.model import Container as BCContainer

FLASK_REQUEST_LATENCY = Histogram('flask_request_latency_seconds', 'Flask Request Latency',
    ['method', 'endpoint'])
FLASK_REQUEST_COUNT = Counter('flask_request_count', 'Flask Request Count',
    ['method', 'endpoint', 'http_status'])

def before_request():
    request.start_time = time.time()

def after_request(response):
    request_latency = time.time() - request.start_time
    FLASK_REQUEST_LATENCY.labels(request.method, request.path).observe(request_latency)
    FLASK_REQUEST_COUNT.labels(request.method, request.path, response.status_code).inc()
    return response

config_file = 'config.yml'
if 'BC_CONFIG' in os.environ:
        config_file = os.environ['BC_CONFIG']

config = {}
if os.path.exists(config_file):
    with open(config_file, 'r') as ymlfile:
        config = yaml.load(ymlfile)

if 'prefix' not in config['web'] or not config['web']['prefix']:
    config['web']['prefix'] = '/'
if 'BC_PREFIX' in os.environ and os.environ['BC_PREFIX']:
    config['web']['prefix'] = os.environ['BC_PREFIX']

cassandra_hosts = ['127.0.0.1']
cassandra_cluster = 'sysdig'
if 'cassandra' in config:
    if 'hosts' in config['cassandra']:
        cassandra_hosts = config['cassandra']['hosts']
    if 'cluster' in config['cassandra']:
        cassandra_cluster = config['cassandra']['cluster']

if 'CASSANDRA_HOST' in os.environ:
    cassandra_hosts = [os.environ['CASSANDRA_HOST']]
if 'CASSANDRA_CLUSTER' in os.environ:
    cassandra_cluster = os.environ['CASSANDRA_CLUSTER']

if 'AUTH_SECRET' in os.environ:
    config['auth']['secret'] = os.environ['AUTH_SECRET']

if 'AUTH_DISABLE' in os.environ:
    config['auth']['enable'] = False

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

if os.environ.get('BC_MYSQL_URL', None):
    config['mysql']['url'] = os.environ['BC_MYSQL_URL']

'''
cluster = Cluster(cassandra_hosts)
session = cluster.connect(cassandra_cluster)
'''

rabbit = 'localhost'
if 'RABBITMQ_HOST' in os.environ:
    rabbit = os.environ['RABBITMQ_HOST']

rabbitmq_user = None
rabbitmq_password = None
if 'RABBITMQ_USER' in os.environ and 'RABBITMQ_PASSWORD' in os.environ:
        rabbitmq_user = os.environ['RABBITMQ_USER']
        rabbitmq_password = os.environ['RABBITMQ_PASSWORD']

db_influx = None
if config['influxdb']['host']:
    host = config['influxdb']['host']
    port = config['influxdb'].get('port', 8086)
    username = config['influxdb']['user']
    password = config['influxdb']['password']
    database = config['influxdb']['db']
db_influx = influxdb.InfluxDBClient(host, port, username, password, database)

connection = None
if rabbitmq_user:
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, credentials=credentials, heartbeat_interval=0))
else:
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, heartbeat_interval=0))

channel = connection.channel()


sql_engine = create_engine(config['mysql']['url'], pool_recycle=3600, echo=config.get('debug', False))
sql_session_maker = sessionmaker(bind=sql_engine)

def __cassandra_load_api():
    res = []
    sql_session = sql_session_maker()
    rows = sql_session.query(BCApiKey).all()
    for row in rows:
        res.append(row.key)
    sql_session.close()
    return res

apikeys = __cassandra_load_api()
# last_check = datetime.datetime.now()


top_n = 10

def consul_declare(config):
    if config['consul']['host']:
        logging.warn("Register to consul")
        consul_agent = consul.Consul(host=config['consul']['host'])
        consul_agent.agent.service.register('bubble-watcher', service_id=config['consul']['id'], address=config['web']['hostname'], port=config['web']['port'], tags=['biomaj'])
        check = consul.Check.http(url='http://' + config['web']['hostname'] + ':' + str(config['web']['port']) + '/ping', interval=60)
        consul_agent.agent.check.register(config['consul']['id'] + '_check', check=check, service_id=config['consul']['id'])

consul_declare(config)

def check_auth(container_id):
    if not config['auth']['enable']:
        return True
    # print(str(request.headers))
    if 'Authorization' not in request.headers:
        return False
    (bearer, token) = request.headers['Authorization'].split(' ')
    if bearer != 'token':
        return False
    data = None
    try:
        data = jwt.decode(token, config['auth']['secret'], audience='urn:cb/stat')
    except Exception:
        return False

    if data and 'container' in data and data['container'] == container_id:
        return True
    return False

def __rabbitmq_send_event(event):
    try:
        channel.basic_publish(
            exchange='',
            routing_key='bc_record',
            body=json.dumps(event),
            properties=pika.BasicProperties(
                # make message persistent
                delivery_mode=2
            ))
    except Exception as e:
        logging.exception('Failed to send record: ' + str(e))


def __influx_query(query):
    result = db_influx.query(query)
    return result.raw

def __select_procs(container):
    result = {'data': []}
    sql_session = sql_session_maker()
    rows = sql_session.query(BCProcess).filter_by(container=container).order_by(BCProcess.last_updated).all()
    for row in rows:
        result['data'].append({
            'id': row.process_id,
            'exe': row.exe,
            'args': row.arguments,
            'name': row.name,
            'parent': row.parent_id,
            'is_root': row.is_root,
            'last_updated': row.last_updated
        })
    sql_session.close()
    return result

def __cassandra_select_mem(container, interval='s', top=10):
    sql_session = sql_session_maker()
    cont = sql_session.query(BCContainer).filter_by(container=container).first()
    cont_last_updated = cont.last_updated
    sql_session.close()
    rows = []
    result = {}
    table = 'cpu';
    table_all = 'cpu_all'
    group_interval = '10s'
    start_time = cont.last_updated - datetime.timedelta(seconds=3600)
    if interval == 'm':
        table = 'cpu_per_m';
        table_all = 'cpu_all_per_m'
        group_interval = '60s'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24)
    if interval == 'h':
        table = 'cpu_per_h';
        table_all = 'cpu_all_per_h'
        group_interval = '1h'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24*2)
    if interval == 'd':
        table = 'cpu_per_d';
        table_all = 'cpu_all_per_d'
        group_interval = '1d'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24*120)

    logging.debug('Query stats after ' + str(start_time))
    start_time = time.mktime(start_time.timetuple()) * 1000000000
    up_to = time.mktime(cont_last_updated.timetuple()) * 1000000000
    query = 'select SUM("bytes") from "bc:container:%s:mem:vm_size" where time >= %d and time <= %d group by time(%s),"proc";' % (container, start_time, up_to, group_interval)
    # logging.error(str(query))
    res = __influx_query(query)
    # logging.error(json.dumps(res))
    series = res.get('series', None)
    top_procs = {}
    tmp_result = {}
    if not series:
        return tmp_result

    for serie in series:
        proc_id = serie['tags']['proc']
        values = serie['values']
        for value in values:
            utc_dt = datetime.datetime.strptime(value[0], '%Y-%m-%dT%H:%M:%SZ')
            timestamp = (utc_dt - datetime.datetime(1970, 1, 1)).total_seconds()

            res = {
                'proc_name': proc_id,
                'ts': timestamp,
                'vm_size': value[1] or 0,
            }

            if proc_id not in top_procs:
                top_procs[proc_id] = 0
            top_procs[proc_id] += res['vm_size']
            if res['proc_name'] not in tmp_result:
                tmp_result[res['proc_name']] = []
            tmp_result[res['proc_name']].append(res)

    proc_array = []
    for proc_id in list(top_procs.keys()):
        proc_array.append({'id': proc_id, 'vm_size': top_procs[proc_id]})
    proc_array.sort(key=lambda x: x['vm_size'], reverse=True)
    if len(proc_array) < top:
        result = tmp_result
    else:
        for proc in proc_array[0:top]:
            result[proc['id']] = tmp_result[proc['id']]
    return result


def __cassandra_select_io_ts(container, proc_id, interval='s', system=False, top=10):
    sql_session = sql_session_maker()
    cont = sql_session.query(BCContainer).filter_by(container=container).first()
    cont_last_updated = cont.last_updated
    sql_session.close()
    group_interval = '10s'
    start_time = cont.last_updated - datetime.timedelta(seconds=3600)
    if interval == 'm':
        group_interval = '60s'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24)
    if interval == 'h':
        group_interval = '1h'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24*2)
    if interval == 'd':
        group_interval = '1d'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24*120)
    result = {}
    # "measurement": "bc:container:" + event['container']+ ":fd:io:in"
    start_time = time.mktime(start_time.timetuple()) * 1000000000
    up_to = time.mktime(cont_last_updated.timetuple()) * 1000000000
    system = True
    if system:
        query = 'select SUM("bytes") from "bc:container:%s:fd:io:total" where time >= %d and time <= %d group by time(%s),"proc";' % (container, start_time, up_to, group_interval)
    else:
        query = 'select SUM("bytes") from "bc:container:%s:fd:io:total" where "system" = 0 and time >= %d and time <= %d group by time(%s),"proc";' % (container, start_time, up_to, group_interval)

    top_procs = {}
    tmp_result = {}
    res = __influx_query(query)
    logging.error(query)
    series = res.get('series', None)
    # logging.error(str(res))
    if not series:
        return {}

    for serie in series:
        logging.error(json.dumps(serie))
        if proc_id and proc_id != serie['tags']['proc']:
            continue
        proc_id = serie['tags']['proc']
        values = serie['values']
        for value in values:
            utc_dt = datetime.datetime.strptime(value[0], '%Y-%m-%dT%H:%M:%SZ')
            timestamp = (utc_dt - datetime.datetime(1970, 1, 1)).total_seconds()

            res = {
                'proc_name': proc_id,
                'ts': timestamp,
                'bytes': value[1] or 0,
            }

            if proc_id not in top_procs:
                top_procs[proc_id] = 0
            top_procs[proc_id] += res['bytes']
            if res['proc_name'] not in tmp_result:
                tmp_result[res['proc_name']] = []
            tmp_result[res['proc_name']].append(res)

    proc_array = []
    for proc_id in list(top_procs.keys()):
        proc_array.append({'id': proc_id, 'io': top_procs[proc_id]})
    proc_array.sort(key=lambda x: x['io'], reverse=True)
    if len(proc_array) < top:
        result = tmp_result
    else:
        for proc in proc_array[0:top]:
            result[proc['id']] = tmp_result[proc['id']]
    return result

def __cassandra_select_io(container, proc_id=None, interval=None, system=False, top=10):
    result = {}
    if interval is None:
        sql_session = sql_session_maker()
        rows = sql_session.query(BCFile).filter_by(container=container).order_by(BCFile.last_updated).all()
        for row in rows:
            if proc_id and proc_id!=row.proc_id:
                continue
            if row.process_id not in result:
                result[row.process_id] = []
            result[row.process_id].append({
                'file_name': row.name,
                'proc_id': row.process_id,
                'io_in': row.io_in,
                'io_out': row.io_out,
                'io_total': row.io_total,
                'last_updated': row.last_updated
            })
        sql_session.close()
    else:
        result = __cassandra_select_io_ts(container, proc_id, interval, system, top)
    return result

def __cassandra_select_cpu(container, proc_id=None, interval='s', top=10):
    sql_session = sql_session_maker()
    cont = sql_session.query(BCContainer).filter_by(container=container).first()
    cont_last_updated = cont.last_updated
    sql_session.close()
    rows = []
    result = {}
    group_interval = '10s'
    start_time = cont.last_updated - datetime.timedelta(seconds=3600)
    if interval == 'm':
        group_interval = '60s'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24)
    if interval == 'h':
        group_interval = '1h'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24*2)
    if interval == 'd':
        group_interval = '1d'
        start_time = datetime.datetime.now() - datetime.timedelta(seconds=3600*24*120)

    if proc_id:
        start_time = time.mktime(start_time.timetuple()) * 1000000000
        up_to = time.mktime(cont_last_updated.timetuple()) * 1000000000
        query = 'select SUM("duration") from "bc:container:%s:cpu:duration" where "proc" = \'%s\' and time >= %d and time <= %d group by time(%s);' % (container, str(proc_id), start_time, up_to, group_interval)
        # logging.error(str(query))
        res = __influx_query(query)
        series = res.get('series', None)
        # logging.error(str(res))
        if not series:
            return {}
        for serie in series:
            values = serie['values']
            for value in values:
                utc_dt = datetime.datetime.strptime(value[0], '%Y-%m-%dT%H:%M:%SZ')
                timestamp = (utc_dt - datetime.datetime(1970, 1, 1)).total_seconds()
                res = {
                    'proc_name': proc_id,
                    'ts': timestamp,
                    'duration': value[1] or 0,
                }

                if res['proc_name'] not in result:
                    result[res['proc_name']] = []
                result[res['proc_name']].append(res)
    else:
        logging.debug('Query stats after ' + str(start_time))
        start_time = time.mktime(start_time.timetuple()) * 1000000000
        up_to = time.mktime(cont_last_updated.timetuple()) * 1000000000
        query = 'select SUM("duration") from "bc:container:%s:cpu:duration" where time >= %d and time <= %d group by time(%s),"proc";' % (container, start_time, up_to, group_interval)
        # logging.error(str(query))
        res = __influx_query(query)
        # logging.error(json.dumps(res))
        series = res.get('series', None)
        top_procs = {}
        tmp_result = {}
        if not series:
            return tmp_result

        for serie in series:
            proc_id = serie['tags']['proc']
            values = serie['values']
            for value in values:
                utc_dt = datetime.datetime.strptime(value[0], '%Y-%m-%dT%H:%M:%SZ')
                timestamp = (utc_dt - datetime.datetime(1970, 1, 1)).total_seconds()

                res = {
                    'proc_name': proc_id,
                    'ts': timestamp,
                    'duration': value[1] or 0,
                }

                if proc_id not in top_procs:
                    top_procs[proc_id] = 0
                top_procs[proc_id] += res['duration']
                if res['proc_name'] not in tmp_result:
                    tmp_result[res['proc_name']] = []
                tmp_result[res['proc_name']].append(res)

        proc_array = []
        for proc_id in list(top_procs.keys()):
            proc_array.append({'id': proc_id, 'duration': top_procs[proc_id]})
        proc_array.sort(key=lambda x: x['duration'], reverse=True)
        if len(proc_array) < top:
            result = tmp_result
        else:
            for proc in proc_array[0:top]:
                result[proc['id']] = tmp_result[proc['id']]
    return result

app = Flask(__name__)
app.before_request(before_request)
app.after_request(after_request)
#logging.warn("Using prefix " + str(config['web']['prefix']))
#bp = Blueprint('frontend', __name__)
#app.register_blueprint(bp, url_prefix=config['web']['prefix'])

@app.route('/ping', methods=['GET'])
def ping():
    logging.warn(str(app.url_map))

    return jsonify({'msg': 'pong'})


@app.route('/metrics', methods=['GET'])
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return generate_latest(registry)

@app.route("/container/<cid>/cpu/<proc_id>")
def container_cpu(cid, proc_id):
    res = check_auth(cid)
    if not res:
        return "not authorized", 401
    interval = request.args.get('interval')
    if interval is None:
        interval = 's'
    return jsonify(__cassandra_select_cpu(cid, proc_id, interval=interval))

@app.route("/container/<cid>/cpu")
def container_cpus(cid):
    res = check_auth(cid)
    if not res:
        return "not authorized", 401
    interval = request.args.get('interval')
    if interval is None:
        interval = 's'
    top_res = request.args.get('top')
    if top_res is None:
        top_res = top_n
    else:
        top_res = int(top_res)
    return jsonify(__cassandra_select_cpu(cid, interval=interval, top=top_res))


@app.route("/container/<cid>/proc")
def container_procs(cid):
    res = check_auth(cid)
    if not res:
        return "not authorized", 401
    return jsonify(__select_procs(cid))

@app.route("/container/<cid>/mem")
def container_mem(cid):
    res = check_auth(cid)
    if not res:
        return "not authorized", 401
    interval = request.args.get('interval')
    if interval is None:
        interval = 's'
    top_res = request.args.get('top')
    if top_res is None:
        top_res = top_n
    else:
        top_res = int(top_res)
    return jsonify(__cassandra_select_mem(cid, interval=interval, top=top_res))


@app.route("/container/<cid>/io")
def container_io(cid):
    res = check_auth(cid)
    if not res:
        return "not authorized", 401
    interval = request.args.get('interval')
    system = request.args.get('system')
    if system == 'true':
        system = True
    else:
        system = False
    top_res = request.args.get('top')
    if top_res is None:
        top_res = top_n
    else:
        top_res = int(top_res)
    return jsonify(__cassandra_select_io(cid, interval=interval, system=system, top=top_res))
    return jsonify({})


@app.route("/event", methods=['POST'])
def get_event_auth_header():
    api = None
    if 'Authorization' in request.headers:
        (bearer, api) = request.headers['Authorization'].split(' ')
    if api is None:
        return "Not authorized", 401
    return get_event(api)

@app.route("/event/api/<api>", methods=['POST'])
def get_event(api):
    if config['auth'].get('skip', False) is True:
        if api not in apikeys:
            apikeys.append(api)
    if api not in apikeys:
        # check if reload necessary
        new_check = datetime.datetime.now()
        # if last check > 60s
        last_check = g.get('last_check', None)
        if last_check is None or last_check < new_check - datetime.timedelta(seconds=60):
            logging.debug("Reload api keys")
            g.last_check = new_check
            # last_check = new_check
            __cassandra_load_api()
        if api not in apikeys:
            logging.warn("InvalidApiKey:%s" % (str(api)))
            return "invalid api key", 401
    # print(str(request.data))
    content = request.get_json(silent=True)
    __rabbitmq_send_event({'event': content})
    return "ok"

if __name__ == "__main__":
    bc_debug = False
    if 'BC_DEBUG' in os.environ:
        bc_debug = True
    logging.warn(str(app.url_map))
    app.run(host="0.0.0.0", debug=bc_debug)
