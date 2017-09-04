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
from cassandra.cluster import Cluster
import consul

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


cluster = Cluster(cassandra_hosts)
session = cluster.connect(cassandra_cluster)


rabbit = 'localhost'
if 'RABBITMQ_HOST' in os.environ:
    rabbit = os.environ['RABBITMQ_HOST']

rabbitmq_user = None
rabbitmq_password = None
if 'RABBITMQ_USER' in os.environ and 'RABBITMQ_PASSWORD' in os.environ:
        rabbitmq_user = os.environ['RABBITMQ_USER']
        rabbitmq_password = os.environ['RABBITMQ_PASSWORD']

connection = None
if rabbitmq_user:
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, credentials=credentials, heartbeat_interval=0))
else:
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit, heartbeat_interval=0))

channel = connection.channel()

def __cassandra_load_api():
    res = []
    rows = session.execute("SELECT * FROM api");
    for row in rows:
        res.append(row.id)
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

def __cassandra_update_procs(event):
        '''
        Record process info
        '''
        if not event:
            return
        if 'is_root' not in event:
            event['is_root'] = 0

        session.execute(
        """
        UPDATE proc
        SET parent_id = %s,
            proc_name = %s,
            exe=%s,
            args=%s,
            is_root=%s
        WHERE proc_id=%s and container=%s
        """,
        (event['parent_id'], event['proc_name'], event['exe'], event['args'],  event['is_root'], event['proc'], event['container'])
        )

def __cassandra_update_io(event):
        session.execute(
        """
        UPDATE io_all
        SET io_in = io_in + %s,
            io_out = io_out + %s
        WHERE proc_id=%s AND file_name=%s AND container=%s
        """,
        (event['in'], event['out'], event['proc'], event['name'], event['container'])
        )

def __cassandra_update_per_cpu(event):
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

def __cassandra_update_cpu_all(event):
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

def __cassandra_update_cpu(event):
        __cassandra_update_per_cpu(event)
        __cassandra_update_cpu_all(event)
        if event:
            session.execute(
            'UPDATE container SET status=3 WHERE id=\'' + event['container'] + '\''
            )

def __cassandra_update_mem(event):
        if not event:
            return
        session.execute(
        """
        UPDATE mem
        SET vm_size = %s,
            vm_rss = %s,
            vm_swap = %s
        WHERE ts=%s AND proc_id=%s AND container=%s
        """,
        (event['vm_size'], event['vm_rss'], event['vm_swap'], event['start'], event['proc'], event['container'])
        )



def __cassandra_select_procs(container):
    result = {'data': []}
    rows = session.execute("SELECT * FROM proc WHERE container='"+container+"'");
    for row in rows:
        result['data'].append({
            'id': row.proc_id,
            'exe': row.exe,
            'args': row.args,
            'name': row.proc_name,
            'parent': row.parent_id,
            'is_root': row.is_root
        })
    return result

def __cassandra_select_mem(container, interval='s', top=10):
    table = 'mem';
    if interval == 'm':
        table = 'mem_per_m';
    if interval == 'h':
        table = 'mem_per_h';
    if interval == 'd':
        table = 'mem_per_d';
    result = {}
    rows = session.execute("SELECT * FROM " + table + " WHERE container='"+container+"'");


    top_procs = {}
    tmp_result = {}
    for row in rows:
        res = {
            'ts': time.mktime(row.ts.timetuple()),
            'vm_size': row.vm_size,
            'proc_name': row.proc_id
        }
        if row.proc_id not in top_procs:
            top_procs[row.proc_id] = 0
        top_procs[row.proc_id] += row.vm_size
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

def __cassandra_select_io(container, proc_id=None):
    result = {}
    rows = session.execute("SELECT * FROM io_all WHERE container='"+container+"'");
    for row in rows:
        if proc_id and proc_id!=row.proc_id:
            continue
        if row.proc_id not in result:
            result[row.proc_id] = []
        result[row.proc_id].append({
            'file_name': row.file_name,
            'proc_id': row.proc_id,
            'io_in': row.io_in,
            'io_out': row.io_out
        })
    return result

def __cassandra_select_cpu(container, proc_id=None, interval='s', top=10):
    rows = []
    result = {}
    table = 'cpu';
    table_all = 'cpu_all'
    if interval == 'm':
        table = 'cpu_per_m';
        table_all = 'cpu_all_per_m'
    if interval == 'h':
        table = 'cpu_per_h';
        table_all = 'cpu_all_per_h'
    if interval == 'd':
        table = 'cpu_per_d';
        table_all = 'cpu_all_per_d'
    if proc_id:
        rows = session.execute("SELECT * FROM " + table + " WHERE container='"+container+"' AND proc_id="+str(proc_id))
        for row in rows:
            res = {
                'proc_name': row.proc_id,
                'ts': time.mktime(row.ts.timetuple()),
                'duration': row.duration,
            }
            if proc_id:
                res['cpu'] = row.cpu
            if res['proc_name'] not in result:
                result[res['proc_name']] = []

            result[res['proc_name']].append(res)
    else:
        rows = session.execute("SELECT * FROM " + table_all + " WHERE container='"+container+"'")
        top_procs = {}
        tmp_result = {}
        for row in rows:
            res = {
                'proc_name': row.proc_id,
                'ts': time.mktime(row.ts.timetuple()),
                'duration': row.duration,
            }
            if row.proc_id not in top_procs:
                top_procs[row.proc_id] = 0
            top_procs[row.proc_id] += row.duration
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
    return jsonify(__cassandra_select_procs(cid))

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
    return jsonify(__cassandra_select_io(cid))


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
