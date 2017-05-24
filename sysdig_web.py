from flask import Flask
from flask import request
from flask.json import jsonify
import os
import json
import logging
import sys
import datetime
import time

from bson import json_util

from cassandra.cluster import Cluster


cluster = Cluster()
session = cluster.connect('sysdig')

top_n = 10

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
        UPDATE io
        SET io_in = io_in + %s,
            io_out = io_out + %s
        WHERE ts=%s AND proc_id=%s AND file_name=%s AND container=%s
        """,
        (event['in'], event['out'], event['start'], event['proc'], event['name'], event['container'])
        )
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

@app.route("/container/<cid>/cpu/<proc_id>")
def container_cpu(cid, proc_id):
    interval = request.args.get('interval')
    if interval is None:
        interval = 's'
    return jsonify(__cassandra_select_cpu(cid, proc_id, interval=interval))

@app.route("/container/<cid>/cpu")
def container_cpus(cid):
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
    return jsonify(__cassandra_select_procs(cid))

@app.route("/container/<cid>/mem")
def container_mem(cid):
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
    return jsonify(__cassandra_select_io(cid))

@app.route("/event", methods=['POST'])
def get_event():
    # print(str(request.data))
    content = request.get_json(silent=True)
    if content['evt_type'] == 'fd':
        for data in content['data']:
            # event['in'], event['out'], event['proc'], event['name'], event['container']
            event = {
                'proc': int(data[0]),
                'container': data[2],
                'name': data[3],
                'in': 0,
                'out': 0,
                'start':  long(content['ts'])/1000000
            }
            if data[4] == 'in':
                event['in'] = data[5]
            else:
                event['out'] = data[5]
            __cassandra_update_io(event)
        pass
    elif content['evt_type'] == 'cpu':
        for data in content['data']:
            is_root = 0
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
                'is_root': is_root
            }
            __cassandra_update_procs(event)
            __cassandra_update_cpu(event)
            __cassandra_update_mem(event)
        #(event['duration'], event['start'], int(event['proc']), event['cpu'], event['container'])
        # (event['vm_size'], event['vm_rss'], event['vm_swap'], event['start'], event['proc'], event['container'])
        # (event['parent_id'], event['proc_name'], event['exe'], event['args'], event['proc_id'], event['container'])
    # print(json.dumps(content))
    return "ok"

if __name__ == "__main__":
    app.run(host="0.0.0.0")
