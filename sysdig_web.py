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


def __cassandra_select_procs(container):
    result = {'data': []}
    rows = session.execute("SELECT * FROM proc WHERE container='"+container+"'");
    for row in rows:
        result['data'].append({
            'id': row.proc_id,
            'exe': row.exe,
            'args': row.args,
            'name': row.proc_name,
            'parent': row.parent_id
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

if __name__ == "__main__":
    app.run()
