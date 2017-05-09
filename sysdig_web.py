from flask import Flask
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

def __cassandra_select_mem(container):
    result = {}
    rows = session.execute("SELECT * FROM mem WHERE container='"+container+"'");
    for row in rows:
        if row.proc_name not in result:
            result[row.proc_name] = []
        result[row.proc_name].append({
            'ts': time.mktime(row.ts.timetuple()),
            'vm_size': row.vm_size,
            'proc_name': row.proc_name
        })
    return result

def __cassandra_select_io(container, proc_name=None):
    result = {}
    rows = session.execute("SELECT * FROM io_all WHERE container='"+container+"'");
    for row in rows:
        if proc_name and proc_name!=row.proc_name:
            continue
        if row.proc_name not in result:
            result[row.proc_name] = []
        result[row.proc_name].append({
            'file_name': row.file_name,
            'proc_id': row.proc_name,
            'io_in': row.io_in,
            'io_out': row.io_out
        })
    return result

def __cassandra_select_cpu(container, proc_id=None):
    rows = []
    result = {}
    if proc_id:
        rows = session.execute("SELECT * FROM cpu WHERE container='"+container+"' AND proc_id="+str(proc_id))
    else:
        rows = session.execute("SELECT * FROM cpu_all WHERE container='"+container+"'")
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
    return result

app = Flask(__name__)

@app.route("/container/<cid>/cpu/<proc_id>")
def container_cpu(cid, proc_id):
    return jsonify(__cassandra_select_cpu(cid, proc_id))

@app.route("/container/<cid>/cpu")
def container_cpus(cid):
    return jsonify(__cassandra_select_cpu(cid))

@app.route("/container/<cid>/proc")
def container_procs(cid):
    return jsonify(__cassandra_select_procs(cid))

@app.route("/container/<cid>/mem")
def container_mem(cid):
    return jsonify(__cassandra_select_mem(cid))

@app.route("/container/<cid>/io")
def container_io(cid):
    return jsonify(__cassandra_select_io(cid))

if __name__ == "__main__":
    app.run()
