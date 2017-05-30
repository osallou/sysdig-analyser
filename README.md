# About

this work is in progress and should not be used as is.

program reads sysdig events (live) or scap output files and calculate cpu usage, fd accesses etc.. for a container.
result is inserted in cassandra.
The web server records live events from sysdig (with a special chisel) and display container information.

# License

Apache 2.0 (see https://www.apache.org/licenses/LICENSE-2.0)


# Requirements

    python setup.py install

# Live recording of sysdig events

## Api keys

To send events to web server, one need an API key. Script bc_api helps managing api keys

    python bc_api.py create --owner me@mail.com
    Api key EERZERZEZT  <= generated api key

    python bc_api.py delete --api EERZERZEZT

For help and cassandra connection info parameters:

    python bc_api.py  create --help
    python bc_api.py  delete --help

## Manual

Need lua-socket lua module and to define lua path:

    export LUA_PATH="/usr/share/lua/5.1/?.lua"
    export LUA_CPATH="/usr/lib/x86_64-linux-gnu/lua/5.1/?.so"

Custom lua script (sysdigdocker.lua to add to chisels) run

    sysdig -pc -c sysdigdocker http://x.y.z/event/api/<APIKEY> -j
    # x.y.z being the web ui address, possibly load-balanced
    # <APIKEY> is a valid API key


## Docker

You can use Dockerfile (in docker dir to create a sysdig image containing the lua chisel with env already set up)

    docker run -d --restart=always --name=sysdig --privileged=true \
               -v ${PWD}:/mnt/sysdig \
               --volume=/var/run/docker.sock:/host/var/run/docker.sock \
               --volume=/dev:/host/dev \
               --volume=/proc:/host/proc:ro \
               --volume=/boot:/host/boot:ro \
               --volume=/lib/modules:/host/lib/modules:ro \
               --volume=/usr:/host/usr:ro \
               osallou/bubble-chamber sysdig -pc -c sysdigdocker http://x.y.z/event/api/<APIKEY> -j

x.y.z being the web ui address, possibly load-balanced


# Using sysdig records

## To execute sysdig

docker run -it --rm --name=sysdig --privileged=true \
           -v ${PWD}:/mnt/sysdig \
           --volume=/var/run/docker.sock:/host/var/run/docker.sock \
           --volume=/dev:/host/dev \
           --volume=/proc:/host/proc:ro \
           --volume=/boot:/host/boot:ro \
           --volume=/lib/modules:/host/lib/modules:ro \
           --volume=/usr:/host/usr:ro \
           sysdig/sysdig

## Sysdig recording

    sysdig -pc -w myoutput.scap



## Analyse saved file


    sysdig -pc -j  -r test.scap > test.json


then

    python analysis_sysdig test.json


# Web UI

Web UI is used to display container info but also to receive live events from sysdig

## development

    python sysdig_web.py

## production

    rm -rf ..path_to/prometheus-multiproc
    mkdir -p ..path_to/prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/prometheus-multiproc
    gunicorn -c ../path_to/gunicorn_conf.py --bind 0.0.0.0 sysdig_web:app

## in docker

    docker build -t osallou/bubble-web .
    docker run -p 80:8000 -d -e CASSANDRA_HOST="192.168.101.131" -e AUTH_SECRET="XXXX"  osallou/bubble-web gunicorn -c /root/sysdig-analyser/gunicorn_conf.py --bind 0.0.0.0 sysdig_web:app

Optionally add --link to your cassandra docker cluster:

    docker run -p 80:8000 -d -e CASSANDRA_HOST="mycassandra" --link mycassandra:mycassandra -e AUTH_SECRET="XXXX"  osallou/bubble-web gunicorn -c /root/sysdig-analyser/gunicorn_conf.py --bind 0.0.0.0 sysdig_web:app

Other env vars:

 * CASSANDRA_HOST
 * CASSANDRA_CLUSTER
 * AUTH_SECRET (secret to use)
 * AUTH_DISABLE (disable token checks, users can access any container)

=> http://localhost:5000/static/index.html?container=262b281ffa9d&token=XXXXXX

If auth is enabled in config, a token is expected to give access to container data.
A test token can be generated via test_token.py, else your proxy app should generate one before linking to app.

# Cassandra

## Data model

            CREATE KEYSPACE sysdig WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

            USE sysdig;
            CREATE TABLE io (container varchar, io_in counter, io_out counter, ts timestamp, proc_id int, file_name varchar, PRIMARY KEY (container, proc_id, ts, file_name));
            CREATE TABLE io_all (container varchar, io_in counter, io_out counter, proc_id int, file_name varchar, PRIMARY KEY (container, proc_id, file_name));
            CREATE TABLE mem (container varchar,vm_size bigint, vm_rss bigint, vm_swap bigint, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            CREATE TABLE cpu (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu));
            CREATE TABLE cpu_all (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            CREATE TABLE proc (container varchar, start timestamp, end timestamp, proc_name varchar, exe varchar, args varchar, proc_id int, parent_id int, is_root int, PRIMARY KEY (container, proc_id));
            CREATE TABLE proc_cpu (container varchar, cpu counter, proc_id int, PRIMARY KEY (container, proc_id));


            // retention 10h
            CREATE TABLE cpu_per_m (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu));
            CREATE TABLE cpu_all_per_m (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            // retention 24h*30 ~ 1m
            CREATE TABLE cpu_per_h (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu));
            CREATE TABLE cpu_all_per_h (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            // retention 120d ~ 3m
            CREATE TABLE cpu_per_d (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu));
            CREATE TABLE cpu_all_per_d (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));


            CREATE TABLE mem_per_m (container varchar,vm_size bigint, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            CREATE TABLE mem_per_h (container varchar,vm_size bigint, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            CREATE TABLE mem_per_d (container varchar,vm_size bigint, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));


            CREATE TABLE retention(id int, ts timestamp, PRIMARY KEY(id));

            CREATE TABLE api(id varchar, owner varchar, PRIMARY KEY(id));
