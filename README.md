# About

this work is in progress and should not be used as is.

program reads sysdig events (live) or scap output files and calculate cpu usage, fd accesses etc.. for a container.
result is inserted in cassandra.
The web server records live events from sysdig (with a special chisel) and display container information.

# License

Apache 2.0 (see https://www.apache.org/licenses/LICENSE-2.0)


# Requirements

    python setup.py install

System: cassandra, rabbitmq, redis


# Installation

## Using docker-compose

    docker-compose up -d


Script launches cassandra, consul, redis and bc web servers, record and clean processes

bc processes may fail to start as they need cassandra to be ready to accept connections.
In this case, wait for cassandra to be ready and execute the command again.

Available env variables:

 * CASSANDRA_HOST=bc-cassandra
 * CASSANDRA_CLUSTER=sysdig
 * RABBITMQ_HOST=bc-rabbitmq
 * RABBITMQ_USER=test
 * RABBITMQ_PASSWORD=XXX
 * REDIS_HOST=bc-redis
 * BC_RETENTION_SECONDS=3600
 * BC_RETENTION_MINUTES=172800
 * BC_RETENTION_HOURS=10368000

BC_RETENTION_XX are the number of seconds to keep in database for a container. Older records will be deleted.
Delete occurs after an event is received. If there is no new record, then last BC_RETENTION_XX are kept.

## Setup

To create or upgrade database schema, use bc_db.py

At first launch:

    python bc_db.py init

For upgrades:

    python bc_db.py upgrade


Use *-h* for options to specify cassandra ip and cluster to use.


## Api keys

To send events to web server, one need an API key. Script bc_api helps managing api keys

    python bc_api.py create --owner me@mail.com
    Api key EERZERZEZT  <= generated api key

    python bc_api.py delete --api EERZERZEZT

For help and cassandra connection info parameters:

    python bc_api.py  create --help
    python bc_api.py  delete --help

# Live recording of sysdig events

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


# Processes

Web UI is used to display container info but also to receive live events from sysdig
Events are then dispatched to bc_record to record events in database then to clean process if old events need to be deleted.

# Background processes

## bc_record

Listen to rabbitmq for sysdig events sent by web process and record events in cassandra (per seconds, minutes, hours and days)
At regular interval, bc_record will send events to bc_clean for old record deletion

## bc_clean

Delete from database old records (older than BC_RETENTION_XX seconds)

# Web UI

## development

    python bc_web.py

## production

    rm -rf ..path_to/prometheus-multiproc
    mkdir -p ..path_to/prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/prometheus-multiproc
    gunicorn -c ../path_to/gunicorn_conf.py --bind 0.0.0.0 bc_web:app

## in docker

    docker build -t osallou/bubble-web .
    docker run -p 80:8000 -d -e CASSANDRA_HOST="192.168.101.131" -e AUTH_SECRET="XXXX"  osallou/bubble-web gunicorn -c /root/sysdig-analyser/gunicorn_conf.py --bind 0.0.0.0 bc_web:app

Optionally add --link to your cassandra docker cluster:

    docker run -p 80:8000 -d -e CASSANDRA_HOST="mycassandra" --link mycassandra:mycassandra -e AUTH_SECRET="XXXX"  osallou/bubble-web gunicorn -c /root/sysdig-analyser/gunicorn_conf.py --bind 0.0.0.0 bc_web:app

Other env vars:

 * CASSANDRA_HOST
 * CASSANDRA_CLUSTER
 * AUTH_SECRET (secret to use)
 * AUTH_DISABLE (disable token checks, users can access any container)

=> http://localhost:5000/static/index.html?container=262b281ffa9d&token=XXXXXX

If auth is enabled in config, a token is expected to give access to container data.
A test token can be generated via test_token.py, else your proxy app should generate one before linking to app.
