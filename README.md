# About

Bubble-chamber records processes usage (cpu, ram, io) inside containers or slurm cgroup jobs, using Sysdig and specific chisels.
It provides a live and past view of container usage, on a per process basis. Container is not a blackbox anymore, you get per process usage and process relationships (which process was called by which process) inside the container (or slurm cgroup).

You get usage info on a per second/minute/hour/day basis, each timeframe keeping data for a configurable amount of time (for example per seconds usage is kept for 1 hour, while per minute usage is kept for 2 days).
You can see which process, and when is consuming cpu or ram, as well as all files or remote networks accessed by the processes.

The web interface gives a live view of the container, but view can also be seen after the container is over as all records are stored in a database. An API is also available to query raw recorded data.

Bubble-chamber does not keep track of all system events, like Sysdig, it only records main usage and access information.

# License

Apache 2.0 (see https://www.apache.org/licenses/LICENSE-2.0)


# Requirements

    python setup.py install

System: mariadb, influxdb, rabbitmq, redis

# Screenshots

![web ui screenshots](screenshots/screen1.png?raw=true)
![web ui screenshots](screenshots/screen2.png?raw=true)
![web ui screenshots](screenshots/screen3.png?raw=true)
![web ui screenshots](screenshots/screen4.png?raw=true)
![web ui screenshots](screenshots/screen5.png?raw=true)

# Installation

## Using docker-compose

    docker-compose up -d


Script launches influxdb, mariadb, consul, redis and bc web servers and record

bc processes may fail to start as they need mariadb/influxdb/rabbitmq to be ready to accept connections.
In this case, wait for mani processes to be ready and execute the command again.

Available env variables:

 * RABBITMQ_HOST=bc-rabbitmq
 * RABBITMQ_USER=test
 * RABBITMQ_PASSWORD=XXX
 * REDIS_HOST=bc-redis
 * BC_MYSQL_URL=mysql+mysqldb://bc:bc@bc-mysql/bc


## Setup

To create or upgrade database schema, use bc_db.py

At first launch:

    python bc_db.py init

For upgrades:

    python bc_db.py upgrade


## Api keys

To send events to web server, one need an API key. Script bc_api helps managing api keys

    python bc_api.py create --owner me@mail.com
    Api key EERZERZEZT  <= generated api key

    python bc_api.py delete --api EERZERZEZT

For help:

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


## Slurm monitoring

If using slurm with cgroups enabled, it is possible to use sysdigslurm chisel instead of sysdigdocker, simply replace name in sysdig command.
In Web UI, jobs are reachable at http://a.b.c.d/static/index.html?container=job_XX with XX=Job Identifier

# Processes

Web UI is used to display container info but also to receive live events from sysdig
Events are then dispatched to bc_record to record events in database.

# Background processes

## bc_record

Listen to rabbitmq for sysdig events sent by web process and record events in mariadb/influxdb

## bc_clean

bc_clean process can be manually launched at regular interval to cleanup old containers that did not received events since X days (or one can specify a specific container).
All related containers stats will be removed.


    python bc_clean.py  clean --days=90
    python bc_clean.py  clean --container=XYZ


It is possible to execute it via a cron:

    docker run osallou/bubble-web python bc_clean.py clean --days=90

## production

bc_record should be scaled to handle the events sent by listeners. 1 process per listening host should be fine. It is possible to look at rabbitmq queues to see if message flow is correct.


# Web UI

## development

    python bc_web_record.py

## production

    rm -rf ..path_to/prometheus-multiproc
    mkdir -p ..path_to/prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/prometheus-multiproc
    gunicorn -c ../path_to/gunicorn_conf.py --bind 0.0.0.0 bc_web_record:app

## in docker

    docker build -t osallou/bubble-web .
    docker run -p 80:8000 -d -e AUTH_SECRET="XXXX"  osallou/bubble-web gunicorn -c /root/sysdig-analyser/gunicorn_conf.py --bind 0.0.0.0 bc_web_record:app

Optionally add --link to your databases docker cluster with according env variables (see docker-compose):


Other env vars:

 * AUTH_SECRET (secret to use)
 * AUTH_DISABLE (disable token checks, users can access any container)

=> http://localhost:5000/static/index.html?container=262b281ffa9d&token=XXXXXX

If auth is enabled in config, a token is expected to give access to container data.
A test token can be generated via test_token.py, else your proxy app should generate one before linking to app.
