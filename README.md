# About

this work is in progress and should not be used as is.

program reads sysdig events (live) or scap output files and calculate cpu usage, fd accesses etc.. for a container.
result is inserted in cassandra.
The web server records live events from sysdig (with a special chisel) and display container information.

# License

Apache 2.0 (see https://www.apache.org/licenses/LICENSE-2.0)


# Requirements

    python setup.py install

System: cassandra, rabbitmq


# Installation

## Using docker-compose

    docker-compose up -d


Script launch cassandra, consul, retention and web servers.

retention and web servers mail fail to start as they need cassandra to be ready to accept conenctions.
In this case, wait for cassandra to be ready and execute the command again.

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

    python bc_analyse.py test.json


# Retention

bc_retention is in charge of merging records in a larger window (from seconds to minutes etc...)

Execute via cron *bc_retention.py retain* with desired retention at regular interval (m, h, d). Program will send some requests via rabbitmq to *bc_retention.py listen* instances (can be scaled to manage many stored containers). Request will be sent for each container for desired retention.

Some env variables can be defined vs cmd line:

 * CASSANDRA_HOST
 * CASSANDRA_CLUSTER
 * RABBITMQ_HOST

 For rabbitmq credentials, credentials MUST use env variables *RABBITMQ_USER* and *RABBITMQ_PASSWORD*

Example, execute 2 retention instances:

    python bc_retention.py listen --host 127.0.0.1 --cluster sysdig --rabbit 127.0.0.1
    python bc_retention.py listen --host 127.0.0.1 --cluster sysdig --rabbit 127.0.0.1

Then via cron :

    # every 5 minutes
    */5 * * * * python bc_retention.py retain --host 127.0.0.1 --cluster sysdig --rabbit 127.0.0.1 --retention m
    # every 5 hours
    0  */5 * * python bc_retention.py retain --host 127.0.0.1 --cluster sysdig --rabbit 127.0.0.1 --retention h
    # every day
    0 * * * * python bc_retention.py retain --host 127.0.0.1 --cluster sysdig --rabbit 127.0.0.1 --retention d

# Web UI

Web UI is used to display container info but also to receive live events from sysdig

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
