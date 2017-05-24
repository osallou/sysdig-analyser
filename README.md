# About

this work is in progress and should not be used as is.

program reads sysdig scap output files and calculate cpu usage, fd accesses etc.. for a container.
result is inserted in cassandra.



# requirements

    pip install click cassandra-driver mongo progressbar flask gunicorn PyYAML PyJWT prometheus_client python-consul

# To execute sysdig

docker run -it --rm --name=sysdig --privileged=true \
           -v ${PWD}:/mnt/sysdig \
           --volume=/var/run/docker.sock:/host/var/run/docker.sock \
           --volume=/dev:/host/dev \
           --volume=/proc:/host/proc:ro \
           --volume=/boot:/host/boot:ro \
           --volume=/lib/modules:/host/lib/modules:ro \
           --volume=/usr:/host/usr:ro \
           sysdig/sysdig


# sysdig execution

sysdig -pc -w myoutput.scap


# list containers

sysdig -pc -r test.scap  -j -c lscontainers


# what should be done

save to scap at regular interval

read scap files , and export to json:

    sysdig -pc -j  -r test.scap > test.json


then

    python analysis_sysdig test.json


## Web UI


## dev

    python sysdig_web.py

## prod

    rm -rf ..path_to/prometheus-multiproc
    mkdir -p ..path_to/prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/prometheus-multiproc
    gunicorn -c ../path_to/gunicorn_conf.py --bind 0.0.0.0 sysdig_web:app

=> http://localhost:5000/static/index.html?container=262b281ffa9d

## cassandra

            CREATE KEYSPACE <ksname>
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

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


            CREATE table retention(id int, ts timestamp, PRIMARY KEY(id))
# Dev

custom lua script (sysdigdocker.lua to add to chisels) run

    sysdig -pc -c sysdigdocker http://x.y.z/event -j


Need lua-socket lua module and define lua path:

    export LUA_PATH="/usr/share/lua/5.1/?.lua"
    export LUA_CPATH="/usr/lib/x86_64-linux-gnu/lua/5.1/?.so"



sysdig_web: add configuration file for cassandra cluster conn info
