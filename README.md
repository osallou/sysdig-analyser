# About

this work is in progress and should not be used as is.

program reads sysdig scap output files and calculate cpu usage, fd accesses etc.. for a container.
result is inserted in cassandra.

# status

io access and mem cassandra insertion looks fine
cpu usage is approximate, can go over 100% as cpu usage per second is calculated and inserted at the end of the event.
So event can overlap the next second. This gives overview a good overview of usage and a correct mean value on several seconds.

insertion is based on per second aggregation, then other process could aggregate per 10s/1h/1d or like for graph and access, old  per second data would be deleted

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

python sysdig_web.py

=> http://localhost:5000/static/index.html?container=262b281ffa9d

## cassandra


TODO: change proc_name varchar to proc_id int except in table proc

            CREATE KEYSPACE <ksname>
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

            USE sysdig;
            CREATE TABLE io (container varchar, io_in counter, io_out counter, ts timestamp, proc_name varchar, file_name varchar, PRIMARY KEY (container, proc_name, ts, file_name));
            CREATE TABLE io_all (container varchar, io_in counter, io_out counter, proc_name varchar, file_name varchar, PRIMARY KEY (container, proc_name, file_name));
            CREATE TABLE mem (container varchar,vm_size bigint, vm_rss bigint, vm_swap bigint, ts timestamp, proc_name varchar, PRIMARY KEY (container, proc_name, ts));
            CREATE TABLE cpu (container varchar, duration counter, cpu int, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts, cpu));
            CREATE TABLE cpu_all (container varchar, duration counter, ts timestamp, proc_id int, PRIMARY KEY (container, proc_id, ts));
            CREATE TABLE proc (container varchar, start timestamp, end timestamp, proc_name varchar, exe varchar, args varchar, proc_id int, parent_id int, PRIMARY KEY (container, proc_id));
            CREATE TABLE proc_cpu (container varchar, cpu counter, proc_id int, PRIMARY KEY (container, proc_id));
