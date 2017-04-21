# About

this work is in progress and should not be used as is.

program reads sysdig scap output files and calculate cpu usage, fd accesses etc.. for a container.

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

read scap files , get stats per container

# Others

# influxdb + grafana

docker run --rm -d -p 8083:8083 -p 8086:8086 \
      -v $PWD:/var/lib/influxdb --name sysdig-influxdb \
      influxdb

docker run -d --link sysdig-influxdb:sysdig-influxdb  -p 3000:3000 grafana/grafana

## cpu

influx:

SELECT sum("value")/1000000000 FROM "cpu_9f8aea456941" WHERE $timeFilter GROUP BY time($interval),"process" fill(null)

should divide by number of CPUs for container to get percent

SELECT sum("value")/(10000000 * $nbcpu) FROM "cpu_$containerid" WHERE $timeFilter GROUP BY time($interval),"process" fill(null)


From url set nbcpu:

http://localhost:3000/dashboard/db/container-analysis?from=1490964299726&to=1490964337850&var-nbcpu=2&var-containerid=9f8aea456941

replace from/to/nbcpu with known values from job



## Memory

    SELECT max("value")*1000 FROM "memory_9f8aea456941" WHERE $timeFilter GROUP BY time($interval),"process" fill(null)


## cassandra

            CREATE KEYSPACE <ksname>
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

            USE sysdig;
            CREATE TABLE io (io_in counter, io_out counter, ts timestamp, proc_name varchar, file_name varchar, PRIMARY KEY (ts, proc_name, file_name));
            CREATE TABLE mem (vm_size counter, vm_rss counter, vm_swap counter, ts timestamp, proc_name varchar, PRIMARY KEY (ts, proc_name));
            CREATE TABLE cpu (duration counter, cpu int, ts timestamp, proc_name varchar, PRIMARY KEY (ts, proc_name, cpu));
