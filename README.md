# About

this work is in progress and should not be used as is.

program reads sysdig scap output files and calculate cpu usage, fd accesses etc.. for a container.

# To execute sysdig

docker run -it --rm --name=sysdig --privileged=true \
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

docker run --rm -d -p 8083:8083 -p 8086:8086 \
      -v $PWD:/var/lib/influxdb --name sysdig-influxdb \
      influxdb

docker run -d --link sysdig-influxdb:sysdig-influxdb  -p 3000:3000 grafana/grafana

influx:

SELECT sum("value")/1000000000 FROM "cpu_9f8aea456941" WHERE $timeFilter GROUP BY time($interval),"process" fill(null)

should divide by number of CPUs for container to get percent

SELECT sum("value")/(10000000 * $nbcpu) FROM "cpu_$containerid" WHERE $timeFilter GROUP BY time($interval),"process" fill(null)
