FROM sysdig/sysdig

ENV LUA_PATH="/usr/share/lua/5.1/?.lua"
ENV LUA_CPATH="/usr/lib/x86_64-linux-gnu/lua/5.1/?.so"

RUN apt-get update && apt-get install -y lua-socket
COPY sysdigdocker.lua /usr/share/sysdig/chisels/
COPY sysdigslurm.lua /usr/share/sysdig/chisels/
