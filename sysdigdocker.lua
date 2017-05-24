--[[
Copyright (C) 2013-2015 Draios inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License version 2 as
published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
--]]

-- Chisel description
description = "Show the top process defined by the highest CPU utilization. This chisel is compatible with containers using the sysdig -pc or -pcontainer argument, otherwise no container information will be shown."
short_description = "Top processes by CPU usage"
category = "CPU Usage"

remote_server = nil
-- Chisel argument list
args = {
    {
        name = "url",
        description = "remote http server utl",
        argtype = "string",
        optional = false
    },
}

-- Argument notification callback
function on_set_arg(name, val)
    if name == "url" then
        remote_server = val
    end
    return true
end

require "common"
terminal = require "ansiterminal"

grtable = {}
fdtable = {}
islive = false
fkeys = {}
fdkeys = {}
grevents = 0
fdevents = 0
-- print(package.path)
-- print(package.cpath)
-- TODO set this via LUA_PATH env variable
-- package.path = package.path .. ";/usr/share/lua/5.1/?.lua"
-- package.cpath = package.cpath .. ";/usr/lib/x86_64-linux-gnu/lua/5.1/?.so"
local print_container = false
local http = require("socket.http")
local ltn12 = require("ltn12")


function send_data(stable, ts_s, ts_ns, timedelta, viz_info, evt_type)
	local sorted_grtable = pairs_top_by_val(stable, viz_info.top_number, function(t,a,b) return t[b] < t[a] end)

		local jdata = {}
		local out = {}
		local j = 1
		
		for k,v in sorted_grtable do
			local vals = split(k, "\001\001")
			vals[#vals + 1] = v
			jdata[j] = vals
			j = j + 1
		end

		local jinfo = {}
				
		for i, keyname in ipairs(viz_info.key_fld) do
			jinfo[i] = {name = keyname, desc = viz_info.key_desc[i], is_key = true}
		end
			
		local res = {ts = sysdig.make_ts(ts_s, ts_ns), data = jdata, evt_type=evt_type, jinfo=jinfo}
			
		local str = json.encode(res)
                print("send to " .. remote_server)
		http.request{
                    -- url = "http://131.254.17.40:8000/event",
                    url = remote_server,
		    method = "POST",
		    headers = {
                      ["Content-Type"] = "application/json",
		      ["Content-Length"] = string.len(str)
		    },
                    source = ltn12.source.string(str),
		    sink = ltn12.sink.table(out)
		}
end

vizinfo =
{
	key_fld = {"proc.name","proc.pid", "proc.vmsize_kb"," proc.exe", "proc.args"},
	key_desc = {"Process", "PID", "VM size", "Exe", "Args"},
	value_fld = "thread.exectime",
	value_desc = "CPU%",
	--value_units = "timepct",
	value_units = "time",
	top_number = 100,
	output_format = "normal"
}

vizinfofd = {}

-- Initialization callback
function on_init()
	-- The -pc or -pcontainer options was supplied on the cmd line
	print_container = sysdig.is_print_container_data()

	-- Print container info as well
	if print_container then
		-- Modify host pid column name and add container information
		vizinfo.key_fld = {"evt.cpu", "proc.name", "proc.pid", "proc.vpid", "proc.vmsize_kb", "container.id", "proc.exe", "proc.args", "proc.ppid"}
		vizinfo.key_desc = {"CPU nb", "Process", "Host_pid", "Container_pid", "VM size", "container.id", "exe", "args"," Process parent id"}
		vizinfofd.key_fld = {"proc.pid", "proc.vpid", "container.id", "fd.name"}
		vizinfofd.key_desc = {"Host_pid", "Container_pid", "Container_id", "File name"}
		vizinfofd.value_fld = "files_io"
	        vizinfofd.value_desc = "files io"
		vizinfofd.value_units = "none"
		vizinfofd.output_format = "normal"
		vizinfofd.top_number = 100
	end

	-- Request the fields we need
	for i, name in ipairs(vizinfo.key_fld) do
		fkeys[i] = chisel.request_field(name)
	end
        for i, name in ipairs(vizinfofd.key_fld) do
                fdkeys[i] = chisel.request_field(name)
        end

	-- Request the fields we need
	fexe = chisel.request_field("proc.exe")
	fvalue = chisel.request_field(vizinfo.value_fld)
	fcpu = chisel.request_field("thread.cpu")
	-- ffd = chisel.request_field("evt.rawarg.res")
	ffd = chisel.request_field("evt.buflen")
	ffn = chisel.request_field("fd.name")
	cname = chisel.request_field("container.name")
	cid = chisel.request_field("container.id")
	ppid = chisel.request_field("proc.ppid")
	is_io_read = chisel.request_field("evt.is_io_read")
	is_io_write = chisel.request_field("evt.is_io_write")
	
	chisel.set_filter("evt.type=procinfo or (fd.type=file and evt.is_io=true)")

	return true
end

-- Final chisel initialization
function on_capture_start()
	islive = sysdig.is_live()
	vizinfo.output_format = sysdig.get_output_format()
	vizinfofd.output_format = sysdig.get_output_format()

	if islive then
		chisel.set_interval_s(1)
		if vizinfo.output_format ~= "json" then
			terminal.clearscreen()
			terminal.hidecursor()
		end
	end

	return true
end

-- Event parsing callback
function on_event()
	local key = nil
	local kv = nil
	local containerName = evt.field(cname)

	if containerName == 'sysdig' or containerName == 'host' or containerName == 'cassandra' then
	 	 return true
        end

        local yyy = evt.field(ffn)
        if yyy ~= nil then
                print("??? " .. yyy)
        end

        local nokey = false
	for i, fld in ipairs(fkeys) do
		kv = evt.field(fld)
		if kv == nil then
			nokey = true
			break
		end

		if key == nil then
			key = kv
		else
			key = key .. "\001\001" .. evt.field(fld)
		end
	end
	-- exename = evt.field(fexe)
        -- if evt.get_type() == 'procinfo' then
	if nokey == false then
	    local cpu = evt.field(fcpu)
	    if cpu == nil then
		    cpu = 0
	    end
	    if cpu ~= nil then
	    if grtable[key] == nil then
	  	    grtable[key] = cpu * 10000000
	    else
		    grtable[key] = grtable[key] + (cpu * 10000000)
	    end
	    grevents = grevents + 1
            end
        end

	nokey = false
        key = nil
        for i, fld in ipairs(fdkeys) do
                kv = evt.field(fld)
                if kv == nil then
			nokey = true
                        break
                end

                if key == nil then
                        key = kv
                else
                        key = key .. "\001\001" .. evt.field(fld)
                end
        end
	if nokey then
		return true
	end
	local is_read = evt.field(is_io_read)
	local is_write = evt.field(is_io_write)
	if evt.field(ffd) == nil then
            return true
	end
        if (is_read ~=nil and is_read) or (is_write ~= nil and is_write) then
             if is_read then
	         key = key .. "\001\001" .. "in"
             else
                 key = key .. "\001\001" .. "out"
             end
             if fdtable[key] == nil then
                   fdtable[key] = evt.field(ffd)
             else
                   fdtable[key] = fdtable[key] + evt.field(ffd)
             end
             fdevents = fdevents + 1
        end

	return true
end

-- Periodic timeout callback
function on_interval(ts_s, ts_ns, delta)
	if vizinfo.output_format ~= "json" then
		terminal.clearscreen()
		terminal.moveto(0, 0)
	end
	
	-- print_sorted_table(grtable, ts_s, 0, delta, vizinfo)
	-- print_sorted_table(fdtable, ts_s, 0, delta, vizinfofd)
	if grevents > 0 then
	    send_data(grtable, ts_s, 0, delta, vizinfo, "cpu")
	end
	if fdevents > 0 then
	    send_data(fdtable, ts_s, 0, delta, vizinfofd, "fd")
        end
	
	-- Clear the table
	grtable = {}
	fdtable = {}
	grevents = 0
	fdevents = 0
	return true
end

-- Called by the engine at the end of the capture (Ctrl-C)
function on_capture_end(ts_s, ts_ns, delta)
	if islive and vizinfo.output_format ~= "json" then
		terminal.clearscreen()
		terminal.moveto(0 ,0)
		terminal.showcursor()
		return true
	end
	
	-- print_sorted_table(grtable, ts_s, 0, delta, vizinfo)
	-- print_sorted_table(fdtable, ts_s, 0, delta, vizinfofd)
	if grevents > 0 then
	     send_data(grtable, ts_s, 0, delta, vizinfo, "cpu")
        end
	if fdevents > 0 then
	    send_data(fdtable, ts_s, 0, delta, vizinfofd, "fd")
	end

	
	return true
end
