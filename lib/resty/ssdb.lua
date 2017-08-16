-- Copyright (C) 2017 Risent Zhang (risent.net)
-- Copyright (C) 2013 LazyZhu (lazyzhu.com)
-- Copyright (C) 2013 IdeaWu (ideawu.com)
-- Copyright (C) 2012 Yichun Zhang (agentzh)


local sub = string.sub
local tcp = ngx.socket.tcp
local insert = table.insert
local concat = table.concat
local len = string.len
local null = ngx.null
local pairs = pairs
local unpack = unpack
local setmetatable = setmetatable
local tonumber = tonumber
local tostring = tostring
local rawget = rawget
local error = error
local gmatch = string.gmatch
local remove = table.remove


local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end


-- string split
-- can be replace by `ngx.re.split` in `nginx-lua-module==0.12`
function split(s, delimiter)
    local result = {}
    local from = 1
    local delim_from, delim_to = string.find(s, delimiter, from)
    while delim_from do
        insert(result, string.sub(s, from, delim_from - 1))
        from = delim_to + 1
        delim_from, delim_to = string.find(s, delimiter, from)
    end
    insert(result, string.sub(s, from))
    return result
end


-- check if *val* in *tab*
function has_value (tab, val)
    for index, value in ipairs (tab) do
        if value == val then
            return true
        end
    end
    return false
end


local _M = new_tab(0, 128)

_M._VERSION = '0.03'


local commands = {
    -- Server
    "auth",                 "dbsize",              "flushdb",
    "info",

    -- Key Value
    "set",                  "setx",                "setnx",
    "expire",               "ttl",                 "get",
    "getset",               "del",                 "incr",
    "exists",               "getbit",              "setbit",
    "bitcount",             "countbit",            "substr",
    "strlen",               "keys",                "rkeys",
    "scan",                 "rscan",               "multi_set",
    "multi_get",            "multi_del",

    -- hashmap
    "hset",                 "hget",                "hdel",
    "hincr",                "hexists",             "hsize",
    "hlist",                "hrlist",              "hkeys",
    "hgetall",              "hscan",               "hrscan",
    "hclear",               --[[ "multi_hset", ]]  "multi_hget",
    "multi_hdel",

    -- Sorted Set
    "zset",                 "zget",                "zdel",
    "zincr",                "zexists",             "zsize",
    "zlist",                "zrlist",              "zkeys",
    "zscan",                "zrscan",              "zrank",
    "zrrank",               "zrange",              "zrrange",
    "zclear",               "zcount",              "zsum",
    "zavg",                 "zremrangebyrank",     "zremrangebyscore",
    "zpop_front",           "zpop_back",           --[[ "multi_zset"]]
    "multi_zget",           "multi_zdel",

    -- List
    "qpush_front",          "qpush_back",          "qpop_front",
    "qpop_back",            "qpush",               "qpop",
    "qfront",               "qback",               "qsize",
    "qclear",               "qget",                "qset",
    "qrange",               "qslice",              "qtrim_front",
    "qtrim_back",           "qlist",               "qrlist"

}


-- START command groups
--[[ command group info from python ssdb:
https://github.com/wrongwaycn/ssdb-py/blob/ce7b1542f0faa06fe71a60c667fe15992af0f621/ssdb/client.py#L132-L183
--]]

-- response: 1
local bool_resp_cmds = {'set', 'setnx', 'del', 'exists', 'expire', 'setbit',
                        'getbit', 'hset', 'hdel', 'hexists', 'zset', 'zdel',
                        'zexists'}

-- response string
local raw_resp_cmds = {'get', 'hget', 'getset', 'substr', 'qfront', 'qback', 'qget'}

-- response int
local int_resp_cmds = {'incr', 'decr', 'multi_set', 'multi_del', 'ttl', 'countbit',
                       'strlen', 'hincr', 'hdecr', 'hsize', 'hclear', 'multi_hset',
                       'multi_hdel', 'zincr', 'zdecr', 'zsize', 'zclear', 'multi_zset',
                       'multi_zdel', 'zget', 'zrank', 'zrrank', 'zcount', 'zsum',
                       'zavg', 'zremrangebyrank', 'zremrangebyscore', 'qsize',
                       'qclear', 'qpush_back', 'qpush_front', 'qtrim_back',
                       'qtrim_front'}

-- response float
local float_resp_cmds = {"zavg"}

-- response table: >>{"k1":"1","k2":"2"}
local dict_resp_cmds = {'multi_get', 'multi_hget', 'hgetall', 'multi_zget'}

-- reponse array: >>[{"k1":"v1"},{"k2":"v2"}]
local order_dict_resp_cmds = {'scan', 'rscan', 'hscan', 'hrscan'}

-- response array: >>[{"k1":1},{"k2":2},{"k3":3}]
local int_order_dict_resp_cmds = {'zscan', 'zrscan', 'zrange', 'zrrange'}

-- response table: >>{"k1":"1","k2":"2"}
local int_dict_resp_cmds = {"multi_zget"}

local raw_all_resp_cmds = {'keys', 'hkeys', 'hlist', 'hrlist', 'zkeys', 'zlist',
                           'zrlist', 'qlist', 'qrlist', 'qrange', 'qslice',
                           'qpop_back', 'qpop_front'}

local true_resp_cmds = {"qset"} -- always true

-- END command groups --


local mt = { __index = _M }


function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end


function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


local function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end
_M.close = close


local function parse_response(cmd, resp)
    local ret

    -- ngx.log(ngx.ERR, cmd)
    if cmd == "flushdb" then
        ret = true
    elseif has_value(dict_resp_cmds, cmd) then
        ret = {}
        for i = 2, #resp, 2 do
            ret[resp[i]] = resp[i+1]
        end
    elseif has_value(int_dict_resp_cmds, cmd) then
        ret = {}
        for i = 2, #resp, 2 do
            ret[resp[i]] = tonumber(resp[i+1])
        end
    elseif has_value(int_resp_cmds, cmd) or has_value(float_resp_cmds, cmd) then
        ret = tonumber(resp[2])
    elseif has_value(bool_resp_cmds, cmd) then
        ret = not not resp[2]
    elseif has_value(order_dict_resp_cmds, cmd) then
        ret = {}
        for i = 2, #resp, 2 do
            local t = {}
            t[resp[i]]=resp[i+1]
            insert(ret, t)
        end
    elseif has_value(int_order_dict_resp_cmds, cmd) then
        ret = {}
        for i = 2, #resp, 2 do
            local t = {}
            t[resp[i]]=tonumber(resp[i+1])
            insert(ret, t)
        end
    elseif has_value(raw_all_resp_cmds, cmd) then
        -- ngx.log(ngx.ERR, cmd .. "IN RAW ALL RESP")
        ret = {}
        if resp ~= nil then
            for i = 2, #resp do
                insert(ret, resp[i])
            end
        end
    elseif has_value(true_resp_cmds, cmd) then
        ret = true
    else
        ret = resp[2]
    end

    return ret
end


local function _read_reply(self, sock, ...)
    local args = {...}
    local val = {}
    local ret = nil
    local cmd = args[1]

    while true do
        -- read block size
        local line, err, partial = sock:receive()
        if not line or len(line)==0 then
            -- packet end
            break
        end
        local d_len = tonumber(line)

        -- read block data
        local data, err = sock:receive(d_len)
        if not data then
            return nil, err
        end
        insert(val, data)

        local dummy, err = sock:receive(1) -- ignore LF
        if not dummy then
            return nil, err
        end
    end

    if val[1] == 'not_found' then
        ret = null
    elseif val[1] == 'client_error' then
        return nil, val[2]
    else
        ret = parse_response(cmd, val)
        -- ret = val[2]
    end

    return ret
end


local function _gen_req(args)
    local nargs = #args
    local req = {}

    for i = 1, nargs do
        local arg = args[i]

        if type(arg) ~= "string" then
            arg = tostring(arg)
        end

        if arg then
            insert(req, len(arg))
            insert(req, "\n")
            insert(req, arg)
            insert(req, "\n")
        else
            return nil, err
        end
    end
    insert(req, "\n")

    -- it is faster to do string concatenation on the Lua land
    -- print("request: ", table.concat(req, ""))

    return concat(req, "")
end


local function _do_cmd(self, ...)
    local args = {...}
    local sock = rawget(self, "sock")
    if not sock then
        return nil, "not initialized"
    end

    local req = _gen_req(args)

    local reqs = rawget(self, "_reqs")

    local t1 = self._t1

    if reqs then
        insert(reqs, req)
        return
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return  _read_reply(self, sock, args[1])
end




for i = 1, #commands do
    local cmd = commands[i]

    _M[cmd] =
        function (self, ...)
            return _do_cmd(self, cmd, ...)
        end
end

function _M.connect(self, host, port, auth, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end
    local ok, err = sock:connect(host, port)
	if not ok then
	    return nil, err
	end
	-- make auth
	if auth then
        local req = {"4", "\n", "auth", "\n", len(auth), "\n", auth, "\n", "\n"}
		local bytes, err = sock:send(req)
		if not bytes then
            return nil, err
        end
		local err = _read_reply(self, sock)
        if err and err ~= '1'  then
		    return nil, err
		end
	end
	return ok, err
end


function _M.multi_hset(self, hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", hashname)
        return _do_cmd(self, "multi_hset", hashname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_hset", hashname, ...)
end


function _M.multi_zset(self, keyname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", keyname)
        return _do_cmd(self, "multi_zset", keyname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_zset", keyname, ...)
end


function _M.init_pipeline(self)
    self._reqs = {}
end


function _M.cancel_pipeline(self)
    self._reqs = nil
end


function _M.commit_pipeline(self)
    local reqs = self._reqs
    if not reqs then
        return nil, "no pipeline"
    end

    self._reqs = nil

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send(reqs)
    if not bytes then
        return nil, err
    end

    local vals = {}
    for i = 1, #reqs do
        local res, err = _read_reply(self, sock)
        if res then
            insert(vals, res)

        elseif res == nil then
            return nil, err

        else
            insert(vals, err)
        end
    end

    return vals
end


function _M.array_to_hash(self, t)
    local h = {}
    for i = 1, #t, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end


function _M.add_commands(...)
    local cmds = {...}
    for i = 1, #cmds do
        local cmd = cmds[i]
        _M[cmd] =
            function (self, ...)
                return _do_cmd(self, cmd, ...)
            end
    end
end


setmetatable(_M, {__index = function(self, cmd)
                      local method =
                          function (self, ...)
                              return _do_cmd(self, cmd, ...)
                          end

                      -- cache the lazily generated method in our
                      -- module table
                      _M[cmd] = method
                      return method
end})


return _M
