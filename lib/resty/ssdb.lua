-- Copyright (C) 2019 lezo (lezomao.com)
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
-- local error = error
-- local gmatch = string.gmatch
-- local remove = table.remove


local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 128)

_M._VERSION = '0.0.1'


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


local function toboolean (resp)
    if resp[2] ~= "0" then
        return true
    else
        return false
    end
end


local function totable (resp)
    local ret = new_tab(0, math.floor(#resp/2))
    for i = 2, #resp, 2 do
        ret[resp[i]] = resp[i+1]
    end
    return ret
end


local function tonumber_table (resp)
    local ret = new_tab(0, math.floor(#resp/2))
    for i = 2, #resp, 2 do
        ret[resp[i]] = tonumber(resp[i+1])
    end
    return ret
end


local function toordered_table (resp)
    local ret = new_tab(0, math.floor(#resp/2))
    for i = 2, #resp, 2 do
        local t = {}
        t[resp[i]]=resp[i+1]
        insert(ret, t)
    end
    return ret
end


local function tonumber_ordered_table (resp)
    local ret = new_tab(0, math.floor(#resp/2))
    for i = 2, #resp, 2 do
        local t = {}
        t[resp[i]]=tonumber(resp[i+1])
        insert(ret, t)
    end
    return ret
end


local function toarray (resp)
    local ret = new_tab(0, math.floor(#resp))
    if resp ~= nil then
        for i = 2, #resp do
            insert(ret, resp[i])
        end
    end
    return ret
end


local resp_parser = {
    boolean = toboolean,
    string = function (resp) return tostring(resp[2]) end,
    number = function(resp) return tonumber(resp[2]) end,
    table = totable,
    number_table = tonumber_table,
    ordered_table = toordered_table,
    number_ordered_table = tonumber_ordered_table,
    array = toarray,
    always_true = function () return true end
}


--[[ command group info from python ssdb:
    https://github.com/wrongwaycn/ssdb-py/blob/ce7b1542f0faa06fe71a60c667fe15992af0f621/ssdb/client.py#L132-L183
--]]


local resp_parser_map = {
    boolean = {
        'set','setx', 'setnx', 'del', 'exists', 'expire', 'setbit',
        'getbit', 'hset', 'hdel', 'hexists', 'zset', 'zdel',
        'zexists'
    },
    string = {
        'get', 'hget', 'getset', 'substr', 'qfront', 'qback', 'qget'
    },
    number = {
        'incr', 'decr', 'multi_set', 'multi_del', 'ttl', 'countbit',
        'strlen', 'hincr', 'hdecr', 'hsize', 'hclear', 'multi_hset',
        'multi_hdel', 'zincr', 'zdecr', 'zsize', 'zclear', 'multi_zset',
        'multi_zdel', 'zget', 'zrank', 'zrrank', 'zcount', 'zsum',
        'zavg', 'zremrangebyrank', 'zremrangebyscore', 'qsize',
        'qclear', 'qpush_back', 'qpush_front', 'qtrim_back',
        'qtrim_front', 'zavg'
    },
    table = {
        -- response table: >>{"k1":"1","k2":"2"}
        'multi_get', 'multi_hget', 'hgetall', 'multi_zget'
    },
    number_table = {
        -- response table: >>{"k1":1,"k2":2}
        "multi_zget"
    },
    ordered_table = {
        -- reponse array: >>[{"k1":"v1"},{"k2":"v2"}]
        'scan', 'rscan', 'hscan', 'hrscan'
    },
    number_ordered_table = {
        -- response array: >>[{"k1":1},{"k2":2},{"k3":3}]
        'zscan', 'zrscan', 'zrange', 'zrrange'
    },
    array = {
        'keys', 'hkeys', 'hlist', 'hrlist', 'zkeys', 'zlist',
        'zrlist', 'qlist', 'qrlist', 'qrange', 'qslice',
        'qpop_back', 'qpop_front'
    },
    always_true = {
        'qset'
    }
}


local function build_parser_map ()
    local resp_map = new_tab(0, 95)
    for dtype, cmds in pairs(resp_parser_map) do
        for _, cmd in pairs(cmds) do
            resp_map[cmd] = resp_parser[dtype]
        end
    end
    return resp_map
end


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


local cmd_table = build_parser_map()


local function parse_response (cmd, resp)
    if cmd == "flushdb" then
        return true
    end
    -- json = require "cjson"
    -- ngx.log(ngx.ERR, json.encode(cmd_table))
    return cmd_table[cmd](resp)
end


local function _read_reply(self, sock, ...)
    local args = {...}
    local val = {}
    local ret
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
            return nil
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
    local cmd = args[1]

    local reqs = rawget(self, "_reqs")
    local cmds = rawget(self, "_cmds")

    if reqs then
        insert(reqs, req)
        insert(cmds, cmd)
        return
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return  _read_reply(self, sock, cmd)
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
    self._reqs = new_tab(n or 4, 0)
    self._cmds = new_tab(n or 4, 0)
end


function _M.cancel_pipeline(self)
    self._reqs = nil
    self._cmds = nil
end


function _M.commit_pipeline(self)
    local reqs = self._reqs
    local cmds = self._cmds
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
        local cmd = cmds[i]
        local res, err = _read_reply(self, sock, cmd)
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
