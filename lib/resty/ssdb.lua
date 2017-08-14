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
local rawget = rawget
local error = error
local gmatch = string.gmatch
local remove = table.remove


local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
    new_tab = function (narr, nrec) return {} end
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


function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end
_M.close = close


local function _read_reply(self, sock)
    local val = {}
    local ret = nil

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

    for index, v in pairs(val) do
        ngx.log(ngx.ERR, index .. ":" .. v)
    end

    if val[1] == 'not_found' then
        ret = null
    elseif val[2] then
        ret = val[2]
    end

    return ret
end


local function _gen_req(args)
    local req = {}

    for i = 1, #args do
        local arg = args[i]

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

    return  _read_reply(self, sock)
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
		local err = _read_reply(sock)
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
        local res, err = _read_reply(sock)
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
