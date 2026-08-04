-- Dead-letter a job immediately, bypassing any attempts it has left.
--
-- For failures that retrying cannot fix: a malformed payload, a reference to
-- something that no longer exists. Burning twenty-four more attempts on those
-- wastes capacity and buries the real error under noise.
--
-- KEYS[1] job hash
-- KEYS[2] dead-letter sorted set
-- KEYS[3] failed counter
--
-- ARGV[1] worker ID claiming ownership
-- ARGV[2] reason
-- ARGV[3] active sorted-set key prefix
--
-- Returns {1} on success, {0,'notfound'}, or {0,'lease',state,owner}.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local job = KEYS[1]

if redis.call('EXISTS', job) == 0 then
  return { 0, 'notfound' }
end

local state = redis.call('HGET', job, 'state')
local owner = redis.call('HGET', job, 'owner')
if state ~= 'active' or owner ~= ARGV[1] then
  return { 0, 'lease', tostring(state), tostring(owner) }
end

local id = redis.call('HGET', job, 'id')
local queue = redis.call('HGET', job, 'queue')
redis.call('ZREM', ARGV[3] .. queue, id)

redis.call('HSET', job, 'state', 'dead', 'owner', '', 'last_err', ARGV[2])
redis.call('ZADD', KEYS[2], now_ms(), id)
redis.call('INCR', KEYS[3])

return { 1 }
