-- Report a failed attempt: schedule a retry, or dead-letter the job if its
-- attempts are spent.
--
-- Fenced on owner for the same reason as ack.
--
-- KEYS[1] job hash
-- KEYS[2] retry sorted set
-- KEYS[3] dead-letter sorted set
-- KEYS[4] failed counter
--
-- ARGV[1] worker ID claiming ownership
-- ARGV[2] retry-at, unix ms
-- ARGV[3] failure reason
-- ARGV[4] active sorted-set key prefix
--
-- Returns {1,'retry'} or {1,'dead'} on success, {0,'notfound'}, or
-- {0,'lease',state,owner}.

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
redis.call('ZREM', ARGV[4] .. queue, id)

redis.call('HSET', job, 'last_err', ARGV[3], 'owner', '')

-- A plain comparison rather than an increment: the attempt was already
-- consumed when the job was claimed.
local attempt = tonumber(redis.call('HGET', job, 'attempt'))
local max_attempts = tonumber(redis.call('HGET', job, 'max_attempts'))

if attempt >= max_attempts then
  redis.call('HSET', job, 'state', 'dead')
  redis.call('ZADD', KEYS[3], now_ms(), id)
  redis.call('INCR', KEYS[4])
  return { 1, 'dead' }
end

redis.call('HSET', job, 'state', 'retry')
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), id)
return { 1, 'retry' }
