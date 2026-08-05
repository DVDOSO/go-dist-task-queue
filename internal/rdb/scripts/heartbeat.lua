-- Register or refresh a worker's liveness record.
--
-- Heartbeats are not what recovers lost jobs -- the visibility lease does that,
-- and it works whether or not anyone is watching. This exists so an operator
-- can see which workers are alive and what they are carrying, which is the
-- difference between "the queue is backed up" and "the queue is backed up
-- because four of six workers are gone".
--
-- The record expires on its own, so a worker that dies removes itself rather
-- than lingering as a phantom until something notices.
--
-- KEYS[1] workers sorted set, scored by last heartbeat
-- KEYS[2] this worker's metadata hash
--
-- ARGV[1] worker ID
-- ARGV[2] TTL in ms
-- ARGV[3] host
-- ARGV[4] pid
-- ARGV[5] queues, comma separated
-- ARGV[6] concurrency
-- ARGV[7] in-flight count
-- ARGV[8] started-at, unix ms
--
-- Returns 1.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local ttl = tonumber(ARGV[2])

redis.call('ZADD', KEYS[1], now, ARGV[1])

redis.call('HSET', KEYS[2],
  'id', ARGV[1],
  'host', ARGV[3],
  'pid', ARGV[4],
  'queues', ARGV[5],
  'concurrency', ARGV[6],
  'in_flight', ARGV[7],
  'started_at', ARGV[8],
  'last_beat', now)

redis.call('PEXPIRE', KEYS[2], ttl)

-- The hashes expire themselves but their sorted-set entries do not, so prune
-- anything that has stopped reporting. Doing it here rather than in a
-- dedicated sweeper keeps the set self-maintaining for as long as any worker
-- is alive.
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - ttl)

return 1
