-- Complete a job, releasing its visibility lease.
--
-- Fenced on owner: a worker whose lease expired and whose job was re-delivered
-- must not be able to mark it done. Without this check a stalled worker waking
-- up late would ack work another worker is actively running, and that job's
-- second execution would be silently dropped on the floor.
--
-- KEYS[1] job hash
-- KEYS[2] processed counter
--
-- ARGV[1] worker ID claiming ownership
-- ARGV[2] completed-retention TTL in ms; 0 deletes the envelope outright
-- ARGV[3] active sorted-set key prefix
--
-- Returns {1} on success, {0,'notfound'}, or {0,'lease',state,owner}.

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
redis.call('INCR', KEYS[2])

-- The unique key is deliberately left to expire on its own TTL rather than
-- being released here: releasing on completion would let a duplicate enqueue
-- slip through the moment the first copy finished.
local ttl = tonumber(ARGV[2])
if ttl > 0 then
  redis.call('HSET', job, 'state', 'completed', 'owner', '')
  redis.call('PEXPIRE', job, ttl)
else
  redis.call('DEL', job)
end

return { 1 }
