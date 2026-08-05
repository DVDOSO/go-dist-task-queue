-- Recover jobs whose visibility lease expired, one queue at a time.
--
-- This is the script that turns at-least-once from a claim into a fact. A
-- worker that is SIGKILLed, OOM-killed, or loses its host never gets to nack
-- anything: its jobs simply sit in the active set with a deadline in the past.
-- Nothing else in the system would ever look at them again.
--
-- The ZREM return value is the concurrency control. Many workers reap at once
-- by design -- reaping is idempotent, so keeping it off the leader's critical
-- path means reliability does not depend on leader election -- and only the
-- caller whose ZREM actually removed the member goes on to requeue it. Two
-- reapers cannot both recover the same job.
--
-- KEYS[1] active sorted set for this queue
-- KEYS[2] dead-letter sorted set
-- KEYS[3] ready list for this queue
-- KEYS[4] failed counter
--
-- ARGV[1] maximum jobs to act on in this call
-- ARGV[2] job key prefix
--
-- Returns {recovered, dead_lettered}.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local limit = tonumber(ARGV[1])
local job_prefix = ARGV[2]

local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)

local recovered = 0
local dead = 0

for _, id in ipairs(expired) do
  if redis.call('ZREM', KEYS[1], id) == 1 then
    local job = job_prefix .. id
    if redis.call('EXISTS', job) == 1 then
      -- Recoveries is observational only. It is what tells you, from a
      -- dead-letter entry, whether a job was failing or killing its workers.
      redis.call('HINCRBY', job, 'recoveries', 1)
      redis.call('HSET', job, 'owner', '')

      local attempt = tonumber(redis.call('HGET', job, 'attempt'))
      local max_attempts = tonumber(redis.call('HGET', job, 'max_attempts'))

      if attempt >= max_attempts then
        -- An orphan that has burned through its attempts never reaches nack,
        -- because the worker that would have called it is gone. Without this
        -- branch a job that reliably crashes its worker cycles forever.
        redis.call('HSET', job,
          'state', 'dead',
          'last_err', 'orphaned: lease expired after ' .. attempt .. ' attempts')
        redis.call('ZADD', KEYS[2], now, id)
        redis.call('INCR', KEYS[4])
        dead = dead + 1
      else
        redis.call('HSET', job, 'state', 'pending')
        -- Head of the queue, not the tail: this job already waited out a full
        -- visibility timeout and should not now queue behind fresh work.
        redis.call('LPUSH', KEYS[3], id)
        recovered = recovered + 1
      end
    end
  end
end

return { recovered, dead }
