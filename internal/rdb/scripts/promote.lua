-- Move due jobs from the delayed and retry sets onto their ready queues.
--
-- Without this, a nacked job is scheduled for a retry that never arrives and a
-- delayed job never runs at all: both sets are write-only until something
-- promotes out of them.
--
-- Like reap, the ZREM return value is what makes this safe to run from many
-- processes at once. Only the caller whose ZREM removed the member pushes it
-- onto the queue, so N schedulers racing promote a job exactly once. Leader
-- election is therefore a way to avoid redundant work, not a correctness
-- requirement -- which is worth knowing when the leader lease lapses and two
-- schedulers briefly overlap.
--
-- KEYS[1] delayed sorted set
-- KEYS[2] retry sorted set
--
-- ARGV[1] maximum jobs to promote in this call
-- ARGV[2] job key prefix
-- ARGV[3] ready list key prefix
--
-- Returns the number promoted.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local limit = tonumber(ARGV[1])
local job_prefix = ARGV[2]
local queue_prefix = ARGV[3]

local promoted = 0

for _, zset in ipairs({ KEYS[1], KEYS[2] }) do
  if promoted < limit then
    local due = redis.call('ZRANGEBYSCORE', zset, '-inf', now, 'LIMIT', 0, limit - promoted)

    for _, id in ipairs(due) do
      if redis.call('ZREM', zset, id) == 1 then
        local job = job_prefix .. id
        if redis.call('EXISTS', job) == 1 then
          local queue = redis.call('HGET', job, 'queue')
          redis.call('HSET', job, 'state', 'pending', 'owner', '')
          -- Tail, not head: a job whose backoff has elapsed has no claim on
          -- jumping ahead of work that has been waiting.
          redis.call('RPUSH', queue_prefix .. queue, id)
          promoted = promoted + 1
        end
      end
    end
  end
end

return promoted
