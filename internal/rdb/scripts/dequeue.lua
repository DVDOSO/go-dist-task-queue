-- Atomically claim the next available job across an ordered list of queues.
--
-- This is the script that makes at-least-once delivery work. The pop from the
-- ready list and the insert into the active set with a visibility deadline
-- happen in one Redis command, so there is no instant at which a job exists in
-- neither place. A client doing LPOP then ZADD would lose every job that was
-- in flight when it crashed between the two.
--
-- KEYS are (ready, active) pairs, one pair per queue, in the order the caller
-- wants them tried. That ordering is where the worker's weighting policy has
-- already been applied; this script deliberately knows nothing about priority.
--
-- ARGV[1] worker ID, which becomes the job's owner and fences later mutations
-- ARGV[2] visibility timeout in ms
-- ARGV[3] job key prefix
--
-- Returns the job hash as a flat field/value array, or nil when every queue
-- was empty.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local worker = ARGV[1]
local vt = tonumber(ARGV[2])
local prefix = ARGV[3]
local nqueues = #KEYS / 2

for i = 1, nqueues do
  local ready = KEYS[i * 2 - 1]
  local active = KEYS[i * 2]

  -- The inner loop is bounded rather than infinite: a ready list can contain
  -- an ID whose envelope has been purged, and skipping those must not let one
  -- call spin over a huge backlog while holding the Redis event loop.
  for _ = 1, 20 do
    local id = redis.call('LPOP', ready)
    if not id then
      break
    end

    local job = prefix .. id
    if redis.call('EXISTS', job) == 1 then
      local deadline = now + vt

      -- Attempts are consumed at claim time, not at failure time. A worker
      -- that is SIGKILLed never reports a failure, so counting failures would
      -- let a job that reliably crashes its worker cycle forever.
      redis.call('HINCRBY', job, 'attempt', 1)
      redis.call('HSET', job,
        'state', 'active',
        'owner', worker,
        'started_at', now,
        'deadline', deadline)
      redis.call('ZADD', active, deadline, id)

      return redis.call('HGETALL', job)
    end
  end
end

return nil
