-- Atomically store a job envelope and place it on either its ready queue or
-- the delayed set, honouring a unique key if one was supplied.
--
-- The whole point of doing this in Lua is that the hash write and the list
-- push cannot be observed separately. A crash between them in a
-- read-modify-write client would leave either a job nobody will ever run or a
-- queue entry pointing at nothing.
--
-- KEYS[1] job hash
-- KEYS[2] ready list for the job's queue
-- KEYS[3] delayed sorted set
-- KEYS[4] set of known queue names
-- KEYS[5] unique key (a placeholder that is never touched when ARGV[7] is 0)
--
-- ARGV[1] job ID
-- ARGV[2] queue
-- ARGV[3] task type
-- ARGV[4] payload
-- ARGV[5] max attempts
-- ARGV[6] run-at, unix ms; 0 or past means run now
-- ARGV[7] unique TTL in ms; 0 disables the unique check
-- ARGV[8] unique key as stored on the envelope
--
-- Returns {1} on success, or {0, existing_job_id} when a unique key was held.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local id = ARGV[1]
local unique_ttl = tonumber(ARGV[7])

if unique_ttl > 0 then
  -- SET NX is the whole idempotency mechanism. Losing this race means another
  -- producer got there first and its job ID is the one that matters.
  local won = redis.call('SET', KEYS[5], id, 'NX', 'PX', unique_ttl)
  if not won then
    -- The holder can expire between the failed SET and this GET, so coalesce
    -- a nil into an empty string: a Lua nil would silently truncate the
    -- returned array and the caller would see a bare {0}.
    return { 0, redis.call('GET', KEYS[5]) or '' }
  end
end

local run_at = tonumber(ARGV[6])
local state = 'pending'
if run_at > now then
  state = 'scheduled'
end

redis.call('HSET', KEYS[1],
  'id', id,
  'queue', ARGV[2],
  'type', ARGV[3],
  'payload', ARGV[4],
  'attempt', 0,
  'max_attempts', ARGV[5],
  'recoveries', 0,
  'state', state,
  'owner', '',
  'unique_key', ARGV[8],
  'last_err', '',
  'enqueued_at', now,
  'run_at', ARGV[6],
  'started_at', 0,
  'deadline', 0)

if state == 'scheduled' then
  redis.call('ZADD', KEYS[3], run_at, id)
else
  redis.call('RPUSH', KEYS[2], id)
end

-- Tracked so stats and the CLI can enumerate queues without ever running KEYS.
redis.call('SADD', KEYS[4], ARGV[2])

return { 1 }
