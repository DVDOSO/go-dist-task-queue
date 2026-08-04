-- Renew the visibility lease on a batch of jobs a worker is running.
--
-- Batched on purpose: a worker with fifty jobs in flight renews all of them in
-- one round trip rather than fifty. At a renewal interval of a third of the
-- visibility timeout, per-job renewals would put a worker's heartbeat traffic
-- in the same order of magnitude as its actual work.
--
-- Jobs the worker no longer owns are collected and returned rather than
-- aborting the call, so one expired lease cannot stop the other forty-nine
-- from renewing.
--
-- ARGV[1]  worker ID claiming ownership
-- ARGV[2]  visibility timeout in ms
-- ARGV[3]  job key prefix
-- ARGV[4]  active sorted-set key prefix
-- ARGV[5:] job IDs to renew
--
-- Returns an array of the job IDs whose leases were lost.

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local now = now_ms()
local worker = ARGV[1]
local vt = tonumber(ARGV[2])
local job_prefix = ARGV[3]
local active_prefix = ARGV[4]

local lost = {}

for i = 5, #ARGV do
  local id = ARGV[i]
  local job = job_prefix .. id
  local renewed = false

  if redis.call('EXISTS', job) == 1 then
    local state = redis.call('HGET', job, 'state')
    local owner = redis.call('HGET', job, 'owner')
    if state == 'active' and owner == worker then
      local deadline = now + vt
      local queue = redis.call('HGET', job, 'queue')
      redis.call('HSET', job, 'deadline', deadline)
      redis.call('ZADD', active_prefix .. queue, deadline, id)
      renewed = true
    end
  end

  if not renewed then
    table.insert(lost, id)
  end
end

return lost
