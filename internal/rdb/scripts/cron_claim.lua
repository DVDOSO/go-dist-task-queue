-- Claim one tick of a cron schedule by advancing it to its next fire time.
--
-- A compare-and-set rather than a blind ZADD. Two schedulers that both read the
-- same entry as due will both call this; the one whose expected score still
-- matches wins, and the other sees a score that has already moved and backs
-- off. That is what keeps a schedule firing once per tick even during the brief
-- window where a lapsed leader and its successor overlap.
--
-- KEYS[1] cron sorted set, member = entry ID, score = next fire in unix ms
--
-- ARGV[1] entry ID
-- ARGV[2] the fire time this caller believes is due
-- ARGV[3] the next fire time to advance to
--
-- Returns 1 if this caller won the tick, 0 otherwise.

local score = redis.call('ZSCORE', KEYS[1], ARGV[1])

if not score then
  -- The schedule was removed underneath us.
  return 0
end

if tonumber(score) ~= tonumber(ARGV[2]) then
  -- Someone else already advanced it.
  return 0
end

redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), ARGV[1])
return 1
