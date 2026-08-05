-- Take or renew a named lease.
--
-- Acquisition and renewal are the same operation: the current holder refreshing
-- its own lease and a newcomer taking a lapsed one differ only in what GET
-- returns. Collapsing them means there is one code path to reason about instead
-- of two that must agree.
--
-- The owner check is what stops a lapsed leader from stomping its successor.
-- Without it, a process that paused past its TTL would wake up, blindly SET the
-- key, and steal leadership back from whoever had legitimately taken over.
--
-- KEYS[1] lease key
-- ARGV[1] owner ID
-- ARGV[2] TTL in ms
--
-- Returns 1 if the caller now holds the lease, 0 if someone else does.

local current = redis.call('GET', KEYS[1])

if not current or current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
  return 1
end

return 0
