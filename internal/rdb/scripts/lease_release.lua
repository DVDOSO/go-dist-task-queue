-- Release a lease the caller holds.
--
-- Fenced on owner so a lapsed holder cannot delete its successor's lease. The
-- GET and DEL have to be one script for that check to mean anything: between a
-- separate GET and DEL the lease could expire and be taken by someone else,
-- and the DEL would then free a lease the caller no longer owned.
--
-- Releasing on shutdown is what makes failover immediate rather than
-- TTL-delayed, which is the difference between a rolling deploy pausing the
-- scheduler for a moment and pausing it for a full lease period.
--
-- KEYS[1] lease key
-- ARGV[1] owner ID
--
-- Returns 1 if a lease was released, 0 if the caller did not hold it.

if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end

return 0
