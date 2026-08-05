package taskq

import "math/rand/v2"

// queueSelector decides, for each poll, which order to try the queues in.
//
// This is where priority policy lives. Deliberately not in the broker: keeping
// it in Go means it is testable as a pure function and cannot drift between
// broker implementations, and for Redis it means the Lua script never has to
// know what a weight is. The broker just walks the order it is given.
//
// Not safe for concurrent use. Only the fetch loop calls it, and there is one
// per worker.
type queueSelector struct {
	names   []string
	weights []int
	total   int
	strict  bool

	// order and pool are reused across calls so that selecting a queue on
	// every poll — thousands per second per worker — allocates nothing.
	order []string
	pool  []int
}

func newQueueSelector(names []string, weights map[string]int, strict bool) *queueSelector {
	s := &queueSelector{
		names:   names,
		weights: make([]int, len(names)),
		strict:  strict,
		order:   make([]string, 0, len(names)),
		pool:    make([]int, 0, len(names)),
	}
	for i, n := range names {
		w := 1
		if weights != nil {
			if configured, ok := weights[n]; ok {
				w = configured
			}
		}
		s.weights[i] = w
		s.total += w
	}
	return s
}

// next returns the queue order to try.
//
// The returned slice is owned by the selector and overwritten on the next call,
// which is fine because the broker reads it synchronously during the claim.
func (s *queueSelector) next() []string {
	// Nothing to decide.
	if s.strict || len(s.names) < 2 {
		return s.names
	}

	// Weighted random sampling without replacement: repeatedly pick from the
	// remaining queues in proportion to their weights.
	//
	// The property that matters is the *first* position, since the broker
	// returns the first job it finds: a queue weighted 6 out of 10 is tried
	// first 60% of the time. The rest of the order is the fallback when the
	// earlier queues turn out to be empty.
	//
	// O(n^2) in the number of queues, which is irrelevant — a worker consumes a
	// handful of queues, not thousands, and this avoids any allocation.
	s.pool = s.pool[:0]
	for i := range s.names {
		s.pool = append(s.pool, i)
	}
	s.order = s.order[:0]

	remaining := s.total
	for len(s.pool) > 0 {
		// rand.IntN over the remaining weight, then walk the cumulative sum.
		r := rand.IntN(remaining) //nolint:gosec // G404: queue fairness, not security
		pick, acc := 0, 0
		for k, idx := range s.pool {
			acc += s.weights[idx]
			if r < acc {
				pick = k
				break
			}
		}

		idx := s.pool[pick]
		s.order = append(s.order, s.names[idx])
		remaining -= s.weights[idx]
		s.pool = append(s.pool[:pick], s.pool[pick+1:]...)
	}

	return s.order
}
