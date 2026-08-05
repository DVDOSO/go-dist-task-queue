package taskq

import (
	"math"
	"testing"
)

// TestWeightedSelectionApproximatesRatios is the whole point of weighting: over
// many polls, the share of first positions should track the configured weights.
//
// Statistical, so it asserts a tolerance rather than an exact figure. The
// sample is large enough that the tolerance is far outside normal sampling
// error — a genuinely wrong distribution fails, a slightly unlucky run does not.
func TestWeightedSelectionApproximatesRatios(t *testing.T) {
	t.Parallel()

	const samples = 60_000
	names := []string{"critical", "default", "low"}
	weights := map[string]int{"critical": 6, "default": 3, "low": 1}

	s := newQueueSelector(names, weights, false)
	firstCount := map[string]int{}
	for range samples {
		firstCount[s.next()[0]]++
	}

	want := map[string]float64{"critical": 0.6, "default": 0.3, "low": 0.1}
	const tolerance = 0.02

	for name, wantShare := range want {
		got := float64(firstCount[name]) / float64(samples)
		if math.Abs(got-wantShare) > tolerance {
			t.Errorf("%s was tried first %.1f%% of the time, want %.1f%% (±%.0f%%)",
				name, got*100, wantShare*100, tolerance*100)
		}
	}
}

// TestWeightedSelectionNeverStarves is the reason weighted random beats strict
// priority: a low-weight queue is tried first sometimes, so it makes progress
// even while a high-weight queue is saturated.
func TestWeightedSelectionNeverStarves(t *testing.T) {
	t.Parallel()

	s := newQueueSelector([]string{"critical", "low"}, map[string]int{"critical": 99, "low": 1}, false)

	seen := false
	for range 10_000 {
		if s.next()[0] == "low" {
			seen = true
			break
		}
	}
	if !seen {
		t.Error("the low queue was never tried first in 10,000 polls; it would starve")
	}
}

func TestWeightedSelectionReturnsEveryQueue(t *testing.T) {
	t.Parallel()

	names := []string{"a", "b", "c", "d"}
	s := newQueueSelector(names, map[string]int{"a": 5, "b": 1, "c": 3, "d": 2}, false)

	for range 1_000 {
		order := s.next()
		if len(order) != len(names) {
			t.Fatalf("order has %d entries, want %d — every queue must remain reachable", len(order), len(names))
		}
		seen := map[string]bool{}
		for _, n := range order {
			if seen[n] {
				t.Fatalf("queue %q appeared twice in %v", n, order)
			}
			seen[n] = true
		}
	}
}

// TestStrictPriorityIsDeterministic: opting into strict priority means the
// configured order, every time, starvation included.
func TestStrictPriorityIsDeterministic(t *testing.T) {
	t.Parallel()

	names := []string{"critical", "default", "low"}
	s := newQueueSelector(names, map[string]int{"critical": 1, "low": 100}, true)

	for range 100 {
		order := s.next()
		for i := range names {
			if order[i] != names[i] {
				t.Fatalf("strict order = %v, want %v — weights must be ignored", order, names)
			}
		}
	}
}

func TestSingleQueueIsPassedThrough(t *testing.T) {
	t.Parallel()

	s := newQueueSelector([]string{"only"}, nil, false)
	for range 10 {
		order := s.next()
		if len(order) != 1 || order[0] != "only" {
			t.Fatalf("order = %v, want [only]", order)
		}
	}
}

func TestUnweightedQueuesDefaultToEqualShares(t *testing.T) {
	t.Parallel()

	const samples = 30_000
	s := newQueueSelector([]string{"a", "b"}, nil, false)

	count := map[string]int{}
	for range samples {
		count[s.next()[0]]++
	}

	got := float64(count["a"]) / float64(samples)
	if math.Abs(got-0.5) > 0.02 {
		t.Errorf("queue a was first %.1f%% of the time, want ~50%% when no weights are configured", got*100)
	}
}

// TestPartialWeightsDefaultTheRest: naming a weight for one queue must not
// silently zero the others.
func TestPartialWeightsDefaultTheRest(t *testing.T) {
	t.Parallel()

	const samples = 30_000
	s := newQueueSelector([]string{"heavy", "light"}, map[string]int{"heavy": 9}, false)

	count := map[string]int{}
	for range samples {
		count[s.next()[0]]++
	}

	got := float64(count["light"]) / float64(samples)
	if math.Abs(got-0.1) > 0.02 {
		t.Errorf("light was first %.1f%% of the time, want ~10%% (implicit weight 1 against 9)", got*100)
	}
}
