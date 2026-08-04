package taskq

import (
	"math"
	"testing"
	"time"
)

// maxRand is a deterministic randFunc returning the largest value in [0, n),
// which makes jittered output exactly predictable.
func maxRand(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return n - 1
}

// zeroRand always returns the low end of the range.
func zeroRand(int64) int64 { return 0 }

func TestExponentialNext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		backoff Exponential
		attempt int
		want    time.Duration
	}{
		{
			name:    "first attempt is base",
			backoff: Exponential{Base: time.Second, Cap: time.Hour, Jitter: JitterNone},
			attempt: 1,
			want:    time.Second,
		},
		{
			name:    "doubles each attempt",
			backoff: Exponential{Base: time.Second, Cap: time.Hour, Jitter: JitterNone},
			attempt: 4,
			want:    8 * time.Second,
		},
		{
			name:    "saturates at cap",
			backoff: Exponential{Base: time.Second, Cap: 10 * time.Second, Jitter: JitterNone},
			attempt: 20,
			want:    10 * time.Second,
		},
		{
			name:    "attempt zero is treated as first attempt",
			backoff: Exponential{Base: time.Second, Cap: time.Hour, Jitter: JitterNone},
			attempt: 0,
			want:    time.Second,
		},
		{
			name:    "negative attempt is treated as first attempt",
			backoff: Exponential{Base: time.Second, Cap: time.Hour, Jitter: JitterNone},
			attempt: -5,
			want:    time.Second,
		},
		{
			name:    "base above cap is clamped to cap",
			backoff: Exponential{Base: time.Hour, Cap: time.Second, Jitter: JitterNone},
			attempt: 1,
			want:    time.Second,
		},
		{
			// The regression this guards: computing base<<(attempt-1) directly
			// overflows int64 here and wraps negative, which would turn the
			// longest backoff into an instant retry.
			name:    "absurd attempt count does not overflow",
			backoff: Exponential{Base: time.Second, Cap: time.Hour, Jitter: JitterNone},
			attempt: math.MaxInt32,
			want:    time.Hour,
		},
		{
			name:    "zero value uses documented defaults",
			backoff: Exponential{Jitter: JitterNone},
			attempt: 1,
			want:    DefaultBackoffBase,
		},
		{
			name:    "zero value saturates at default cap",
			backoff: Exponential{Jitter: JitterNone},
			attempt: 100,
			want:    DefaultBackoffCap,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := tt.backoff
			b.rnd = zeroRand
			if got := b.Next(tt.attempt); got != tt.want {
				t.Errorf("Next(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestJitterFuncs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		jitter JitterFunc
		d      time.Duration
		rnd    randFunc
		want   time.Duration
	}{
		{"none passes through", JitterNone, 8 * time.Second, zeroRand, 8 * time.Second},
		{"full at low end is zero", JitterFull, 8 * time.Second, zeroRand, 0},
		{"full at high end is just under d", JitterFull, 8 * time.Second, maxRand, 8*time.Second - 1},
		{"equal at low end is half", JitterEqual, 8 * time.Second, zeroRand, 4 * time.Second},
		{"equal at high end is just under d", JitterEqual, 8 * time.Second, maxRand, 8*time.Second - 1},
		{"full of zero is zero", JitterFull, 0, maxRand, 0},
		{"equal of zero is zero", JitterEqual, 0, maxRand, 0},
		{"full of negative is zero", JitterFull, -time.Second, maxRand, 0},
		{"equal of negative is zero", JitterEqual, -time.Second, maxRand, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.jitter(tt.d, tt.rnd); got != tt.want {
				t.Errorf("jitter(%v) = %v, want %v", tt.d, got, tt.want)
			}
		})
	}
}

// TestJitterFullStaysInRange exercises the real randomness source rather than a
// stub, since the whole point of jitter is what the live distribution does.
func TestJitterFullStaysInRange(t *testing.T) {
	t.Parallel()

	b := &Exponential{Base: time.Second, Cap: time.Minute, Jitter: JitterFull}
	for attempt := 1; attempt <= 12; attempt++ {
		for range 200 {
			got := b.Next(attempt)
			if got < 0 {
				t.Fatalf("Next(%d) = %v, must never be negative", attempt, got)
			}
			if got >= time.Minute {
				t.Fatalf("Next(%d) = %v, must stay under the cap", attempt, got)
			}
		}
	}
}

// TestJitterFullDecorrelates is the property that actually matters: two jobs
// failing at the same instant must not retry at the same instant.
func TestJitterFullDecorrelates(t *testing.T) {
	t.Parallel()

	b := &Exponential{Base: time.Second, Cap: time.Minute, Jitter: JitterFull}
	seen := make(map[time.Duration]int)
	const samples = 500
	for range samples {
		seen[b.Next(8)]++
	}
	// With a ~60s range quantised to nanoseconds, collisions should be
	// essentially impossible. Allow a wide margin so this can never flake.
	if len(seen) < samples/2 {
		t.Errorf("only %d distinct delays across %d samples; jitter is not spreading retries",
			len(seen), samples)
	}
}
