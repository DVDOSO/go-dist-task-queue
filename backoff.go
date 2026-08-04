package taskq

import (
	"math/rand/v2"
	"time"
)

// Default retry policy. Twenty-five attempts under a ten-minute ceiling spans
// roughly three and a half hours of retrying, which is long enough to ride out
// a dependency outage without holding a job forever.
const (
	DefaultBackoffBase = 1 * time.Second
	DefaultBackoffCap  = 10 * time.Minute
	DefaultMaxAttempts = 25
)

// Backoff computes how long to wait before re-running a job.
//
// attempt is the number of the attempt that just failed, counting from 1, so
// the first retry is Next(1).
type Backoff interface {
	Next(attempt int) time.Duration
}

// randFunc returns a pseudo-random value in [0, n). It is injectable purely so
// that tests can assert on exact durations instead of ranges.
type randFunc func(n int64) int64

// JitterFunc perturbs a computed backoff delay to decorrelate retries.
type JitterFunc func(d time.Duration, rnd randFunc) time.Duration

// JitterNone returns the delay unchanged.
//
// Useful for tests and for single-producer workloads, but a poor default: if a
// downstream dependency fails for a thousand jobs at once, all thousand retry
// in lockstep and hit it again simultaneously.
func JitterNone(d time.Duration, _ randFunc) time.Duration { return d }

// JitterFull picks uniformly from [0, d).
//
// This is the variant that decorrelates a thundering herd most aggressively,
// which is why it is the default. The cost is that early retries can fire almost
// immediately; that is acceptable because the ceiling still grows exponentially.
func JitterFull(d time.Duration, rnd randFunc) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rnd(int64(d)))
}

// JitterEqual picks uniformly from [d/2, d).
//
// A middle ground: it keeps a guaranteed minimum wait, at the cost of a tighter
// clustering of retries than JitterFull.
func JitterEqual(d time.Duration, rnd randFunc) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rnd(int64(d-half)))
}

// Exponential is a doubling backoff with a ceiling and configurable jitter:
//
//	delay = jitter(min(Base * 2^(attempt-1), Cap))
//
// The zero value is usable and behaves as the documented defaults.
type Exponential struct {
	// Base is the delay before the first retry, pre-jitter. Zero means
	// DefaultBackoffBase.
	Base time.Duration
	// Cap is the ceiling on the pre-jitter delay. Zero means DefaultBackoffCap.
	Cap time.Duration
	// Jitter perturbs the capped delay. Nil means JitterFull.
	Jitter JitterFunc
	// rnd is the randomness source; nil means the global math/rand/v2.
	rnd randFunc
}

// Compile-time proof that Exponential satisfies the interface.
var _ Backoff = (*Exponential)(nil)

// Next implements Backoff.
func (e *Exponential) Next(attempt int) time.Duration {
	base := e.Base
	if base <= 0 {
		base = DefaultBackoffBase
	}
	ceiling := e.Cap
	if ceiling <= 0 {
		ceiling = DefaultBackoffCap
	}
	if base > ceiling {
		base = ceiling
	}
	jitter := e.Jitter
	if jitter == nil {
		jitter = JitterFull
	}
	rnd := e.rnd
	if rnd == nil {
		// Jitter does not need cryptographic randomness; predicting a retry
		// delay grants an attacker nothing.
		rnd = rand.Int64N //nolint:gosec // G404: not security-sensitive
	}

	if attempt < 1 {
		attempt = 1
	}

	// Double with saturation rather than computing base<<(attempt-1) directly:
	// a large attempt count would overflow int64 and wrap to a negative
	// duration, turning a backoff into an immediate retry storm. The loop exits
	// after at most ~63 iterations because d doubles every pass.
	d := base
	for range attempt - 1 {
		if d >= ceiling {
			break
		}
		next := d * 2
		if next < d { // overflowed
			d = ceiling
			break
		}
		d = next
	}
	if d > ceiling {
		d = ceiling
	}

	return jitter(d, rnd)
}
