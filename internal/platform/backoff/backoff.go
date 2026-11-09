// Package backoff computes retry delays. Pure functions: no sleeping, no
// randomness of their own, so they are trivial to test.
package backoff

import "time"

// Exponential returns base * 2^(attempt-1), capped at max.
// attempt starts at 1: Exponential(1, 1s, 30s) = 1s, (2) = 2s, (3) = 4s ...
func Exponential(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	return min(d, max)
}

// EqualJitter returns a delay in [d/2, d], picked by r in [0, 1).
// Spreading retries this way stops many clients that failed together from
// retrying in lockstep, while still waiting at least half the delay.
func EqualJitter(d time.Duration, r float64) time.Duration {
	half := d / 2
	return half + time.Duration(r*float64(half))
}
