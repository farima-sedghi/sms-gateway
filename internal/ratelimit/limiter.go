package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Limit is one token bucket: Rate tokens per second, up to Burst saved up.
type Limit struct {
	Rate  float64 // sustained requests per second
	Burst int     // requests allowed at once after a quiet period
}

// refillTime is how long an empty bucket takes to fill up completely.
func (l Limit) refillTime() time.Duration {
	if l.Rate <= 0 {
		return 0
	}
	return time.Duration(float64(l.Burst) / l.Rate * float64(time.Second))
}

// Config sets the limits per class (e.g. "express", "normal").
type Config struct {
	Classes map[string]Limit
	// IdleTTL: buckets unused for this long are deleted. It is raised to at
	// least each class's refill time, so a deleted bucket was always full,
	// exactly like a new one: eviction frees memory but never changes a decision.
	IdleTTL time.Duration
	// Now is the clock; nil means time.Now. Tests inject a fake clock.
	Now func() time.Time
}

type key struct {
	customerID int64
	class      string
}

type bucket struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// Memory is an in-process token bucket per (customer, class).
//
// It is per instance: with N API replicas behind a round-robin load
// balancer, configure each with global limit / N (see docs/adr/0007).
// Safe for concurrent use.
type Memory struct {
	cfg     Config
	log     *slog.Logger
	mu      sync.Mutex
	buckets map[key]*bucket
}

// NewMemory returns an in-memory limiter.
func NewMemory(cfg Config, log *slog.Logger) *Memory {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	for _, l := range cfg.Classes {
		cfg.IdleTTL = max(cfg.IdleTTL, l.refillTime())
	}
	return &Memory{cfg: cfg, log: log, buckets: make(map[key]*bucket)}
}

// Allow takes one token for (customerID, class). If the bucket is empty it
// returns false and how long until a token is available (for Retry-After).
// A class with no configured limit is not limited.
func (m *Memory) Allow(customerID int64, class string) (bool, time.Duration) {
	limit, ok := m.cfg.Classes[class]
	if !ok {
		return true, 0
	}
	now := m.cfg.Now()

	m.mu.Lock()
	b, ok := m.buckets[key{customerID, class}]
	if !ok {
		// A new bucket starts full: a customer that has been quiet gets its burst.
		b = &bucket{limiter: rate.NewLimiter(rate.Limit(limit.Rate), limit.Burst)}
		m.buckets[key{customerID, class}] = b
	}
	b.lastUsed = now
	m.mu.Unlock()

	// rate.Limiter has its own lock, so the map lock is not held here.
	// Reserve tells us exactly when the token would be available; if that is
	// in the future we cancel, so a rejected request does not use a token.
	r := b.limiter.ReserveN(now, 1)
	if !r.OK() { // Burst is 0: never allowed
		return false, time.Second
	}
	if delay := r.DelayFrom(now); delay > 0 {
		r.CancelAt(now)
		return false, delay
	}
	return true, 0
}

// Run deletes idle buckets every interval until ctx is cancelled. Without
// it, every customer that ever sent a message would keep a bucket forever.
func (m *Memory) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := m.Evict(); n > 0 {
				m.log.Debug("rate limiter evicted idle buckets", "evicted", n, "remaining", m.Len())
			}
		}
	}
}

// Evict deletes buckets unused for longer than IdleTTL and returns how many.
func (m *Memory) Evict() int {
	cutoff := m.cfg.Now().Add(-m.cfg.IdleTTL)
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, b := range m.buckets {
		if b.lastUsed.Before(cutoff) {
			delete(m.buckets, k) // deleting during range is safe in Go
			n++
		}
	}
	return n
}

// Len returns the number of live buckets.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buckets)
}
