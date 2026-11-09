package ratelimit

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Classes of traffic with separate buckets. They match sms priorities.
const (
	ClassNormal  = "normal"
	ClassExpress = "express"
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

// LimitFunc returns the current limit for (customer, class). ok=false means
// "not limited". It is called on every request, so it must not do I/O: in
// production it reads the in-memory PlanCache (see plans.go).
type LimitFunc func(customerID int64, class string) (limit Limit, ok bool)

// Static returns a LimitFunc with the same limit per class for everyone.
func Static(classes map[string]Limit) LimitFunc {
	return func(_ int64, class string) (Limit, bool) {
		l, ok := classes[class]
		return l, ok
	}
}

// Config configures a Memory limiter.
type Config struct {
	Limits LimitFunc
	// IdleTTL: buckets unused for this long are deleted. Each bucket is also
	// kept for at least its own refill time, so a deleted bucket was always
	// full, exactly like a new one: eviction frees memory but never changes a
	// decision.
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
// balancer, limits are configured as global / N (see docs/adr/0007).
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
	return &Memory{cfg: cfg, log: log, buckets: make(map[key]*bucket)}
}

// Allow takes one token for (customerID, class). If the bucket is empty it
// returns false and how long until a token is available (for Retry-After).
func (m *Memory) Allow(customerID int64, class string) (bool, time.Duration) {
	limit, ok := m.cfg.Limits(customerID, class)
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

	// rate.Limiter has its own lock, so the map lock is not held below.
	// If the customer's plan changed, update the bucket in place: the new
	// rate applies from now on, and tokens already saved are kept (capped at
	// the new burst).
	lim := b.limiter
	if lim.Limit() != rate.Limit(limit.Rate) {
		lim.SetLimitAt(now, rate.Limit(limit.Rate))
	}
	if lim.Burst() != limit.Burst {
		lim.SetBurstAt(now, limit.Burst)
	}

	// Reserve tells us exactly when the token would be available; if that is
	// in the future we cancel, so a rejected request does not use a token.
	r := lim.ReserveN(now, 1)
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

// Evict deletes buckets unused for longer than max(IdleTTL, their refill
// time) and returns how many.
func (m *Memory) Evict() int {
	now := m.cfg.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for k, b := range m.buckets {
		keep := max(m.cfg.IdleTTL, Limit{Rate: float64(b.limiter.Limit()), Burst: b.limiter.Burst()}.refillTime())
		if now.Sub(b.lastUsed) > keep {
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
