package ratelimit

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a time source the test moves by hand.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newLimiter(clock *fakeClock) *Memory {
	return NewMemory(Config{
		Classes: map[string]Limit{
			"normal":  {Rate: 10, Burst: 5},
			"express": {Rate: 2, Burst: 2},
		},
		IdleTTL: time.Minute,
		Now:     clock.Now,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestBurstThenDeniedWithRetryAfter(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := newLimiter(clock)

	for i := 0; i < 5; i++ {
		if ok, _ := l.Allow(1, "normal"); !ok {
			t.Fatalf("request %d within burst was denied", i+1)
		}
	}
	ok, retryAfter := l.Allow(1, "normal")
	if ok {
		t.Fatal("request after burst was allowed")
	}
	// 10 tokens/s -> next token in 100ms.
	if retryAfter < 90*time.Millisecond || retryAfter > 110*time.Millisecond {
		t.Errorf("retryAfter = %v, want ~100ms", retryAfter)
	}
}

func TestDeniedRequestsDoNotConsumeTokens(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := newLimiter(clock)
	for i := 0; i < 5; i++ {
		l.Allow(1, "normal")
	}
	for i := 0; i < 100; i++ { // hammering while empty must not push the wait further out
		l.Allow(1, "normal")
	}
	clock.Advance(100 * time.Millisecond)
	if ok, _ := l.Allow(1, "normal"); !ok {
		t.Error("token not available 100ms later: rejected requests consumed tokens")
	}
}

func TestTokensRefillOverTime(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := newLimiter(clock)
	for i := 0; i < 5; i++ {
		l.Allow(1, "normal")
	}
	clock.Advance(time.Second) // 10 tokens/s, capped at burst 5
	allowed := 0
	for i := 0; i < 10; i++ {
		if ok, _ := l.Allow(1, "normal"); ok {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("allowed after refill = %d, want 5 (burst cap)", allowed)
	}
}

func TestClassesAndCustomersAreIndependent(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := newLimiter(clock)
	for i := 0; i < 2; i++ {
		l.Allow(1, "express")
	}
	if ok, _ := l.Allow(1, "express"); ok {
		t.Fatal("express burst not enforced")
	}
	if ok, _ := l.Allow(1, "normal"); !ok {
		t.Error("an empty express bucket blocked normal traffic")
	}
	if ok, _ := l.Allow(2, "express"); !ok {
		t.Error("customer 1's empty bucket blocked customer 2")
	}
	if ok, _ := l.Allow(1, "unconfigured"); !ok {
		t.Error("a class without a limit must not be limited")
	}
}

func TestEvictRemovesOnlyIdleBuckets(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	l := newLimiter(clock)
	l.Allow(1, "normal")
	clock.Advance(2 * time.Minute)
	l.Allow(2, "normal") // recent

	if n := l.Evict(); n != 1 {
		t.Errorf("evicted = %d, want 1", n)
	}
	if l.Len() != 1 {
		t.Errorf("remaining buckets = %d, want 1", l.Len())
	}
}

// IdleTTL shorter than the refill time would let eviction reset a bucket
// that was still refilling; NewMemory raises it.
func TestIdleTTLIsAtLeastRefillTime(t *testing.T) {
	l := NewMemory(Config{
		Classes: map[string]Limit{"slow": {Rate: 1, Burst: 600}}, // 10 minutes to refill
		IdleTTL: time.Minute,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if l.cfg.IdleTTL < 10*time.Minute {
		t.Errorf("IdleTTL = %v, want >= 10m", l.cfg.IdleTTL)
	}
}

func TestConcurrentAllowNeverExceedsBurst(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)} // frozen: no refill during the test
	l := newLimiter(clock)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.Allow(1, "normal"); ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 5 {
		t.Errorf("allowed = %d, want exactly the burst (5)", allowed.Load())
	}
}
