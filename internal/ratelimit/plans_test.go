package ratelimit

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres/pgtest"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var defaults = Defaults{
	Normal:  Limit{Rate: 200, Burst: 400},
	Express: Limit{Rate: 50, Burst: 100},
}

// send fires n requests evenly over one simulated second and returns how
// many were allowed.
func send(l *Memory, clock *fakeClock, customerID int64, class string, n int) int {
	allowed := 0
	step := time.Second / time.Duration(n)
	for i := 0; i < n; i++ {
		if ok, _ := l.Allow(customerID, class); ok {
			allowed++
		}
		clock.Advance(step)
	}
	return allowed
}

func newPlanLimiter(cache *PlanCache, clock *fakeClock, instances int) *Memory {
	return NewMemory(Config{Limits: cache.LimitFunc(defaults, instances), IdleTTL: time.Minute, Now: clock.Now}, quietLogger())
}

// The requirement: a customer on a high plan isn't throttled at 1000/s, a
// customer on the default plan is.
func TestHighPlanIsNotThrottledAt1000PerSecondDefaultIs(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cache := NewPlanCache(nil, quietLogger())
	cache.Set(1, Plan{NormalRPS: 2000, ExpressRPS: 100, Burst: 2000})
	l := newPlanLimiter(cache, clock, 1)

	if got := send(l, clock, 1, ClassNormal, 1000); got != 1000 {
		t.Errorf("high-plan customer: allowed %d of 1000, want all", got)
	}
	// Default plan: 400 burst + ~200 refilled during the second.
	got := send(l, clock, 2, ClassNormal, 1000)
	if got < 590 || got > 610 {
		t.Errorf("default customer: allowed %d of 1000, want ~600 (burst 400 + 200/s)", got)
	}
}

// A plan change applies to a customer that already has a bucket.
func TestPlanChangeAppliesToExistingBucket(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	cache := NewPlanCache(nil, quietLogger())
	l := newPlanLimiter(cache, clock, 1)

	send(l, clock, 7, ClassNormal, 1000) // default plan: bucket exists and is drained
	cache.Set(7, Plan{NormalRPS: 5000, ExpressRPS: 50, Burst: 5000})
	clock.Advance(time.Second) // refills at the new rate, up to the new burst
	if got := send(l, clock, 7, ClassNormal, 3000); got != 3000 {
		t.Errorf("after upgrade: allowed %d of 3000, want all", got)
	}
}

// Each instance enforces its share of the global plan.
func TestPlanIsDividedAcrossInstances(t *testing.T) {
	cache := NewPlanCache(nil, quietLogger())
	cache.Set(1, Plan{NormalRPS: 3000, ExpressRPS: 300, Burst: 600})
	lim, ok := cache.LimitFunc(defaults, 3)(1, ClassExpress)
	if !ok || lim.Rate != 100 || lim.Burst != 200 {
		t.Errorf("per-instance express limit = %+v, want rate 100 burst 200", lim)
	}
	def, _ := cache.LimitFunc(defaults, 3)(99, ClassNormal)
	if def.Rate < 66 || def.Rate > 67 || def.Burst != 133 {
		t.Errorf("per-instance default = %+v, want rate ~66.7 burst 133", def)
	}
}

func TestPlanValidation(t *testing.T) {
	if err := (Plan{NormalRPS: 1, ExpressRPS: 1, Burst: 1}).Validate(); err != nil {
		t.Errorf("minimal plan rejected: %v", err)
	}
	for _, p := range []Plan{
		{NormalRPS: 0, ExpressRPS: 1, Burst: 1},
		{NormalRPS: 1, ExpressRPS: -1, Burst: 1},
		{NormalRPS: 1, ExpressRPS: 1, Burst: MaxPlanValue + 1},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("plan %+v accepted", p)
		}
	}
}

// Through HTTP and Postgres: PUT stores the plan, applies it on this
// instance at once, and another instance sees it after a refresh.
func TestPutPlanStoresAndApplies(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	cache := NewPlanCache(pool, quietLogger())
	pgtest.Must(t, cache.Refresh(ctx))

	mux := http.NewServeMux()
	NewHandler(pool, cache, quietLogger()).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	put := func(path, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		pgtest.Must(t, err)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	code, body := put("/customers/42/limits", `{"normal_rps":2000,"express_rps":100,"burst":4000}`)
	var got planResponse
	pgtest.Must(t, json.Unmarshal([]byte(body), &got))
	if code != 200 || got.CustomerID != 42 || got.NormalRPS != 2000 || got.Burst != 4000 {
		t.Fatalf("PUT: %d %s", code, body)
	}
	if p, ok := cache.Get(42); !ok || p.NormalRPS != 2000 {
		t.Errorf("local cache not updated: %+v %v", p, ok)
	}
	if n := pgtest.QueryInt(t, pool, `SELECT normal_rps FROM customer_limits WHERE customer_id = 42`); n != 2000 {
		t.Errorf("stored normal_rps = %d, want 2000", n)
	}

	// Another instance (its own cache) picks it up on refresh.
	other := NewPlanCache(pool, quietLogger())
	pgtest.Must(t, other.Refresh(ctx))
	if p, ok := other.Get(42); !ok || p.ExpressRPS != 100 {
		t.Errorf("other instance after refresh: %+v %v", p, ok)
	}

	// Update in place.
	if code, body := put("/customers/42/limits", `{"normal_rps":10,"express_rps":5,"burst":20}`); code != 200 {
		t.Fatalf("second PUT: %d %s", code, body)
	}
	pgtest.Must(t, other.Refresh(ctx))
	if p, _ := other.Get(42); p.NormalRPS != 10 {
		t.Errorf("after update: %+v, want normal_rps 10", p)
	}

	for name, body := range map[string]string{
		"zero rate":     `{"normal_rps":0,"express_rps":1,"burst":1}`,
		"missing field": `{"normal_rps":10,"burst":1}`,
		"unknown field": `{"normal_rps":10,"express_rps":1,"burst":1,"rps":5}`,
		"not json":      `nope`,
	} {
		if code, _ := put("/customers/42/limits", body); code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, code)
		}
	}
}
