package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidPlan wraps every plan validation problem (400).
var ErrInvalidPlan = errors.New("invalid plan")

// MaxPlanValue bounds each plan field. Far above anything one wallet row can
// sustain (see docs/architecture.md §2), so it only rejects typos.
const MaxPlanValue = 100_000

// Plan is a customer's rate-limit plan: GLOBAL messages per second per class,
// and the bucket size used for both classes.
type Plan struct {
	NormalRPS  int `json:"normal_rps"`
	ExpressRPS int `json:"express_rps"`
	Burst      int `json:"burst"`
}

// Validate checks every field is in 1..MaxPlanValue.
func (p Plan) Validate() error {
	for name, v := range map[string]int{"normal_rps": p.NormalRPS, "express_rps": p.ExpressRPS, "burst": p.Burst} {
		if v < 1 || v > MaxPlanValue {
			return fmt.Errorf("%w: %s must be between 1 and %d", ErrInvalidPlan, name, MaxPlanValue)
		}
	}
	return nil
}

// Defaults are the limits for customers without a plan.
type Defaults struct {
	Normal  Limit
	Express Limit
}

// PlanCache holds every customer's plan in memory, so rate limiting never
// queries the database. The map is replaced as a whole on each refresh and
// read through an atomic pointer: readers never take a lock.
type PlanCache struct {
	pool  *pgxpool.Pool
	log   *slog.Logger
	plans atomic.Pointer[map[int64]Plan]
}

// NewPlanCache returns an empty cache. Call Refresh (or Run) to load it.
func NewPlanCache(pool *pgxpool.Pool, log *slog.Logger) *PlanCache {
	c := &PlanCache{pool: pool, log: log}
	empty := map[int64]Plan{}
	c.plans.Store(&empty)
	return c
}

// Get returns the customer's plan, if it has one.
func (c *PlanCache) Get(customerID int64) (Plan, bool) {
	p, ok := (*c.plans.Load())[customerID]
	return p, ok
}

// Refresh reloads all plans from the database. A full reload is cheap for
// tens of thousands of rows; for far more, load only rows with
// updated_at > the last refresh instead.
func (c *PlanCache) Refresh(ctx context.Context) error {
	plans, err := loadPlans(ctx, c.pool)
	if err != nil {
		return err
	}
	c.plans.Store(&plans)
	return nil
}

// Set updates one plan in the local copy, right after this instance wrote
// it, so it applies here at once (other instances pick it up on refresh).
// Copy-on-write: readers keep using the old map until the swap.
func (c *PlanCache) Set(customerID int64, p Plan) {
	for {
		old := c.plans.Load()
		next := make(map[int64]Plan, len(*old)+1)
		for k, v := range *old {
			next[k] = v
		}
		next[customerID] = p
		if c.plans.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Run refreshes every interval until ctx is cancelled. A failed refresh
// keeps the previous plans (stale limits beat no limits).
func (c *PlanCache) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
				c.log.Warn("rate limit plans refresh failed; keeping previous plans", "err", err)
			}
		}
	}
}

// LimitFunc turns plans into per-instance limits: the customer's plan if it
// has one, else the defaults, divided by the number of API instances (each
// instance enforces its share of the global limit; burst never below 1).
func (c *PlanCache) LimitFunc(d Defaults, instances int) LimitFunc {
	share := func(l Limit) Limit {
		return Limit{Rate: l.Rate / float64(instances), Burst: max(l.Burst/instances, 1)}
	}
	return func(customerID int64, class string) (Limit, bool) {
		if p, ok := c.Get(customerID); ok {
			switch class {
			case ClassNormal:
				return share(Limit{Rate: float64(p.NormalRPS), Burst: p.Burst}), true
			case ClassExpress:
				return share(Limit{Rate: float64(p.ExpressRPS), Burst: p.Burst}), true
			}
			return Limit{}, false
		}
		switch class {
		case ClassNormal:
			return share(d.Normal), true
		case ClassExpress:
			return share(d.Express), true
		}
		return Limit{}, false
	}
}
