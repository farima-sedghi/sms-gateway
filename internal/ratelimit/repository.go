package ratelimit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQL for the customer_limits table (owned by this module).

func upsertPlan(ctx context.Context, pool *pgxpool.Pool, customerID int64, p Plan) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO customer_limits (customer_id, normal_rps, express_rps, burst)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (customer_id) DO UPDATE SET
		  normal_rps  = EXCLUDED.normal_rps,
		  express_rps = EXCLUDED.express_rps,
		  burst       = EXCLUDED.burst,
		  updated_at  = now()`,
		customerID, p.NormalRPS, p.ExpressRPS, p.Burst,
	)
	if err != nil {
		return fmt.Errorf("upsert plan: %w", err)
	}
	return nil
}

func loadPlans(ctx context.Context, pool *pgxpool.Pool) (map[int64]Plan, error) {
	rows, err := pool.Query(ctx, `SELECT customer_id, normal_rps, express_rps, burst FROM customer_limits`)
	if err != nil {
		return nil, fmt.Errorf("load plans: %w", err)
	}
	plans := make(map[int64]Plan)
	var (
		id int64
		p  Plan
	)
	_, err = pgx.ForEachRow(rows, []any{&id, &p.NormalRPS, &p.ExpressRPS, &p.Burst}, func() error {
		plans[id] = p
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load plans: %w", err)
	}
	return plans, nil
}
