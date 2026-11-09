package report

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service answers report queries. Read-only.
type Service struct {
	pool *pgxpool.Pool
}

// NewService returns the report service. In production the pool could point
// at a read replica, since reports tolerate a little replication lag.
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// List returns one page of a customer's messages in [From, To), newest first.
func (s *Service) List(ctx context.Context, q Query) (Page, error) {
	cur, err := q.normalize()
	if err != nil {
		return Page{}, err
	}

	// Fetch one extra row: if it exists, there is a next page.
	rows, err := listMessages(ctx, s.pool, q, cur, q.Limit+1)
	if err != nil {
		return Page{}, err
	}

	page := Page{Messages: rows}
	if len(rows) > q.Limit {
		page.Messages = rows[:q.Limit]
		last := page.Messages[q.Limit-1]
		next := encodeCursor(Cursor{CreatedAt: last.CreatedAt, ID: last.ID, Filter: q.filterHash()})
		page.NextCursor = &next
	}
	if page.Messages == nil {
		page.Messages = []MessageView{} // JSON [] instead of null
	}
	return page, nil
}

// Get returns one message of a customer. Another customer's message, or an
// id that is not a UUIDv7 (we never issue those), is ErrNotFound.
func (s *Service) Get(ctx context.Context, customerID int64, id uuid.UUID) (MessageView, error) {
	t, err := TimeFromUUIDv7(id)
	if err != nil {
		return MessageView{}, ErrNotFound
	}
	m, err := getMessage(ctx, s.pool, customerID, id, t.Add(-idWindow), t.Add(idWindow))
	if errors.Is(err, pgx.ErrNoRows) {
		return MessageView{}, ErrNotFound
	}
	return m, err
}
