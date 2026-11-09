package sms

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

// Lifecycle owns every status change after a message is accepted. The
// dispatcher calls it; it never touches the messages table directly.
//
// Every method is guarded by the allow-list in allowedFrom and returns
// changed=false (not an error) when the message is not in an allowed state:
// that is the normal result of a duplicate or late Kafka delivery.
// Messages are addressed by their full primary key (id, created_at).
type Lifecycle struct {
	pool *pgxpool.Pool
}

// NewLifecycle returns the status-transition service.
func NewLifecycle(pool *pgxpool.Pool) *Lifecycle {
	return &Lifecycle{pool: pool}
}

// MarkSending: queued | retrying | sending -> sending, and counts the attempt.
// Re-entry from sending is allowed so a redelivered record can finish the
// job; changed=false means the message is already terminal: skip it.
func (l *Lifecycle) MarkSending(ctx context.Context, id uuid.UUID, createdAt time.Time) (bool, error) {
	changed, _, err := l.apply(ctx, id, createdAt, change{to: StatusSending, countAttempt: true})
	return changed, err
}

// MarkSent: sending -> sent (terminal). Records the operator's id and
// handed_at, which it returns (for the end-to-end latency).
func (l *Lifecycle) MarkSent(ctx context.Context, id uuid.UUID, createdAt time.Time, operatorMsgID string) (bool, time.Time, error) {
	changed, res, err := l.apply(ctx, id, createdAt, change{
		to: StatusSent, operatorMsgID: &operatorMsgID, setHandedAt: true,
	})
	if err != nil || !changed || res.handedAt == nil {
		return changed, time.Time{}, err
	}
	return true, *res.handedAt, nil
}

// MarkRetrying: sending -> retrying.
func (l *Lifecycle) MarkRetrying(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) (bool, error) {
	changed, _, err := l.apply(ctx, id, createdAt, change{to: StatusRetrying, lastError: &lastErr})
	return changed, err
}

// MarkUnknown: sending | retrying -> unknown (terminal). The operator may
// have delivered the message, so there is NO refund.
func (l *Lifecycle) MarkUnknown(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) (bool, error) {
	changed, _, err := l.apply(ctx, id, createdAt, change{to: StatusUnknown, lastError: &lastErr})
	return changed, err
}

// FailAndRefund marks a message failed and refunds its cost, in one
// transaction. The refund happens only if the status UPDATE changed exactly
// one row, which makes the whole operation idempotent: a second call, or a
// late "fail" for a message that is already sent, matches 0 rows and does
// nothing. refunded reports whether money was returned.
func (l *Lifecycle) FailAndRefund(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) (refunded bool, err error) {
	err = pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		changed, res, err := transition(ctx, tx, id, createdAt, change{to: StatusFailed, lastError: &lastErr})
		if err != nil || !changed {
			return err
		}
		if err := wallet.Refund(ctx, tx, res.customerID, id, res.cost); err != nil {
			return fmt.Errorf("refund message %s: %w", id, err)
		}
		refunded = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return refunded, nil
}

func (l *Lifecycle) apply(ctx context.Context, id uuid.UUID, createdAt time.Time, c change) (bool, transitioned, error) {
	var (
		changed bool
		res     transitioned
	)
	err := pgx.BeginFunc(ctx, l.pool, func(tx pgx.Tx) error {
		var err error
		changed, res, err = transition(ctx, tx, id, createdAt, c)
		return err
	})
	return changed, res, err
}
