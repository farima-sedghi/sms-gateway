package report

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

// report only READS the messages table (owned by sms). It never writes.

const selectColumns = `id, recipient, priority, status, cost, attempts, created_at, handed_at, last_error`

// listMessages returns up to limit rows of one page, newest first.
//
// The SQL is built from fixed fragments (never from user input; all values
// are parameters) so that the conditions the planner needs are always plain
// AND terms it can use:
//   - created_at >= from AND created_at < to: partition pruning, and the
//     bounds of the (customer_id, created_at DESC) index scan.
//   - created_at <= cursor_t: lets the index scan START at the cursor,
//     so page 1000 costs the same as page 1 (no OFFSET).
//   - (created_at, id) < (cursor_t, cursor_id): exact keyset position,
//     correct even when two rows share a created_at (id breaks the tie).
func listMessages(ctx context.Context, pool *pgxpool.Pool, q Query, cur *Cursor, limit int) ([]MessageView, error) {
	var sb strings.Builder
	args := []any{q.CustomerID, q.From, q.To}
	sb.WriteString(`SELECT ` + selectColumns + ` FROM messages
		WHERE customer_id = $1 AND created_at >= $2 AND created_at < $3`)

	if q.Status != "" {
		args = append(args, string(q.Status))
		fmt.Fprintf(&sb, ` AND status = $%d`, len(args))
	}
	if cur != nil {
		args = append(args, cur.CreatedAt, cur.ID)
		t, id := len(args)-1, len(args)
		fmt.Fprintf(&sb, ` AND created_at <= $%d AND (created_at, id) < ($%d, $%d)`, t, t, id)
	}
	args = append(args, limit)
	fmt.Fprintf(&sb, ` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	out, err := pgx.CollectRows(rows, scanView)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	return out, nil
}

// getMessageSQL finds one message. The created_at window comes from the
// UUIDv7 time, so Postgres reads one daily partition (two around midnight)
// instead of probing every partition's primary key.
const getMessageSQL = `SELECT ` + selectColumns + ` FROM messages
	WHERE id = $1 AND customer_id = $2 AND created_at >= $3 AND created_at < $4`

func getMessage(ctx context.Context, pool *pgxpool.Pool, customerID int64, id uuid.UUID, from, to time.Time) (MessageView, error) {
	rows, err := pool.Query(ctx, getMessageSQL, id, customerID, from, to)
	if err != nil {
		return MessageView{}, fmt.Errorf("get message: %w", err)
	}
	return pgx.CollectExactlyOneRow(rows, scanView) // pgx.ErrNoRows if absent
}

func scanView(row pgx.CollectableRow) (MessageView, error) {
	var (
		m                MessageView
		priority, status string
	)
	err := row.Scan(&m.ID, &m.Recipient, &priority, &status, &m.Cost, &m.Attempts,
		&m.CreatedAt, &m.HandedAt, &m.LastError)
	m.Priority, m.Status = sms.Priority(priority), sms.Status(status)
	m.CreatedAt = m.CreatedAt.UTC()
	if m.HandedAt != nil {
		t := m.HandedAt.UTC()
		m.HandedAt = &t
	}
	return m, err
}
