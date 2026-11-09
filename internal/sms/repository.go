package sms

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// SQL for the messages and idempotency_keys tables (owned by this module).

// claimIdempotencyKey tries to reserve (customerID, key) for this request.
// It returns claimed=true and the row's created_at if this request now owns
// the key. If another transaction holds the same key uncommitted, Postgres
// makes this INSERT wait until that transaction ends: on commit we get
// claimed=false; on rollback our INSERT goes through.
func claimIdempotencyKey(ctx context.Context, tx pgx.Tx, customerID int64, key, hash string, messageID uuid.UUID) (bool, time.Time, error) {
	var createdAt time.Time
	err := tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (customer_id, key, request_hash, message_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (customer_id, key) DO NOTHING
		RETURNING created_at`,
		customerID, key, hash, messageID,
	).Scan(&createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, time.Time{}, nil
	}
	if err != nil {
		return false, time.Time{}, fmt.Errorf("claim idempotency key: %w", err)
	}
	return true, createdAt, nil
}

type storedKey struct {
	hash      string
	messageID uuid.UUID
	createdAt time.Time
}

func selectIdempotencyKey(ctx context.Context, tx pgx.Tx, customerID int64, key string) (storedKey, error) {
	var k storedKey
	err := tx.QueryRow(ctx, `
		SELECT request_hash, message_id, created_at
		FROM idempotency_keys WHERE customer_id = $1 AND key = $2`,
		customerID, key,
	).Scan(&k.hash, &k.messageID, &k.createdAt)
	if err != nil {
		return storedKey{}, fmt.Errorf("select idempotency key: %w", err)
	}
	return k, nil
}

// insertMessage stores a new message and returns its created_at and
// expires_at. createdAt == nil means "use now()".
// With an idempotency key, createdAt is the key row's created_at, so a replay
// can find the message by its full primary key (id, created_at) and Postgres
// only looks in that one day's partition.
// ttl != nil (express) sets expires_at = created_at + ttl, computed in SQL so
// both timestamps come from the same clock.
func insertMessage(ctx context.Context, tx pgx.Tx, m Message, createdAt *time.Time, ttl *time.Duration) (time.Time, *time.Time, error) {
	var ttlSeconds *float64
	if ttl != nil {
		secs := ttl.Seconds()
		ttlSeconds = &secs
	}
	var stored time.Time
	var expiresAt *time.Time
	err := tx.QueryRow(ctx, `
		WITH t AS (SELECT COALESCE($8::timestamptz, now()) AS created_at)
		INSERT INTO messages (id, customer_id, recipient, body, priority, status, cost, created_at, expires_at)
		SELECT $1::uuid, $2::bigint, $3::text, $4::text, $5::text, $6::text, $7::bigint, t.created_at,
		       t.created_at + make_interval(secs => $9::float8)  -- NULL when $9 is NULL
		FROM t
		RETURNING created_at, expires_at`,
		m.ID, m.CustomerID, m.Recipient, m.Body, string(m.Priority), string(m.Status), m.Cost, createdAt, ttlSeconds,
	).Scan(&stored, &expiresAt)
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("insert message: %w", err)
	}
	return stored, expiresAt, nil
}

func selectMessage(ctx context.Context, tx pgx.Tx, id uuid.UUID, createdAt time.Time) (Message, error) {
	var m Message
	var priority, status string
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, recipient, body, priority, status, cost, created_at, expires_at
		FROM messages WHERE id = $1 AND created_at = $2`,
		id, createdAt,
	).Scan(&m.ID, &m.CustomerID, &m.Recipient, &m.Body, &priority, &status, &m.Cost, &m.CreatedAt, &m.ExpiresAt)
	if err != nil {
		return Message{}, fmt.Errorf("select message %s: %w", id, err)
	}
	m.Priority, m.Status = Priority(priority), Status(status)
	return m, nil
}

// change describes one status transition plus the columns it also sets.
type change struct {
	to            Status
	countAttempt  bool    // attempts = attempts + 1 (entering sending)
	lastError     *string // nil = keep
	operatorMsgID *string // nil = keep
	setHandedAt   bool    // handed_at = now() (entering sent)
}

// transitioned is what a successful transition returns.
type transitioned struct {
	customerID int64
	cost       int64
	handedAt   *time.Time
}

// transition moves a message to c.to, but only from one of the allowed
// source states (allowedFrom). changed=false means the message was not in
// an allowed state (duplicate or late event) and nothing was written.
func transition(ctx context.Context, tx pgx.Tx, id uuid.UUID, createdAt time.Time, c change) (changed bool, res transitioned, err error) {
	from := allowedFrom(c.to)
	fromStrings := make([]string, len(from))
	for i, s := range from {
		fromStrings[i] = string(s)
	}
	attemptDelta := 0
	if c.countAttempt {
		attemptDelta = 1
	}

	err = tx.QueryRow(ctx, `
		UPDATE messages SET
		  status          = $3,
		  updated_at      = now(),
		  attempts        = attempts + $5,
		  last_error      = COALESCE($6, last_error),
		  operator_msg_id = COALESCE($7, operator_msg_id),
		  handed_at       = CASE WHEN $8 THEN now() ELSE handed_at END
		WHERE id = $1 AND created_at = $2 AND status = ANY($4)
		RETURNING customer_id, cost, handed_at`,
		id, createdAt, string(c.to), fromStrings, attemptDelta, c.lastError, c.operatorMsgID, c.setHandedAt,
	).Scan(&res.customerID, &res.cost, &res.handedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, transitioned{}, nil
	}
	if err != nil {
		return false, transitioned{}, fmt.Errorf("transition message %s to %s: %w", id, c.to, err)
	}
	return true, res, nil
}
