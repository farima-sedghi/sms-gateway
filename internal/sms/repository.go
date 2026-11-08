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

// insertMessage stores a new message. createdAt == nil means "use now()".
// With an idempotency key, createdAt is the key row's created_at, so a replay
// can find the message by its full primary key (id, created_at) and Postgres
// only looks in that one day's partition.
func insertMessage(ctx context.Context, tx pgx.Tx, m Message, createdAt *time.Time) (time.Time, error) {
	var stored time.Time
	err := tx.QueryRow(ctx, `
		INSERT INTO messages (id, customer_id, recipient, body, priority, status, cost, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8::timestamptz, now()))
		RETURNING created_at`,
		m.ID, m.CustomerID, m.Recipient, m.Body, string(m.Priority), string(m.Status), m.Cost, createdAt,
	).Scan(&stored)
	if err != nil {
		return time.Time{}, fmt.Errorf("insert message: %w", err)
	}
	return stored, nil
}

func selectMessage(ctx context.Context, tx pgx.Tx, id uuid.UUID, createdAt time.Time) (Message, error) {
	var m Message
	var priority, status string
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, recipient, body, priority, status, cost, created_at
		FROM messages WHERE id = $1 AND created_at = $2`,
		id, createdAt,
	).Scan(&m.ID, &m.CustomerID, &m.Recipient, &m.Body, &priority, &status, &m.Cost, &m.CreatedAt)
	if err != nil {
		return Message{}, fmt.Errorf("select message %s: %w", id, err)
	}
	m.Priority, m.Status = Priority(priority), Status(status)
	return m, nil
}

// transition moves a message to status `to`, but only from one of the
// allowed source states. changed=false means the message was not in an
// allowed state (duplicate or late event) and nothing was written.
func transition(ctx context.Context, tx pgx.Tx, id uuid.UUID, createdAt time.Time, to Status) (changed bool, customerID, cost int64, err error) {
	from := allowedFrom(to)
	fromStrings := make([]string, len(from))
	for i, s := range from {
		fromStrings[i] = string(s)
	}

	err = tx.QueryRow(ctx, `
		UPDATE messages SET status = $3, updated_at = now()
		WHERE id = $1 AND created_at = $2 AND status = ANY($4)
		RETURNING customer_id, cost`,
		id, createdAt, string(to), fromStrings,
	).Scan(&customerID, &cost)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, 0, 0, nil
	}
	if err != nil {
		return false, 0, 0, fmt.Errorf("transition message %s to %s: %w", id, to, err)
	}
	return true, customerID, cost, nil
}
