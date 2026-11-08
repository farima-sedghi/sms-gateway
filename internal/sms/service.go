package sms

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/farima-sedghi/sms-gateway/internal/outbox"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

// Service holds the message use cases.
type Service struct {
	pool  *pgxpool.Pool
	price int64 // cost of one message in minor units (same for every priority)
}

// NewService returns the sms service. price must be > 0 (checked by the caller's config).
func NewService(pool *pgxpool.Pool, price int64) *Service {
	return &Service{pool: pool, price: price}
}

// AcceptResult is the outcome of Accept.
type AcceptResult struct {
	Message Message
	// Replayed is true when the Idempotency-Key was already used with the
	// same request: the original message is returned and nothing is charged.
	Replayed bool
}

// outboxPayload is the event the dispatcher consumes. created_at is included
// so the dispatcher can address the message by its full primary key
// (id, created_at) and hit a single partition.
type outboxPayload struct {
	MessageID  uuid.UUID `json:"message_id"`
	CustomerID int64     `json:"customer_id"`
	Recipient  string    `json:"recipient"`
	Body       string    `json:"body"`
	Priority   Priority  `json:"priority"`
	Cost       int64     `json:"cost"`
	CreatedAt  time.Time `json:"created_at"`
	Attempt    int       `json:"attempt"`
}

// Accept validates, charges and queues one message, all in ONE transaction:
//
//  1. claim the idempotency key (if given); replay or 409 if it exists
//  2. wallet.Debit (ErrInsufficientBalance -> everything rolls back)
//  3. insert the message with status queued
//  4. insert the outbox event for sms.express / sms.normal
//
// idemKey == "" skips step 1 entirely: a client retry then creates a second
// message and a second charge.
func (s *Service) Accept(ctx context.Context, customerID int64, req SendRequest, idemKey string) (AcceptResult, error) {
	if customerID <= 0 {
		return AcceptResult{}, fmt.Errorf("%w: customer_id must be a positive integer", ErrValidation)
	}
	req = req.Normalize()
	if err := req.Validate(); err != nil {
		return AcceptResult{}, err
	}
	if err := ValidateIdempotencyKey(idemKey); err != nil {
		return AcceptResult{}, err
	}

	// UUIDv7 starts with a millisecond timestamp, so IDs are roughly ordered
	// by time and new index entries go to the end of the B-tree (UUIDv4
	// would scatter writes across the whole index).
	id, err := uuid.NewV7()
	if err != nil {
		return AcceptResult{}, fmt.Errorf("generate message id: %w", err)
	}

	var result AcceptResult
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var createdAt *time.Time // nil -> messages.created_at = now()

		if idemKey != "" {
			hash := req.Hash()
			claimed, keyCreatedAt, err := claimIdempotencyKey(ctx, tx, customerID, idemKey, hash, id)
			if err != nil {
				return err
			}
			if !claimed {
				stored, err := selectIdempotencyKey(ctx, tx, customerID, idemKey)
				if err != nil {
					return err
				}
				if stored.hash != hash {
					return ErrIdempotencyConflict
				}
				msg, err := selectMessage(ctx, tx, stored.messageID, stored.createdAt)
				if err != nil {
					return err
				}
				result = AcceptResult{Message: msg, Replayed: true}
				return nil // read-only: commit changes nothing
			}
			createdAt = &keyCreatedAt
		}

		if err := wallet.Debit(ctx, tx, customerID, id, s.price); err != nil {
			return err
		}

		msg := Message{
			ID:         id,
			CustomerID: customerID,
			Recipient:  req.Recipient,
			Body:       req.Body,
			Priority:   req.Priority,
			Status:     StatusQueued,
			Cost:       s.price,
		}
		stored, err := insertMessage(ctx, tx, msg, createdAt)
		if err != nil {
			return err
		}
		msg.CreatedAt = stored

		payload, err := json.Marshal(outboxPayload{
			MessageID:  msg.ID,
			CustomerID: msg.CustomerID,
			Recipient:  msg.Recipient,
			Body:       msg.Body,
			Priority:   msg.Priority,
			Cost:       msg.Cost,
			CreatedAt:  msg.CreatedAt,
			Attempt:    0,
		})
		if err != nil {
			return fmt.Errorf("marshal outbox payload: %w", err)
		}
		if err := outbox.Insert(ctx, tx, TopicFor(msg.Priority), msg.ID.String(), payload); err != nil {
			return err
		}

		result = AcceptResult{Message: msg}
		return nil
	})
	if err != nil {
		return AcceptResult{}, err
	}
	return result, nil
}

// MarkSending moves a message from queued/retrying to sending.
// changed=false means it was not in one of those states (duplicate delivery).
func (s *Service) MarkSending(ctx context.Context, id uuid.UUID, createdAt time.Time) (bool, error) {
	return s.simpleTransition(ctx, id, createdAt, StatusSending)
}

// MarkSent moves a message from sending to sent (terminal).
func (s *Service) MarkSent(ctx context.Context, id uuid.UUID, createdAt time.Time) (bool, error) {
	return s.simpleTransition(ctx, id, createdAt, StatusSent)
}

func (s *Service) simpleTransition(ctx context.Context, id uuid.UUID, createdAt time.Time, to Status) (bool, error) {
	var changed bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		changed, _, _, err = transition(ctx, tx, id, createdAt, to)
		return err
	})
	return changed, err
}

// FailAndRefund marks a message failed and refunds its cost, in one
// transaction. The refund happens only if the status UPDATE changed exactly
// one row, which makes the whole operation idempotent: a second call, or a
// late "fail" for a message that is already sent, matches 0 rows and does
// nothing. refunded reports whether money was returned.
func (s *Service) FailAndRefund(ctx context.Context, id uuid.UUID, createdAt time.Time) (refunded bool, err error) {
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		changed, customerID, cost, err := transition(ctx, tx, id, createdAt, StatusFailed)
		if err != nil || !changed {
			return err
		}
		if err := wallet.Refund(ctx, tx, customerID, id, cost); err != nil {
			return err
		}
		refunded = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return refunded, nil
}
