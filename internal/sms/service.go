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

// RateLimiter decides whether a customer may send one more message of a
// class ("express" or "normal"). Defined here, where it is used:
// *ratelimit.Memory satisfies it today, a Redis-backed limiter can later.
type RateLimiter interface {
	Allow(customerID int64, class string) (ok bool, retryAfter time.Duration)
}

// Service accepts messages (used by the API). Status changes after
// acceptance live in Lifecycle (used by the dispatcher).
type Service struct {
	pool       *pgxpool.Pool
	price      int64         // cost of one message in minor units (same for every priority)
	expressTTL time.Duration // express messages must reach the operator within this
	limiter    RateLimiter   // nil = no rate limiting
}

// NewService returns the sms service. price and expressTTL must be > 0
// (checked by the caller's config). limiter may be nil (no limiting).
func NewService(pool *pgxpool.Pool, price int64, expressTTL time.Duration, limiter RateLimiter) *Service {
	return &Service{pool: pool, price: price, expressTTL: expressTTL, limiter: limiter}
}

// AcceptResult is the outcome of Accept.
type AcceptResult struct {
	Message Message
	// Replayed is true when the Idempotency-Key was already used with the
	// same request: the original message is returned and nothing is charged.
	Replayed bool
}

// Accept validates, rate-limits, then charges and queues one message in
// ONE transaction:
//
//  0. per-customer rate limit, BEFORE any database work (429 if exceeded)
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

	// Rate limit after validation (invalid requests don't use tokens) and
	// before the transaction (a limited request never touches Postgres).
	if s.limiter != nil {
		if ok, retryAfter := s.limiter.Allow(customerID, string(req.Priority)); !ok {
			return AcceptResult{}, &RateLimitError{RetryAfter: retryAfter}
		}
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
		var ttl *time.Duration // only express messages expire
		if msg.Priority == PriorityExpress {
			ttl = &s.expressTTL
		}
		msg.CreatedAt, msg.ExpiresAt, err = insertMessage(ctx, tx, msg, createdAt, ttl)
		if err != nil {
			return err
		}

		payload, err := json.Marshal(Event{
			MessageID:  msg.ID,
			CustomerID: msg.CustomerID,
			Recipient:  msg.Recipient,
			Body:       msg.Body,
			Priority:   msg.Priority,
			Cost:       msg.Cost,
			CreatedAt:  msg.CreatedAt,
			ExpiresAt:  msg.ExpiresAt,
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
