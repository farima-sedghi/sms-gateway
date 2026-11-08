package wallet

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Service holds the wallet use cases that own their transaction.
type Service struct {
	pool *pgxpool.Pool
}

// NewService returns a wallet service backed by pool.
func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// TopUp adds amount (minor units) to the customer's balance and returns the
// new balance. The wallet is created on the first top-up.
func (s *Service) TopUp(ctx context.Context, customerID, amount int64) (int64, error) {
	if err := validateTopUp(amount); err != nil {
		return 0, err
	}
	var balance int64
	// pgx.BeginFunc commits if the function returns nil and rolls back
	// otherwise, like a TransactionScope that completes only on success.
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		if balance, err = addBalance(ctx, tx, customerID, amount); err != nil {
			return err
		}
		return insertLedger(ctx, tx, customerID, amount, KindTopUp, nil)
	})
	return balance, err
}

// Balance returns the customer's current balance (0 if they have no wallet).
func (s *Service) Balance(ctx context.Context, customerID int64) (int64, error) {
	return selectBalance(ctx, s.pool, customerID)
}

// Debit charges cost for one message inside the caller's transaction.
// It returns ErrInsufficientBalance if the balance is lower than cost (or the
// customer has no wallet); the caller must then roll back.
//
// Debit takes the caller's tx on purpose (see docs/adr/0001): the sms module
// needs the debit, the message row and the outbox row to commit atomically.
func Debit(ctx context.Context, tx pgx.Tx, customerID int64, messageID uuid.UUID, cost int64) error {
	if err := validateCost(cost); err != nil {
		return err
	}
	ok, err := subtractBalance(ctx, tx, customerID, cost)
	if err != nil {
		return err
	}
	if !ok {
		return ErrInsufficientBalance
	}
	return insertLedger(ctx, tx, customerID, -cost, KindDebit, &messageID)
}

// Refund gives cost back for one message inside the caller's transaction.
//
// Refund is NOT idempotent by itself: calling it twice refunds twice. The
// caller must guard it with a status transition in the same transaction and
// call Refund only if that transition changed exactly one row (see
// sms.Service.FailAndRefund and docs/adr/0003).
func Refund(ctx context.Context, tx pgx.Tx, customerID int64, messageID uuid.UUID, cost int64) error {
	if err := validateCost(cost); err != nil {
		return err
	}
	if err := creditExisting(ctx, tx, customerID, cost); err != nil {
		return err
	}
	return insertLedger(ctx, tx, customerID, cost, KindRefund, &messageID)
}
