package wallet

import (
	"errors"
	"fmt"
)

// Domain errors. Callers compare with errors.Is; the HTTP layer maps them to
// status codes. (Sentinel errors are the Go equivalent of a small set of
// custom exception types; they are immutable values, not shared state.)
var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrInvalidAmount       = errors.New("invalid amount")
)

// Kind is the reason for a ledger entry. Values match the CHECK constraint
// on ledger_entries.kind.
type Kind string

const (
	KindTopUp  Kind = "topup"
	KindDebit  Kind = "debit"
	KindRefund Kind = "refund"
)

// MaxTopUp caps one top-up (in minor units). It keeps balance + amount far
// from the BIGINT limit (~9.2e18), so the sum can never overflow.
const MaxTopUp int64 = 1_000_000_000_000

// validateTopUp checks a top-up amount.
func validateTopUp(amount int64) error {
	if amount <= 0 || amount > MaxTopUp {
		return fmt.Errorf("%w: must be between 1 and %d", ErrInvalidAmount, MaxTopUp)
	}
	return nil
}

// validateCost checks a debit/refund amount. It must be positive: the sign of
// the ledger row comes from the Kind, never from the caller.
func validateCost(cost int64) error {
	if cost <= 0 {
		return fmt.Errorf("%w: cost must be positive", ErrInvalidAmount)
	}
	return nil
}
