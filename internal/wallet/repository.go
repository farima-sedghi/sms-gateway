package wallet

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SQL for the wallets and ledger_entries tables. Every function that changes
// money takes a pgx.Tx, so it can only run inside a transaction and the
// balance change and its ledger row always commit together.

// addBalance creates the wallet if needed and adds amount. Returns the new balance.
func addBalance(ctx context.Context, tx pgx.Tx, customerID, amount int64) (int64, error) {
	var balance int64
	err := tx.QueryRow(ctx, `
		INSERT INTO wallets (customer_id, balance) VALUES ($1, $2)
		ON CONFLICT (customer_id)
		DO UPDATE SET balance = wallets.balance + EXCLUDED.balance, updated_at = now()
		RETURNING balance`,
		customerID, amount,
	).Scan(&balance)
	if err != nil {
		return 0, fmt.Errorf("add balance: %w", err)
	}
	return balance, nil
}

// subtractBalance is the atomic debit. The WHERE clause is the whole
// concurrency story: Postgres locks the row, re-checks balance >= cost on the
// latest committed value, and either updates it or matches 0 rows. No read
// before the write, so there is no window for a race.
func subtractBalance(ctx context.Context, tx pgx.Tx, customerID, cost int64) (bool, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE wallets SET balance = balance - $2, updated_at = now()
		WHERE customer_id = $1 AND balance >= $2`,
		customerID, cost,
	)
	if err != nil {
		return false, fmt.Errorf("subtract balance: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// creditExisting adds amount to an existing wallet (used by refunds: the
// wallet must exist, because the message was debited from it).
func creditExisting(ctx context.Context, tx pgx.Tx, customerID, amount int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE wallets SET balance = balance + $2, updated_at = now()
		WHERE customer_id = $1`,
		customerID, amount,
	)
	if err != nil {
		return fmt.Errorf("credit wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("credit wallet: wallet for customer %d not found", customerID)
	}
	return nil
}

// insertLedger appends one ledger row. amount is signed (debit < 0).
// messageID is nil for top-ups.
func insertLedger(ctx context.Context, tx pgx.Tx, customerID, amount int64, kind Kind, messageID *uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (customer_id, amount, kind, message_id)
		VALUES ($1, $2, $3, $4)`,
		customerID, amount, string(kind), messageID,
	)
	if err != nil {
		return fmt.Errorf("insert ledger entry: %w", err)
	}
	return nil
}

// selectBalance reads the cached balance. A customer without a wallet has 0.
func selectBalance(ctx context.Context, pool *pgxpool.Pool, customerID int64) (int64, error) {
	var balance int64
	err := pool.QueryRow(ctx,
		`SELECT balance FROM wallets WHERE customer_id = $1`, customerID,
	).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("select balance: %w", err)
	}
	return balance, nil
}
