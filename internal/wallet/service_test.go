package wallet_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres/pgtest"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

// The core money invariant: N parallel debits on one balance never overspend
// and never go negative. 100 goroutines race for a balance that pays for 50.
func TestConcurrentDebitsNeverOverspend(t *testing.T) {
	pool := pgtest.New(t)
	svc := wallet.NewService(pool)
	ctx := context.Background()

	const (
		customerID = 1
		cost       = 100
		attempts   = 100
		affordable = 50
	)
	_, err := svc.TopUp(ctx, customerID, cost*affordable)
	pgtest.Must(t, err)

	var succeeded, insufficient atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{}) // released at once so the debits really overlap
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				return wallet.Debit(ctx, tx, customerID, uuid.New(), cost)
			})
			switch {
			case errors.Is(err, wallet.ErrInsufficientBalance):
				insufficient.Add(1)
			case err != nil:
				t.Errorf("unexpected debit error: %v", err)
			default:
				succeeded.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := succeeded.Load(); got != affordable {
		t.Errorf("succeeded debits = %d, want %d", got, affordable)
	}
	if got := insufficient.Load(); got != attempts-affordable {
		t.Errorf("rejected debits = %d, want %d", got, attempts-affordable)
	}
	balance, err := svc.Balance(ctx, customerID)
	pgtest.Must(t, err)
	if balance != 0 {
		t.Errorf("balance = %d, want 0", balance)
	}
	// The ledger must agree with the cached balance.
	ledgerSum := pgtest.QueryInt(t, pool,
		`SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE customer_id = $1`, customerID)
	if ledgerSum != balance {
		t.Errorf("ledger sum = %d, balance = %d; they must match", ledgerSum, balance)
	}
	debits := pgtest.QueryInt(t, pool,
		`SELECT COUNT(*) FROM ledger_entries WHERE customer_id = $1 AND kind = 'debit'`, customerID)
	if debits != affordable {
		t.Errorf("debit ledger rows = %d, want %d", debits, affordable)
	}
}

func TestTopUpCreatesAndAddsToWallet(t *testing.T) {
	pool := pgtest.New(t)
	svc := wallet.NewService(pool)
	ctx := context.Background()

	if b, _ := svc.Balance(ctx, 7); b != 0 {
		t.Fatalf("balance before first top-up = %d, want 0", b)
	}
	b, err := svc.TopUp(ctx, 7, 500)
	pgtest.Must(t, err)
	if b != 500 {
		t.Errorf("after first top-up: %d, want 500", b)
	}
	b, err = svc.TopUp(ctx, 7, 250)
	pgtest.Must(t, err)
	if b != 750 {
		t.Errorf("after second top-up: %d, want 750", b)
	}
	if n := pgtest.QueryInt(t, pool, `SELECT COUNT(*) FROM ledger_entries WHERE kind = 'topup'`); n != 2 {
		t.Errorf("topup ledger rows = %d, want 2", n)
	}
}

func TestTopUpRejectsInvalidAmount(t *testing.T) {
	pool := pgtest.New(t)
	svc := wallet.NewService(pool)

	for _, amount := range []int64{0, -1, wallet.MaxTopUp + 1} {
		if _, err := svc.TopUp(context.Background(), 1, amount); !errors.Is(err, wallet.ErrInvalidAmount) {
			t.Errorf("TopUp(%d) error = %v, want ErrInvalidAmount", amount, err)
		}
	}
}

func TestDebitWithoutWalletIsInsufficient(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return wallet.Debit(ctx, tx, 404, uuid.New(), 100)
	})
	if !errors.Is(err, wallet.ErrInsufficientBalance) {
		t.Fatalf("error = %v, want ErrInsufficientBalance", err)
	}
}
