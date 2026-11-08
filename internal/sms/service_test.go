package sms_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres/pgtest"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

const price = 100

type env struct {
	pool   *pgxpool.Pool
	sms    *sms.Service
	wallet *wallet.Service
}

func newEnv(t *testing.T) env {
	t.Helper()
	pool := pgtest.New(t)
	return env{pool: pool, sms: sms.NewService(pool, price), wallet: wallet.NewService(pool)}
}

func (e env) topUp(t *testing.T, customerID, amount int64) {
	t.Helper()
	_, err := e.wallet.TopUp(context.Background(), customerID, amount)
	pgtest.Must(t, err)
}

func (e env) balance(t *testing.T, customerID int64) int64 {
	t.Helper()
	b, err := e.wallet.Balance(context.Background(), customerID)
	pgtest.Must(t, err)
	return b
}

func (e env) count(t *testing.T, table string) int64 {
	t.Helper()
	return pgtest.QueryInt(t, e.pool, "SELECT COUNT(*) FROM "+table)
}

func (e env) status(t *testing.T, id uuid.UUID) sms.Status {
	t.Helper()
	var s string
	pgtest.Must(t, e.pool.QueryRow(context.Background(),
		`SELECT status FROM messages WHERE id = $1`, id).Scan(&s))
	return sms.Status(s)
}

var hello = sms.SendRequest{Recipient: "+989121234567", Body: "hello"}

func TestAcceptSameKeyTwiceChargesOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)

	first, err := e.sms.Accept(ctx, 1, hello, "key-1")
	pgtest.Must(t, err)
	second, err := e.sms.Accept(ctx, 1, hello, "key-1")
	pgtest.Must(t, err)

	if first.Replayed || !second.Replayed {
		t.Errorf("replayed flags = (%v, %v), want (false, true)", first.Replayed, second.Replayed)
	}
	if first.Message.ID != second.Message.ID || !first.Message.CreatedAt.Equal(second.Message.CreatedAt) ||
		first.Message.Cost != second.Message.Cost || first.Message.Priority != second.Message.Priority {
		t.Errorf("replay returned a different message:\n first=%+v\nsecond=%+v", first.Message, second.Message)
	}
	if b := e.balance(t, 1); b != 1000-price {
		t.Errorf("balance = %d, want %d (one charge)", b, 1000-price)
	}
	for _, table := range []string{"messages", "outbox", "idempotency_keys"} {
		if n := e.count(t, table); n != 1 {
			t.Errorf("%s rows = %d, want 1", table, n)
		}
	}
}

func TestAcceptSameKeyDifferentBodyConflicts(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)

	_, err := e.sms.Accept(ctx, 1, hello, "key-1")
	pgtest.Must(t, err)

	other := hello
	other.Body = "a different text"
	_, err = e.sms.Accept(ctx, 1, other, "key-1")
	if !errors.Is(err, sms.ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}
	if b := e.balance(t, 1); b != 1000-price {
		t.Errorf("balance = %d, want %d", b, 1000-price)
	}
}

func TestIdempotencyKeyIsScopedPerCustomer(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)
	e.topUp(t, 2, 1000)

	a, err := e.sms.Accept(ctx, 1, hello, "shared-key")
	pgtest.Must(t, err)
	b, err := e.sms.Accept(ctx, 2, hello, "shared-key")
	pgtest.Must(t, err)
	if b.Replayed || a.Message.ID == b.Message.ID {
		t.Error("the same key from another customer must create a new message")
	}
}

// Ten clients retrying at the same moment with the same key: one charge.
func TestAcceptSameKeyConcurrentlyChargesOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)

	const n = 10
	results := make([]sms.AcceptResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = e.sms.Accept(ctx, 1, hello, "race-key")
		}(i)
	}
	close(start)
	wg.Wait()

	originals := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if results[i].Message.ID != results[0].Message.ID {
			t.Errorf("request %d got message %s, want %s", i, results[i].Message.ID, results[0].Message.ID)
		}
		if !results[i].Replayed {
			originals++
		}
	}
	if originals != 1 {
		t.Errorf("non-replayed responses = %d, want exactly 1", originals)
	}
	if b := e.balance(t, 1); b != 1000-price {
		t.Errorf("balance = %d, want %d", b, 1000-price)
	}
}

// A 402 rolls back everything, including the idempotency key, so the client
// can top up and retry with the same key.
func TestAcceptInsufficientBalanceWritesNothing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	_, err := e.sms.Accept(ctx, 1, hello, "key-1")
	if !errors.Is(err, wallet.ErrInsufficientBalance) {
		t.Fatalf("error = %v, want ErrInsufficientBalance", err)
	}
	for _, table := range []string{"messages", "outbox", "idempotency_keys", "ledger_entries"} {
		if n := e.count(t, table); n != 0 {
			t.Errorf("%s rows = %d after a rejected message, want 0", table, n)
		}
	}

	e.topUp(t, 1, price)
	res, err := e.sms.Accept(ctx, 1, hello, "key-1")
	pgtest.Must(t, err)
	if res.Replayed {
		t.Error("retry after top-up must be processed, not replayed")
	}
	if b := e.balance(t, 1); b != 0 {
		t.Errorf("balance = %d, want 0", b)
	}
}

// Documents the trade-off of an optional key: without one, a retry is a new message.
func TestAcceptWithoutKeyChargesEveryCall(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)

	for i := 0; i < 2; i++ {
		_, err := e.sms.Accept(ctx, 1, hello, "")
		pgtest.Must(t, err)
	}
	if n := e.count(t, "messages"); n != 2 {
		t.Errorf("messages = %d, want 2", n)
	}
	if n := e.count(t, "idempotency_keys"); n != 0 {
		t.Errorf("idempotency_keys = %d, want 0", n)
	}
	if b := e.balance(t, 1); b != 1000-2*price {
		t.Errorf("balance = %d, want %d", b, 1000-2*price)
	}
}

func TestAcceptWritesOutboxEvent(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)

	express := hello
	express.Priority = sms.PriorityExpress
	res, err := e.sms.Accept(ctx, 1, express, "")
	pgtest.Must(t, err)

	var topic, key string
	var payload []byte
	pgtest.Must(t, e.pool.QueryRow(ctx,
		`SELECT topic, key, payload FROM outbox`).Scan(&topic, &key, &payload))

	if topic != sms.TopicExpress {
		t.Errorf("topic = %q, want %q", topic, sms.TopicExpress)
	}
	if key != res.Message.ID.String() {
		t.Errorf("key = %q, want message id %s", key, res.Message.ID)
	}
	var got struct {
		MessageID uuid.UUID `json:"message_id"`
		CreatedAt time.Time `json:"created_at"`
		Attempt   int       `json:"attempt"`
	}
	pgtest.Must(t, json.Unmarshal(payload, &got))
	if got.MessageID != res.Message.ID || !got.CreatedAt.Equal(res.Message.CreatedAt) || got.Attempt != 0 {
		t.Errorf("payload = %+v, want id %s, created_at %s, attempt 0", got, res.Message.ID, res.Message.CreatedAt)
	}
	if res.Message.ID.Version() != 7 {
		t.Errorf("message id version = %d, want UUIDv7", res.Message.ID.Version())
	}
}

func TestFailAndRefundTwiceRefundsOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)
	res, err := e.sms.Accept(ctx, 1, hello, "")
	pgtest.Must(t, err)
	m := res.Message

	refunded, err := e.sms.FailAndRefund(ctx, m.ID, m.CreatedAt)
	pgtest.Must(t, err)
	if !refunded {
		t.Fatal("first FailAndRefund did not refund")
	}
	refunded, err = e.sms.FailAndRefund(ctx, m.ID, m.CreatedAt)
	pgtest.Must(t, err)
	if refunded {
		t.Error("second FailAndRefund refunded again")
	}

	if b := e.balance(t, 1); b != 1000 {
		t.Errorf("balance = %d, want 1000 (charged once, refunded once)", b)
	}
	if n := pgtest.QueryInt(t, e.pool, `SELECT COUNT(*) FROM ledger_entries WHERE kind = 'refund'`); n != 1 {
		t.Errorf("refund ledger rows = %d, want 1", n)
	}
	if s := e.status(t, m.ID); s != sms.StatusFailed {
		t.Errorf("status = %q, want failed", s)
	}
}

// A late "fail" event for a message that was already delivered must not
// change its status or refund it: sent is terminal.
func TestLateFailAfterSentIsIgnored(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)
	res, err := e.sms.Accept(ctx, 1, hello, "")
	pgtest.Must(t, err)
	m := res.Message

	for _, step := range []func(context.Context, uuid.UUID, time.Time) (bool, error){e.sms.MarkSending, e.sms.MarkSent} {
		changed, err := step(ctx, m.ID, m.CreatedAt)
		pgtest.Must(t, err)
		if !changed {
			t.Fatal("setup transition did not apply")
		}
	}

	refunded, err := e.sms.FailAndRefund(ctx, m.ID, m.CreatedAt)
	pgtest.Must(t, err)
	if refunded {
		t.Error("a sent message was refunded")
	}
	if s := e.status(t, m.ID); s != sms.StatusSent {
		t.Errorf("status = %q, want sent", s)
	}
	if b := e.balance(t, 1); b != 1000-price {
		t.Errorf("balance = %d, want %d", b, 1000-price)
	}
}

func TestTerminalStatesCannotBeLeft(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	e.topUp(t, 1, 1000)
	res, err := e.sms.Accept(ctx, 1, hello, "")
	pgtest.Must(t, err)
	m := res.Message

	_, err = e.sms.FailAndRefund(ctx, m.ID, m.CreatedAt)
	pgtest.Must(t, err)

	if changed, _ := e.sms.MarkSending(ctx, m.ID, m.CreatedAt); changed {
		t.Error("failed -> sending was allowed")
	}
	if changed, _ := e.sms.MarkSent(ctx, m.ID, m.CreatedAt); changed {
		t.Error("failed -> sent was allowed")
	}
	if s := e.status(t, m.ID); s != sms.StatusFailed {
		t.Errorf("status = %q, want failed", s)
	}
}
