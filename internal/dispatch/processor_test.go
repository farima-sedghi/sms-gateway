package dispatch

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/farima-sedghi/sms-gateway/internal/operator"
	"github.com/farima-sedghi/sms-gateway/internal/operator/operatormock"
	"github.com/farima-sedghi/sms-gateway/internal/outbox"
	"github.com/farima-sedghi/sms-gateway/internal/platform/kafka/kafkatest"
	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres/pgtest"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
	"github.com/farima-sedghi/sms-gateway/internal/wallet"
)

// --- test fixtures -----------------------------------------------------------

const (
	price       = 100
	maxAttempts = 5
	customer    = 1
	startFunds  = 1000
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeSender returns scripted results in order (nil = success); the last
// entry repeats. It counts calls.
type fakeSender struct {
	mu     sync.Mutex
	script []error
	calls  int
}

func (f *fakeSender) Send(_ context.Context, _, _, _ string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := min(f.calls, len(f.script)-1)
	f.calls++
	if len(f.script) == 0 || f.script[i] == nil {
		return "op-1", nil
	}
	return "", f.script[i]
}

func (f *fakeSender) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	brokers []string
	retry   string // test retry topic
	dlq     string // test DLQ topic
	accept  *sms.Service
	wallet  *wallet.Service
	life    *sms.Lifecycle
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := pgtest.New(t)
	brokers := kafkatest.Brokers(t)
	e := &env{
		t:       t,
		pool:    pool,
		brokers: brokers,
		retry:   kafkatest.Topic(t, brokers, 1),
		dlq:     kafkatest.Topic(t, brokers, 1),
		accept:  sms.NewService(pool, price, 2*time.Minute, nil),
		wallet:  wallet.NewService(pool),
		life:    sms.NewLifecycle(pool),
	}
	_, err := e.wallet.TopUp(context.Background(), customer, startFunds)
	pgtest.Must(t, err)
	return e
}

func (e *env) processor(sender Sender) *Processor {
	return NewProcessor(e.life, sender,
		kafkatest.Client(e.t, e.brokers, outbox.ProducerOptions(false)...),
		Config{
			RetryTopic:     e.retry,
			DLQTopic:       e.dlq,
			MaxAttempts:    maxAttempts,
			RetryBaseDelay: time.Second,
			RetryMaxDelay:  30 * time.Second,
			BreakerWait:    10 * time.Millisecond,
			DBTimeout:      5 * time.Second,
		}, quietLogger())
}

// accepted creates a real, charged message through sms.Accept and returns
// the event the dispatcher would receive for it.
func (e *env) accepted(priority sms.Priority) sms.Event {
	e.t.Helper()
	res, err := e.accept.Accept(context.Background(), customer,
		sms.SendRequest{Recipient: "+989121234567", Body: "hello", Priority: priority}, "")
	pgtest.Must(e.t, err)
	m := res.Message
	return sms.Event{
		MessageID: m.ID, CustomerID: m.CustomerID, Recipient: m.Recipient, Body: m.Body,
		Priority: m.Priority, Cost: m.Cost, CreatedAt: m.CreatedAt, ExpiresAt: m.ExpiresAt,
	}
}

func (e *env) handle(p *Processor, ev sms.Event) Result {
	e.t.Helper()
	res, err := p.Handle(context.Background(), ev)
	pgtest.Must(e.t, err)
	return res
}

func (e *env) status(id uuid.UUID) sms.Status {
	e.t.Helper()
	var s string
	pgtest.Must(e.t, e.pool.QueryRow(context.Background(), `SELECT status FROM messages WHERE id = $1`, id).Scan(&s))
	return sms.Status(s)
}

func (e *env) balance() int64 {
	e.t.Helper()
	b, err := e.wallet.Balance(context.Background(), customer)
	pgtest.Must(e.t, err)
	return b
}

func (e *env) refunds() int64 {
	return pgtest.QueryInt(e.t, e.pool, `SELECT COUNT(*) FROM ledger_entries WHERE kind = 'refund'`)
}

func (e *env) events(topic string, want int) []sms.Event {
	e.t.Helper()
	if got := kafkatest.RecordCount(e.t, e.brokers, topic); got != int64(want) {
		e.t.Fatalf("records on %s = %d, want %d", topic, got, want)
	}
	if want == 0 {
		return nil
	}
	var out []sms.Event
	for _, r := range kafkatest.ConsumeAll(e.t, e.brokers, topic, want, 20*time.Second) {
		var ev sms.Event
		pgtest.Must(e.t, json.Unmarshal(r.Value, &ev))
		out = append(out, ev)
	}
	return out
}

func expectOutcome(t *testing.T, res Result, want Outcome) {
	t.Helper()
	if res.Outcome != want {
		t.Fatalf("outcome = %q, want %q (err: %s)", res.Outcome, want, res.Err)
	}
}

// --- tests -------------------------------------------------------------------

func TestDuplicateDeliveryDoesNotResend(t *testing.T) {
	e := newEnv(t)
	sender := &fakeSender{}
	p := e.processor(sender)
	ev := e.accepted(sms.PriorityNormal)

	expectOutcome(t, e.handle(p, ev), OutcomeSent)
	expectOutcome(t, e.handle(p, ev), OutcomeSkipped)

	if sender.Calls() != 1 {
		t.Errorf("operator calls = %d, want 1", sender.Calls())
	}
	if e.status(ev.MessageID) != sms.StatusSent {
		t.Errorf("status = %q, want sent", e.status(ev.MessageID))
	}
}

// Two deliveries of the same record processed at the same time (e.g. during
// a rebalance) may both enter `sending` and both call the operator; the
// operator's dedupe by ref keeps it to one real send. Real client + mock.
func TestConcurrentDuplicatesSendOnceThroughOperatorDedupe(t *testing.T) {
	e := newEnv(t)
	mock := operatormock.New(operatormock.Config{Latency: 100 * time.Millisecond, Rand: func() float64 { return 0.99 }}, quietLogger())
	mux := http.NewServeMux()
	mock.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := operator.NewClient(operator.Config{
		BaseURL: srv.URL, Timeout: 2 * time.Second, BreakerFailures: 5, BreakerOpenFor: time.Minute,
	}, quietLogger())

	p := e.processor(client)
	ev := e.accepted(sms.PriorityNormal)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Handle(context.Background(), ev); err != nil {
				t.Errorf("handle: %v", err)
			}
		}()
	}
	wg.Wait()

	if s := mock.Stats(); s.Sends != 1 {
		t.Errorf("operator real sends = %d (dups %d), want exactly 1", s.Sends, s.Duplicates)
	}
	if e.status(ev.MessageID) != sms.StatusSent {
		t.Errorf("status = %q, want sent", e.status(ev.MessageID))
	}
	if e.balance() != startFunds-price {
		t.Errorf("balance = %d, want %d", e.balance(), startFunds-price)
	}
}

// A retry event that arrives after the message was already sent must not
// send again or refund.
func TestLateRetryAfterSentIsIgnored(t *testing.T) {
	e := newEnv(t)
	sender := &fakeSender{}
	p := e.processor(sender)
	ev := e.accepted(sms.PriorityNormal)
	expectOutcome(t, e.handle(p, ev), OutcomeSent)

	late := ev
	late.Attempt = 1
	late.LastError = "operator: temporary failure"
	expectOutcome(t, e.handle(p, late), OutcomeSkipped)

	if sender.Calls() != 1 || e.refunds() != 0 || e.status(ev.MessageID) != sms.StatusSent {
		t.Errorf("calls=%d refunds=%d status=%s; want 1, 0, sent", sender.Calls(), e.refunds(), e.status(ev.MessageID))
	}
}

func TestExpiredExpressIsRefundedAndNotSent(t *testing.T) {
	e := newEnv(t)
	e.accept = sms.NewService(e.pool, price, time.Millisecond, nil) // express expires after 1ms
	sender := &fakeSender{}
	p := e.processor(sender)
	ev := e.accepted(sms.PriorityExpress)
	time.Sleep(10 * time.Millisecond)

	expectOutcome(t, e.handle(p, ev), OutcomeExpired)
	if sender.Calls() != 0 {
		t.Errorf("operator calls = %d, want 0", sender.Calls())
	}
	if e.status(ev.MessageID) != sms.StatusFailed || e.balance() != startFunds || e.refunds() != 1 {
		t.Errorf("status=%s balance=%d refunds=%d; want failed, %d, 1", e.status(ev.MessageID), e.balance(), e.refunds(), startFunds)
	}
}

// Expired after an earlier Unknown outcome: it may have been delivered, so
// it ends as unknown and is NOT refunded.
func TestExpiredWhileUncertainIsUnknownNotRefunded(t *testing.T) {
	e := newEnv(t)
	e.accept = sms.NewService(e.pool, price, time.Millisecond, nil)
	p := e.processor(&fakeSender{})
	ev := e.accepted(sms.PriorityExpress)
	ev.Attempt, ev.Uncertain = 1, true
	time.Sleep(10 * time.Millisecond)

	expectOutcome(t, e.handle(p, ev), OutcomeUnknown)
	if e.status(ev.MessageID) != sms.StatusUnknown || e.refunds() != 0 {
		t.Errorf("status=%s refunds=%d; want unknown, 0", e.status(ev.MessageID), e.refunds())
	}
}

func TestPermanentFailureRefundsOnce(t *testing.T) {
	e := newEnv(t)
	p := e.processor(&fakeSender{script: []error{operator.ErrPermanent}})
	ev := e.accepted(sms.PriorityNormal)

	expectOutcome(t, e.handle(p, ev), OutcomeFailed)
	expectOutcome(t, e.handle(p, ev), OutcomeSkipped) // duplicate delivery

	if e.status(ev.MessageID) != sms.StatusFailed || e.refunds() != 1 || e.balance() != startFunds {
		t.Errorf("status=%s refunds=%d balance=%d; want failed, 1, %d", e.status(ev.MessageID), e.refunds(), e.balance(), startFunds)
	}
}

func TestTemporaryFailureRetriesThenDeadLetters(t *testing.T) {
	e := newEnv(t)
	p := e.processor(&fakeSender{script: []error{operator.ErrTemporary}})
	ev := e.accepted(sms.PriorityNormal)

	before := time.Now()
	expectOutcome(t, e.handle(p, ev), OutcomeRetry)
	if e.status(ev.MessageID) != sms.StatusRetrying {
		t.Fatalf("status = %q, want retrying", e.status(ev.MessageID))
	}
	retry := e.events(e.retry, 1)[0]
	if retry.MessageID != ev.MessageID || retry.Attempt != 1 || retry.Uncertain {
		t.Errorf("retry event = %+v, want same id, attempt 1, not uncertain", retry)
	}
	if retry.NextAttemptAt == nil || retry.NextAttemptAt.Sub(before) < 900*time.Millisecond || retry.NextAttemptAt.Sub(before) > 2*time.Second {
		t.Errorf("next_attempt_at = %v, want about 1s after %v", retry.NextAttemptAt, before)
	}

	// The last allowed attempt fails too: DLQ, failed, refunded once.
	last := retry
	last.Attempt = maxAttempts - 1
	last.NextAttemptAt = nil
	expectOutcome(t, e.handle(p, last), OutcomeDeadLetter)
	e.events(e.dlq, 1)
	if e.status(ev.MessageID) != sms.StatusFailed || e.refunds() != 1 || e.balance() != startFunds {
		t.Errorf("status=%s refunds=%d balance=%d; want failed, 1, %d", e.status(ev.MessageID), e.refunds(), e.balance(), startFunds)
	}
}

func TestUnknownNeverRefunds(t *testing.T) {
	e := newEnv(t)
	p := e.processor(&fakeSender{script: []error{operator.ErrUnknown}})
	ev := e.accepted(sms.PriorityNormal)

	expectOutcome(t, e.handle(p, ev), OutcomeRetry)
	retry := e.events(e.retry, 1)[0]
	if !retry.Uncertain {
		t.Fatal("retry event after Unknown must be marked uncertain")
	}

	last := retry
	last.Attempt = maxAttempts - 1
	last.NextAttemptAt = nil
	expectOutcome(t, e.handle(p, last), OutcomeUnknown)
	e.events(e.dlq, 1)
	if e.status(ev.MessageID) != sms.StatusUnknown || e.refunds() != 0 || e.balance() != startFunds-price {
		t.Errorf("status=%s refunds=%d balance=%d; want unknown, 0, %d", e.status(ev.MessageID), e.refunds(), e.balance(), startFunds-price)
	}
}

// A definitive answer after an Unknown one settles the question: the
// operator dedupes by ref, so a 4xx proves it was never delivered.
func TestUncertaintyIsClearedByDefinitiveAnswer(t *testing.T) {
	t.Run("unknown then permanent -> failed and refunded", func(t *testing.T) {
		e := newEnv(t)
		p := e.processor(&fakeSender{script: []error{operator.ErrUnknown, operator.ErrPermanent}})
		ev := e.accepted(sms.PriorityNormal)
		expectOutcome(t, e.handle(p, ev), OutcomeRetry)
		retry := e.events(e.retry, 1)[0]
		retry.NextAttemptAt = nil
		expectOutcome(t, e.handle(p, retry), OutcomeFailed)
		if e.refunds() != 1 || e.status(ev.MessageID) != sms.StatusFailed {
			t.Errorf("refunds=%d status=%s; want 1, failed", e.refunds(), e.status(ev.MessageID))
		}
	})
	t.Run("unknown then success -> sent", func(t *testing.T) {
		e := newEnv(t)
		p := e.processor(&fakeSender{script: []error{operator.ErrUnknown, nil}})
		ev := e.accepted(sms.PriorityNormal)
		expectOutcome(t, e.handle(p, ev), OutcomeRetry)
		retry := e.events(e.retry, 1)[0]
		retry.NextAttemptAt = nil
		expectOutcome(t, e.handle(p, retry), OutcomeSent)
		if e.refunds() != 0 || e.status(ev.MessageID) != sms.StatusSent {
			t.Errorf("refunds=%d status=%s; want 0, sent", e.refunds(), e.status(ev.MessageID))
		}
	})
}

// Breaker open: hold the message, don't count attempts, don't flood the
// retry topic; send once the breaker lets requests through again.
func TestBreakerOpenHoldsMessageWithoutRetryEvents(t *testing.T) {
	e := newEnv(t)
	sender := &fakeSender{script: []error{operator.ErrCircuitOpen, operator.ErrCircuitOpen, nil}}
	p := e.processor(sender)
	ev := e.accepted(sms.PriorityNormal)

	expectOutcome(t, e.handle(p, ev), OutcomeSent)
	e.events(e.retry, 0)
	if n := pgtest.QueryInt(t, e.pool, `SELECT attempts FROM messages WHERE id = $1`, ev.MessageID); n != 1 {
		t.Errorf("attempts = %d, want 1 (breaker waits are not attempts)", n)
	}
	if sender.Calls() != 3 {
		t.Errorf("sender calls = %d, want 3", sender.Calls())
	}
}

func TestRetryEventWaitsForNextAttemptAt(t *testing.T) {
	e := newEnv(t)
	p := e.processor(&fakeSender{})
	ev := e.accepted(sms.PriorityNormal)
	at := time.Now().Add(300 * time.Millisecond)
	ev.Attempt, ev.NextAttemptAt = 1, &at

	start := time.Now()
	expectOutcome(t, e.handle(p, ev), OutcomeSent)
	if waited := time.Since(start); waited < 290*time.Millisecond {
		t.Errorf("sent after %v, want to wait until next_attempt_at (~300ms)", waited)
	}
}

// End to end through Kafka: events on a topic are consumed, sent, and the
// offsets committed, so a new consumer in the same group gets nothing.
func TestConsumerProcessesAndCommits(t *testing.T) {
	e := newEnv(t)
	_, err := e.wallet.TopUp(context.Background(), customer, 100*price)
	pgtest.Must(t, err)
	topic := kafkatest.Topic(t, e.brokers, 3)
	sender := &fakeSender{}
	p := e.processor(sender)

	const n = 50
	producer := kafkatest.Client(t, e.brokers)
	for i := 0; i < n; i++ {
		ev := e.accepted(sms.PriorityNormal)
		value, _ := json.Marshal(ev)
		pgtest.Must(t, producer.ProduceSync(context.Background(),
			&kgo.Record{Topic: topic, Key: []byte(ev.MessageID.String()), Value: value}).FirstErr())
	}

	lane := Lane{Name: "test", Topic: topic, Group: "test-group-" + uuid.NewString(), Workers: 8, BatchSize: 16}
	client := kafkatest.Client(t, e.brokers, ConsumerOptions(lane)...)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { NewConsumer(client, lane, p, quietLogger()).Run(ctx); close(done) }()

	deadline := time.Now().Add(30 * time.Second)
	for pgtest.QueryInt(t, e.pool, `SELECT COUNT(*) FROM messages WHERE status = 'sent'`) < n {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for all messages to be sent")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond) // let the last batch commit
	cancel()
	<-done
	client.Close()

	if sender.Calls() != n {
		t.Errorf("operator calls = %d, want %d", sender.Calls(), n)
	}

	// Same group again: committed offsets mean nothing is left to read.
	again := kafkatest.Client(t, e.brokers, ConsumerOptions(lane)...)
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pollCancel()
	if recs := again.PollFetches(pollCtx).Records(); len(recs) != 0 {
		t.Errorf("new consumer in the same group got %d records, want 0 (offsets not committed)", len(recs))
	}
}
