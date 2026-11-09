package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/farima-sedghi/sms-gateway/internal/operator"
	"github.com/farima-sedghi/sms-gateway/internal/platform/backoff"
	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

// Sender hands one SMS to the operator. Defined here, where it is consumed:
// *operator.Client satisfies it, and tests use a scripted fake.
// Errors must wrap operator.ErrPermanent, ErrTemporary or ErrUnknown
// (anything else is treated as Temporary).
type Sender interface {
	Send(ctx context.Context, ref, to, text string) (operatorMsgID string, err error)
}

// Outcome is what happened to one message in one Handle call.
type Outcome string

const (
	OutcomeSent       Outcome = "sent"        // operator accepted it
	OutcomeSkipped    Outcome = "skipped"     // already terminal (duplicate/late delivery)
	OutcomeFailed     Outcome = "failed"      // permanent error: failed + refunded
	OutcomeExpired    Outcome = "expired"     // express past expires_at: failed + refunded, not sent
	OutcomeRetry      Outcome = "retry"       // temporary/unknown: queued on the retry topic
	OutcomeDeadLetter Outcome = "dead_letter" // retries exhausted: DLQ + failed + refunded
	OutcomeUnknown    Outcome = "unknown"     // gave up while uncertain: DLQ/expired + unknown, NOT refunded
)

// Result describes one Handle call.
type Result struct {
	Outcome Outcome
	Attempt int           // attempts that failed before this one
	Latency time.Duration // created_at -> handed_at, for OutcomeSent
	Err     string        // last operator error, if any
}

// Config tunes the processor.
type Config struct {
	RetryTopic     string
	DLQTopic       string
	MaxAttempts    int           // total send attempts before giving up
	RetryBaseDelay time.Duration // first retry delay; doubles per attempt
	RetryMaxDelay  time.Duration
	BreakerWait    time.Duration // how long to hold a message while the breaker is open
	DBTimeout      time.Duration // bound for one status update
}

// Processor runs the per-message dispatch flow (decision 19).
type Processor struct {
	life     *sms.Lifecycle
	sender   Sender
	producer *kgo.Client // for sms.retry and sms.dlq (acks=all, idempotent)
	cfg      Config
	log      *slog.Logger
}

// NewProcessor returns a processor.
func NewProcessor(life *sms.Lifecycle, sender Sender, producer *kgo.Client, cfg Config, log *slog.Logger) *Processor {
	return &Processor{life: life, sender: sender, producer: producer, cfg: cfg, log: log}
}

// Handle dispatches one message event:
//
//  1. retry events wait until next_attempt_at
//  2. guarded transition to sending (0 rows = already terminal -> skip)
//  3. express past expires_at -> do not send (failed + refund, or unknown if uncertain)
//  4. send and record the outcome
//
// The returned error is only for infrastructure problems that outlived ctx
// (shutdown while Postgres/Kafka was failing); the caller must then not
// commit the record. Business outcomes are in Result.
//
// ctx is the shutdown signal. It interrupts waits (retry time, open
// breaker, infra retries) but never a single in-flight DB statement or
// operator call: those run to completion so we don't abandon a send halfway.
func (p *Processor) Handle(ctx context.Context, ev sms.Event) (Result, error) {
	res := Result{Attempt: ev.Attempt}

	// 1. Retry events carry the earliest time they may be sent.
	if ev.NextAttemptAt != nil {
		if err := sleep(ctx, time.Until(*ev.NextAttemptAt)); err != nil {
			return res, err
		}
	}

	// 2. queued | retrying | sending -> sending. Re-entry from sending lets a
	// redelivered record finish the job; the operator dedupes by ref.
	var changed bool
	err := p.retryInfra(ctx, "mark sending", func(c context.Context) (err error) {
		changed, err = p.life.MarkSending(c, ev.MessageID, ev.CreatedAt)
		return err
	})
	if err != nil {
		return res, err
	}
	if !changed {
		res.Outcome = OutcomeSkipped
		return res, nil
	}

	work := context.WithoutCancel(ctx) // in-flight sends are never cut by shutdown
	for {
		// 3. Express deadline, checked before every send (also after
		// waiting for an open breaker).
		if ev.Expired(time.Now()) {
			return p.giveUp(ctx, ev, res, "express message expired before it could be sent", OutcomeExpired)
		}

		// 4. Send.
		opID, sendErr := p.sender.Send(work, ev.MessageID.String(), ev.Recipient, ev.Body)
		switch {
		case sendErr == nil:
			return p.markSent(ctx, ev, res, opID)

		case errors.Is(sendErr, operator.ErrCircuitOpen):
			// Nothing was sent and the operator is known to be down: hold
			// this message (no attempt counted, nothing on sms.retry). The
			// whole batch waits, so the consumer stops fetching: back-pressure.
			p.log.Debug("operator breaker open, waiting", "message_id", ev.MessageID)
			if err := sleep(ctx, p.cfg.BreakerWait); err != nil {
				return res, err
			}
			continue

		case errors.Is(sendErr, operator.ErrPermanent):
			// Definitely not delivered (and if an earlier attempt had been,
			// the operator would have answered with its id, not a 4xx).
			res.Err = sendErr.Error()
			ev.Uncertain = false
			return p.failAndRefund(ctx, ev, res, sendErr.Error(), OutcomeFailed)

		case errors.Is(sendErr, operator.ErrUnknown):
			ev.Uncertain = true // may have been delivered: never refund from here on
			res.Err = sendErr.Error()
			return p.retryOrGiveUp(ctx, ev, res, sendErr)

		default: // ErrTemporary (or unclassified, treated the same)
			res.Err = sendErr.Error()
			return p.retryOrGiveUp(ctx, ev, res, sendErr)
		}
	}
}

func (p *Processor) markSent(ctx context.Context, ev sms.Event, res Result, opID string) (Result, error) {
	var (
		changed  bool
		handedAt time.Time
	)
	err := p.retryInfra(ctx, "mark sent", func(c context.Context) (err error) {
		changed, handedAt, err = p.life.MarkSent(c, ev.MessageID, ev.CreatedAt, opID)
		return err
	})
	if err != nil {
		return res, err
	}
	res.Outcome = OutcomeSent
	if changed { // a concurrent duplicate may have recorded it first
		res.Latency = handedAt.Sub(ev.CreatedAt)
	}
	return res, nil
}

// retryOrGiveUp schedules another attempt on the retry topic, or, after
// MaxAttempts, moves the message to the DLQ and ends it.
//
// The Kafka event is produced BEFORE the status update: if the update then
// fails, the retry event still finds the message in `sending` (allowed).
// The other order could leave a message in `retrying` with no event to
// ever pick it up again.
func (p *Processor) retryOrGiveUp(ctx context.Context, ev sms.Event, res Result, sendErr error) (Result, error) {
	ev.Attempt++
	ev.LastError = sendErr.Error()

	if ev.Attempt >= p.cfg.MaxAttempts {
		ev.NextAttemptAt = nil
		if err := p.produce(ctx, p.cfg.DLQTopic, ev); err != nil {
			return res, err
		}
		return p.giveUp(ctx, ev, res, "retries exhausted: "+sendErr.Error(), OutcomeDeadLetter)
	}

	next := time.Now().Add(backoff.Exponential(ev.Attempt, p.cfg.RetryBaseDelay, p.cfg.RetryMaxDelay))
	ev.NextAttemptAt = &next
	if err := p.produce(ctx, p.cfg.RetryTopic, ev); err != nil {
		return res, err
	}
	err := p.retryInfra(ctx, "mark retrying", func(c context.Context) error {
		_, err := p.life.MarkRetrying(c, ev.MessageID, ev.CreatedAt, sendErr.Error())
		return err
	})
	if err != nil {
		return res, err
	}
	res.Outcome = OutcomeRetry
	return res, nil
}

// giveUp ends a message that will not be sent (any more): failed + refund if
// non-delivery is certain, unknown (no refund) if any attempt was Unknown.
func (p *Processor) giveUp(ctx context.Context, ev sms.Event, res Result, reason string, certain Outcome) (Result, error) {
	if !ev.Uncertain {
		return p.failAndRefund(ctx, ev, res, reason, certain)
	}
	err := p.retryInfra(ctx, "mark unknown", func(c context.Context) error {
		_, err := p.life.MarkUnknown(c, ev.MessageID, ev.CreatedAt, reason+" (an earlier attempt had an unknown outcome)")
		return err
	})
	if err != nil {
		return res, err
	}
	res.Outcome = OutcomeUnknown
	return res, nil
}

func (p *Processor) failAndRefund(ctx context.Context, ev sms.Event, res Result, reason string, outcome Outcome) (Result, error) {
	err := p.retryInfra(ctx, "fail and refund", func(c context.Context) error {
		_, err := p.life.FailAndRefund(c, ev.MessageID, ev.CreatedAt, reason)
		return err
	})
	if err != nil {
		return res, err
	}
	res.Outcome = outcome
	return res, nil
}

// produce writes ev to topic (key = message_id), retrying until it succeeds
// or ctx ends.
func (p *Processor) produce(ctx context.Context, topic string, ev sms.Event) error {
	value, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err) // cannot happen for this struct
	}
	rec := &kgo.Record{Topic: topic, Key: []byte(ev.MessageID.String()), Value: value}
	return p.retryInfra(ctx, "produce to "+topic, func(c context.Context) error {
		return p.producer.ProduceSync(c, rec).FirstErr()
	})
}

// DeadLetterRaw sends a record that could not even be decoded to the DLQ,
// so a poison message never blocks its partition.
func (p *Processor) DeadLetterRaw(ctx context.Context, rec *kgo.Record, decodeErr error) error {
	dlq := &kgo.Record{
		Topic: p.cfg.DLQTopic,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: append(rec.Headers,
			kgo.RecordHeader{Key: "dlq-reason", Value: []byte("decode: " + decodeErr.Error())},
			kgo.RecordHeader{Key: "dlq-source", Value: []byte(fmt.Sprintf("%s/%d@%d", rec.Topic, rec.Partition, rec.Offset))},
		),
	}
	return p.retryInfra(ctx, "dead-letter raw record", func(c context.Context) error {
		return p.producer.ProduceSync(c, dlq).FirstErr()
	})
}

// retryInfra runs fn until it succeeds, backing off between failures
// (100ms .. 5s). Postgres or Kafka being briefly unavailable must not turn
// into a lost or wrongly-failed message. Each call gets a context that
// ignores shutdown but has DBTimeout, so a single statement is never cut
// halfway; shutdown (ctx) only stops the waiting between attempts.
func (p *Processor) retryInfra(ctx context.Context, what string, fn func(context.Context) error) error {
	for attempt := 1; ; attempt++ {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.DBTimeout)
		err := fn(c)
		cancel()
		if err == nil {
			return nil
		}
		wait := backoff.EqualJitter(backoff.Exponential(attempt, 100*time.Millisecond, 5*time.Second), rand.Float64())
		p.log.Warn("infrastructure error, retrying", "op", what, "attempt", attempt, "retry_in", wait, "err", err)
		if err := sleep(ctx, wait); err != nil {
			return fmt.Errorf("%s: gave up on shutdown: %w", what, err)
		}
	}
}

// sleep waits for d, or returns ctx.Err() if ctx ends first.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
