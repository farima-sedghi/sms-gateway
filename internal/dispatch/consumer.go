package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/farima-sedghi/sms-gateway/internal/sms"
)

// Lane is one consumer group reading one topic with its own worker pool.
// Express, normal and retry each get a lane, so express never shares
// workers (or a fetch loop) with normal traffic.
type Lane struct {
	Name      string // for logs: express | normal | retry
	Topic     string
	Group     string
	Workers   int // max messages processed at the same time
	BatchSize int // max records fetched per poll
}

// ConsumerOptions returns the franz-go options for a lane's client.
// Auto-commit is disabled: offsets are committed only after the whole batch
// is processed (decision 12), so a crash never skips a message.
// New groups start at the earliest offset, so nothing produced before the
// first deploy is skipped.
func ConsumerOptions(lane Lane) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumerGroup(lane.Group),
		kgo.ConsumeTopics(lane.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
}

// Consumer runs one lane: fetch a batch, process it with a bounded pool,
// wait for all of it, commit, repeat.
type Consumer struct {
	client *kgo.Client
	lane   Lane
	proc   *Processor
	log    *slog.Logger
}

// NewConsumer returns a consumer. client must be built with ConsumerOptions(lane).
func NewConsumer(client *kgo.Client, lane Lane, proc *Processor, log *slog.Logger) *Consumer {
	return &Consumer{client: client, lane: lane, proc: proc, log: log.With("lane", lane.Name)}
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("dispatch lane started", "topic", c.lane.Topic, "group", c.lane.Group,
		"workers", c.lane.Workers, "batch_size", c.lane.BatchSize)
	defer c.log.Info("dispatch lane stopped")

	for {
		fetches := c.client.PollRecords(ctx, c.lane.BatchSize)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.Warn("fetch error", "topic", topic, "partition", partition, "err", err)
		})
		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		if !c.processBatch(ctx, records) {
			// Shutdown interrupted the batch: commit nothing. The records are
			// redelivered after restart; idempotent handling absorbs it.
			c.log.Info("batch interrupted by shutdown, not committing", "records", len(records))
			return
		}

		// The batch is done even if shutdown started meanwhile: commit it.
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err := c.client.CommitRecords(commitCtx, records...)
		cancel()
		if err != nil {
			// E.g. a rebalance moved the partition. The new owner gets the
			// records again; the status guards make that harmless.
			c.log.Warn("commit failed; records will be redelivered", "records", len(records), "err", err)
		}
	}
}

// processBatch handles every record with at most Workers in flight and
// returns true only if all of them finished.
func (c *Consumer) processBatch(ctx context.Context, records []*kgo.Record) bool {
	var (
		wg         sync.WaitGroup
		incomplete atomic.Bool
		slots      = make(chan struct{}, c.lane.Workers) // semaphore: like SemaphoreSlim(Workers)
	)
	for _, rec := range records {
		select {
		case slots <- struct{}{}: // take a slot (blocks while all workers are busy)
		case <-ctx.Done():
			incomplete.Store(true) // don't start new work on shutdown
		}
		if incomplete.Load() {
			break
		}
		wg.Add(1)
		go func(rec *kgo.Record) {
			defer wg.Done()
			defer func() { <-slots }() // give the slot back
			if !c.handle(ctx, rec) {
				incomplete.Store(true)
			}
		}(rec)
	}
	wg.Wait()
	return !incomplete.Load()
}

// handle processes one record and logs its outcome. It returns false if the
// record was not fully handled (shutdown during an infrastructure problem).
func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) bool {
	var ev sms.Event
	if err := json.Unmarshal(rec.Value, &ev); err != nil || ev.MessageID == uuid.Nil {
		if err == nil {
			err = errMissingMessageID
		}
		c.log.Error("undecodable record, sending to DLQ", "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return c.proc.DeadLetterRaw(ctx, rec, err) == nil
	}

	res, err := c.proc.Handle(ctx, ev)
	if err != nil {
		c.log.Warn("message not finished", "message_id", ev.MessageID, "err", err)
		return false
	}

	attrs := []any{"message_id", ev.MessageID, "outcome", res.Outcome, "attempt", res.Attempt}
	if res.Outcome == OutcomeSent && res.Latency > 0 {
		attrs = append(attrs, "latency_ms", res.Latency.Milliseconds(), "priority", ev.Priority)
	}
	if res.Err != "" {
		attrs = append(attrs, "operator_error", res.Err)
	}
	c.log.Info("message dispatched", attrs...)
	return true
}

// errMissingMessageID: the record is JSON but not a message event.
var errMissingMessageID = errors.New("event has no message_id")
