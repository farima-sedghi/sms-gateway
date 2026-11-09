package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

// HeaderOutboxID is the Kafka header carrying the outbox row id, for tracing
// a record back to its row.
const HeaderOutboxID = "outbox-id"

// batchTimeout bounds one claim -> produce -> mark-published cycle. It is
// longer than the producer's RecordDeliveryTimeout (see ProducerOptions), so
// a Kafka outage surfaces as a produce error, not as this timeout.
const batchTimeout = 15 * time.Second

// Lane is one independent publishing loop for one topic. Each lane has its
// own goroutine, its own transaction per batch and its own Kafka client, so a
// backlog on one lane never delays another (see docs/adr/0004).
type Lane struct {
	Topic        string
	BatchSize    int
	IdleInterval time.Duration // sleep after a partial or empty batch
	Producer     *kgo.Client
}

// Config holds the relay's housekeeping settings. A zero interval disables
// that job (handy in tests).
type Config struct {
	Retention       time.Duration // delete published rows older than this
	CleanupInterval time.Duration
	LagInterval     time.Duration // how often to log each lane's lag
}

// Relay publishes outbox rows to Kafka.
type Relay struct {
	pool      *pgxpool.Pool
	log       *slog.Logger
	cfg       Config
	lanes     []Lane
	published atomic.Int64 // rows this relay published (tests, future metrics)
}

// NewRelay returns a relay for the given lanes.
func NewRelay(pool *pgxpool.Pool, log *slog.Logger, cfg Config, lanes ...Lane) *Relay {
	return &Relay{pool: pool, log: log, cfg: cfg, lanes: lanes}
}

// Published returns how many rows this relay instance has published.
func (r *Relay) Published() int64 { return r.published.Load() }

// Run starts every lane, the cleanup job and the lag reporter, and blocks
// until ctx is cancelled and all of them have stopped.
func (r *Relay) Run(ctx context.Context) {
	var wg sync.WaitGroup
	start := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
		}()
	}

	for _, lane := range r.lanes {
		start(func(ctx context.Context) { r.runLane(ctx, lane) })
	}
	if r.cfg.CleanupInterval > 0 {
		start(r.runCleanup)
	}
	if r.cfg.LagInterval > 0 {
		start(r.runLagReporter)
	}
	wg.Wait()
}

// runLane loops: publish a batch, then decide how long to wait.
func (r *Relay) runLane(ctx context.Context, lane Lane) {
	log := r.log.With("lane", lane.Topic)
	log.Info("outbox lane started", "batch_size", lane.BatchSize, "idle_interval", lane.IdleInterval)
	defer log.Info("outbox lane stopped")

	failures := 0
	for ctx.Err() == nil {
		// The batch runs on a context that ignores shutdown, so a SIGTERM
		// mid-produce lets the batch finish and commit instead of rolling
		// back records Kafka already has (which would publish them twice).
		// ctx is only checked between batches.
		batchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), batchTimeout)
		start := time.Now()
		n, err := r.publishBatch(batchCtx, lane)
		cancel()

		var wait time.Duration
		switch {
		case err != nil:
			failures++
			wait = backoff(failures, rand.Float64())
			log.Warn("outbox batch failed", "err", err, "attempt", failures, "retry_in", wait)
		default:
			failures = 0
			if n > 0 {
				r.published.Add(int64(n))
				log.Info("outbox batch published", "count", n,
					"duration_ms", float64(time.Since(start).Microseconds())/1000)
			}
			// Full batch: there is probably more, loop at once.
			// Partial or empty: the lane is drained, wait a little.
			if n < lane.BatchSize {
				wait = lane.IdleInterval
			}
		}
		sleep(ctx, wait)
	}
}

type outboxRow struct {
	id      int64
	key     string
	payload []byte
}

// publishBatch is one claim -> produce -> mark cycle in ONE transaction.
//
// FOR UPDATE SKIP LOCKED makes concurrent relays (other lanes, other
// replicas) skip rows that are already claimed instead of waiting for them,
// so two relays never publish the same row in the same pass.
//
// If producing fails, the transaction rolls back and the rows stay
// unpublished. Records that did reach Kafka before the failure are published
// again next time: delivery is at-least-once, and consumers deduplicate.
func (r *Relay) publishBatch(ctx context.Context, lane Lane) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	// Rollback after Commit is a no-op, so this is safe on every path.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	rows, err := claimBatch(ctx, tx, lane.Topic, lane.BatchSize)
	if err != nil || len(rows) == 0 {
		return 0, err
	}

	records := make([]*kgo.Record, len(rows))
	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.id
		records[i] = &kgo.Record{
			Topic: lane.Topic,
			Key:   []byte(row.key), // message_id: spreads load over partitions (decision 7)
			Value: row.payload,
			Headers: []kgo.RecordHeader{
				{Key: HeaderOutboxID, Value: []byte(strconv.FormatInt(row.id, 10))},
			},
		}
	}

	// ProduceSync waits until Kafka acknowledged every record (acks=all).
	if err := lane.Producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce %d records: %w", len(records), err)
	}
	if err := markPublished(ctx, tx, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(rows), nil
}

func claimBatch(ctx context.Context, tx pgx.Tx, topic string, limit int) ([]outboxRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, key, payload FROM outbox
		WHERE published_at IS NULL AND topic = $1
		ORDER BY id
		LIMIT $2
		FOR UPDATE SKIP LOCKED`,
		topic, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claim batch: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outboxRow, error) {
		var o outboxRow
		err := row.Scan(&o.id, &o.key, &o.payload)
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("claim batch: %w", err)
	}
	return out, nil
}

func markPublished(ctx context.Context, tx pgx.Tx, ids []int64) error {
	tag, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	if int(tag.RowsAffected()) != len(ids) {
		// Cannot happen while we hold the row locks; fail loudly if it does.
		return fmt.Errorf("mark published: updated %d rows, want %d", tag.RowsAffected(), len(ids))
	}
	return nil
}

// cleanupChunk is how many rows one DELETE removes. Small chunks keep each
// statement's locks and WAL burst short on a hot table.
const cleanupChunk = 5000

// runCleanup periodically deletes rows published more than Retention ago.
// Safe to run on every replica: concurrent DELETEs just find less to do.
func (r *Relay) runCleanup(ctx context.Context) {
	t := time.NewTicker(r.cfg.CleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := r.Cleanup(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				r.log.Warn("outbox cleanup failed", "err", err, "deleted", n)
			} else if n > 0 {
				r.log.Info("outbox cleanup", "deleted", n)
			}
		}
	}
}

// Cleanup deletes published rows older than Retention, in chunks, and
// returns how many rows it deleted.
//
// ORDER BY id walks the primary key from the oldest rows, which are almost
// always the published ones, so no index on published_at is needed.
func (r *Relay) Cleanup(ctx context.Context) (int64, error) {
	var total int64
	for {
		tag, err := r.pool.Exec(ctx, `
			DELETE FROM outbox WHERE id IN (
				SELECT id FROM outbox
				WHERE published_at < now() - make_interval(secs => $1)
				ORDER BY id
				LIMIT $2)`,
			r.cfg.Retention.Seconds(), cleanupChunk,
		)
		if err != nil {
			return total, fmt.Errorf("delete published rows: %w", err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < cleanupChunk {
			return total, nil
		}
	}
}

// runLagReporter logs, per lane, the age of the oldest unpublished row.
// This is the number to alert on: it grows when the relay falls behind or
// Kafka is unavailable.
func (r *Relay) runLagReporter(ctx context.Context) {
	t := time.NewTicker(r.cfg.LagInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, lane := range r.lanes {
				lag, err := r.Lag(ctx, lane.Topic)
				if err != nil {
					if ctx.Err() == nil {
						r.log.Warn("outbox lag query failed", "lane", lane.Topic, "err", err)
					}
					continue
				}
				r.log.Info("outbox lag", "lane", lane.Topic, "lag_ms", lag.Milliseconds())
			}
		}
	}
}

// Lag returns the age of the oldest unpublished row for topic (0 if none).
// Served by the (topic, id) partial index: it reads one index entry.
func (r *Relay) Lag(ctx context.Context, topic string) (time.Duration, error) {
	var seconds float64
	err := r.pool.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM now() - created_at)::float8 FROM outbox
		WHERE published_at IS NULL AND topic = $1
		ORDER BY id LIMIT 1`, topic,
	).Scan(&seconds)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("outbox lag: %w", err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

// backoff returns the wait before retry number attempt (1, 2, 3, ...):
// exponential from 100ms, capped at 5s, with "equal jitter" (between half and
// all of the step) so replicas that failed together don't retry in lockstep.
// jitter must be in [0, 1). Pure function, so it is easy to test.
func backoff(attempt int, jitter float64) time.Duration {
	const (
		base    = 100 * time.Millisecond
		maxWait = 5 * time.Second
	)
	d := maxWait
	if attempt < 7 { // 100ms << 6 = 6.4s, already past the cap
		d = min(base<<(attempt-1), maxWait)
	}
	half := d / 2
	return half + time.Duration(jitter*float64(half))
}

// sleep waits for d or until ctx is cancelled, whichever comes first.
func sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
