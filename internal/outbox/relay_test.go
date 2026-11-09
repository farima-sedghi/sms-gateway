package outbox

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/farima-sedghi/sms-gateway/internal/platform/kafka/kafkatest"
	"github.com/farima-sedghi/sms-gateway/internal/platform/postgres/pgtest"
)

// --- helpers ---------------------------------------------------------------

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// insertRows adds n unpublished outbox rows for topic in one statement.
func insertRows(t *testing.T, pool *pgxpool.Pool, topic string, n int) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO outbox (topic, key, payload)
		SELECT $1, 'key-' || i, jsonb_build_object('i', i)
		FROM generate_series(1, $2) AS i`, topic, n)
	pgtest.Must(t, err)
}

func unpublished(t *testing.T, pool *pgxpool.Pool, topic string) int64 {
	t.Helper()
	return pgtest.QueryInt(t, pool,
		`SELECT COUNT(*) FROM outbox WHERE published_at IS NULL AND topic = $1`, topic)
}

// waitUntil polls cond every 20ms until it is true or timeout passes.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// runRelays starts the relays and returns a stop function that cancels them
// and waits for them to exit.
func runRelays(relays ...*Relay) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, r := range relays {
		wg.Add(1)
		go func(r *Relay) {
			defer wg.Done()
			r.Run(ctx)
		}(r)
	}
	return func() { cancel(); wg.Wait() }
}

func newLane(t *testing.T, brokers []string, topic string, batch int, lowLatency bool) Lane {
	return Lane{
		Topic:        topic,
		BatchSize:    batch,
		IdleInterval: 10 * time.Millisecond,
		Producer:     kafkatest.Client(t, brokers, ProducerOptions(lowLatency)...),
	}
}

// outboxIDs returns how many times each outbox-id header appears.
func outboxIDs(t *testing.T, records []*kgo.Record) map[string]int {
	t.Helper()
	seen := make(map[string]int, len(records))
	for _, r := range records {
		var id string
		for _, h := range r.Headers {
			if h.Key == HeaderOutboxID {
				id = string(h.Value)
			}
		}
		if id == "" {
			t.Fatalf("record without %s header", HeaderOutboxID)
		}
		seen[id]++
	}
	return seen
}

// --- tests -----------------------------------------------------------------

// Normal case: every row ends up in Kafka exactly once and is marked published.
func TestRelayPublishesEachRowExactlyOnce(t *testing.T) {
	pool := pgtest.New(t)
	brokers := kafkatest.Brokers(t)
	topic := kafkatest.Topic(t, brokers, 3)

	const n = 1000
	insertRows(t, pool, topic, n)

	relay := NewRelay(pool, quietLogger(), Config{}, newLane(t, brokers, topic, 500, false))
	stop := runRelays(relay)
	waitUntil(t, 30*time.Second, "all rows published", func() bool { return unpublished(t, pool, topic) == 0 })
	stop()

	if got := kafkatest.RecordCount(t, brokers, topic); got != n {
		t.Fatalf("records in Kafka = %d, want exactly %d", got, n)
	}
	ids := outboxIDs(t, kafkatest.ConsumeAll(t, brokers, topic, n, 30*time.Second))
	if len(ids) != n {
		t.Errorf("distinct outbox ids in Kafka = %d, want %d", len(ids), n)
	}
	if relay.Published() != n {
		t.Errorf("relay reports %d published, want %d", relay.Published(), n)
	}
}

// Two relays (e.g. two replicas) on the same lane at the same time: SKIP
// LOCKED gives each row to exactly one of them.
func TestTwoRelaysNeverPublishTheSameRow(t *testing.T) {
	pool := pgtest.New(t)
	brokers := kafkatest.Brokers(t)
	topic := kafkatest.Topic(t, brokers, 3)

	const n = 5000
	insertRows(t, pool, topic, n)

	a := NewRelay(pool, quietLogger(), Config{}, newLane(t, brokers, topic, 100, false))
	b := NewRelay(pool, quietLogger(), Config{}, newLane(t, brokers, topic, 100, false))
	stop := runRelays(a, b)
	waitUntil(t, 60*time.Second, "all rows published", func() bool { return unpublished(t, pool, topic) == 0 })
	stop()

	t.Logf("relay A published %d, relay B published %d", a.Published(), b.Published())
	if a.Published()+b.Published() != n {
		t.Errorf("A+B published = %d, want %d", a.Published()+b.Published(), n)
	}
	if got := kafkatest.RecordCount(t, brokers, topic); got != n {
		t.Fatalf("records in Kafka = %d, want exactly %d (no duplicates)", got, n)
	}
	for id, count := range outboxIDs(t, kafkatest.ConsumeAll(t, brokers, topic, n, 30*time.Second)) {
		if count != 1 {
			t.Errorf("outbox id %s published %d times", id, count)
		}
	}
}

// A large normal backlog must not delay the express lane.
func TestExpressLaneNotBlockedByNormalBacklog(t *testing.T) {
	pool := pgtest.New(t)
	brokers := kafkatest.Brokers(t)
	normal := kafkatest.Topic(t, brokers, 3)
	express := kafkatest.Topic(t, brokers, 3)

	insertRows(t, pool, normal, 20000)
	insertRows(t, pool, express, 10) // inserted after: higher ids than the whole backlog

	relay := NewRelay(pool, quietLogger(), Config{},
		newLane(t, brokers, express, 500, true),
		newLane(t, brokers, normal, 500, false),
	)
	start := time.Now()
	stop := runRelays(relay)
	defer stop()

	waitUntil(t, time.Second, "express rows published", func() bool { return unpublished(t, pool, express) == 0 })
	t.Logf("express drained in %s; normal rows still unpublished at that moment: %d",
		time.Since(start).Round(time.Millisecond), unpublished(t, pool, normal))
}

// If Kafka rejects the batch, the transaction rolls back: no row is marked
// published, so the batch is retried later.
func TestProduceErrorLeavesRowsUnpublished(t *testing.T) {
	pool := pgtest.New(t)
	brokers := kafkatest.Brokers(t)
	missing := "test.outbox.does-not-exist"

	insertRows(t, pool, missing, 5)
	lane := Lane{
		Topic:     missing,
		BatchSize: 10,
		Producer: kafkatest.Client(t, brokers,
			kgo.RecordDeliveryTimeout(2*time.Second), // fail fast in the test
			kgo.UnknownTopicRetries(1),
		),
	}
	relay := NewRelay(pool, quietLogger(), Config{}, lane)

	n, err := relay.publishBatch(context.Background(), lane)
	if err == nil {
		t.Fatalf("publishBatch to a missing topic succeeded (%d rows)", n)
	}
	if got := unpublished(t, pool, missing); got != 5 {
		t.Errorf("unpublished rows = %d, want 5 (rolled back)", got)
	}
}

func TestCleanupDeletesOnlyOldPublishedRows(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		INSERT INTO outbox (topic, key, payload, published_at) VALUES
		  ('t', 'old-published',    '{}', now() - interval '2 hours'),
		  ('t', 'recent-published', '{}', now() - interval '10 minutes'),
		  ('t', 'old-unpublished',  '{}', NULL)`)
	pgtest.Must(t, err)

	relay := NewRelay(pool, quietLogger(), Config{Retention: time.Hour})
	deleted, err := relay.Cleanup(ctx)
	pgtest.Must(t, err)
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}
	rows, err := pool.Query(ctx, `SELECT key FROM outbox ORDER BY key`)
	pgtest.Must(t, err)
	var keys []string
	for rows.Next() {
		var k string
		pgtest.Must(t, rows.Scan(&k))
		keys = append(keys, k)
	}
	if len(keys) != 2 || keys[0] != "old-unpublished" || keys[1] != "recent-published" {
		t.Errorf("remaining rows = %v, want [old-unpublished recent-published]", keys)
	}
}

func TestLagIsAgeOfOldestUnpublishedRow(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	relay := NewRelay(pool, quietLogger(), Config{})

	if lag, err := relay.Lag(ctx, "t"); err != nil || lag != 0 {
		t.Fatalf("empty lane: lag = %v, err = %v; want 0, nil", lag, err)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO outbox (topic, key, payload, created_at)
		VALUES ('t', 'a', '{}', now() - interval '30 seconds')`)
	pgtest.Must(t, err)

	lag, err := relay.Lag(ctx, "t")
	pgtest.Must(t, err)
	if lag < 29*time.Second || lag > 40*time.Second {
		t.Errorf("lag = %v, want about 30s", lag)
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	// jitter 0 -> lower bound (half the step), jitter ~1 -> upper bound.
	cases := []struct {
		attempt  int
		min, max time.Duration
	}{
		{1, 50 * time.Millisecond, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 200 * time.Millisecond},
		{4, 400 * time.Millisecond, 800 * time.Millisecond},
		{7, 2500 * time.Millisecond, 5 * time.Second},
		{100, 2500 * time.Millisecond, 5 * time.Second}, // no overflow on large attempts
	}
	for _, c := range cases {
		lo, hi := backoff(c.attempt, 0), backoff(c.attempt, 0.999999)
		if lo != c.min || hi > c.max || hi < lo {
			t.Errorf("backoff(%d): [%v, %v], want [%v, %v]", c.attempt, lo, hi, c.min, c.max)
		}
	}
}
