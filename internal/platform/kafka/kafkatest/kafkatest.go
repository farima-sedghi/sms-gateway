// Package kafkatest gives tests a real Kafka with throw-away topics.
//
// Set TEST_KAFKA_BROKERS (e.g. localhost:29092, the compose host listener).
// Without it, Kafka tests are skipped (`make test` sets it). Every test gets
// its own uniquely named topic, deleted afterwards, so tests never touch the
// application's sms.* topics or each other's data.
package kafkatest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Brokers returns the test brokers, or skips the test if none are configured.
func Brokers(t testing.TB) []string {
	t.Helper()
	v := os.Getenv("TEST_KAFKA_BROKERS")
	if v == "" {
		t.Skip("TEST_KAFKA_BROKERS not set; skipping Kafka test (run `make test` or start compose)")
	}
	return strings.Split(v, ",")
}

// Client returns a client for brokers with extra opts, closed when the test ends.
func Client(t testing.TB, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// Topic creates a unique topic with the given partition count, waits until
// it is visible in metadata, and deletes it when the test ends.
func Topic(t testing.TB, brokers []string, partitions int32) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adm := kadm.NewClient(Client(t, brokers))
	name := "test.outbox." + randomSuffix(t)

	if _, err := adm.CreateTopic(ctx, partitions, 1, nil, name); err != nil {
		t.Fatalf("create topic %s: %v", name, err)
	}
	t.Cleanup(func() {
		_, _ = adm.DeleteTopics(context.Background(), name)
	})

	for ctx.Err() == nil {
		details, err := adm.ListTopics(ctx, name)
		if err == nil && details.Has(name) && details[name].Err == nil {
			return name
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s did not appear in metadata", name)
	return ""
}

// RecordCount returns how many records were ever written to topic: the sum of
// the end offsets of its partitions. It counts duplicates too, which makes it
// a precise "published exactly N" check.
func RecordCount(t testing.TB, brokers []string, topic string) int64 {
	t.Helper()
	adm := kadm.NewClient(Client(t, brokers))
	offsets, err := adm.ListEndOffsets(context.Background(), topic)
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}
	var total int64
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			t.Fatalf("end offset %s/%d: %v", o.Topic, o.Partition, o.Err)
		}
		total += o.Offset
	})
	return total
}

// ConsumeAll reads want records from the start of topic, or fails after timeout.
func ConsumeAll(t testing.TB, brokers []string, topic string, want int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	cl := Client(t, brokers,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var out []*kgo.Record
	for len(out) < want {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consumed %d of %d records before timeout", len(out), want)
		}
		fetches.EachError(func(topic string, p int32, err error) {
			t.Fatalf("fetch %s/%d: %v", topic, p, err)
		})
		out = append(out, fetches.Records()...)
	}
	return out
}

func randomSuffix(t testing.TB) string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	return hex.EncodeToString(b)
}
