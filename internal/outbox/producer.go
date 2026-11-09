package outbox

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerOptions returns the franz-go options for a relay lane's producer.
//
//   - acks=all: a record counts as published only when every in-sync
//     replica has it, so a broker crash right after the ack loses nothing.
//   - Idempotent producer: franz-go enables it by default (we never call
//     DisableIdempotentWrite). The broker drops duplicates caused by the
//     client's own network retries. It does not cover a relay crash
//     between produce and commit; consumers handle that (decision 11).
//   - RecordDeliveryTimeout: if Kafka is unreachable, ProduceSync fails
//     after this long instead of blocking, the batch rolls back and the
//     lane backs off.
//   - lowLatency (express lane): no linger, so records are sent at once
//     instead of waiting to fill a bigger batch.
//
// Topics are never auto-created: they are created by kafka-init with chosen
// partition counts.
func ProducerOptions(lowLatency bool) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(10 * time.Second),
	}
	if lowLatency {
		opts = append(opts, kgo.ProducerLinger(0))
	}
	return opts
}
