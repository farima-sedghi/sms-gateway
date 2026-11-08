# ADR 0002: Kafka as the message backbone, fed by a transactional outbox

- **Status:** Accepted
- **Date:** 2026-11-08

## Context

Accepted messages must be delivered to the operator asynchronously. The API must
return quickly and stay unaffected by operator latency. The queue must:

- sustain ~1,200 msg/s on average and several times that at peak, with headroom;
- keep **express** traffic isolated from normal traffic (latency guarantee);
- support retries with backoff and a dead-letter path;
- let consumers scale horizontally;
- not lose a message that was paid for.

Writing to Postgres and publishing to a broker in the same request is a
**dual write**. If the process crashes between the two, the message is either
paid for and never sent, or sent and never recorded.

## Decision

1. **Kafka** (KRaft mode, no ZooKeeper) with client `twmb/franz-go`.
2. **Transactional outbox.** The API writes an `outbox` row in the same transaction
   as the debit and the message. `outbox-relay` polls with
   `SELECT ... FOR UPDATE SKIP LOCKED`, publishes, and marks rows sent.
   Delivery is **at-least-once**.
3. **Topics** (created explicitly by `kafka-init`; auto-create is off):

   | Topic         | Partitions | Purpose                                       |
   |---------------|-----------:|-----------------------------------------------|
   | `sms.express` | 6          | Express messages, own consumer group and pool |
   | `sms.normal`  | 12         | Normal messages (bulk of the traffic)         |
   | `sms.retry`   | 6          | Failed sends with attempt count and backoff   |
   | `sms.dlq`     | 3          | Exhausted retries, for inspection             |
   | `sms.status`  | 6          | Status events (sent/failed)                   |

4. **Partition key = `message_id`**, not `customer_id`. A few customers send at huge
   rates. Keying by customer would pin each heavy customer to one partition and one
   consumer, which creates hot partitions. Keying by message spreads load evenly.
   We don't need per-customer ordering: every SMS is independent.
5. **Idempotent consumers.** Duplicates (from the outbox or consumer rebalances) are
   neutralised by guarded status transitions in SQL
   (`UPDATE ... WHERE status = 'queued'`). Offsets are committed only after
   processing finishes.

## Consequences

**Positive**
- Partitioned log gives horizontal consumer scaling up to the partition count, high
  throughput and replay. Consumer lag is a clear autoscaling signal.
- Separate topics and consumer groups mean express is never stuck behind a normal
  backlog.
- The outbox removes the dual-write problem. A Kafka outage delays delivery but
  never loses a paid message.

**Negative / risks**
- Operational weight (a JVM broker). Locally this is one container. In production,
  a managed service or a 3+ broker cluster with RF=3.
- At-least-once means every consumer must be idempotent. This is designed in, not
  optional.
- Kafka has no native delayed delivery. Retry backoff is done by the retry consumer
  (it waits until `next_attempt_at`). Tiered retry topics are the future option.
- Partition count caps parallelism per group. Partitions can be increased later
  (key-to-partition mapping changes, which is fine with no ordering requirement).

**Alternatives considered**
- *RabbitMQ:* has good per-message ack and delayed queues, but lower throughput per
  node, no replay, and harder horizontal scaling at this volume.
- *Postgres as the queue (SKIP LOCKED workers only):* simplest, but puts all queue
  churn on the same database that guards money. It becomes the bottleneck at
  100M rows/day.
- *Redis Streams:* fast, but memory-bound with weaker durability guarantees.
