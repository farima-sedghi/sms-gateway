# ADR 0004: Per-lane outbox relay

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

`POST /messages` writes an outbox row in the same transaction as the debit and the
message (ADR 0003). Something must move those rows to Kafka. It must:

- never lose a committed row, even if Kafka is down for a while;
- never let express messages wait behind a normal backlog: express has a latency
  guarantee, normal does not;
- scale horizontally: several relay replicas at once, without publishing the same
  row twice in normal operation;
- keep the outbox table from growing without limit.

## Decision

### Lanes
The relay runs **one independent loop per topic lane**, today `sms.express` and
`sms.normal`. Each lane has its own goroutine, its own transaction per batch, its
own idle interval (express 20ms, normal 100ms), and **its own Kafka producer
client**. Separate clients matter because a client shares its record buffer and
broker connections: a 500-record normal batch would otherwise sit in front of an
express record.

Lane isolation also has to hold **inside Postgres**. The original index
`(id) WHERE published_at IS NULL` made the express query
(`WHERE topic = 'sms.express' ORDER BY id`) walk past every unpublished normal
row. Migration 0003 replaces it with `(topic, id) WHERE published_at IS NULL`,
so each lane reads only its own part of the index. In the test, 10 express rows
are published in about 45ms while 20,000 normal rows are still waiting.

### One batch
```
BEGIN
  SELECT id, key, payload FROM outbox
  WHERE published_at IS NULL AND topic = $lane
  ORDER BY id LIMIT 500
  FOR UPDATE SKIP LOCKED
  ProduceSync(records)            -- acks=all, idempotent producer, 10s delivery timeout
  UPDATE outbox SET published_at = now() WHERE id = ANY($ids)
COMMIT
```
- **SKIP LOCKED:** a second replica skips rows another one has claimed instead of
  waiting for them. In the test, two relays split 5,000 rows (2,600 and 2,400)
  with zero duplicates.
- **Produce error:** roll back and back off exponentially (100ms up to 5s, with
  jitter, reset after a success). The rows stay unpublished and are retried.
- **Pacing:** after a full batch the lane loops again at once, because there is
  probably more. After a partial or empty batch it sleeps for its idle interval.

### Delivery guarantee: at-least-once
Kafka and Postgres don't share a transaction. Two cases cause a duplicate:
- a crash after Kafka acknowledged the batch but before the commit;
- a partial produce failure, where some records got in and the rollback causes the
  whole batch to be sent again.

The idempotent producer only removes duplicates caused by the client's own network
retries. Consumers therefore have to be idempotent: guarded status transitions,
decision 11. To avoid needless duplicates on shutdown, a batch in flight runs on a
context that ignores SIGTERM (`context.WithoutCancel` plus a 15s timeout). The loop
only checks for shutdown between batches.

### Holding the transaction during the produce
Row locks are held while Kafka acknowledges (milliseconds normally, at most the
10s delivery timeout).

The alternative was claim-then-publish: mark rows with a lease in one
transaction, publish, then mark them sent in a second transaction. It holds no
locks during network I/O, but needs lease columns, lease expiry and more states.
With SKIP LOCKED, held locks block no one: other replicas take the next rows. So
the simpler design wins.

### Cleanup and lag
- **Cleanup:** every minute, delete rows published more than 1h ago, in chunks of
  5,000 (`ORDER BY id` walks the primary key from the oldest rows, so no index on
  `published_at` is needed). Short statements keep locks and WAL bursts small. It
  is safe on every replica. The 1h of history helps debugging.
- **Lag:** every 10s, each lane logs the age of its oldest unpublished row. One
  index lookup does it. This is the number to alert on: it grows when the relay
  falls behind or Kafka is down.

## Alternatives considered
- *One loop for all topics, express first:* a big normal batch waiting on Kafka
  still delays the next express batch. Separate lanes share nothing.
- *Claim-then-publish with a lease (no transaction held during the produce):*
  needs lease columns, expiry and recovery. With SKIP LOCKED, held locks block no
  one, so it buys nothing at our latencies (see "Holding the transaction" above).
- *Delete rows on publish instead of marking + hourly cleanup:* fewer writes, but
  no short history for debugging, and it is the same churn in a different place.
- *CDC (Debezium / logical decoding):* push-based and no UPDATE/DELETE churn, but
  another system to run. It is the documented future path.
- *Keeping the old index `(id) WHERE published_at IS NULL`:* the express query
  would scan the whole normal backlog (see "Lanes").

## Consequences

- Express latency from the outbox to Kafka is about its idle interval plus one
  produce, whatever the normal backlog.
- The relay scales by adding replicas. The real limit is Postgres write throughput
  (the `published_at` UPDATE plus the DELETE per row).
- The outbox is a high-churn table, so autovacuum must keep up. In production, tune
  `autovacuum_vacuum_scale_factor` for this table.
- A new lane (for example `sms.retry`, if it ever goes through the outbox) is one
  more `Lane` in `cmd/outbox-relay`.

## Future work
- Replace polling with logical decoding / CDC (Debezium, or `pgoutput` read
  directly). That gives push-based latency and no UPDATE/DELETE churn, at the cost
  of more operational complexity.
- Expose lag and batch metrics to Prometheus instead of only logging them.
