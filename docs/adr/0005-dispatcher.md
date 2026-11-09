# ADR 0005: Dispatcher, operator errors and circuit breaker

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

The dispatcher takes accepted messages from Kafka and hands them to the SMS
operator. It must:

- never send a message twice and never refund twice, even though Kafka delivers
  at-least-once and the dispatcher can crash at any point;
- refund only when the message was **certainly** not delivered;
- keep the express latency guarantee: an express message that can't reach the
  operator in time is not sent;
- survive an operator outage without flooding the retry topic or burning through
  retry attempts.

## Decisions

### 1. Three outcome classes (decision 16)

| Class     | Cause | Delivered? | Action |
|-----------|-------|------------|--------|
| Permanent | 4xx (except 429) | No, certainly | `failed` + refund |
| Temporary | 5xx, 429, connection error **before** the request was written, breaker open | No, certainly | retry |
| Unknown   | timeout or connection reset **after** the request was written, unreadable 2xx | Maybe | retry with the same ref; never refund |

The line between Temporary and Unknown comes from `httptrace.WroteRequest`. If
the request bytes never left, the operator can't have acted on them.

**The operator dedupes by ref (= our message_id).** That turns "maybe delivered"
into something we can work with:
- retrying with the same ref is safe;
- a later definitive answer settles an earlier Unknown. Success means sent.
  A 4xx proves the message was never delivered, because a delivered ref would get
  its original id back instead.

**Uncertainty is sticky.** Once an attempt is Unknown, the retry event carries
`uncertain=true`. If the message expires or runs out of attempts while still
uncertain, it ends as `unknown`: terminal, not refunded, left for manual
reconciliation. Refunding a message that may have been delivered loses money and
looks like a free SMS. Not refunding a message that wasn't delivered is fixable
with a reconciliation report.

### 2. Circuit breaker (decision 17)
`sony/gobreaker` v1. There is one breaker per operator, shared by every lane,
because the operator is one system.
- **Opens** after 5 consecutive failures, where a failure is a 5xx, a timeout or a
  transport error. 4xx responses (including 429) are answers from a healthy
  operator about a single request, so they don't count.
- **Half-open** after 30s, with 1 trial request.
- **While open**, `Send` returns `ErrCircuitOpen` (which wraps `ErrTemporary`)
  without calling the operator.

**Back-pressure instead of retry-topic flooding.** On `ErrCircuitOpen`, the
worker holds the message in place and tries again every second. No attempt is
counted and nothing goes to `sms.retry`. The batch can't finish, so the lane stops
fetching. Lag builds up in Kafka, where it is cheap and visible, instead of
turning into thousands of retry events and exhausted attempts. In the end-to-end
run, 10 messages sent during an outage stayed in `sending` with a single retry
event, and were all sent within seconds of the breaker closing.

### 3. Lanes
| Group | Topic | Workers | Batch |
|-------|-------|--------:|------:|
| `dispatcher-express` | `sms.express` | 32 | 64 |
| `dispatcher-normal`  | `sms.normal`  | 64 | 256 |
| `dispatcher-retry`   | `sms.retry`   | 16 | 64 |

- Each lane has its own Kafka client, consumer group and worker pool, so express
  never waits for a normal fetch or a normal worker.
- Lanes scale by adding dispatcher replicas, up to the partition count.

### 4. Batch processing and commits
- Auto-commit is off. Each lane polls a batch, processes it with a bounded pool
  (a semaphore of N workers), waits for every record, then commits the batch.
- A crash before the commit means redelivery, never a skipped message.
- **Trade-off:** the slowest message in a batch delays the next fetch. The
  operator timeout (5s) bounds this, and batches are small. Committing per
  partition up to the first unfinished record would cut that delay but adds
  bookkeeping; it isn't needed at the current numbers.

### 5. Idempotency under redelivery
- Every status change is a guarded `UPDATE ... WHERE status = ANY(<allowed>)`
  (decision 11).
- **Re-entry `sending -> sending` is allowed.** A record redelivered after a crash
  mid-send must be able to finish the job. Two concurrent deliveries of the same
  record (during a rebalance) may then both call the operator, and the operator's
  dedupe by ref keeps it to one real send. This is tested with the real client
  and mock: 2 concurrent handlers produce exactly 1 operator send.
- A terminal message (`sent`, `failed`, `unknown`) matches 0 rows and is skipped.
- A refund happens only inside the transaction that moves the message to
  `failed` (ADR 0003).

### 6. Produce before update
For a retry or DLQ, the event is produced to Kafka **first**, then the status is
updated.
- If the update fails, the retry event finds the message still in `sending`,
  which it may re-enter.
- The reverse order could leave a message in `retrying` with no event to ever
  pick it up again.

Infrastructure errors (Postgres or Kafka down) are retried in place with backoff
and never turned into a business outcome such as `failed`.

### 7. Retry timing
`sms.retry` events carry `next_attempt_at = now + 1s·2^(attempt-1)`, capped at
30s. The retry consumer sleeps until that time. Kafka has no per-message delay,
so an event with a long backoff delays the events behind it in its partition.
The 30s cap bounds that, and express retries are further bounded by
`expires_at`. If retry volume grows, the next step is tiered retry topics
(`sms.retry.5s`, `sms.retry.30s`, ...), where every event in a topic has the same
delay.

### 8. Express deadline
- `expires_at = created_at + EXPRESS_TTL` (default 2m), set in the same
  transaction that accepts the message.
- It is checked before every send, including after waiting for the breaker.
- An expired express message is not sent: `failed` + refund, or `unknown` if it
  is uncertain.

## Alternatives considered
- *Refund on any error after the retries run out, including timeouts:* simpler,
  but refunds messages the operator may have delivered: free SMS and an
  unreconcilable ledger. Unknown outcomes are never refunded.
- *A delay-capable broker (RabbitMQ delayed exchange, SQS delay) for retries:*
  per-message delays with no head-of-line effect, but a second broker to run.
  Kafka plus `next_attempt_at` is good enough with the 30s cap.
- *Tiered retry topics (`sms.retry.1s`, `sms.retry.30s`, ...):* removes the
  head-of-line delay inside the retry topic. It is the next step if retry volume
  grows.
- *Commit offsets per record or per partition prefix:* less redelivery after a
  crash and no batch barrier, but more bookkeeping. Batch commits plus
  idempotent handling are simpler and correct.
- *No circuit breaker (just retries):* during an outage every message burns its
  attempts against a dead operator and floods `sms.retry`, then fails and is
  refunded although the operator would have come back.
- *Forbid re-entry into `sending`:* a crash between `MarkSending` and the send
  would leave the message stuck in `sending` forever. Re-entry plus operator
  dedupe by ref is safe.

## Consequences

- Money stays correct under redelivery, crashes and concurrent duplicates. Every
  path is covered by tests against real Postgres and Kafka.
- An operator outage shows up as consumer lag and `sending` rows. It doesn't eat
  retry attempts or refunds.
- `unknown` messages need a reconciliation job against the operator's records
  (future work).
- Latency is logged per message (`latency_ms = handed_at - created_at`).
  Prometheus histograms per priority are future work, needed to show the express
  guarantee in the load-test results.
