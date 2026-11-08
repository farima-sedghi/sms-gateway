# ADR 0003: Money, idempotency and message state

- **Status:** Accepted
- **Date:** 2026-11-08

## Context

Customers prepay. The balance must **never** go negative, a customer must be able
to spend all of it, and a client retry or a duplicate Kafka delivery must never
charge, send or refund twice. At ~100M messages/day the hot path (accepting one
message) must be a short transaction.

## Decisions

### 1. Balance as a cache, ledger as the source of truth
- `wallets.balance` (BIGINT, minor units, `CHECK (balance >= 0)`) is what the hot
  path reads and writes.
- Every change also appends a row to `ledger_entries`, in the same transaction.
  The amount is **signed** (top-up and refund > 0, debit < 0). So
  `SUM(amount) = balance` per customer is an invariant that one query can check.
  The concurrency test asserts it.
- The ledger is partitioned by month. It is append-only, so old months can be
  archived.

### 2. Atomic debit with one conditional UPDATE
```sql
UPDATE wallets SET balance = balance - $cost
WHERE customer_id = $1 AND balance >= $cost
```
- 0 rows changed means insufficient balance (402), and the transaction rolls back.
- The row lock plus the re-check of the WHERE clause on the latest committed row
  make this safe under any concurrency. There is no read-then-write window.
- **Rejected: `SELECT ... FOR UPDATE`, then check in Go, then UPDATE.** It is just
  as correct, but needs two round trips while holding the lock.
- **Rejected: SERIALIZABLE isolation.** Serialization failures under contention
  would need retries.
- The `CHECK` constraint is a second safety net, in case a code path ever forgets
  the guard.

### 3. Debit, message and outbox in ONE transaction
`sms.Accept` passes its `pgx.Tx` to `wallet.Debit` and `outbox.Insert`
(see ADR 0001). Either all of these exist or none do: the idempotency key, the
debit, the ledger row, the message and the outbox event. A 402 also rolls back the
idempotency key, so the client can top up and retry with the same key.

### 4. Idempotency keys in their own table
- The header is optional, at most 255 characters, and scoped per customer
  (`PRIMARY KEY (customer_id, key)`).
- **Why a separate table:** `messages` is partitioned, and Postgres requires a
  unique index on a partitioned table to include the partition key (`created_at`).
  A unique `(customer_id, key)` across all days is impossible there.
- `request_hash` (SHA-256 of recipient, body and priority) separates a retry
  (same hash: return the original message, no charge) from a client bug (a
  different body under the same key: 409).
- **Concurrency:** the claim is `INSERT ... ON CONFLICT DO NOTHING`. A second
  request with the same key blocks on the unique index until the first commits,
  then sees the conflict and replays. If the first rolled back, the second goes
  ahead.
- The message reuses the key row's `created_at`, so a replay loads the message by
  its full primary key and touches one partition.
- **Without a key** there is no protection. The README documents that.
- *Future:* a TTL cleanup job for keys (e.g. after 24h).

### 5. Status transitions use an allow-list
Every status UPDATE is guarded with `WHERE status = ANY(<allowed sources>)`:

| To         | Allowed from                    |
|------------|---------------------------------|
| `sending`  | `queued`, `retrying`            |
| `retrying` | `sending`                       |
| `sent`     | `sending`                       |
| `failed`   | `queued`, `sending`, `retrying` |

- `sent` and `failed` are terminal.
- A deny-list (`status <> 'failed'`) was rejected. It would let a late "fail"
  event refund a message that was already delivered, and every new status added
  later would silently be allowed.

### 6. Refund idempotency comes from the status transition
`FailAndRefund` runs the guarded `UPDATE ... SET status = 'failed'` and calls
`wallet.Refund` **only if exactly one row changed**, in the same transaction.
Calling it twice, or after `sent`, is a no-op.

**Rejected: a unique index on `ledger_entries (message_id) WHERE kind = 'refund'`.**
`ledger_entries` is partitioned by `created_at`, so such an index can't be unique
globally. It would also make the ledger enforce message rules. `wallet.Refund` is
documented as not idempotent by itself.

### 7. UUIDv7 message IDs
- Generated in the application, so the ID is known before the insert and can be
  used as the Kafka key and for idempotency.
- Time-ordered, so primary-key inserts append to the B-tree instead of scattering
  writes the way UUIDv4 does.

### 8. Daily partitions for `messages`
- The migration creates partitions for today plus 7 days (UTC), and a DEFAULT
  partition.
- Reports prune to the days they query. Old days can be detached or dropped
  without a mass `DELETE`.
- **Caveat:** a daily maintenance job has to create partitions ahead of time (a
  later step; pg_partman is an option). If rows land in DEFAULT for a date,
  creating that date's partition later fails until those rows are moved. The job
  must stay ahead.

## Consequences

- One short transaction per message: about six statements, a single row lock on
  the wallet, and no distributed coordination.
- **Hot wallet:** all debits for one customer serialise on that customer's wallet
  row. For very heavy senders the mitigations are, in order: the rate limiter
  (decision 8), batching debits per request, and splitting a wallet into N
  sub-wallets.
- Every consumer has to address messages by `(id, created_at)`. The outbox payload
  carries `created_at` for that reason.
