# ADR 0006: Reports API (keyset pagination over daily partitions)

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

Customers need to list their sent messages and look up a single one. The
`messages` table gets about 100M rows a day and is range-partitioned by day on
`created_at` (decision 14). A few customers send millions of messages a day. A
report query must stay fast for them and must never scan the whole history.

## Decisions

### 1. Endpoints
- `GET /customers/{id}/messages?from=&to=&status=&limit=&cursor=` returns
  `{"messages": [...], "next_cursor": "..." | null}`.
- `GET /customers/{id}/messages/{message_id}` returns one message.

Each message carries id, recipient, priority, status, cost, attempts,
created_at, handed_at and last_error. The body is left out: reports are about
delivery, and bodies are large and may be sensitive.

The `report` module **reads** `messages` (owned by `sms`) and never writes. In
production its pool can point at a read replica.

### 2. A range is required, and at most 31 days
- `from` and `to` (RFC3339; `from` inclusive, `to` exclusive) are required.
  Because they are plain AND conditions on `created_at`, Postgres prunes to the
  daily partitions inside the range.
- An unbounded query would touch every partition ever created. 31 days covers
  monthly statements.

### 3. Keyset pagination, never OFFSET
```sql
WHERE customer_id = $1 AND created_at >= $from AND created_at < $to
  [AND status = $s]
  AND created_at <= $cursor_t                 -- index range starts at the cursor
  AND (created_at, id) < ($cursor_t, $cursor_id)  -- exact position, ties broken by id
ORDER BY created_at DESC, id DESC
LIMIT $limit + 1                              -- the extra row means "there is a next page"
```
- `OFFSET n` reads and throws away n rows, so deep pages get slower and slower.
  Keyset starts reading at the cursor, so page 1,000 costs the same as page 1.
- Keyset is also stable while new messages arrive: rows are never skipped or
  repeated across pages. OFFSET shifts when new rows are inserted at the top.
- `limit` defaults to 50 and allows at most 100.
- The SQL is put together from fixed fragments, with all values as parameters.
  Optional conditions are added only when present, rather than as
  `($x IS NULL OR ...)`, which would hide the cursor bound from the index scan.

### 4. Opaque cursor bound to its query
- `next_cursor = base64url(JSON{t: created_at, id, f: filter_hash})`.
- *Opaque:* clients pass it back unchanged, so we can change its contents later.
- *Bound:* `f` is a hash of (customer, from, to, status). Reusing a cursor with
  different filters returns 400 instead of silently returning a wrong page.
- The cursor is not signed. Changing it only changes the starting point within
  the customer's own data, because `customer_id` always comes from the path.

### 5. The index stays `(customer_id, created_at DESC)`, without `id`

This is a deliberate trade-off. The system is **write-heavy**: about 100M inserts
a day against far fewer report reads, and every extra index column costs on
every insert.
- `created_at` has microsecond precision and comes from `now()` of the accepting
  transaction, one message per transaction. So two messages from one customer
  with the same `created_at` are rare.
- When ties happen, the row comparison keeps pagination correct. Postgres sorts
  the few tied rows by id (incremental sort), which is cheap.
- A test puts the cursor exactly between two rows that share a `created_at`. It
  fails if `id` is dropped from the comparison.

**When to switch:** if batch inserts are introduced (many messages in one
transaction share one `now()`), ties become common. Then replace the index with
`(customer_id, created_at DESC, id DESC)`, so both the cursor condition and the
ORDER BY come straight from the index.

### 6. Get by id: the time is inside the UUIDv7
- Message ids are UUIDv7, whose first 48 bits are Unix milliseconds.
- The handler reads that time and queries
  `id = $1 AND customer_id = $2 AND created_at` within ±5 minutes of it. The
  window absorbs clock skew, since the id comes from the API clock and
  `created_at` from Postgres.
- Postgres reads one partition (two right at midnight) instead of probing every
  daily partition's primary key. In the test, `EXPLAIN` shows **1 of 9
  partitions** scanned.
- Another customer's message, or a non-v7 id, returns 404 rather than 403, so ids
  can't be probed.

### 7. Status filter
`status=` is optional and checked against the known statuses. It is applied as
a filter on the index range scan. A rare status on a very heavy customer can
mean scanning many rows per page. If that becomes a real query pattern, a
partial index such as `(customer_id, created_at DESC) WHERE status = 'failed'`
is the targeted fix.

## Alternatives considered
- *OFFSET pagination:* simple, but page N reads and discards every earlier row,
  and pages shift as new messages arrive.
- *Cursor on `created_at` only:* skips or repeats rows that share a timestamp
  across a page boundary. The test for that fails without `id`.
- *Index `(customer_id, created_at DESC, id DESC)`:* no sort for ties, but a
  wider index on every one of ~100M daily inserts. Deferred until ties are common.
- *No required range (or a larger maximum):* the query would touch every daily
  partition ever created, and its cost would grow with history, not page size.
- *Get by id with the primary key only (`WHERE id = $1`):* the primary key is
  `(id, created_at)`, so without `created_at` Postgres probes the index of every
  partition.
- *Serving reports from ClickHouse now:* the right tool for aggregates, but
  another system to build. The OLTP query is cheap with keyset pagination and
  pruning.

## Consequences
- Report latency depends on page size, not on history size or page depth.
- Heavy analytics (totals per day, status breakdowns over months) don't belong on
  the OLTP database. The future path is streaming `sms.status` into
  **ClickHouse** and serving aggregates from there.
