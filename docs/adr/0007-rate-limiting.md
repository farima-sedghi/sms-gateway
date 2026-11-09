# ADR 0007: Per-customer rate limiting

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

Load is very uneven: a few customers send at huge rates, most rarely. Without a
limit, one customer's burst fills the database, the outbox and the operator
queue, and every other customer's messages wait behind it. Express traffic has
a latency guarantee that a normal flood must not break.

## Decisions

### 1. A token bucket per (customer, class)
- `internal/ratelimit.Memory` keeps one `golang.org/x/time/rate.Limiter` per
  `(customer_id, class)`, where class is `express` or `normal`.
- **Rate** is the sustained messages per second. **Burst** is how many can be
  sent at once after a quiet period.
- The two classes have separate buckets, so a customer's normal flood never
  uses up its express allowance.

| Env | Default | Meaning |
|-----|--------:|---------|
| `RATE_LIMIT_NORMAL_RPS` / `_BURST` | 200 / 400 | per customer, global |
| `RATE_LIMIT_EXPRESS_RPS` / `_BURST` | 50 / 100 | per customer, global |
| `API_INSTANCES` | 1 | API replicas sharing the limit |

A token bucket allows short bursts, which real senders have, while capping the
long-run rate. It needs constant memory per key.

### 2. Checked in `sms.Accept`, after validation and before the transaction
- **After validation:** malformed requests don't use up tokens.
- **Before the transaction:** a rejected request never touches Postgres.
  Protecting the database is the point of the limit.
- A rejected request returns **429** with `Retry-After`. The value is the exact
  wait until the next token (from `Reserve().Delay()`, then the reservation is
  cancelled), rounded up to whole seconds.
- Rejected requests **don't consume tokens**, because the reservation is
  cancelled. A client that retries in a tight loop doesn't push its own wait
  further out.
- Accepted requests that later fail (402, a 409 idempotency conflict) or are
  idempotent replays still use a token. The limiter protects capacity, and those
  requests cost capacity too. Refunding tokens would let a client loop on
  failing requests for free.

### 3. Interface where it is used
`sms` defines `RateLimiter { Allow(customerID, class) (bool, time.Duration) }`.
`*ratelimit.Memory` satisfies it without saying so (Go interfaces are satisfied
implicitly), and a Redis limiter can replace it without touching `sms`. A nil
limiter means no limiting (tests).

### 4. Per-instance limit = global / replicas
Each API process enforces `global / API_INSTANCES` (burst at least 1).
- **Assumption:** the load balancer spreads one customer's requests evenly
  (round robin). Then the sum across instances is close to the global limit.
- **How wrong it can be:**
  - With uneven spreading (sticky connections, HTTP keep-alive pinning a client
    to one instance), a customer can be limited at 1/N of its allowance while
    other instances sit idle.
  - When instances are added or removed, the limit is wrong until
    `API_INSTANCES` is updated.
- It is a deliberate approximation: no network call on the hot path and no
  shared dependency. Good enough to stop floods; not exact.

### 5. Evicting idle buckets
- `Run(ctx)` deletes buckets idle for longer than `RATE_LIMIT_IDLE_TTL` (10m).
  Otherwise every customer that ever sent a message keeps a bucket forever.
- **Eviction never changes a decision.** The TTL is raised to at least each
  class's refill time (`burst / rate`), so an evicted bucket was already full,
  exactly like the new full bucket that replaces it.
- One mutex guards the map, and each `rate.Limiter` has its own lock, so the map
  lock is held only for the lookup. If contention shows up in profiles, the
  next step is to shard the map by `customer_id % N`.

## Production path: Redis (documented, not built)
A shared limiter makes the limit exact regardless of instance count or load
balancing:
- One Redis key per (customer, class), holding the bucket state (tokens and
  last refill time) or a GCRA "theoretical arrival time".
- A **Lua script** refills and takes a token atomically, in **one round trip**,
  and returns the wait time for `Retry-After`. Keys expire after the refill time,
  which gives the same eviction for free.
- **If Redis is down, fail open to the local limiter:** fall back to the
  in-memory limiter at the per-instance limit. A Redis outage must not stop
  message acceptance; the local limiter still blocks floods.
- Cost: about 0.2–0.5ms per request plus Redis capacity. At 1,200 msg/s average
  and several times that at peak, that is well within a single Redis primary.

## Consequences
- One customer can't swamp the database, the outbox, Kafka or the operator, and
  express capacity per customer is protected.
- Limits are approximate across instances until the Redis limiter is built.
- Load tests (k6) must use many customers, or raised limits, so they measure
  system throughput and not the limiter.
