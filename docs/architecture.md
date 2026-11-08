# SMS Gateway: Architecture

> Living document. Sections marked **TODO** are filled in as each step lands.
> Decisions are recorded in [`docs/adr/`](adr/).

## 1. Context and goals
TODO: problem statement, actors (businesses via REST API, SMS operator), non-goals
(auth, UI, multi-page SMS).

## 2. Requirements and numbers
TODO: ~100M SMS/day = ~1,160 msg/s average; peak factor; express latency budget;
"balance never negative" invariant; storage growth per day.

## 3. Components
TODO: diagram and table of the four binaries (`api`, `dispatcher`, `outbox-relay`,
`operator-mock`) plus Postgres and Kafka; what each scales on.
See [ADR 0001](adr/0001-modular-monolith.md).

## 4. Module map and boundaries
TODO: modules, owned tables, allowed dependencies, the shared-transaction exception.

## 5. Request flow: sending an SMS
TODO: sequence: rate limit -> validate -> price -> tx(debit + ledger + message +
outbox) -> 202 -> relay -> Kafka -> dispatcher -> operator -> status.

## 6. Money: balance, ledger, idempotency
TODO: minor units, conditional UPDATE, ledger as source of truth, Idempotency-Key,
refunds.

## 7. Messaging topology
TODO: topics, partitions, keys, consumer groups. See [ADR 0002](adr/0002-kafka.md).

## 8. Fairness and rate limiting
TODO: per-customer token bucket, per-instance share, Redis production path.

## 9. Express path
TODO: isolation (topic, group, pool), latency metric, budget.

## 10. Failure handling: retry, DLQ, refunds
TODO: status machine, backoff, max attempts, idempotent refund.

## 11. Reports
TODO: daily partitions, index, keyset pagination; ClickHouse as future path.

## 12. Operations
TODO: health vs readiness, graceful shutdown, config, logs, metrics.

## 13. Scalability evidence (k6)
TODO: throughput, p95/p99, express vs normal latency, hardware used.

## 14. Future work
TODO: Redis rate limiter, ClickHouse reports, sharding, multi-operator routing.
