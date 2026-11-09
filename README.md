# SMS Gateway

A prepaid SMS gateway in Go, designed for about 100M messages a day. Businesses
top up a balance, send normal or express SMS through a REST API, and fetch
reports of what they sent. The balance never goes negative, and express messages
have their own path to the operator, so normal traffic can't slow them down.

- **Architecture, capacity, failure modes:** [docs/architecture.md](docs/architecture.md)
- **Design decisions:** [docs/adr/](docs/adr/) (ADRs 0001–0007)

Stack: Go 1.22 (`net/http`, `log/slog`), PostgreSQL 16 (pgx), Kafka in KRaft
mode (franz-go), golang-migrate, Docker Compose.

## Quick start

Requirements: Docker Engine with Compose v2. Go 1.22+ and `make` are needed only
to run the tests.

```bash
cp .env.example .env          # defaults work; edit ports, limits or GOPROXY if needed
docker compose -f deployments/docker-compose.yml --env-file .env up -d --build
# or: make up

curl -s localhost:8081/readyz
# {"postgres":"ok"}
```

Compose starts Postgres and Kafka, runs the migrations and creates the topics,
then starts `api`, `outbox-relay`, `dispatcher`, `operator-mock` and `kafka-ui`.
Check status with `make ps` (or `docker compose ... ps -a`). Stop with
`make down`; `make clean` also deletes the data volumes.

## API

Base URL `http://localhost:8081`. All amounts are **integers in minor units**
(e.g. rials), never decimals. The price of one SMS is `SMS_PRICE` (default 100).
Errors always look like `{"error": "..."}`.

### Health
```bash
curl -s localhost:8081/healthz     # liveness: the process is up
# {"status":"ok"}
curl -s localhost:8081/readyz      # readiness: Postgres reachable (503 otherwise)
# {"postgres":"ok"}
```

### Wallet
```bash
# Top up. The wallet is created on first use. Returns the new balance.
curl -s -X POST localhost:8081/customers/42/topup -d '{"amount": 1000}'
# {"customer_id":42,"balance":1000}

# Balance (0 for a customer that never topped up)
curl -s localhost:8081/customers/42/balance
# {"customer_id":42,"balance":1000}
```
An `amount` that is ≤ 0 or above 10¹² returns 400.

### Send an SMS
```bash
curl -s -i -X POST localhost:8081/messages \
  -H 'Idempotency-Key: order-123' \
  -d '{"customer_id": 42, "recipient": "+989121234567", "body": "سلام", "priority": "express"}'
# HTTP/1.1 202 Accepted
# {"message_id":"01a1bfc0-a46f-7e21-8c84-0e795a169c81","status":"queued","priority":"express",
#  "cost":100,"created_at":"2026-11-09T10:00:35.696009Z"}
```

| Field | Rules |
|---|---|
| `customer_id` | positive integer (there is no auth; see the assumptions in the architecture doc) |
| `recipient` | 8–15 digits, optional leading `+` |
| `body` | not empty, one SMS page: 160 Latin/GSM characters, or 70 for other scripts (e.g. Persian) |
| `priority` | `normal` (default) or `express` |
| `Idempotency-Key` header | optional, at most 255 characters, scoped per customer |

| Status | When | Example body |
|---:|---|---|
| 202 | Accepted, charged and queued | as above |
| 400 | Invalid JSON, unknown field, recipient, body, priority or key | `{"error":"validation failed: recipient must be 8-15 digits, optionally starting with +"}` |
| 402 | Balance lower than the price | `{"error":"insufficient balance"}` |
| 409 | Idempotency-Key reused with a different body | `{"error":"idempotency key reused with a different request"}` |
| 429 | Over your rate limit. The `Retry-After` header gives seconds. Not charged. | `{"error":"rate limit exceeded: retry after 913.832406ms"}` |

**Idempotency-Key (recommended).**
- Sending the same request again with the same key returns the original 202
  response, with the header `Idempotent-Replayed: true`, and charges nothing.
- **Without a key there is no retry protection.** If a request times out and you
  retry, the message may be **sent and charged twice**.
- A request rejected with 402 does not use up its key: top up and retry with the
  same key.

**Rate limits.**
- Each customer has a token bucket per priority, so heavy normal traffic never
  uses up the express allowance.
- Customers without a plan get the defaults: normal 200 msg/s (burst 400) and
  express 50 msg/s (burst 100).
- A customer can be given its own plan:

```bash
curl -s -X PUT localhost:8081/customers/42/limits \
  -d '{"normal_rps": 2000, "express_rps": 100, "burst": 4000}'
# {"customer_id":42,"normal_rps":2000,"express_rps":100,"burst":4000}
```
Each value must be 1–100,000. The plan applies at once on the instance that
received the PUT, and within 10 s on the others. `burst` is the bucket size for
both priorities.

### What happens after 202
The dispatcher hands the message to the operator. The final status is one of:

| Status | Meaning | Charged |
|---|---|---|
| `sent` | Handed to the operator (not a handset delivery report) | yes |
| `failed` | Certainly not delivered: rejected by the operator, retries exhausted, or an express message older than `EXPRESS_TTL` (2 min) | **no, refunded once** |
| `unknown` | Maybe delivered: the operator never answered and retries ran out | yes, until reconciled |

In between, the status is `queued`, `sending` or `retrying`.

### Reports
```bash
# Messages in [from, to): RFC3339, required, at most 31 days. Newest first.
curl -s 'localhost:8081/customers/42/messages?from=2026-11-09T00:00:00Z&to=2026-11-10T00:00:00Z&limit=1'
# {"messages":[{"message_id":"01a1bfc0-a46f-7e21-8c84-0e795a169c81","recipient":"+989121234567",
#   "priority":"express","status":"sent","cost":100,"attempts":1,
#   "created_at":"2026-11-09T10:00:35.696009Z","handed_at":"2026-11-09T10:00:35.79617Z",
#   "last_error":null}],
#  "next_cursor":"eyJ0IjoiMjAyNi0xMS0wOVQx..."}

# Next page: same from/to/status, plus the cursor exactly as returned
curl -s 'localhost:8081/customers/42/messages?from=2026-11-09T00:00:00Z&to=2026-11-10T00:00:00Z&limit=1&cursor=eyJ0IjoiMjAyNi0xMS0wOVQx...'

# Filter by status
curl -s 'localhost:8081/customers/42/messages?from=2026-11-09T00:00:00Z&to=2026-11-10T00:00:00Z&status=failed'

# One message
curl -s localhost:8081/customers/42/messages/01a1bfc0-a46f-7e21-8c84-0e795a169c81
# {"message_id":"01a1bfc0-a46f-7e21-8c84-0e795a169c81","recipient":"+989121234567","priority":"express",
#  "status":"sent","cost":100,"attempts":1,"created_at":"2026-11-09T10:00:35.696009Z",
#  "handed_at":"2026-11-09T10:00:35.79617Z","last_error":null}
```
- `limit` is 1–100 (default 50). `next_cursor` is `null` on the last page.
- `status` is one of `queued`, `sending`, `retrying`, `sent`, `failed` or
  `unknown`.
- A range over 31 days, a missing `from`/`to`, or a cursor reused with different
  filters returns 400.
- Another customer's message returns 404.

### Fake operator (`operator-mock`, port 8090)
```bash
curl -s localhost:8090/stats
# {"sends":142,"duplicates":0}
```
It dedupes by message id, as a real operator would: `duplicates` counts repeat
requests that were answered without sending again.

Its behaviour is set in `.env`:

| Variable | Effect |
|---|---|
| `OPERATOR_LATENCY` | latency of a successful send (default 50ms) |
| `OPERATOR_TEMP_FAILURE_RATE` | share answered 503, so the message is retried |
| `OPERATOR_PERM_FAILURE_RATE` | share answered 400, so the message fails and is refunded |
| `OPERATOR_TIMEOUT_RATE` | share delivered but answered after `OPERATOR_TIMEOUT_DELAY`, so the outcome is unknown |

To make every message fail and be refunded:
`OPERATOR_PERM_FAILURE_RATE=1 docker compose -f deployments/docker-compose.yml --env-file .env up -d operator-mock`

## Running the tests

The tests use the **real Postgres and Kafka** from the compose stack, not mocks.
Each test creates its own database and Kafka topics and deletes them afterwards.

```bash
docker compose -f deployments/docker-compose.yml --env-file .env up -d postgres kafka   # or the full stack: make up
make test
# or without make:
TEST_DATABASE_URL='postgres://sms:sms@localhost:5432/sms?sslmode=disable' \
TEST_KAFKA_BROKERS=localhost:29092 \
go test -race -count=1 ./...
```

Without those two environment variables, the database and Kafka tests are
skipped and only the pure unit tests run. What's covered:
- 100 concurrent debits never overspend;
- idempotent replays and conflicts;
- exactly-once refunds;
- duplicate and late Kafka deliveries;
- unknown outcomes are never refunded;
- the circuit breaker;
- outbox lanes and parallel relays;
- keyset pagination, including ties;
- partition pruning;
- rate limiting.

## Load test

```bash
make loadtest      # needs the stack running; k6 runs in Docker (grafana/k6:0.54.0)
# or one run with your own numbers:
HEAVY_CUSTOMERS=10 HEAVY_RATE=800 EXPRESS_RATE=100 DURATION_S=120 ./loadtest/run.sh capacity
HEAVY_RATE=1000 ./loadtest/run.sh fairness
```
- **capacity:** heavy customers on a high plan plus 100 express customers.
- **fairness:** one heavy customer above its default plan; it must get 429s
  while express is unaffected.

Each run writes `loadtest/results/<mode>-<run_id>.txt`, containing:
- the k6 summary;
- a `docker stats` snapshot taken mid-run;
- p50/p95/p99 of accept → handed per priority, from
  [`loadtest/latency.sql`](loadtest/latency.sql), after the backlog drains.

Results and the capacity extrapolation are in
[architecture.md §10](docs/architecture.md#10-load-test-results). For
comparable numbers, start the stack with `LOG_LEVEL=warn`.

## Services

| Service | Host port | What it does |
|---|---:|---|
| `api` | 8081 | REST API |
| `outbox-relay` | — | outbox table → Kafka, one loop per lane |
| `dispatcher` | — | Kafka → operator; status updates, retries, refunds |
| `operator-mock` | 8090 | fake operator |
| `postgres` | 5432 | PostgreSQL 16 |
| `kafka` | 29092 | Kafka (KRaft) for host tools; `kafka:9092` inside the network |
| `kafka-ui` | 8080 | browse topics and consumer groups |
| `migrate` | — | one-shot: applies `migrations/` |
| `kafka-init` | — | one-shot: creates topics |

All configuration is through environment variables. Every variable, with its
default and meaning, is in [`.env.example`](.env.example).

## Layout

```
cmd/            one main.go per binary: api, outbox-relay, dispatcher, operator-mock
internal/       modules (domain, service, repository, handler as files):
  wallet/       balances, ledger, top-up, debit, refund
  sms/          accept messages (idempotency, debit, outbox), status lifecycle
  outbox/       transactional outbox and the relay
  dispatch/     Kafka consumers, retries, DLQ
  operator/     operator HTTP client (error classes, circuit breaker) and mock
  report/       read-only reports
  ratelimit/    per-customer token buckets and plans (customer_limits)
  platform/     config, logging, postgres, kafka, httpserver, backoff
migrations/     golang-migrate SQL files
deployments/    docker-compose and helper scripts
docs/           architecture.md and ADRs
```

`make help` lists every make target.
