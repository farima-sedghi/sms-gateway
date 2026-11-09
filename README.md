# SMS Gateway

A prepaid SMS gateway, written in Go, for ~100M messages/day. Customers top up a
balance, send SMS (normal or express) through a REST API, and fetch reports of
what they sent.

Architecture: [docs/architecture.md](docs/architecture.md) · Decisions: [docs/adr/](docs/adr/)

## Quick start

Requirements: Docker Engine with `docker compose` v2, Go 1.22+, `make`.

```bash
cp .env.example .env        # optional: override ports, GOPROXY, etc.
make up                     # builds images, starts everything
make ps                     # all services healthy; migrate and kafka-init exited 0
curl -s localhost:8081/readyz
# {"kafka":"ok","postgres":"ok"}
make topics                 # describe Kafka topics
make down                   # stop (make clean also deletes volumes)
```

## API

Amounts are integers in minor units (e.g. rials), never decimals. The price of
one SMS is set by `SMS_PRICE`.

```bash
# Top up (creates the wallet on first use)
curl -s -X POST localhost:8081/customers/42/topup -d '{"amount": 1000}'
# {"customer_id":42,"balance":1000}

# Balance
curl -s localhost:8081/customers/42/balance

# Send an SMS (priority: "normal" (default) or "express")
curl -s -X POST localhost:8081/messages \
  -H 'Idempotency-Key: order-123' \
  -d '{"customer_id": 42, "recipient": "+989121234567", "body": "سلام", "priority": "express"}'
# 202 {"message_id":"019a...","status":"queued","priority":"express","cost":100,"created_at":"..."}
```

| Status | When |
|-------:|------|
| 202 | Message accepted, charged and queued |
| 400 | Invalid JSON, recipient, body (must fit one SMS page), priority or Idempotency-Key |
| 402 | Balance lower than the message price |
| 409 | Idempotency-Key already used with a different request body |

### Idempotency-Key (optional, recommended)

- With a key (max 255 characters, scoped per customer): retrying the same request
  returns the original response with the header `Idempotent-Replayed: true`, and
  you are charged only once. Reusing the key with a different body returns 409.
- **Without a key there is no retry protection.** If a request times out and
  you retry, the message may be **sent and charged twice**. Always send a key if
  your client retries.
- A request rejected with 402 does not store the key: top up and retry with the
  same key.

### What happens after 202

The message is charged and queued. The dispatcher then hands it to the
operator, and its status ends as one of:

| Status | Meaning | Charged? |
|--------|---------|----------|
| `sent` | The operator accepted it | Yes |
| `failed` | Certainly not delivered: the operator rejected it, retries ran out, or an express message expired (`EXPRESS_TTL`, default 2m) | No, refunded |
| `unknown` | Maybe delivered: the operator never answered and retries ran out | Yes, until reconciled with the operator |

In between it is `queued`, `sending` or `retrying`.

## Fake operator (`operator-mock`)

Configure it in `.env` (see `.env.example`), then run `make up`:

| Variable | Effect |
|----------|--------|
| `OPERATOR_LATENCY` | Time for a successful send (default 50ms) |
| `OPERATOR_TEMP_FAILURE_RATE` | Share of requests answered 503: the message is retried |
| `OPERATOR_PERM_FAILURE_RATE` | Share answered 400: the message fails and is refunded |
| `OPERATOR_TIMEOUT_RATE` | Share delivered but answered only after `OPERATOR_TIMEOUT_DELAY` (outcome unknown) |

It dedupes by message id, as a real operator would. `curl localhost:8090/stats`
shows `{"sends": N, "duplicates": M}`, and `duplicates` counts requests answered
from the dedupe map, which were not sent again.

Example: `OPERATOR_PERM_FAILURE_RATE=1 docker compose -f deployments/docker-compose.yml --env-file .env up -d operator-mock`
makes every message fail and get refunded.

## Services

| Service         | Host port | What it does                                   |
|-----------------|----------:|------------------------------------------------|
| `api`           | 8081      | REST API (`/healthz`, `/readyz`)               |
| `dispatcher`    | —         | Kafka consumers -> operator                    |
| `outbox-relay`  | —         | Outbox table -> Kafka                          |
| `operator-mock` | 8090      | Fake operator (configurable latency/failures)  |
| `postgres`      | 5432      | PostgreSQL 16                                  |
| `kafka`         | 29092     | Kafka (KRaft) for host tools; `kafka:9092` inside the network |
| `kafka-ui`      | 8080      | Browse topics and consumer groups              |
| `migrate`       | —         | One-shot: applies `migrations/`                |
| `kafka-init`    | —         | One-shot: creates topics                       |

## Layout

```
cmd/            one main.go per binary (api, dispatcher, outbox-relay, operator-mock)
internal/       modules: wallet, sms, dispatch, report, ratelimit, outbox, operator
  platform/     shared setup: config, logging, postgres, kafka, httpserver
migrations/     golang-migrate SQL files
deployments/    docker-compose and helper scripts
docs/           architecture and ADRs
```

`make help` lists every target.

## Renaming the Go module

The module path is a placeholder. To use your own:

```bash
go mod edit -module github.com/<you>/sms-gateway
grep -rl 'github.com/your-user/sms-gateway' --include='*.go' . | xargs sed -i 's#github.com/your-user/sms-gateway#github.com/<you>/sms-gateway#g'
```
