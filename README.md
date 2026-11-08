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

## Services

| Service         | Host port | What it does                                   |
|-----------------|----------:|------------------------------------------------|
| `api`           | 8081      | REST API (`/healthz`, `/readyz`)               |
| `worker`        | —         | Kafka consumers -> operator                    |
| `outbox-relay`  | —         | Outbox table -> Kafka                          |
| `operator-mock` | 8090      | Fake operator (configurable latency/failures)  |
| `postgres`      | 5432      | PostgreSQL 16                                  |
| `kafka`         | 29092     | Kafka (KRaft) for host tools; `kafka:9092` inside the network |
| `kafka-ui`      | 8080      | Browse topics and consumer groups              |
| `migrate`       | —         | One-shot: applies `migrations/`                |
| `kafka-init`    | —         | One-shot: creates topics                       |

## Layout

```
cmd/            one main.go per binary (api, worker, outbox-relay, operator-mock)
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
