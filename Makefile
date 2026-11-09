# Run `make help` for the list of targets.
# Compose loads the root .env (if present) for variable interpolation.
COMPOSE := docker compose -f deployments/docker-compose.yml $(if $(wildcard .env),--env-file .env)
GO      := GOTOOLCHAIN=local go

.DEFAULT_GOAL := help
.PHONY: help build test lint fmt tidy up down clean logs ps topics migrate-up loadtest

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

build: ## Build all binaries into ./bin
	$(GO) build -o bin/ ./cmd/...

# DB tests create (and drop) their own database on this server; needs `make up` first.
# Kafka tests create (and delete) their own topics on this broker.
TEST_DATABASE_URL ?= postgres://sms:sms@localhost:5432/sms?sslmode=disable
TEST_KAFKA_BROKERS ?= localhost:29092

test: ## Run unit + Postgres/Kafka tests (needs `make up` first)
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TEST_KAFKA_BROKERS='$(TEST_KAFKA_BROKERS)' $(GO) test -race -count=1 ./...

lint: ## go vet + gofmt check
	$(GO) vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run: make fmt" && exit 1)

fmt: ## Format code
	gofmt -w .

tidy: ## Sync go.mod/go.sum
	$(GO) mod tidy

up: ## Build images and start the full stack
	$(COMPOSE) up -d --build

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

clean: ## Stop the stack and delete its volumes (all local data)
	$(COMPOSE) down -v

logs: ## Follow logs (make logs s=api for one service)
	$(COMPOSE) logs -f $(s)

ps: ## Show service status
	$(COMPOSE) ps -a

topics: ## Describe Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

migrate-up: ## Re-run migrations
	$(COMPOSE) run --rm migrate

# The runs documented in docs/architecture.md §10.
loadtest: ## Load test (k6 in Docker): capacity run, then fairness run. Needs `make up`
	HEAVY_CUSTOMERS=10 HEAVY_RATE=800 ./loadtest/run.sh capacity
	HEAVY_CUSTOMERS=1 HEAVY_RATE=1000 ./loadtest/run.sh fairness
