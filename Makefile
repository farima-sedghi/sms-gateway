# Run `make help` for the list of targets.
# Compose loads the root .env (if present) for variable interpolation.
COMPOSE := docker compose -f deployments/docker-compose.yml $(if $(wildcard .env),--env-file .env)
GO      := GOTOOLCHAIN=local go

.DEFAULT_GOAL := help
.PHONY: help build test lint fmt tidy up down clean logs ps topics migrate-up

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

build: ## Build all binaries into ./bin
	$(GO) build -o bin/ ./cmd/...

test: ## Run unit and integration tests
	$(GO) test -race -count=1 ./...

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
