# Tapeline
#
# `make help` lists everything. `make check` is what CI runs, so a green
# local check is a green pipeline.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- build -------------------------------------------------------------------

.PHONY: build
build: build-ingest build-stream build-serving ## Build every tier

.PHONY: build-ingest
build-ingest: ## Build the Go ingestion binary
	cd ingest && go build -o bin/ingestd ./cmd/ingestd

.PHONY: build-stream
build-stream: ## Build the Flink job jar
	cd stream && mvn -B package -DskipTests

.PHONY: build-serving
build-serving: ## Build the serving jar
	cd serving && mvn -B package -DskipTests

# --- test --------------------------------------------------------------------

.PHONY: test
test: test-ingest test-stream test-serving ## Run every test suite

.PHONY: test-ingest
test-ingest: ## Go tests, with the race detector
	cd ingest && go test -race -count=1 ./...

.PHONY: test-stream
test-stream: ## Scala tests
	cd stream && mvn -B test

.PHONY: test-serving
test-serving: ## Java tests
	cd serving && mvn -B test

.PHONY: cover
cover: ## Go coverage report
	cd ingest && go test -coverprofile=coverage.out -coverpkg=./internal/...,./schemas/... ./... >/dev/null \
	  && go tool cover -func=coverage.out | tail -1 \
	  && go tool cover -html=coverage.out -o coverage.html \
	  && echo "wrote ingest/coverage.html"

# --- checks ------------------------------------------------------------------

.PHONY: check
check: fmt-check vet test schema-check ## Everything CI runs

.PHONY: fmt
fmt: ## Format Go sources
	cd ingest && gofmt -w .

.PHONY: fmt-check
fmt-check: ## Fail if anything is unformatted
	@unformatted=$$(cd ingest && gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
	  echo "not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi
	@echo "gofmt clean"

.PHONY: vet
vet: ## go vet
	cd ingest && go vet ./...

.PHONY: schema-check
schema-check: ## Fail if the Avro schema copies have drifted
	@status=0; \
	for s in trade.v1 book_delta.v1 chain_transfer.v1; do \
	  diff -q "ingest/schemas/avro/$$s.avsc" "stream/src/main/resources/avro/$$s.avsc" \
	    || { echo "drift: $$s"; status=1; }; \
	done; \
	for s in quote.v1 window_bar.v1 divergence.v1; do \
	  diff -q "stream/src/main/resources/avro/$$s.avsc" "serving/src/main/resources/avro/$$s.avsc" \
	    || { echo "drift: $$s"; status=1; }; \
	done; \
	[ $$status -eq 0 ] && echo "schemas in sync"; exit $$status

.PHONY: sync-schemas
sync-schemas: ## Copy the canonical schemas over their downstream copies
	cp ingest/schemas/avro/trade.v1.avsc ingest/schemas/avro/book_delta.v1.avsc \
	   ingest/schemas/avro/chain_transfer.v1.avsc stream/src/main/resources/avro/
	cp stream/src/main/resources/avro/quote.v1.avsc \
	   stream/src/main/resources/avro/window_bar.v1.avsc \
	   stream/src/main/resources/avro/divergence.v1.avsc serving/src/main/resources/avro/
	@echo "schemas synced"

# --- local stack -------------------------------------------------------------

.PHONY: up
up: ## Start the local stack
	$(COMPOSE) up -d
	@echo "  Grafana      http://localhost:3000  (anonymous viewer)"
	@echo "  Prometheus   http://localhost:9091"
	@echo "  Flink UI     http://localhost:8082"
	@echo "  REST         http://localhost:8080"
	@echo "  gRPC         localhost:9090"

.PHONY: down
down: ## Stop the local stack
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v
	rm -rf ingest/bin ingest/coverage.* stream/target serving/target

.PHONY: logs
logs: ## Tail every service log
	$(COMPOSE) logs -f

.PHONY: dry-run
dry-run: build-ingest ## Run ingestion against live venues with no broker
	cd ingest && TAPELINE_DRY_RUN=true ./bin/ingestd

# --- jobs --------------------------------------------------------------------

.PHONY: submit-jobs
submit-jobs: build-stream ## Submit the three streaming jobs to the local cluster
	@for job in book trades divergence; do \
	  echo "submitting $$job"; \
	  $(COMPOSE) exec -T jobmanager flink run -d \
	    -c io.tapeline.stream.TapelineJob \
	    /opt/tapeline/tapeline-stream-0.1.0.jar $$job; \
	done

.PHONY: backfill
backfill: ## Replay a range from Iceberg. START/END are epoch microseconds.
	@test -n "$(START)" || { echo "usage: make backfill START=<us> END=<us>"; exit 2; }
	@test -n "$(END)" || { echo "usage: make backfill START=<us> END=<us>"; exit 2; }
	$(COMPOSE) exec -T jobmanager env \
	  TAPELINE_BACKFILL_START_US=$(START) TAPELINE_BACKFILL_END_US=$(END) \
	  flink run -c io.tapeline.stream.TapelineJob \
	  /opt/tapeline/tapeline-stream-0.1.0.jar backfill

# --- verification ------------------------------------------------------------

.PHONY: smoke
smoke: ## End-to-end smoke test against the local stack
	./scripts/smoke-test.sh

.PHONY: load
load: ## k6 load test against the REST tier
	k6 run loadtest/k6/rest-quotes.js

.PHONY: load-grpc
load-grpc: ## k6 load test against the gRPC streaming tier
	k6 run loadtest/k6/grpc-stream.js

.PHONY: chaos
chaos: ## Run every chaos experiment
	./chaos/run-chaos.sh all
