.PHONY: build test run vet fmt lint cover ci help mocks stop up down demo

help:
	@echo "build   - build gateway and mockprovider binaries into bin/"
	@echo "test    - go test -race ./..."
	@echo "vet     - go vet ./..."
	@echo "fmt     - gofmt -l -w ."
	@echo "lint    - golangci-lint run"
	@echo "cover   - coverage profile + summary"
	@echo "ci      - fmt-check, vet, build, test, lint (what CI runs)"
	@echo "mocks   - run three mock providers in the background"
	@echo "run     - run the gateway against config.example.yaml"
	@echo "stop    - kill backgrounded gateway/mockprovider processes"
	@echo "up/down - full stack via docker compose"
	@echo "demo    - ./bench/demo.sh"

build:
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/mockprovider ./cmd/mockprovider

# -race always: router/health stats are concurrent by design
test:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

# fails on non-empty output instead of rewriting files, so it can gate CI
fmt-check:
	@fmt_out="$$(gofmt -l .)"; \
	if [ -n "$$fmt_out" ]; then \
		echo "gofmt needs to be run on:"; echo "$$fmt_out"; exit 1; \
	fi

lint:
	golangci-lint run

cover:
	go test -race -count=1 -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

# mirrors .github/workflows/ci.yml so failures reproduce locally
ci: fmt-check vet build test lint

mocks: build
	./bin/mockprovider -addr :9001 -name mock-a > /tmp/mock-a.log 2>&1 &
	./bin/mockprovider -addr :9002 -name mock-b > /tmp/mock-b.log 2>&1 &
	./bin/mockprovider -addr :9003 -name mock-anthropic > /tmp/mock-anthropic.log 2>&1 &
	@echo "mocks on :9001 :9002 :9003 (logs in /tmp/mock-*.log)"

run:
	go run ./cmd/gateway -config config.example.yaml

stop:
	-pkill -f bin/mockprovider
	-pkill -f bin/gateway

# full stack in containers (Colima or Docker Desktop)
up:
	docker compose -f deploy/compose.yaml up --build -d

down:
	docker compose -f deploy/compose.yaml down

demo:
	./bench/demo.sh
