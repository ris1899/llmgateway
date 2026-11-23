.PHONY: build test run vet fmt mocks stop up down demo bench

build:
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/mockprovider ./cmd/mockprovider

# -race on every run: the router and health stats are concurrent by design, and
# data races there would corrupt routing decisions silently.
test:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

# Three mock providers in the background (two OpenAI-format, one Anthropic-format).
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

# Full stack in containers (Colima or Docker Desktop).
up:
	docker compose -f deploy/compose.yaml up --build -d

down:
	docker compose -f deploy/compose.yaml down

demo:
	./bench/demo.sh

bench:
	./bench/run-baseline.sh
