.PHONY: build test up down restart logs load-test clean

# Build local binaries
build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

# Run unit tests with race detection
test:
	go test -v -race ./...

# Start full multi-container production stack
up:
	docker compose -f deployments/docker-compose.yml up -d --build

# Stop the stack
down:
	docker compose -f deployments/docker-compose.yml down -v

# View live aggregate logs
logs:
	docker compose -f deployments/docker-compose.yml logs -f

# Run k6 load testing against the load balancer
load-test:
	k6 run scripts/load_test.js

# Clean build artifacts
clean:
	rm -rf bin/ coverage.out
