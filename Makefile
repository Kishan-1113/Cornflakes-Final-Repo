.PHONY: all build test drill restore up down

all: build

build:
	go build -v .

test:
	go test -v ./...

up:
	docker-compose up -d --build

down:
	docker-compose down -v

# Runs disaster recovery drills (verification, PITR, failover)
drill:
	go run . -drill

# Point-in-time restore to AT timestamp: make restore AT="2026-10-29 12:00:00"
restore:
	@if [ -z "$(AT)" ]; then echo "Error: AT timestamp is required. Usage: make restore AT=\"2026-10-29 12:00:00\""; exit 1; fi
	@echo "Restoring to timestamp: $(AT)..."
	go run . -restore "$(AT)"
