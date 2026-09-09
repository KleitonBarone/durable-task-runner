DATABASE_URL ?= postgres://durable:durable@localhost:54329/durable?sslmode=disable

.PHONY: deps-up deps-down fmt fmt-check lint test integration check example

deps-up:
	docker compose up -d --wait

deps-down:
	docker compose down

fmt:
	gofmt -w .

fmt-check:
	test -z "$$(gofmt -l .)"

lint:
	go vet ./...
	go vet -tags=integration ./...

test:
	go test ./...

integration:
	DURABLE_TEST_DATABASE_URL="$(DATABASE_URL)" go test -tags=integration ./...

check: fmt-check lint test integration

example:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/example
