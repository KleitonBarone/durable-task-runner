.PHONY: deps-up deps-down fmt fmt-check lint test integration e2e check race example

deps-up:
	go run ./cmd/dev up

deps-down:
	go run ./cmd/dev down

fmt:
	go run ./cmd/dev fmt

fmt-check:
	go run ./cmd/dev fmt-check

lint:
	go run ./cmd/dev lint

test:
	go run ./cmd/dev test

integration:
	go run ./cmd/dev integration

e2e:
	go run ./cmd/dev e2e

check:
	go run ./cmd/dev check

race:
	go run ./cmd/dev race

example:
	go run ./cmd/dev example
