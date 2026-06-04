.PHONY: fmt test bench clean

fmt:
	go fmt ./...

lint:
	golangci-lint run ./...

fix:
	go fix ./...

test:
	go test -race ./... -v -short

bench:
	go test -bench=. ./...

