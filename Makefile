.PHONY: install-tools lint test test-race coverage

install-tools:
	go install github.com/matryer/moq
	go install github.com/mgechev/revive

lint:
	go fmt ./...
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

test:
	go test -v -p 1 -count=1 -covermode=count -coverprofile=coverage.out ./...

test-race:
	go test -v -race -count=1 ./...

coverage:
	go tool cover -func coverage.out | grep ^total
