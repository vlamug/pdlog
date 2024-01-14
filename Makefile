.PHONY: protoc
protoc:
	protoc api/v1/*.proto --go_out=. --go_opt=paths=source_relative --proto_path=.

.PHONY: test
test:
	go test -race ./...
