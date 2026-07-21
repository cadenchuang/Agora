.PHONY: proto proto-protoc tidy build worker coordinator test up down clean

# Preferred: generate stubs with buf (no local plugin install needed).
proto:
	buf generate

# Fallback: generate stubs with protoc + local Go plugins.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
proto-protoc:
	protoc \
		--go_out=. --go_opt=module=github.com/cadenchuang/agora \
		--go-grpc_out=. --go-grpc_opt=module=github.com/cadenchuang/agora \
		proto/agora.proto

tidy:
	go mod tidy

build: worker coordinator

worker:
	go build -o bin/agora-worker ./cmd/worker

coordinator:
	go build -o bin/agora-coordinator ./cmd/coordinator

test:
	go test ./...

up:
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

clean:
	rm -rf bin
