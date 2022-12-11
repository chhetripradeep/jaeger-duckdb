GO_OS ?= $(shell go env GOOS)
GO_ARCH ?= $(shell go env GOARCH)
GO_BUILD ?= CGO_ENABLED=1 go build -trimpath

JAEGER_VERSION ?= 1.40.0

.PHONY: build
build:
	${GO_BUILD} -o jaeger-duckdb-$(GO_OS)-$(GO_ARCH) ./cmd/jaeger-duckdb/main.go

.PHONY: build-linux-amd64
build-linux-amd64:
	GOOS=linux GOARCH=amd64 $(MAKE) build

.PHONY: build-linux-arm64
build-linux-arm64:
	GOOS=linux GOARCH=arm64 $(MAKE) build

.PHONY: build-darwin-amd64
build-darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(MAKE) build

.PHONY: build-darwin-arm64
build-darwin-arm64:
	GOOS=darwin GOARCH=arm64 $(MAKE) build

.PHONY: build-all-platforms
build-all-platforms: build-linux-amd64 build-linux-arm64 build-darwin-amd64 build-darwin-arm64

.PHONY: fmt
fmt: install-tools
	go fmt ./...
	goimports -w -local github.com/jaegertracing/jaeger-clickhouse ./

.PHONY: install-tools
install-tools:
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: lint
lint: install-tools
	golangci-lint -v run --allow-parallel-runners ./...

.PHONY: run
run:
	docker run \
		--rm \
		--name jaeger \
		--env JAEGER_DISABLED=false \
		--interactive \
		--tty \
		--user ${shell id -u} \
		--publish 16686:16686 \
		--publish 14250:14250 \
		--publish 14268:14268 \
		--publish 6831:6831/udp \
		--volume "${PWD}:/data" \
		--env SPAN_STORAGE_TYPE=grpc-plugin \
		jaegertracing/all-in-one:${JAEGER_VERSION} \
		--query.ui-config=/data/jaeger-ui.json \
		--grpc-storage-plugin.binary=/data/jaeger-duckdb-$(GO_OS)-$(GO_ARCH) \
		--grpc-storage-plugin.configuration-file=/data/config.yaml \
		--grpc-storage-plugin.log-level=debug

.PHONY: run-hotrod
run-hotrod:
	docker run \
		--rm \
		--link jaeger \
		--env JAEGER_AGENT_HOST=jaeger \
		--env JAEGER_AGENT_PORT=6831 \
		--publish 8080:8080 \
		jaegertracing/example-hotrod:${JAEGER_VERSION} all

.PHONY: test
test:
	go test ./...
