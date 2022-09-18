GO_OS ?= $(shell go env GOOS)
GO_ARCH ?= $(shell go env GOARCH)
GO_BUILD ?= CGO_LDFLAGS="-L/Users/pradeep/gh/duckdb/build/release/src" CGO_CFLAGS="-I/Users/pradeep/gh/duckdb/src/include" DYLD_LIBRARY_PATH="/Users/pradeep/gh/duckdb/build/release/src" go build -trimpath

JAEGER_VERSION ?= 1.38.0

.PHONY: build
build:
	${GO_BUILD} -o jaeger-duckdb-$(GO_OS)-$(GO_ARCH) ./cmd/jaeger-duckdb/main.go

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
