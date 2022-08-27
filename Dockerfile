FROM docker.io/library/alpine:3.16

ADD jaeger-duckdb-linux-amd64 /go/bin/jaeger-duckdb

RUN mkdir /plugin

CMD ["cp", "/go/bin/jaeger-duckdb", "/plugin/jaeger-duckdb"]
