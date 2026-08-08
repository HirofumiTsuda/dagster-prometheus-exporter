FROM golang:1.26-alpine AS builder

ENV CGO_ENABLED=0

# Surfaced as dagster_exporter_build_info (see internal/version). Left at
# their defaults for a plain `docker build` (e.g. local dev via
# docker-compose); docker-publish.yml passes real values on release builds.
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN go build -ldflags="-w -s \
    -X github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/version.Version=${VERSION} \
    -X github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/version.Commit=${COMMIT}" \
    -o /app/exporter ./cmd/exporter

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=builder /app/exporter .

EXPOSE 9101

ENTRYPOINT ["/app/exporter"]