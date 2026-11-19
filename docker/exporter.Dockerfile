FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder

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

FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

RUN apk --no-cache add ca-certificates tzdata

# Numeric UID/GID so Kubernetes can enforce runAsNonRoot. A named USER
# (appuser) is rejected by the kubelet when runAsNonRoot is set, because it
# cannot resolve the name against the image passwd without running the
# container. 65532 matches the distroless nonroot uid.
RUN addgroup -S -g 65532 appgroup && adduser -S -u 65532 -G appgroup appuser
USER 65532:65532

WORKDIR /app

COPY --from=builder /app/exporter .

EXPOSE 9101

ENTRYPOINT ["/app/exporter"]
