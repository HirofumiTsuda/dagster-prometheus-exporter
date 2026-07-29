FROM golang:1.26-alpine AS builder

ENV CGO_ENABLED=0

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN go build -ldflags="-w -s" -o /app/exporter ./cmd/exporter

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

WORKDIR /app

COPY --from=builder /app/exporter .

EXPOSE 9101

ENTRYPOINT ["/app/exporter"]