# Contributing

## Reporting bugs / requesting features

Open a [GitHub issue](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/new/choose) using the appropriate template.

## Submitting a pull request

1. Fork the repo and create a branch from `main`.
2. Make your change.
3. Run the checks below locally and make sure they pass.
4. Open a pull request against `main` and fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md) (`What` / `Why` / `QA` / `Ref`). Link any related issue in the `Ref` section (e.g. `Closes #26`).

CI (`.github/workflows/ci.yml`) runs the same checks on every push and pull request, plus CodeQL static analysis.

## Running checks locally

```sh
go build ./...
go vet ./...
golangci-lint run ./...
go test ./...
```

## Local development stack

`docker compose up --build` brings up a full stack — a `dagster dev` instance with sample jobs, this exporter, Prometheus, and Grafana — so you can test changes against a real Dagster instance. See [Quick Start](README.md#quick-start) in the README for details.
