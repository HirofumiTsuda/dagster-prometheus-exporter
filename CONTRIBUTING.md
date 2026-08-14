# Contributing

## Reporting bugs / requesting features

Open a [GitHub issue](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/new/choose) using the appropriate template.

## Submitting a pull request

1. Fork the repo and create a branch from `main`.
2. Make your change.
3. Run the checks below locally and make sure they pass.
4. Open a pull request against `main` and fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md) (`What` / `Why` / `QA` / `Ref`). Link any related issue in the `Ref` section (e.g. `Closes #26`).

CI (`.github/workflows/ci.yml`) runs the same checks on every push and pull request, plus CodeQL static analysis.

## Setting up your toolchain

Go comes from `go.mod` — any Go 1.21+ will fetch the toolchain it names automatically.

The other tools are pinned in [`.tool-versions`](.tool-versions) so that a local run matches CI. With [mise](https://mise.jdx.dev/) installed:

```sh
mise install
```

Installing them another way works too, as long as the versions match `.tool-versions`. They differ enough to matter: golangci-lint v1 and v2 enable different linters by default, and Helm 3 and 4 are separate major versions, so running a different one means checking something CI doesn't.

## Running checks locally

```sh
go build ./...
go vet ./...
golangci-lint run ./...
go test -race ./...
```

For chart changes:

```sh
helm lint --strict charts/dagster-prometheus-exporter
helm template charts/dagster-prometheus-exporter
```

## Local development stack

`docker compose up --build` brings up a full stack — a `dagster dev` instance with sample jobs, this exporter, Prometheus, and Grafana — so you can test changes against a real Dagster instance. See [Quick Start](README.md#quick-start) in the README for details.
