---
name: Bug report
about: Report a problem with the exporter
title: ""
labels: bug
---

## Description

What happened, and what did you expect instead?

## Steps to reproduce

## Environment

- Exporter version / image tag:
- Dagster version:
- Deployment (Docker / Kubernetes / binary):
- Helm chart version, if installed via Helm:

The exporter reports its own version and commit, which is more precise than the
image tag if you are running `latest`:

```sh
curl -s localhost:9101/metrics | grep dagster_exporter_build_info
```

## Logs / metrics output

Relevant log output, or the `/metrics` output for the affected series.

If a metric is missing or empty rather than wrong, the exporter's self-health
series are usually the fastest way to narrow it down:

```sh
curl -s localhost:9101/metrics | grep dagster_exporter_
```
