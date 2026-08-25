# Security Policy

## Reporting a vulnerability

Please report it privately via [GitHub Security Advisories](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/security/advisories/new) rather than a public issue. There's no dedicated security email — this is a solo-maintained project, and the private advisory flow already routes reports only to the maintainer.

Include what you'd include in a normal bug report — steps to reproduce, affected version/commit, and what the actual impact is — plus anything specific to why it's a security issue rather than a correctness one.

## Response

Best-effort, not an SLA. This is maintained by one person outside of paid work, not a company with a security team. If you don't hear back within a reasonable time, a follow-up comment on the advisory is welcome.

## Supported versions

Only the latest released version (`v*` tag / `latest` Docker/Helm tag) is supported. There's no LTS or backport policy — fixes land on `main` and go out in the next release.

## Scope

This exporter reads from Dagster's GraphQL API (`DAGSTER_GRAPHQL_ENDPOINT`) and exposes a `/metrics` endpoint; it doesn't hold credentials beyond that endpoint URL and doesn't write to Dagster. Reports involving how it's deployed (e.g. the Helm chart's default RBAC/network exposure) are in scope alongside the Go binary itself.
