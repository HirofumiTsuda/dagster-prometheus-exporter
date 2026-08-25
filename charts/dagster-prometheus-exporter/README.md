# dagster-prometheus-exporter

A Helm chart for [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter), a Prometheus exporter for Dagster run metrics.

## Installing

The chart is published as an OCI artifact to GHCR:

```sh
helm install my-dagster-exporter oci://ghcr.io/hirofumitsuda/charts/dagster-prometheus-exporter \
  --version 0.1.5 \
  --set env.DAGSTER_GRAPHQL_ENDPOINT=http://dagster-webserver.dagster.svc.cluster.local/graphql
```

Or install from a local checkout:

```sh
git clone https://github.com/HirofumiTsuda/dagster-prometheus-exporter.git
helm install my-dagster-exporter ./dagster-prometheus-exporter/charts/dagster-prometheus-exporter \
  --set env.DAGSTER_GRAPHQL_ENDPOINT=http://dagster-webserver.dagster.svc.cluster.local/graphql
```

`env.DAGSTER_GRAPHQL_ENDPOINT` has no sane default for a real deployment and should always be set explicitly. See [values.yaml](values.yaml) for every other setting, which mirror the exporter's own environment variables (see the main [README](../../README.md#configuration)).

## Values

| Key | Default | Description |
| --- | --- | --- |
| `replicaCount` | `1` | Number of exporter pods. |
| `image.repository` | `ghcr.io/hirofumitsuda/dagster-prometheus-exporter` | Image to deploy. |
| `image.tag` | `""` (chart's `appVersion`) | Image tag override. |
| `port` | `9101` | Single source of truth for the container port, Service port, and the `PORT` env var. |
| `service.type` | `ClusterIP` | Kubernetes Service type. |
| `env` | `{}` | Environment variables passed to the exporter via a ConfigMap (`envFrom`). Keys match `internal/config/config.go` exactly. |
| `resources` | `{}` | Standard pod resource requests/limits. |
| `podAnnotations` / `podLabels` | `{}` | Extra pod metadata. |
| `serviceMonitor.enabled` | `false` | Create a prometheus-operator `ServiceMonitor`. Requires the CRD to already be installed. |
| `serviceMonitor.interval` | `30s` | Scrape interval for the `ServiceMonitor`. |
| `nameOverride` / `fullnameOverride` | `""` | Override the chart's computed resource name. |

## Notes

- `readinessProbe`/`livenessProbe` are set to `/healthz`, not `/readyz` — `/healthz` doesn't depend on Dagster connectivity, so a Dagster outage doesn't pull the exporter pod out of the Service's endpoints. Doing so would stop Prometheus from scraping it at all, defeating the point of the exporter continuing to serve last-known state during an outage (see the main README's Motivation section).
- A `checksum/config` pod annotation triggers a rollout whenever `env` changes, since `envFrom`-injected variables are otherwise only read once at container start.
