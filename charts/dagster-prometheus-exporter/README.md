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
| `alerts.enabled` | `false` | Create a prometheus-operator `PrometheusRule` with a best-practice set of alerts (see below). Requires the CRD to already be installed. |
| `alerts.additionalLabels` | `{}` | Extra labels on the `PrometheusRule` object itself, e.g. for a Prometheus `ruleSelector`. |
| `alerts.rules.<name>` | see `values.yaml` | A complete Prometheus alerting rule (`enabled`/`alert`/`expr`/`for`/`labels`/`annotations`), keyed by name. See below for overriding or adding one. |
| `nameOverride` / `fullnameOverride` | `""` | Override the chart's computed resource name. |

### Alerts (`alerts.enabled`)

Off by default. When enabled, ships eight alerts covering daemon liveness, job/schedule/sensor health, queue backlogs, code location load errors, and the exporter's own scrape health — the set proposed in [#112](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/112). See [values.yaml](values.yaml) for the exact rules and [docs/metrics.md](../../docs/metrics.md) for the reasoning behind each threshold.

Each entry under `alerts.rules` is a complete rule (`alert`/`expr`/`for`/`labels`/`annotations`), so a values file only needs to set the fields it's changing — Helm deep-merges maps, and the rest of a built-in rule's defaults pass through untouched. That covers disabling one, tightening or loosening a threshold, or replacing `expr` entirely (e.g. to exclude a specific job/sensor by label). Adding an alert this chart doesn't know about is the same operation: give it a new key with the full rule spec:

```yaml
alerts:
  enabled: true
  rules:
    runStuckInQueue:
      enabled: false # too noisy for our queue depth, handled elsewhere
    sensorTickStale:
      # exclude one hourly sensor from the default 5-minute staleness check
      expr: time() - dagster_sensor_last_tick_timestamp_seconds{sensor_name!="my_hourly_sensor"} > 300
    myJobNeverRan:
      enabled: true
      alert: MyJobNeverRan
      expr: absent(dagster_last_run_info{job_name="my_job"})
      for: 1h
      labels:
        severity: warning
      annotations:
        summary: my_job has never reported a run
```

## Notes

- `readinessProbe`/`livenessProbe` are set to `/healthz`, not `/readyz` — `/healthz` doesn't depend on Dagster connectivity, so a Dagster outage doesn't pull the exporter pod out of the Service's endpoints. Doing so would stop Prometheus from scraping it at all, defeating the point of the exporter continuing to serve last-known state during an outage (see the main README's Motivation section).
- A `checksum/config` pod annotation triggers a rollout whenever `env` changes, since `envFrom`-injected variables are otherwise only read once at container start.
