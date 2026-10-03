# dagster-prometheus-exporter

A Helm chart for [dagster-prometheus-exporter](https://github.com/HirofumiTsuda/dagster-prometheus-exporter), a Prometheus exporter for Dagster run metrics.

## Installing

The chart is published as an OCI artifact to GHCR:

```sh
helm install my-dagster-exporter oci://ghcr.io/hirofumitsuda/charts/dagster-prometheus-exporter \
  --version 0.1.6 \
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
| `env` | `{}` | Environment variables passed to the exporter via a ConfigMap (`envFrom`). Keys match `internal/config/config.go` exactly. Don't put `DAGSTER_CLOUD_API_TOKEN` here; use `dagsterCloudApiToken` below. |
| `dagsterCloudApiToken.existingSecret` | `""` | Name of a Secret holding a Dagster+ API token, passed to the exporter as `DAGSTER_CLOUD_API_TOKEN`. Leave empty for OSS Dagster. See [Dagster+](#dagster). |
| `dagsterCloudApiToken.key` | `token` | Key within that Secret. |
| `resources` | `{}` | Standard pod resource requests/limits. |
| `podAnnotations` / `podLabels` | `{}` | Extra pod metadata. |
| `nodeSelector` | `{}` | Node labels the pod must match to be scheduled. |
| `tolerations` | `[]` | Taints the pod tolerates — e.g. for a dedicated monitoring node pool. |
| `affinity` | `{}` | Node/pod affinity and anti-affinity rules. |
| `serviceMonitor.enabled` | `false` | Create a prometheus-operator `ServiceMonitor`. Requires the CRD to already be installed. |
| `serviceMonitor.interval` | `30s` | Scrape interval for the `ServiceMonitor`. |
| `serviceMonitor.additionalLabels` | `{}` | Extra labels on the `ServiceMonitor`, for matching a Prometheus instance's `serviceMonitorSelector`. |
| `alerts.enabled` | `false` | Create a prometheus-operator `PrometheusRule` with a best-practice set of alerts (see below). Requires the CRD to already be installed. |
| `alerts.additionalLabels` | `{}` | Extra labels on the `PrometheusRule` object itself, e.g. for a Prometheus `ruleSelector`. |
| `alerts.rules.<name>` | see `values.yaml` | A complete Prometheus alerting rule (`enabled`/`alert`/`expr`/`for`/`labels`/`annotations`), keyed by name. See below for overriding or adding one. |
| `nameOverride` / `fullnameOverride` | `""` | Override the chart's computed resource name. |

### Dagster+

Dagster+ needs an API token on every request. Create a Secret holding it, then point the chart at it:

```sh
kubectl create secret generic dagster-cloud-api-token --from-literal=token=<your token>

helm install my-dagster-exporter oci://ghcr.io/hirofumitsuda/charts/dagster-prometheus-exporter \
  --set env.DAGSTER_GRAPHQL_ENDPOINT=https://<org>.dagster.cloud/<deployment>/graphql \
  --set dagsterCloudApiToken.existingSecret=dagster-cloud-api-token \
  --set env.DAGSTER_SCRAPING_TIMEOUT_SECONDS=60 \
  --set env.DAGSTER_SCRAPING_INTERVAL_SECONDS=90
```

The token is injected with `secretKeyRef`, so it never lands in the chart's ConfigMap. The chart has no option to create the Secret from a value in `values.yaml`, since that would just move the plain-text token into the values file. Secrets managed by External Secrets, Sealed Secrets, or similar work the same way, as long as the resulting Secret has the configured key. Setting `env.DAGSTER_CLOUD_API_TOKEN` as well is rejected at render time.

See the main README's [Dagster+ section](../../README.md#dagster) for the endpoint format and why the timeout is raised.

### Alerts (`alerts.enabled`)

Off by default. When enabled, ships thirteen alerts covering daemon liveness, job/asset/schedule/sensor health, run-queue and concurrency-pool backlogs, code location load errors, and the exporter's own scrape health — the set proposed in [#112](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/112), plus asset/concurrency-pool/run-queue-backlog alerts added afterward to cover every alertable metric in [docs/metrics.md](../../docs/metrics.md) (two metrics -- `dagster_daemon_healthy` and `dagster_schedule_last_tick_timestamp_seconds` -- are deliberately left out, see their own docs for why no single alert on them makes sense). See [values.yaml](values.yaml) for the exact rules.

One of the thirteen, `assetMaterializationStale`, ships **disabled**: unlike sensors (one fixed tick interval), assets in the same cluster can materialize on wildly different cadences, so there's no single staleness threshold that fits every installation. Set `expr`'s threshold to match your own assets before enabling it.

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
- That checksum doesn't cover the Secret referenced by `dagsterCloudApiToken.existingSecret`. After rotating the token, restart the pod (`kubectl rollout restart deployment/<name>`) or use a tool like Reloader.
