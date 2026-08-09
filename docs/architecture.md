# Architecture deep dive

Implementation details and design rationale behind the [Architecture overview](../README.md#architecture) in the main README.

## Package layout

The exporter is a single Go binary with no external state store — everything it reports is held in memory and rebuilt on each scrape:

- `cmd/exporter` — entrypoint; loads config and starts the server.
- `internal/config` — reads settings from environment variables.
- `internal/server` — runs the HTTP server (`/metrics`, `/healthz`, `/readyz`) and a background ticker that triggers a scrape on `DAGSTER_SCRAPING_INTERVAL_SECONDS`.
- `internal/collector` — on each scrape, queries Dagster's GraphQL API (active runs, completed runs, the definitions roster, and each code location's load status) and updates the in-memory state behind a mutex; `DagsterCollector` implements `prometheus.Collector` and renders that state into metrics whenever Prometheus scrapes `/metrics`.

Because scraping (writing state) and metrics rendering (reading state) are decoupled, a slow or failing Dagster GraphQL call never blocks or breaks a `/metrics` request — it just serves the last known state.

## Why four collectors, and why they're split the way they are

The four collectors (definitions roster, active runs, completed runs, code-location load status) run concurrently on every scrape — each locks `DagsterCollector`'s own mutex only around its own critical section, so they don't block each other or `/metrics`.

The code-location load status collector is intentionally independent of the definitions-roster one: `repositoriesOrError` (used for the roster) silently omits a code location that fails to load rather than erroring out, so a separate `workspaceOrError` query is needed to detect that failure at all — see `dagster_code_location_load_error` in the [metrics reference](metrics.md).

`dagster_run_queue_concurrency_key_backlog` is folded into the active-runs collector instead of getting its own: it only needs `QUEUED` runs' tags, which that collector already fetches on every page, so a separate query would just re-fetch the same runs a second time for no reason.

## The definitions-roster collector

Named for what it actually fetches: `repositoriesOrError`, which exposes jobs, schedules, and sensors as independent sibling fields on Dagster's `Repository` type (not reachable only via jobs), so all three come back from one query.

It builds the known-jobs set used to prune/seed completed-run counters and last-run status (unchanged since before schedules/sensors existed), plus each schedule's and sensor's enabled/disabled state and most recent tick (`dagster_schedule_status`/`dagster_schedule_last_tick_status`, `dagster_sensor_status`/`dagster_sensor_last_tick_status`). Dagster's `Schedule.scheduleState` and `Sensor.sensorState` are both the same `InstigationState` type under the hood, so the two pairs of metrics are structurally identical.

## Incremental completed-run fetching

Fetching completed runs is incremental, not a full re-scan every cycle: after the first scrape (which backfills `LOOKBACK_WINDOW_MINUTES`), each subsequent scrape only asks Dagster for runs updated since the last-seen watermark (minus a small safety margin, to tolerate a run's DB write committing slightly after its `updateTime`).

Any single fetch — the initial backfill or an unusually large batch of updates — pages through `runsOrError` via cursor (`RUNS_PAGE_SIZE` per page) and folds each page into the in-memory counters as it arrives, rather than buffering the full result set in memory first.
