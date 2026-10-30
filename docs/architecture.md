# Architecture deep dive

Implementation details and design rationale behind the [Architecture overview](../README.md#architecture) in the main README.

## Package layout

The exporter is a single Go binary with no external state store — everything it reports is held in memory and rebuilt on each scrape:

- `cmd/exporter` — entrypoint; loads config and starts the server.
- `internal/config` — reads settings from environment variables.
- `internal/server` — runs the HTTP server (`/metrics`, `/healthz`, `/readyz`) and one background loop per collector, each on its own schedule: `DAGSTER_SCRAPING_INTERVAL_SECONDS` / `DAGSTER_SCRAPING_TIMEOUT_SECONDS` unless the collector has a `<COLLECTOR>_SCRAPING_*` override.
- `internal/collector` — on each scrape, queries Dagster's GraphQL API (active runs, completed runs, the definitions roster, each code location's load status, daemon health, asset status, and op/step pool concurrency) and updates the in-memory state behind a mutex; `DagsterCollector` implements `prometheus.Collector` and renders that state into metrics whenever Prometheus scrapes `/metrics`.

Because scraping (writing state) and metrics rendering (reading state) are decoupled, a slow or failing Dagster GraphQL call never blocks or breaks a `/metrics` request — it just serves the last known state.

## Why seven collectors, and why they're split the way they are

The seven collectors (definitions roster, active runs, completed runs, code-location load status, daemon health, asset status, op pool concurrency) run concurrently on every scrape — each locks `DagsterCollector`'s own mutex only around its own critical section, so they don't block each other or `/metrics`.

The code-location load status collector is intentionally independent of the definitions-roster one: `repositoriesOrError` (used for the roster) silently omits a code location that fails to load rather than erroring out, so a separate `workspaceOrError` query is needed to detect that failure at all — see `dagster_code_location_load_error` in the [metrics reference](metrics.md).

The daemon-health collector is likewise independent: `instance.daemonHealth` is a top-level field, not something reachable from `repositoriesOrError`, so it needs its own query. It is also the only collector that reports on Dagster's *machinery* rather than its work — everything else here describes runs, schedules, sensors and code locations, all of which the webserver keeps answering perfectly while the daemon is down.

The asset-status collector is a third independent one, and the only one that makes two GraphQL round trips instead of one: `assetNodes` (every asset defined, across every code location, no `repositorySelector` needed) has to run first, because `assetsLatestInfo` needs the asset keys it returns as its `assetKeys` argument. Neither field hangs off `repositoriesOrError`. See `dagster_asset_last_materialization_status` in the [metrics reference](metrics.md) for why staleness alone (`assetNodes.staleStatus`) can't tell a failed run apart from one that never happened.

`dagster_run_queue_concurrency_key_backlog` is folded into the active-runs collector instead of getting its own: it only needs `QUEUED` runs' tags, which that collector already fetches on every page, so a separate query would just re-fetch the same runs a second time for no reason.

The op-pool-concurrency collector is the opposite case: it can't be folded into anything else, because `instance.concurrencyLimits` is a top-level field with no relationship to runs, jobs, or repositories at all — it's Dagster's own event-log storage answering "how many slots are claimed against this pool right now," independent of which run or job a step belongs to. It also needs no zero-fill logic of its own (contrast with the active-runs collector's `dagster_run_queue_concurrency_key_backlog` handling, and see [#81](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/81)): Dagster's storage already remembers every pool that has ever claimed a slot and keeps reporting it at zero once idle, so the collector just replaces its whole map on every scrape instead of accumulating one.

## The definitions-roster collector

Named for what it actually fetches: `repositoriesOrError`, which exposes jobs, schedules, and sensors as independent sibling fields on Dagster's `Repository` type (not reachable only via jobs), so all three come back from one query.

It builds the known-jobs set used to prune/seed completed-run counters and last-run status (unchanged since before schedules/sensors existed), plus each schedule's and sensor's enabled/disabled state and most recent tick (`dagster_schedule_status`/`dagster_schedule_last_tick_status`, `dagster_sensor_status`/`dagster_sensor_last_tick_status`). Dagster's `Schedule.scheduleState` and `Sensor.sensorState` are both the same `InstigationState` type under the hood, so the two pairs of metrics are structurally identical. It also reads each job's definition tags, for `dagster_job_tags`, which is held back by the same pruning rule as the other per-job series (below) rather than replaced outright.

Pruning doesn't act on a job's first absence from that set. Because `repositoriesOrError` omits a code location that fails to load, "this job was deleted" and "this job's code location is broken right now" look the same in the roster, and pruning the second case resets `dagster_completed_runs_total` to 0 (a counter reset, so `increase()` over the outage overcounts) and drops `dagster_last_run_info` for exactly the jobs that just broke. So a job is held back while `dagster_code_location_load_error` is 1 for its location, and otherwise until it has been missing from 3 consecutive rosters (`jobAbsenceGraceScrapes`): it survives the first two and is pruned by the third, i.e. roughly 2 × `DAGSTER_SCRAPING_INTERVAL_SECONDS` after it disappears (~30s at the default 15s). The grace period covers the window where the location is already broken but the load error hasn't landed yet, since the two collectors run concurrently — a one-roster lag, so it's counted in rosters rather than wall-clock time and scales with the scrape interval on its own. Only successfully fetched rosters count (a failed fetch changes nothing), and the count is paused, not reset, while the location's load error is set, so a job that's still missing once the location loads again is pruned when the remaining count runs out.

## Incremental completed-run fetching

Fetching completed runs is incremental, not a full re-scan every cycle: after the first scrape (which backfills `LOOKBACK_WINDOW_MINUTES`), each subsequent scrape only asks Dagster for runs updated since the last-seen watermark (minus a small safety margin, to tolerate a run's DB write committing slightly after its `updateTime`).

Any single fetch — the initial backfill or an unusually large batch of updates — pages through `runsOrError` via cursor (`RUNS_PAGE_SIZE` per page) and folds each page into the in-memory counters as it arrives, rather than buffering the full result set in memory first.

## No retries within a scrape

A failed GraphQL call is not retried inside the scrape. The collector returns the error, `dagster_exporter_last_scrape_success` reads `0` for that collector and `dagster_exporter_scrape_errors_total` goes up, and the next attempt comes one interval later (15s by default). `/metrics` keeps serving the last known state in the meantime.

This is deliberate, because waiting costs nothing. The completed-runs fetch only moves its watermark after every page has been folded in, so after a failure the next scrape asks again from the same point, and runs that were already counted are skipped by the dedup cache. A retry inside the scrape would only bring the next attempt forward by a few seconds, and it would spend part of `DAGSTER_SCRAPING_TIMEOUT_SECONDS` asking a Dagster that has just failed to answer. What a single connection reset costs is one stalled interval, shown as `dagster_exporter_last_scrape_success` at `0` until the next scrape succeeds.
