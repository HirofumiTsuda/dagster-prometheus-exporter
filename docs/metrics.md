# Metrics reference

Design rationale, edge cases, and blind spots for each metric. For the short version, see the [Metrics section](../README.md#metrics) in the main README.

The per-job metrics are labeled with `job_name` and `location` (the Dagster code location the job belongs to), so that jobs with the same name in different code locations don't collide. `dagster_code_location_load_error` is location-scoped rather than job-scoped, since it reports on a code location as a whole.

A run records its code location as a launch-time snapshot (`repositoryOrigin`), and that field can be absent — for runs launched outside a code location, or old enough to predate it. Those runs are reported as `location="__unknown__"`. The name is deliberately not something a real code location would be called: the placeholder has to stay distinguishable from a location that genuinely exists, both so the two aren't summed into one series and because the exporter prunes series for locations Dagster no longer reports, which a collision would defeat. (Before v0.3.0 this placeholder was `unknown`, which a real code location could have been named.)

## `dagster_active_runs` (Gauge)

Labels: `job_name`, `location`, `status`

Number of currently active runs (`queued`, `starting`, `started`) per job. Jobs with no active runs are reported as `0` rather than omitted.

Each scrape re-queries Dagster for whatever is active *at that instant* (unlike the completed-runs collector, this isn't incremental) — a run that passes through `queued`/`starting`/`started` and finishes entirely between two scrapes is never observed in any active status at all. Shortening the scrape interval narrows this blind spot but can't close it.

## `dagster_active_run_duration_seconds` (Gauge)

Labels: `job_name`, `location`, `status`

Elapsed time (`now - updateTime`) of the longest-waiting/longest-running active run per job and status — the max across that group, not a sum or average.

Dagster only bumps a run's `updateTime` on a run-level status transition (not on step-level events like op start/success), so it marks exactly when the run entered its *current* status — e.g. for `started` this is time-in-execution, not time-since-queued. `run_id` isn't a label (it would grow unbounded), so this is the closest available signal for "how long has the oldest run in this group been stuck here," useful for spotting queue backlogs.

`0` when there are no active runs in that group, same as `dagster_active_runs` — and it has the same scrape-interval blind spot described above.

## `dagster_completed_runs_total` (Counter)

Labels: `job_name`, `location`, `status`

Total number of completed runs (`success`, `failure`, `canceled`) per job, since the exporter started. Jobs that have never run are seeded at `0`. Series for jobs that no longer exist in Dagster are deleted automatically — not on the first scrape a job is missing, but on the third consecutive one (roughly 2 × `DAGSTER_SCRAPING_INTERVAL_SECONDS` later), and never while its code location has `dagster_code_location_load_error` set to `1`. A code location that fails to load drops out of Dagster's job list, and deleting its counters then would reset them to `0` on recovery, which Prometheus reads as a counter reset. See [architecture.md](architecture.md#the-definitions-roster-collector).

Every attempt counts, including a failed one that Dagster retries (see `dagster_last_run_info` below), so `status="failure"` is the right input for a failure rate but not for "the job failed".

## `dagster_last_run_info` (Gauge)

Labels: `job_name`, `location`, `status`

Always `1`; an "info" metric (same pattern as `kube_pod_info`) reporting the status of the most recently completed run per job. Kept until a newer completion supersedes it or the job is removed from Dagster (with the same grace period as `dagster_completed_runs_total`, so a broken code location doesn't silence "last run failed" alerts) — it does not disappear just because nothing has completed recently. It also survives a restart of the exporter: each job's latest runs are read back from Dagster once, when the exporter first sees the job (see [architecture.md](architecture.md#incremental-completed-run-fetching)). Use the `status` label (`success`, `failure`, or `canceled`) to tell the outcomes apart, e.g. in a Grafana table panel.

With run retries enabled (`run_retries` in `dagster.yaml`), a failed attempt that Dagster will retry is not the job's outcome: the retry's result is. Dagster marks it with the `dagster/will_retry=true` tag, and such a run doesn't replace the last run here, so a `status="failure"` alert fires once the last attempt has failed rather than on an attempt that a retry then fixes. Dagster writes that tag just after the run's failure, so a scrape can briefly report the attempt as the last run; when the tag lands the run comes back on the next scrape and the outcome it replaced is restored. Without run retries Dagster sets no tag and every failure counts, as before.

## `dagster_last_run_duration_seconds` (Gauge)

Labels: `job_name`, `location`, `status`

Duration (`endTime - creationTime`) of the most recently completed run per job. Tracks the same run as `dagster_last_run_info` (same lifetime, same `status` label), so a job that has never completed a run has no series for either — there's no seeded `0`. For a run that Dagster retried, that is the duration of the attempt that settled the outcome, not of all attempts together.

## `dagster_last_run_end_timestamp_seconds` (Gauge)

Labels: `job_name`, `location`, `status`

Unix timestamp (`endTime`) of the most recently completed run per job: the same run as `dagster_last_run_info` (same lifetime, same `status` label). Exported as a timestamp rather than an age, like the tick and heartbeat timestamps, so the age is computed at query time.

It puts a time on the last run's outcome, which `dagster_last_run_info` alone can't. A failure alert can then be limited to recent failures, so a job that failed and was never run again stops alerting on its own instead of firing until someone notices:

```promql
dagster_last_run_info{status="failure"} == 1
and on (job_name, location)
time() - dagster_last_run_end_timestamp_seconds < 24 * 3600
```

Absent for a run that Dagster recorded no end time for, since `0` would read as 1970.

## `dagster_last_success_timestamp_seconds` (Gauge)

Labels: `job_name`, `location`

Unix timestamp (`endTime`) of the most recent successful run per job, whatever ran after it. It is the usual Prometheus signal for batch work: alert when a job hasn't succeeded within its expected cadence.

```promql
time() - dagster_last_success_timestamp_seconds > 26 * 3600
```

That one rule covers failures, runs that never start or hang, and a failed attempt marked `dagster/will_retry=true` whose retry is never launched (which `dagster_last_run_info` doesn't show, see above). It can't fire for a job that has never succeeded, because that job has no series here; if its runs fail, the `dagster_last_run_info{status="failure"}` alert is the one that fires. Jobs run at different cadences, so a single threshold rarely fits all of them; a cadence tag exported through `JOB_TAG_LABELS` lets the threshold differ per group of jobs.

Like `dagster_last_run_info`, it is read back from Dagster when the exporter first sees a job, so a restart doesn't lose it, and a job has no series only if it has never succeeded. Removed along with the job, with the same grace period as `dagster_completed_runs_total`.

## `dagster_job_automation_running` (Gauge)

Labels: `job_name`, `location`

`1` if a schedule or sensor that is turned on launches the job, `0` if none does, one series per job. A job that is only launched some other way (by hand, through the API, by another job) reads `0`.

It tells a job nobody runs any more apart from one that is broken. Scoping an alert to automated jobs makes it stop when someone deliberately switches the schedule or sensor off, instead of firing forever, while a job that is still scheduled keeps alerting until it is fixed:

```promql
(time() - dagster_last_success_timestamp_seconds > 26 * 3600)
and on (job_name, location) dagster_job_automation_running == 1
```

The links come from each schedule's job and each sensor's targets in the definitions roster, so a sensor's targets are only the jobs it declares. A sensor that launches runs of jobs it doesn't declare as targets doesn't mark them. Like `dagster_job_tags`, a job that drops out of the roster keeps its last value while its code location is failing to load, so the broken deploy doesn't drop it out of alerts scoped on this metric.

Asset runs started by declarative automation (an asset with an `automation_condition`) all run in one shared job, `__ASSET_JOB`. Dagster's `default_automation_condition_sensor` targets it, so `__ASSET_JOB` reads `1` while that sensor, or another sensor that targets it, is turned on, and `0` otherwise. On open-source Dagster the sensor is off until you turn it on. With it off, a failed `__ASSET_JOB` run still shows in `dagster_last_run_info`, but an alert scoped on this metric leaves it out, and the chart's `jobLastRunFailed` stops alerting on it 24 hours after the failure. Because every asset run shares that one job, these job metrics cannot say which asset failed; for asset runs use `dagster_asset_last_materialization_status` and the asset alerts, which do not depend on the sensor.

## `dagster_job_tags` (Gauge)

Labels: `job_name`, `location`, plus one `tag_<key>` per key in `JOB_TAG_LABELS`

Always `1`; one series per job, with the tags `JOB_TAG_LABELS` selects as labels (the same pattern as kube-state-metrics' `kube_pod_labels`). There are no series at all while `JOB_TAG_LABELS` is unset.

The other per-job metrics don't carry tags: a job can have any number of them, and they would be repeated on every series of every metric. They live on this one metric instead, and you filter or group by a tag with a join. For example, an alert on long runs of the jobs one team owns:

```promql
dagster_active_run_duration_seconds{status="started"}
* on (job_name, location) group_left (tag_owner)
dagster_job_tags{tag_owner="data-team"}
> 4 * 3600
```

The tags come from the job definition (`tags=` on `@job` or `define_asset_job`; `run_tags=` are not included), read from the definitions roster, not from individual runs, so a run launched with extra tags doesn't change them. A job without one of the selected tags reports it as an empty label, which PromQL treats the same as a missing one. Dagster's API never returns hidden tags (prefixed `.dagster/`), so those can't be selected.

Each label name is `tag_` plus the key, with any character a label name can't contain replaced by `_`: `owner` becomes `tag_owner`, `dagster/priority` becomes `tag_dagster_priority`. A repeated key is ignored. Two different keys that come out as the same label (`team-name` and `team_name`) are rejected at startup instead of one shadowing the other.

A job missing from the roster keeps its series for the same grace period as `dagster_completed_runs_total`, and indefinitely while its code location has `dagster_code_location_load_error` set to `1`, so a broken deploy doesn't drop its jobs out of alerts joined on this metric.

## `dagster_code_location_load_error` (Gauge)

Labels: `location`

`1` if that code location most recently failed to load (e.g. a broken import in user code), `0` if it loaded successfully.

A code location can fail to load independently of any job/run activity — `dagster_active_runs`/`dagster_completed_runs_total` alone can't distinguish "this location has zero jobs" from "this location is broken," so this metric exists to surface that failure mode explicitly. The load-error message and stack trace are logged, not attached as a label, to avoid unbounded label cardinality. They are logged once when a location starts failing (or its error message changes), not on every scrape, and a single line is logged when it loads again.

## `dagster_daemon_healthy` (Gauge)

Labels: `daemon_type`, `required`

`1` if the daemon is currently healthy, `0` otherwise. `daemon_type` is one of Dagster's daemons — on 1.13.15: `SCHEDULER`, `SENSOR`, `BACKFILL`, `QUEUED_RUN_COORDINATOR`, `ASSET`, `FRESHNESS_DAEMON`. Older versions report fewer: 1.10 and 1.11 have no `FRESHNESS_DAEMON`, and 1.9 also has no `QUEUED_RUN_COORDINATOR`. This list isn't hardcoded here — the collector reports whatever `instance.daemonHealth.allDaemonStatuses` returns, so it varies by Dagster version: verified `FRESHNESS_DAEMON` doesn't exist at all on 1.11.16 (only 5 daemon types), appearing sometime in the 1.12.0 cycle ([#67](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/67)). Don't read a missing `FRESHNESS_DAEMON` row as broken on an older Dagster version.

This is the only metric here that reports on Dagster's machinery rather than its work, and it answers a question none of the others can: **is the scheduler actually running?** When the daemon dies, everything else keeps looking healthy — `dagster_schedule_status` still reports `running` (the schedule *is* enabled; that metric is about the on/off state, not the daemon), the last tick status stays frozen at whatever it was, active runs simply go quiet, and `dagster_exporter_last_scrape_success` stays `1` because the GraphQL endpoint is served by the webserver, which is fine. Nothing changes. Without this metric, "the scheduler is dead" and "nothing was scheduled" are indistinguishable.

`required=false` means the instance isn't configured to run that daemon, so it reporting unhealthy is expected rather than an incident — scope any alert to `{required="true"}`.

A null `healthy` from Dagster is reported as `0`, not `1`: this metric exists to catch a daemon that isn't answering, and no answer is not an answer.

`lastHeartbeatErrors` is logged rather than attached as a label, the same treatment `dagster_code_location_load_error` gives its error message, to avoid unbounded label cardinality.

**Don't alert on this metric alone.** `healthy` is not a live check — it is Dagster comparing the last heartbeat against a fixed 30-minute tolerance (`DEFAULT_DAEMON_HEARTBEAT_TOLERANCE_SECONDS = 1800`, against a `DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 30` write interval). A daemon that stopped 14 minutes ago still reports `1`. Verified by freezing the daemon process with `SIGSTOP` while leaving the webserver up:

```
dagster_daemon_healthy{daemon_type="SCHEDULER",required="true"} 1     # after 14 minutes down
time() - dagster_daemon_last_heartbeat_timestamp_seconds  ->  825     # and climbing
```

For an every-minute schedule that tolerance means roughly thirty missed runs before anything fires. Use the heartbeat timestamp below and pick your own threshold.

## `dagster_daemon_last_heartbeat_timestamp_seconds` (Gauge)

Labels: `daemon_type`

Unix timestamp of the daemon's most recent heartbeat, exported as a timestamp rather than an age so staleness is computed at query time (`time() - dagster_daemon_last_heartbeat_timestamp_seconds`) instead of being frozen at scrape time.

A daemon that has never reported a heartbeat produces no series at all, rather than one claiming a heartbeat at the Unix epoch — which would make every staleness alert fire on a freshly started instance.

This, rather than `dagster_daemon_healthy`, is the metric to alert on. Heartbeats are written every 30s, so a threshold of a couple of minutes catches a stopped daemon roughly fifteen times sooner than Dagster's own 30-minute tolerance:

```promql
time() - dagster_daemon_last_heartbeat_timestamp_seconds{daemon_type="SCHEDULER"} > 120
```

Exporting the raw timestamp is what makes that choice yours: an exported age would be computed once per scrape and then served unchanged, so it would understate the outage by up to a full scrape interval.

## `dagster_agents` (Gauge)

Labels: `status` (`running` or `not_running`)

**Dagster+ only.** The number of Dagster+ agents in each state. The agent is the process that launches runs: when it stops, the webserver stays up, schedules, sensors and manual launches stop, and runs sit in `QUEUED` or `STARTING`. `dagster_daemon_healthy` cannot see this, because Dagster+ runs its own daemons.

Both statuses are always exported once the agents have been read, so `running` is `0` when no agent is registered. That is what lets this alert fire in that case:

```promql
dagster_agents{status="running"} == 0
```

It counts agents instead of reporting one series per agent, on purpose. Dagster+ keeps a stopped agent in its list, so a per-agent series would stay at `0` for every agent that ever ran, and on Dagster+ Serverless an agent's id changes on every restart. A status other than `RUNNING` counts as `not_running`.

The collector (`agents`) only runs when `DAGSTER_CLOUD_API_TOKEN` is set. Open-source Dagster has no `agents` field, so against it the query would fail every scrape.

## `dagster_agent_last_heartbeat_timestamp_seconds` (Gauge)

**Dagster+ only.** Unix timestamp of the most recent heartbeat of any agent, exported as a timestamp so staleness is computed at query time. Agents send one about every 30 seconds, so a threshold of a couple of minutes is enough:

```promql
time() - dagster_agent_last_heartbeat_timestamp_seconds > 120
```

It is the newest of all agents, so a stopped agent that is still listed cannot make a running one look stale. It is absent when no agent has reported a heartbeat.

## `dagster_run_queue_concurrency_key_backlog` (Gauge)

Labels: `concurrency_key`

Number of runs currently `QUEUED` because of a tag-based run-queue concurrency limit (`dagster.yaml`'s `concurrency.runs.tag_concurrency_limits`), per `dagster/concurrency_key` tag value. Not job/location-scoped, since a concurrency key can be shared across jobs.

Note: Dagster's `instance.concurrencyLimits` GraphQL query looks like it would answer this directly, but it doesn't — it's backed by a separate op/step "pool" concurrency store and reports `0` for run-level tag-based backlog regardless of how many runs are actually queued behind a key, so this is computed by reading each `QUEUED` run's own tags instead. `instance.concurrencyLimits` is the right field for *that* separate mechanism — see `dagster_op_pool_concurrency_active_slots`.

A concurrency key is zero-filled (not dropped) once its backlog clears, for the same reason as `dagster_active_runs`: a missing series and a `0` mean different things.

This assumes the set of concurrency keys is bounded and small, since a key is zero-filled forever once observed — there's no roster query to prune against (unlike jobs), and nothing currently expires one. That holds for the common case of `tag_concurrency_limits` naming a handful of fixed tag values (e.g. specific `team` or `warehouse` values). It does **not** hold for a limit declared with `applyLimitPerUniqueValue: true` against a high-cardinality tag (a tenant id, a customer id, a date) — every distinct value seen becomes a permanent series until the exporter restarts. See [#81](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/81) if that's your setup.

## `dagster_schedule_status` (Gauge)

Labels: `schedule_name`, `location`, `status`

Always `1`; an "info" metric (same pattern as `dagster_last_run_info`) reporting whether a schedule is currently turned on (`running`) or off (`stopped`) in Dagster. Refetched from scratch on every scrape, unlike `dagster_last_run_info` — a removed schedule just isn't in the response anymore, no pruning needed.

## `dagster_schedule_last_tick_status` (Gauge)

Labels: `schedule_name`, `location`, `status`

Always `1`; status of a schedule's most recently observed tick (`started`, `skipped`, `success`, or `failure`). A schedule that has never ticked yet has no series — no seeded value, same rationale as `dagster_last_run_info` for a job that's never run.

Note this reports *what* the last tick was, not *when*. On its own it can't tell a schedule that is still ticking from one that stopped: the series simply freezes at the last outcome observed. Use `dagster_schedule_last_tick_timestamp_seconds` below for that.

## `dagster_schedule_last_tick_timestamp_seconds` (Gauge)

Labels: `schedule_name`, `location`

Unix timestamp of the schedule's most recent tick. Exported as a timestamp rather than an age so staleness is computed at query time — `time() - dagster_schedule_last_tick_timestamp_seconds` — instead of being frozen at scrape time. Same "no series until the first tick" behavior as the status metric above, and no `status` label, since here the value carries the information and a status label would churn the series on every outcome change.

There is deliberately no single recommended alert threshold. A tick is one evaluation by the daemon, and schedules tick on their cron boundary, so "no tick for an hour" is routine for a daily schedule and alarming for an every-minute one. The threshold has to come from the schedule's own cadence.

This is also not the metric for "is the scheduler daemon alive" — see `dagster_daemon_healthy`, which answers that even in a deployment with no schedules or sensors defined at all. What this one adds is the per-schedule view: a single schedule that stopped firing while the daemon is perfectly healthy (silently turned off, erroring on evaluation, or a cron that never matches).

## `dagster_sensor_status` (Gauge)

Labels: `sensor_name`, `location`, `status`

Same as `dagster_schedule_status`, for sensors: `running`/`stopped`.

## `dagster_sensor_last_tick_status` (Gauge)

Labels: `sensor_name`, `location`, `status`

Same as `dagster_schedule_last_tick_status`, for sensors. Note a sensor tick that decides not to launch anything is `skipped`, not a lack of data — Dagster's sensor daemon evaluates on a fixed interval regardless of whether there's anything to do, so `skipped` is a normal, common outcome, not necessarily a problem.

## `dagster_sensor_last_tick_timestamp_seconds` (Gauge)

Labels: `sensor_name`, `location`

Same as `dagster_schedule_last_tick_timestamp_seconds`, for sensors — and the more useful of the two for spotting a stalled evaluation loop. Because sensors tick on a fixed interval (`minimum_interval_seconds`) whether or not they launch anything, staleness here has a single meaningful threshold, unlike the cron-driven schedule case:

```promql
time() - dagster_sensor_last_tick_timestamp_seconds > 300
```

## `dagster_asset_stale_status` (Gauge)

Labels: `asset_key`, `status`

Always `1`; an "info" metric reporting whether an asset's data is `missing`, `stale`, or `fresh`, from Dagster's `assetNodes.staleStatus`. `asset_key` is the asset's path segments joined with `/` (e.g. `my_dbt_project/customers`), the same separator Dagster's own UI uses. No series at all for an asset Dagster reports no computable stale status for (the field is nullable in the schema). Also no series for a partitioned asset: asked without a partition, Dagster answers `fresh` whatever its partitions' state is, so reporting it would claim a freshness nobody computed. Per-partition status is not exported, since that would be one series per partition. The two metrics below still cover partitioned assets.

Dagster computes `staleStatus` one asset at a time, so the cost grows with the number of assets: about 6ms per asset in a measurement on Dagster+ (1006 assets: `GetAssetNodes` took 5 to 7s with it and about 0.6s without). With `ASSET_STALE_STATUS_ENABLED=false` the exporter does not ask for it and this metric has no series at all. The two metrics below are unaffected.

Note this is derived from `assetMaterializations`, which only ever records successful events — a `failure`-status run in `dagster_asset_last_materialization_status` below still shows `missing`/`stale` here rather than anything reflecting the failure. Use the two metrics together, not this one alone, to tell "never materialized" apart from "materialization keeps failing".

## `dagster_asset_last_materialization_status` (Gauge)

Labels: `asset_key`, `status`

Always `1`; status of an asset's most recently launched materializing run (`assetsLatestInfo.latestRun.status`, e.g. `success`, `failure`, `started`). No series at all for an asset that has never had a run.

This exists because `dagster_asset_stale_status` can't answer "did the last run succeed": `assetMaterializations` (and the `staleStatus` derived from it) only records successful events, so a failing asset and one that has simply never run both look `missing` there. This metric reads the run itself instead, so a failed run is visible even though it left the asset's materialization history untouched.

## `dagster_asset_last_materialization_timestamp_seconds` (Gauge)

Labels: `asset_key`

Unix timestamp (`assetsLatestInfo.latestRun.endTime`) of an asset's most recent materializing run, regardless of whether it succeeded. Exported as a timestamp rather than an age so staleness is computed at query time (`time() - metric`) instead of being frozen at scrape time — the same reasoning as `dagster_daemon_last_heartbeat_timestamp_seconds` and the schedule/sensor tick timestamps. An asset that has never had a run has no series. While the latest run is still in progress, Dagster reports its `endTime` as null, so this keeps the end time of the previous finished run instead of dropping to 0: it always means "when a run last finished", while `dagster_asset_last_materialization_status` already shows `started`. If the exporter starts while an asset's run is in progress, there's nothing to carry over, and the series appears once that run finishes.

This exists because `dagster_asset_stale_status` only detects staleness *relative to an upstream* (a `code_version` comparison, not elapsed time) — a leaf/source asset with no upstream dependency reports `fresh` forever once materialized once, even if it hasn't actually run again in months. This metric is what makes "this asset hasn't been materialized recently" alertable on its own, the same gap [#84](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/84) closed for schedules and sensors.

`endTime`, not `updateTime`, to match the existing convention: `dagster_last_run_duration_seconds` already uses `endTime - creationTime` for completed jobs, so this stays consistent with how "when did this run finish" is computed elsewhere in the codebase.

## `dagster_asset_check_status` (Gauge)

Labels: `asset_key`, `check_name`, `status`, `severity`

Always `1`; the outcome of an asset check's latest execution for the asset's latest materialization (`assetChecksOrError`, `executionForLatestMaterialization`). One series per check that has run.

| `status` | Meaning |
| --- | --- |
| `succeeded` | The check ran and passed. |
| `failed` | The check ran and the data is bad. |
| `execution_failed` | The check could not run, for example it raised an exception. Not a statement about the data. |
| `in_progress` | The check is running now. |
| `skipped` | Dagster skipped the check. |

`severity` is `error` or `warn`, the severity the check reported. A `failed` check with `severity="warn"` is a warning, so an alert that should not page on it filters on `severity="error"`. It is empty for `execution_failed`, because a check that could not run has no evaluation.

```promql
dagster_asset_check_status{status="failed", severity="error"} == 1
```

A failed check does not fail its run, so `dagster_asset_last_materialization_status` stays `success` and cannot show it.

A check that has never run for the asset's latest materialization has no series, the same as an asset that never ran has none in `dagster_asset_last_materialization_status`. When an asset is materialized again, its checks have no execution for the new materialization until they run, and their series disappear until then. If a check is missing, alert on its absence separately; `absent()` needs one expression per check you care about.

An asset whose checks cannot be read (`assetChecksOrError` answers with an error type such as a code location that needs an upgrade) is left out and logged, so it does not hide the checks of the other assets.

This is the `asset_checks` collector, separate from `asset_status` so it can be given its own `ASSET_CHECKS_SCRAPING_INTERVAL_SECONDS`. Verified against open-source Dagster 1.13.25 with one passing check, one failing, one failing with `AssetCheckSeverity.WARN`, one that raised, and one that never ran.

## `dagster_asset_metadata_value` (Gauge)

Labels: `asset_key`, `partition`, `label`

A numeric value (`int` or `float`) that an asset recorded on its latest materialization with `add_output_metadata` or `MaterializeResult(metadata=...)`: a row count, a file size, an accuracy score. A run can succeed while the data is wrong, such as a table with 0 rows, and this is what makes that alertable:

```promql
dagster_asset_metadata_value{asset_key="orders", label="row_count"} == 0
```

It is for knowing that a value dropped, not for long-term history. Trends over months belong in a table or a BI tool.

## Choosing what to export

Nothing is exported until `ASSET_METADATA_VALUES` lists it, for two reasons: every label of every asset would grow the series count without a bound, and Dagster reads each asset's latest materialization separately, so the cost grows with the number of assets asked for. Only the listed assets are asked for.

The value is a JSON object from an asset key (the path segments joined by `/`, as in the `asset_key` label) to a list of metadata labels. Nobody should write that by hand. With the Helm chart, set a YAML map and the chart renders the JSON:

```yaml
assetMetadataValues:
  orders: [row_count, accuracy]
  dbt/customers: [dagster/row_count]
```

Outside Helm, generate it from whatever already holds the list. With Jsonnet:

```jsonnet
std.manifestJsonMinified({ orders: ['row_count', 'accuracy'], 'dbt/customers': ['dagster/row_count'] })
```

An asset key with a `/` inside a segment cannot be written this way. A malformed value, an empty list or an empty label stops the exporter at startup, so a typo does not silently export nothing.

## Partitioned assets

For a partitioned asset the series is the latest materialization, which is **the partition that was materialized last**, not the newest partition key. Its key is in the `partition` label (`2026-01-02` for a daily partition, `eu` for a static one, `2026-01-02|eu` for a multi-partition asset). An asset that is not partitioned has an empty `partition`.

Only that one partition is read, so an asset has one series per label however many partitions it has. When another partition is materialized the series is replaced, not added to, and the old `partition` value goes with it. **Re-running an old partition moves the series back to it**, so a backfill of last month leaves the series on the last partition the backfill finished, until the next daily run.

```promql
dagster_asset_metadata_value{asset_key="orders", label="row_count"} == 0
```

The query above still works, as it does not name `partition`. Because the label changes with each partition, a graph of the value over time shows one line per partition that was ever the latest, and an alert should not match on `partition`.

## dbt assets

A dbt model, seed or snapshot loaded with `dagster-dbt` records three entries by default: `Execution Duration` (a float, in seconds), and `unique_id` and `invocation_id` (text, so skipped). There is no row count unless the asset asks for one, by adding `.fetch_row_counts()` to the stream:

```python
@dbt_assets(manifest=project.manifest_path)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream().fetch_row_counts()
```

Each table and seed then records `dagster/row_count` (an int) and `dagster/storage_kind`. **A view records neither**, so there is no series for it. A label can contain a space or a `/`, as these do, and is written as it is:

```yaml
assetMetadataValues:
  dbt/customers: [dagster/row_count, Execution Duration]
```

The asset key of the dev project's table is `customers`. A project that gives its assets a key prefix has it in front, joined with `/` (`dbt/customers` above), which was not checked here.

Verified against open-source Dagster 1.13.15 and `dagster-dbt` 0.29.15 with the DuckDB adapter, on a seed, a view and a table: without `fetch_row_counts()` the table gave a series for `Execution Duration` only, and with it the table and the seed also gave `dagster/row_count`, and the view gave neither. Not checked: other adapters, and a project with a key prefix.

## What is skipped

- **Text, URL, boolean and other kinds of entry.** Only `int` and `float` become series.
- **NaN and infinity.** Dagster answers `null` for them, as JSON cannot carry them.
- **An asset with no materialization, an asset Dagster does not define, and a label the materialization lacks.** No series, the same way `dagster_asset_last_materialization_status` handles an asset that never ran.

An integer is read as a string and converted to a float64, so a count above 2^53 loses its last digits. Dagster returns `null` for an integer past 32 bits in the field it offers for numbers, which is why the exporter does not use that field: a row count above 2.1 billion would be dropped without a trace.

## `dagster_asset_metadata_timestamp_seconds` (Gauge)

Labels: `asset_key`, `partition`

Unix timestamp of the materialization the values of that asset were read from. A run that fails after a success leaves the last successful materialization in place, so the value stays and this timestamp stays with it. `time() - dagster_asset_metadata_timestamp_seconds` is how long the values have been stale, which `dagster_asset_last_materialization_timestamp_seconds` cannot say: that one is the end time of the latest run and moves on a failed run too.

Verified against open-source Dagster 1.13.25 with an asset recording an int, a float, text, a URL, a boolean and NaN, an int of 5 billion, a daily, a static and a multi-partition asset (a new partition replaced the series, and a re-run of an old one moved it back), an asset that never ran, and an asset whose latest run failed after a success. Not checked: the metadata that integrations other than dbt record, and the cost on Dagster+.

## `dagster_backfills` (Gauge)

Labels: `status`

The number of backfills in each status, among the newest 100 (`partitionBackfillsOrError`). All eight statuses are always present once the backfills have been read, so `dagster_backfills{status="completed_failed"} > 0` can alert without the series having to appear first.

| `status` | Meaning |
| --- | --- |
| `requested` | Active. Dagster has no separate "running" status: a backfill whose runs are executing stays `requested`. |
| `canceling` | Active. A cancel was asked for, and the backfill is still stopping its runs. About 30 seconds in a test. |
| `failing` | Active. Read from the newest 100 only, because Dagster 1.9 has no such status and a query that names it is rejected there. |
| `completed_success` | Finished, all partitions succeeded. |
| `completed_failed` | Finished, at least one partition failed. |
| `completed` | Finished. |
| `failed` | The backfill itself failed. |
| `canceled` | Finished, canceled. |

`failed` and `completed_failed` are different statuses and are not merged. This exporter has seen `completed_failed` for a backfill whose partitions all failed, but has not seen when Dagster uses `failed`, so alert on both: `dagster_backfills{status=~"failed|completed_failed"} > 0`.

The backfill id is not a label, because it would add a series for every backfill ever launched. The count covers the newest 100 backfills, so a finished backfill drops out once 100 newer ones exist. `requested` and `canceling` are counted from a separate list that asks Dagster only for those, so an old backfill that hangs is still counted however many newer ones were launched.

Verified against open-source Dagster 1.13.25 with a backfill that succeeded, one whose partitions all failed, one that was canceled (`requested`, then `canceling`, then `canceled`), and one still running.

## `dagster_backfill_oldest_active_timestamp_seconds` (Gauge)

Unix timestamp at which the oldest active backfill (`requested`, `canceling` or `failing`) was created. No series while none is active. Exported as a timestamp so the age is computed at query time:

```promql
time() - dagster_backfill_oldest_active_timestamp_seconds > 6 * 3600
```

That alerts on a backfill that has been active for six hours. How long a backfill should take depends on the deployment, so no default alert ships with the chart.

## `dagster_op_pool_concurrency_active_slots` (Gauge)

Labels: `pool`

Number of slots currently claimed — steps actively running — against an op/step concurrency pool (Dagster's `pool` argument on `@op`/`@asset`, config'd via `dagster.yaml`'s `concurrency.pools`), per pool.

This is a distinct mechanism from `dagster_run_queue_concurrency_key_backlog`: that one tracks whole *runs* stuck in the queue behind a run-level `dagster/concurrency_key` tag, this tracks individual *steps* sharing a named pool of slots across the whole instance, regardless of which run or job they belong to. Both are sourced from Dagster's `instance.concurrencyLimits` GraphQL field having the wrong name for one of them and the right name for the other — see that metric's own doc for the details of why it can't answer the run-queue question. The label is `pool`, not `concurrency_key`, specifically so the two don't read as the same thing in a dashboard or alert.

A step behind a pool passes through up to three states: pending (waiting for a slot, not yet assigned one) → assigned (a slot is reserved for it, but it hasn't started) → active (the slot is claimed and the step is running) — see `dagster_op_pool_concurrency_assigned_steps` and `dagster_op_pool_concurrency_pending_steps` for the other two.

Unlike the run-queue backlog, this metric needs no exporter-side zero-fill and grows no unbounded map of its own (the concern [#81](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/81) raised for that one): `instance.concurrencyLimits` is sourced from Dagster's own event-log storage, which persistently remembers every pool that has ever claimed a slot and keeps reporting it at zero once it goes idle — confirmed by reading `SqlEventLogStorage.get_concurrency_keys`/`get_concurrency_info` against the pinned Dagster version, not just the GraphQL schema. The exporter just reflects whatever comes back on each scrape; a pool that has never been used simply never appears.

**Requires `concurrency.pools.granularity: "op"` in `dagster.yaml`.** Verified live: under the other setting, `"run"`, a pool's limit is still enforced (at the run-queue admission level — a whole run is blocked from starting if none of its root steps' pools have room), but no per-step slot is ever actually claimed in the tables this metric reads, so `active_slots`/`assigned_steps`/`pending_steps` all stay `0` regardless of real contention. `dagster_op_pool_concurrency_limit` is unaffected either way, since it reads the pool's configured limit directly rather than derived slot state. See [dev/dagster_home/dagster.yaml](../dev/dagster_home/dagster.yaml) and the "Testing op pool concurrency" section in the README for how the dev fixture exercises this.

## `dagster_op_pool_concurrency_assigned_steps` (Gauge)

Labels: `pool`

Number of steps assigned a slot against an op/step concurrency pool but not yet running, per pool — the middle of the three states described under `dagster_op_pool_concurrency_active_slots`. Included separately from pending/active so a full count of "how many steps are behind this pool right now" doesn't undercount by ignoring it.

## `dagster_op_pool_concurrency_pending_steps` (Gauge)

Labels: `pool`

Number of steps waiting for a slot against an op/step concurrency pool, not yet assigned one, per pool — the first of the three states described under `dagster_op_pool_concurrency_active_slots`.

Only counts a step that has actually been submitted for execution and is waiting on a slot — not a whole run still sitting in Dagster's run queue because none of its root steps could get a slot. Verified live: a run blocked at that admission stage never submits a step at all, so it contributes nothing here (it shows up as a `QUEUED` run instead, the same situation `dagster_run_queue_concurrency_key_backlog` was built for, just for a different concurrency mechanism). Seeing a nonzero value needs steps *within an already-started run* genuinely contending for a pool's slot — see the README's "Testing op pool concurrency" section for how the dev fixture sets that up.

## `dagster_op_pool_concurrency_limit` (Gauge)

Labels: `pool`, `using_default_limit`

Effective concurrency limit (number of slots) configured for an op/step concurrency pool, per pool. Divide it into `dagster_op_pool_concurrency_active_slots` for utilization.

`using_default_limit` is `"true"` when the pool has no explicit limit of its own and is running on the instance-wide default (`dagster.yaml`'s `concurrency.pools.default_limit`), `"false"` when it has been given one explicitly.

Absent for a pool Dagster reports no limit for at all — the schema makes `limit` nullable, though in practice (checked against the pinned Dagster version's resolver) that only happens for a pool with no claimed slots and no configured instance-wide default; every pool that has actually done anything has a concrete limit.

## Exporter self-health

These report on the exporter itself, rather than on Dagster's run state. The first three are about whether its own scrapes of Dagster are succeeding, and are labeled `collector`, one of `definitions_roster`, `active_runs`, `completed_runs`, `code_location_status`, `daemon_health`, `asset_status`, `asset_checks`, `asset_metadata`, `backfills`, or `op_pool_concurrency` (the ten collectors described in [docs/architecture.md](architecture.md)), plus `agents` when `DAGSTER_CLOUD_API_TOKEN` is set. Each collector scrapes on its own schedule, so with `<COLLECTOR>_SCRAPING_INTERVAL_SECONDS` overrides set, these series are refreshed at different rates: a collector's value only changes once per its own interval, and `last_scrape_success` of a slow collector can stay `0` for up to that long after Dagster recovers.

### `dagster_exporter_scrape_duration_seconds` (Gauge)

Labels: `collector`

Duration of that collector's most recent scrape. Absent until its first scrape completes.

### `dagster_exporter_last_scrape_success` (Gauge)

Labels: `collector`

`1` if that collector's most recent scrape succeeded, `0` if it failed. Reads `0` from startup until a collector's first scrape completes, so an exporter that never gets a scrape through is caught by `dagster_exporter_last_scrape_success == 0` rather than going silent. The first fetch of the definitions roster finishes before the other collectors start.

### `dagster_exporter_scrape_errors_total` (Counter)

Labels: `collector`

Total number of failed scrapes for that collector, since the exporter started.

### `dagster_exporter_build_info` (Gauge)

Labels: `version`, `commit`

Always `1`; the same `kube_pod_info`-style pattern as `dagster_last_run_info`, but for the exporter binary itself (same idiom as `node_exporter`'s `node_exporter_build_info`). Useful for spotting pods still running an old version after a fleet rollout (e.g. via the Helm chart). The published container image sets real values at build time; a plain `go build`/`go install` reports `version="dev"`, `commit="unknown"`.
