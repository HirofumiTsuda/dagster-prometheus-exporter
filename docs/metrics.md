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

Total number of completed runs (`success`, `failure`) per job, since the exporter started. Jobs that have never run are seeded at `0`. Series for jobs that no longer exist in Dagster are deleted automatically.

## `dagster_last_run_info` (Gauge)

Labels: `job_name`, `location`, `status`

Always `1`; an "info" metric (same pattern as `kube_pod_info`) reporting the status of the most recently completed run per job. Kept until a newer completion supersedes it or the job is removed from Dagster — it does not disappear just because nothing has completed recently. Use the `status` label to tell success from failure, e.g. in a Grafana table panel.

## `dagster_last_run_duration_seconds` (Gauge)

Labels: `job_name`, `location`, `status`

Duration (`endTime - creationTime`) of the most recently completed run per job. Tracks the same run as `dagster_last_run_info` (same lifetime, same `status` label), so a job that has never completed a run has no series for either — there's no seeded `0`.

## `dagster_code_location_load_error` (Gauge)

Labels: `location`

`1` if that code location most recently failed to load (e.g. a broken import in user code), `0` if it loaded successfully.

A code location can fail to load independently of any job/run activity — `dagster_active_runs`/`dagster_completed_runs_total` alone can't distinguish "this location has zero jobs" from "this location is broken," so this metric exists to surface that failure mode explicitly. The load-error message and stack trace are logged, not attached as a label, to avoid unbounded label cardinality.

## `dagster_daemon_healthy` (Gauge)

Labels: `daemon_type`, `required`

`1` if the daemon is currently healthy, `0` otherwise. `daemon_type` is one of Dagster's daemons — on 1.13.15: `SCHEDULER`, `SENSOR`, `BACKFILL`, `QUEUED_RUN_COORDINATOR`, `ASSET`, `FRESHNESS_DAEMON`.

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

## `dagster_run_queue_concurrency_key_backlog` (Gauge)

Labels: `concurrency_key`

Number of runs currently `QUEUED` because of a tag-based run-queue concurrency limit (`dagster.yaml`'s `concurrency.runs.tag_concurrency_limits`), per `dagster/concurrency_key` tag value. Not job/location-scoped, since a concurrency key can be shared across jobs.

Note: Dagster's `instance.concurrencyLimits` GraphQL query looks like it would answer this directly, but it doesn't — it's backed by a separate op/step "pool" concurrency store and reports `0` for run-level tag-based backlog regardless of how many runs are actually queued behind a key, so this is computed by reading each `QUEUED` run's own tags instead.

A concurrency key is zero-filled (not dropped) once its backlog clears, for the same reason as `dagster_active_runs`: a missing series and a `0` mean different things.

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

## Exporter self-health

These report on the exporter itself, rather than on Dagster's run state. The first three are about whether its own scrapes of Dagster are succeeding, and are labeled `collector`, one of `definitions_roster`, `active_runs`, `completed_runs`, `code_location_status`, or `daemon_health` (the five concurrent collectors described in [docs/architecture.md](architecture.md)).

### `dagster_exporter_scrape_duration_seconds` (Gauge)

Labels: `collector`

Duration of that collector's most recent scrape.

### `dagster_exporter_last_scrape_success` (Gauge)

Labels: `collector`

`1` if that collector's most recent scrape succeeded, `0` if it failed.

### `dagster_exporter_scrape_errors_total` (Counter)

Labels: `collector`

Total number of failed scrapes for that collector, since the exporter started.

### `dagster_exporter_build_info` (Gauge)

Labels: `version`, `commit`

Always `1`; the same `kube_pod_info`-style pattern as `dagster_last_run_info`, but for the exporter binary itself (same idiom as `node_exporter`'s `node_exporter_build_info`). Useful for spotting pods still running an old version after a fleet rollout (e.g. via the Helm chart). The published container image sets real values at build time; a plain `go build`/`go install` reports `version="dev"`, `commit="unknown"`.
