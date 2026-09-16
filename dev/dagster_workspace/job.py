import time

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    job,
    multiprocess_executor,
    op,
    sensor,
)


@op
def slow_op():
    """A op for monitoring test: it sleeps for 30 seconds to simulate a long-running operation."""
    time.sleep(30)


@op
def failing_op():
    """A op for testing failure counts: it raises an error intentionally."""
    raise RuntimeError("Intentional error for exporter testing")


@op
def quick_op():
    """A near-instant op, so the schedule below produces real SUCCESS ticks
    quickly for exercising dagster_schedule_status/dagster_schedule_last_tick_status."""


@job(tags={"dagster/concurrency_key": "heavy_limit"})
def heavy_job():
    slow_op()


@job(tags={"dagster/concurrency_key": "failing_limit"})
def failing_job():
    failing_op()


@job
def quick_job():
    quick_op()


@op(pool="heavy_pool")
def heavy_pool_op():
    """An op behind the heavy_pool concurrency pool (not the same mechanism
    as heavy_job's dagster/concurrency_key tag -- see
    dagster_op_pool_concurrency_active_slots in docs/metrics.md). Sleeps long
    enough that launching heavy_pool_job a second time while the first is
    still running holds heavy_pool's only slot (dagster.yaml's
    concurrency.pools.default_limit: 1), for exercising
    dagster_op_pool_concurrency_pending_steps/_assigned_steps."""
    time.sleep(30)


@job
def heavy_pool_job():
    heavy_pool_op()


@op(pool="heavy_pool")
def heavy_pool_op_b():
    """A second op sharing heavy_pool_op's pool, with no data dependency
    between them, so a single run of heavy_pool_fanout_job can exercise
    dagster_op_pool_concurrency_pending_steps within one run. Launching
    heavy_pool_job twice, by contrast, only ever contends at the run-queue
    admission level (see dagster.yaml's concurrency.pools comment) --
    verified live: a run blocked there never submits a step at all, so
    pendingStepCount stays 0 no matter how many runs pile up against the
    pool. Only a step that's actually been submitted for execution and is
    waiting on a slot counts as pending."""
    time.sleep(15)


# max_concurrent=2 so both ops are actually submitted for execution at once
# (the default executor here would run them sequentially in-process, in
# which case op_b would never be submitted until heavy_pool_op finished and
# freed the slot itself -- never observably pending).
@job(executor_def=multiprocess_executor.configured({"max_concurrent": 2}))
def heavy_pool_fanout_job():
    heavy_pool_op()
    heavy_pool_op_b()


jobs = [heavy_job, failing_job, quick_job, heavy_pool_job, heavy_pool_fanout_job]

# default_status=RUNNING so it's already ticking without a manual toggle in
# the UI/API — see the "Testing schedule tick status" section in README.md.
every_minute_schedule = ScheduleDefinition(
    job=quick_job,
    cron_schedule="* * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
)

schedules = [every_minute_schedule]


# default_status=RUNNING, same rationale as the schedule above. Always
# skips (rather than launching quick_job) so its ticks are deterministically
# SKIPPED — a distinct outcome from the schedule's SUCCESS ticks, useful for
# exercising dagster_sensor_status/dagster_sensor_last_tick_status with more
# than one status value present.
@sensor(job=quick_job, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def quick_job_sensor(context: SensorEvaluationContext):
    return SkipReason(
        "dev fixture: intentionally always skips, for exercising dagster_sensor_last_tick_status"
    )


# Also default_status=RUNNING. Raises instead of returning, so its ticks are
# deterministically FAILURE — the third distinct tick status alongside the
# schedule's SUCCESS and quick_job_sensor's SKIPPED. Without it, nothing in
# the dev stack ever produces dagster_sensor_last_tick_status{status="failure"},
# so an alert written against that label can't be tried out locally.
#
# It logs an error every evaluation interval by design, in the same spirit as
# failing_job and the broken code location: intentional, named to say so.
@sensor(job=quick_job, minimum_interval_seconds=30, default_status=DefaultSensorStatus.RUNNING)
def failing_sensor(context: SensorEvaluationContext):
    raise RuntimeError(
        "dev fixture: intentionally always fails, for exercising "
        "dagster_sensor_last_tick_status{status=\"failure\"}"
    )


sensors = [quick_job_sensor, failing_sensor]


@asset
def good_asset():
    """An asset that materializes successfully every time, for exercising
    dagster_asset_last_materialization_status{status="success"} and
    dagster_asset_stale_status."""
    return 1


@asset
def bad_asset():
    """An asset that always fails to materialize, for exercising
    dagster_asset_last_materialization_status{status="failure"} — the same
    reasoning as failing_job/failing_sensor: assetsLatestInfo.latestRun is
    the only source for this, since assetMaterializations only ever records
    successful events (see issue #56's investigation notes)."""
    raise RuntimeError(
        "dev fixture: intentionally always fails, for exercising "
        "dagster_asset_last_materialization_status{status=\"failure\"}"
    )


assets = [good_asset, bad_asset]
