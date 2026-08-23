import time

from dagster import (
    DefaultScheduleStatus,
    DefaultSensorStatus,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    job,
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


jobs = [heavy_job, failing_job, quick_job]

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
