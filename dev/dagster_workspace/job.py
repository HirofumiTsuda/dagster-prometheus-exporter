import time

from dagster import DefaultScheduleStatus, ScheduleDefinition, job, op


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
