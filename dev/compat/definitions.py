"""A minimal code location for dev/compat/check.sh.

It uses only what every Dagster version in the compatibility matrix has, so the
same file loads on all of them: partitioned and unpartitioned assets, a job, a
schedule and a sensor.
"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    sensor,
)


@asset
def plain_asset():
    return 1


@asset(partitions_def=StaticPartitionsDefinition(["a", "b"]))
def partitioned_asset():
    return 1


compat_job = define_asset_job("compat_job", selection=[plain_asset])


@sensor(job=compat_job)
def compat_sensor():
    return []


defs = Definitions(
    assets=[plain_asset, partitioned_asset],
    jobs=[compat_job],
    schedules=[ScheduleDefinition(job=compat_job, cron_schedule="0 0 * * *")],
    sensors=[compat_sensor],
)
