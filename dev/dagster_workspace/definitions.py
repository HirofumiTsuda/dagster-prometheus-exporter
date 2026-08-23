from dagster import Definitions

from dev.dagster_workspace.job import assets, jobs, schedules, sensors

defs = Definitions(jobs=jobs, schedules=schedules, sensors=sensors, assets=assets)
