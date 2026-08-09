from dagster import Definitions

from dev.dagster_workspace.job import jobs, schedules

defs = Definitions(jobs=jobs, schedules=schedules)
