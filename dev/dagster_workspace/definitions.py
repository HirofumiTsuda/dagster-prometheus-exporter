from dagster import Definitions

from dev.dagster_workspace.job import jobs

defs = Definitions(jobs=jobs)
