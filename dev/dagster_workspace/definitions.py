from dagster import Definitions

from dev.dagster_workspace.dbt_assets import dbt_assets_list, dbt_resource
from dev.dagster_workspace.job import asset_checks, assets, jobs, schedules, sensors

defs = Definitions(
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
    assets=assets + dbt_assets_list,
    asset_checks=asset_checks,
    resources={"dbt": dbt_resource},
)
