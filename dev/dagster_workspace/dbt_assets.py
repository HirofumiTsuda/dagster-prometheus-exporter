from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# Added for #98: good_asset/bad_asset (job.py) are two independent assets
# with no dependency between them, which is enough to exercise
# dagster_asset_last_materialization_status but not the `stale` value of
# dagster_asset_stale_status -- that only fires when an asset's upstream has
# been materialized more recently than the asset itself, which needs an
# actual dependency chain. See dev/jaffle_shop/README.md for what this
# project is and why it's hand-written rather than vendored.
jaffle_shop_project = DbtProject(project_dir=Path(__file__).parent.parent / "jaffle_shop")

# Builds the manifest (dbt parse) on `dagster dev`/webserver startup if it's
# missing or stale, so there's no separate "run dbt build once" setup step
# for the dev stack. No-ops (and stays silent) outside of a dev context.
jaffle_shop_project.prepare_if_dev()


@dbt_assets(manifest=jaffle_shop_project.manifest_path)
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


dbt_resource = DbtCliResource(project_dir=jaffle_shop_project)

dbt_assets_list = [jaffle_shop_dbt_assets]
