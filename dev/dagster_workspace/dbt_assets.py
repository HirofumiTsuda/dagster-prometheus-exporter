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

# manifest.json isn't checked in or built by any script here -- this line
# is the only thing that produces it. prepare_if_dev() only acts when
# DAGSTER_IS_DEV_CLI is set (which `dagster dev` sets on itself), and when it
# does, it shells out to `dbt parse --quiet`, writing
# dev/jaffle_shop/target/manifest.json fresh on every `dagster dev` startup
# (see dagster_dbt.dbt_project.DagsterDbtProjectPreparer.prepare). Outside of
# `dagster dev` this is a no-op and dagster-dbt expects manifest.json to
# already exist from a build-time `dbt parse`/`dbt build` -- doesn't apply
# here since docker/dagster-dev.Dockerfile's CMD is always `dagster dev`.
jaffle_shop_project.prepare_if_dev()


@dbt_assets(manifest=jaffle_shop_project.manifest_path)
def jaffle_shop_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


dbt_resource = DbtCliResource(project_dir=jaffle_shop_project)

dbt_assets_list = [jaffle_shop_dbt_assets]
