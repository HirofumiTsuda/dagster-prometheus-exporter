# jaffle_shop (dev fixture)

A minimal, hand-written dbt project used only to give the dev stack a real
asset *dependency chain*: `raw_customers` (seed) → `stg_customers` (staging
model) → `customers` (mart model). `good_asset`/`bad_asset` in
`dev/dagster_workspace/job.py` are two independent assets with no dependency
between them, which is enough to exercise
`dagster_asset_last_materialization_status` but not the `stale` value of
`dagster_asset_stale_status` — that only happens when an asset's upstream has
been materialized more recently than the asset itself, which needs an actual
multi-node graph. See [issue #98](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/98)
and the investigation notes on
[issue #56](https://github.com/HirofumiTsuda/dagster-prometheus-exporter/issues/56)
for why: most real-world Dagster asset graphs are dbt models, so this is
closer to what the metric actually sees in production than two flat assets.

## Attribution

The naming and shape (a `raw_customers` seed feeding a `stg_customers`
staging model feeding a `customers` mart) is modeled after dbt Labs'
tutorial project, [`dbt-labs/jaffle_shop_duckdb`](https://github.com/dbt-labs/jaffle_shop_duckdb)
(the `duckdb` branch). None of its files are vendored or copied — every file
under this directory was written from scratch for this repository, trimmed
to the one dependency chain this dev stack actually needs (the real
tutorial also ships `orders`, `payments`, and several more models/seeds that
have no bearing on exercising `dagster_asset_stale_status`).

## Structure

Materialization is declared per-model via `{{ config(...) }}` in each `.sql`
file (`stg_customers` a view, `customers` a table) rather than by folder in
`dbt_project.yml`, so there's one place to look for a given model's config,
not two. Each of `seeds/`, `models/staging/`, and `models/marts/` has a
`schema.yml` with `unique`/`not_null` data tests on the primary key and
`not_null` on the name columns — `dagster_dbt`'s `@dbt_assets` runs `dbt
build` (not `dbt run`), so these tests execute as part of every
materialization, dbt or dagster-triggered alike.

## Database

Target is DuckDB, file-based at `dev/dagster_home/jaffle_shop.duckdb`
(generated on first `dbt build`/materialization, gitignored — same
treatment as the rest of `dev/dagster_home`'s runtime state). No new
docker-compose service required.

## Triggering `stale`

See the "Testing asset staleness" section in the top-level `README.md`.
Two things had to be verified against a real instance while building this
fixture, because both are easy to get wrong by reasoning alone:

- **Re-materializing an upstream model with unchanged code is not enough.**
  Dagster's staleness for dbt assets is keyed on a `code_version` (a checksum
  of the dbt node's compiled SQL/seed file — see
  `dagster_dbt.asset_utils.default_code_version_fn`), not a materialization
  timestamp, so re-running `stg_customers` without changing it leaves
  `customers` reporting `fresh`. An actual code change to an upstream model
  (e.g. editing `stg_customers.sql`) is required.
- **A running `dagster dev` process doesn't pick up that code change on its
  own.** After editing the SQL, the code location must be explicitly
  reloaded before re-materializing — otherwise the running webserver is
  still comparing against the code version it loaded at startup, and the
  result is `stg_customers` itself incorrectly showing `stale` too (not just
  `customers`), since the just-materialized run used the new file while the
  webserver's view of "current" didn't.
