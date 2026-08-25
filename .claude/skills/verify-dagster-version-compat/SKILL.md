---
name: verify-dagster-version-compat
description: Check whether the exporter's GraphQL queries still work against a Dagster version other than the pinned/verified one (issue #67). Temporarily re-pins dagster/dagster-webserver/dagster-dbt to a candidate version, builds the dev image, and runs the same trigger-and-assert sequence as verify-metric against it. Use when investigating version compatibility, not for routine metric changes -- see verify-metric for that.
---

# Verify Dagster version compatibility

`pyproject.toml`/`uv.lock` pin one Dagster version (currently `1.13.15`) as
the only one actually verified. This checks a *different* candidate version
the same way: real instance, real triggers, real `/metrics` output -- not
just "does it install."

Always test in the current git branch's working tree, uncommitted, and
revert at the end (step 6). Never commit a version bump from this skill
without deliberately deciding to move the pin.

## 1. Find the matching dagster-dbt version

`dagster-dbt` pins an *exact* matching `dagster` patch 1:1 (verified:
`0.29.15` requires `dagster==1.13.15`, `0.28.22` requires `dagster==1.12.22`,
same minor-offset pattern holds across the line). There is no range that
works across multiple dagster patches -- look up the exact pairing:

```sh
curl -s "https://pypi.org/pypi/dagster-dbt/<candidate-dagster-dbt-version>/json" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['info']['requires_dist'])"
```

**The `dbt-core` range that `dagster-dbt` accepts also shifts between
patches within the same dagster minor line** -- don't assume it's fixed.
Verified: `dagster-dbt==0.28.0` requires `dbt-core<1.11`, `0.28.22` requires
`dbt-core<1.12`, despite both being in the `1.12.x` dagster line. Check each
candidate's `requires_dist` for `dbt-core` too, and let `uv lock` resolve
`dbt-duckdb` freely within it rather than hardcoding a range.

## 2. Re-pin and lock

Edit `pyproject.toml` directly (uncommitted):

```toml
dependencies = [
    "dagster==<candidate>",
    "dagster-webserver==<candidate>",
]
```

and in `[dependency-groups].dbt`:

```toml
dbt = [
    "dagster-dbt==<matching version from step 1>",
    "dbt-duckdb>=<lower>,<<upper from step 1>",
]
```

```sh
uv lock
```

If this fails to resolve, that's itself a finding -- record which packages
conflicted.

## 3. Build and boot a standalone Dagster container

```sh
docker build -f docker/dagster-dev.Dockerfile -t dagster-dev-compat-test:<version> .
docker network create compat-test-net
docker run -d --rm --name dagster-compat-test --network compat-test-net dagster-dev-compat-test:<version>
```

**No `curl` inside this image** (`python:3.13-slim` base) -- `docker exec
... curl` will fail with "executable file not found". Poll from a sidecar
container on the same network instead:

```sh
for i in $(seq 1 15); do
  docker run --rm --network compat-test-net curlimages/curl -sf http://dagster-compat-test:3000/server_info && break
  sleep 3
done
```

Startup can take longer than the usual dev stack (manifest.json is built
fresh via `dbt parse` on first import, plus a colder image) -- 30-45s is not
unusual, don't give up after the first few retries.

Confirm the code location actually loaded (not just that the webserver is
up):

```sh
docker run --rm --network compat-test-net curlimages/curl -s -X POST http://dagster-compat-test:3000/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ workspaceOrError { __typename ... on Workspace { locationEntries { name loadStatus locationOrLoadError { __typename ... on PythonError { message } } } } } }"}'
```

`loadStatus: "LOADED"` with `locationOrLoadError.__typename:
"RepositoryLocation"` means clean. A `PythonError` here is itself a
compatibility finding (e.g. a GraphQL/Python API the fixture code relies on
no longer exists in this version) -- capture the message.

## 4. Run the exporter against it and trigger conditions

```sh
docker run -d --rm --name exporter-compat-test --network compat-test-net -p 9103:9101 \
  -e DAGSTER_GRAPHQL_ENDPOINT=http://dagster-compat-test:3000/graphql \
  dagster-prometheus-exporter-exporter   # reuse the image already built by docker compose
curl -s http://127.0.0.1:9103/readyz     # should report the candidate version
```

Same trigger set as `verify-metric`'s e2e equivalent, run once:

```sh
docker exec dagster-compat-test uv run --group dbt dagster asset materialize -m dev.dagster_workspace.definitions --select 'good_asset'

docker run --rm --network compat-test-net curlimages/curl -s -X POST http://dagster-compat-test:3000/graphql \
  -H 'Content-Type: application/json' -d '{
  "query": "mutation($e: ExecutionParams!) { launchPipelineExecution(executionParams: $e) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } } }",
  "variables": {"e": {"selector": {"repositoryLocationName": "dev-dagster-workspace", "repositoryName": "__repository__", "jobName": "quick_job"}, "mode": "default"}}
}'
# heavy_job x3 (concurrency_key=heavy_limit, limit 1 -- produces a backlog)
for i in 1 2 3; do
  docker run --rm --network compat-test-net curlimages/curl -s -X POST http://dagster-compat-test:3000/graphql \
    -H 'Content-Type: application/json' -d '{
    "query": "mutation($e: ExecutionParams!) { launchPipelineExecution(executionParams: $e) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } } }",
    "variables": {"e": {"selector": {"repositoryLocationName": "dev-dagster-workspace", "repositoryName": "__repository__", "jobName": "heavy_job"}, "mode": "default"}}
  }'
done
```

## 5. Assert every metric family, and spot-check the fragile ones

```sh
go test -run TestListMetricNames -v ./internal/server/... \
  | grep '^METRIC:' | sed 's/^METRIC: //' | sort > /tmp/expected-metrics-compat.txt

# poll until stable (up to ~45s)
for i in $(seq 1 20); do
  M=$(curl -s http://127.0.0.1:9103/metrics)
  echo "$M" | grep -q "dagster_last_run_info" && break
  sleep 3
done
MISSING=""
while read -r name; do
  echo "$M" | grep -q "^${name}{" || MISSING="$MISSING $name"
done < /tmp/expected-metrics-compat.txt
echo "missing (only dagster_exporter_scrape_errors_total expected):$MISSING"
```

Then read the actual label values on the historically fragile ones -- a
family being *present* doesn't mean the query behaves correctly (`#69`,
`#85` were both "present but wrong" bugs):

```sh
echo "$M" | grep -E "^dagster_daemon_healthy|^dagster_schedule_last_tick_status|^dagster_sensor_last_tick_status|^dagster_asset_stale_status|^dagster_code_location_load_error"
```

Check specifically: all 6 daemon types present (not fewer -- a version
might rename/add/remove a daemon type), all 3 distinct tick statuses
(success/skipped/failure) actually showing, stale status resolving without
error.

This has already found a real, benign difference: `1.11.16` reports only 5
daemon types, missing `FRESHNESS_DAEMON` (introduced sometime in the
`1.12.0` cycle). Confirmed via the raw `instance.daemonHealth` query, not
assumed -- it's Dagster itself not reporting that daemon type on `1.11.16`,
not an exporter bug (the collector's daemon-health loop has no hardcoded
expected list, so it already handles this correctly). Still worth recording
in `docs/metrics.md` and the issue, since a user on an older version
shouldn't read a missing row as broken.

## 6. Revert -- do not leave the version bump in place

```sh
git checkout -- pyproject.toml uv.lock
uv sync --group dbt   # restore local dev venv to the pinned version
docker rm -f dagster-compat-test exporter-compat-test
docker network rm compat-test-net
docker rmi dagster-dev-compat-test:<version>
rm -f /tmp/expected-metrics-compat.txt
```

## Recording the result

Note the candidate version, whether it resolved/built/loaded/passed, and
the daemon-type list actually observed (Dagster has added daemon types
before -- if the list differs from `1.13.15`'s six, that's worth flagging
even if nothing broke). If something failed, the specific GraphQL error or
missing metric family *is* the actionable finding for issue #67 -- record
it verbatim rather than summarizing it away.
