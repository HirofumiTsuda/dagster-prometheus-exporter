#!/usr/bin/env bash
# Runs the exporter against one Dagster version and fails unless every
# collector scrapes without a GraphQL error.
#
# usage: dev/compat/check.sh [PIP_SPEC]
#   PIP_SPEC is appended to the package names, e.g. "==1.12.22" or
#   ">=1.13,<1.14". Empty means the newest release.
#
# It triggers no runs, so it does not assert every metric family; the e2e on
# the pinned version does that. What it does catch is a query that this
# Dagster version rejects, which makes that collector report failure.
set -euo pipefail

spec="${1:-}"
root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
dagster_pid=""
exporter_pid=""
cleanup() {
  [ -n "$exporter_pid" ] && kill "$exporter_pid" 2>/dev/null || true
  [ -n "$dagster_pid" ] && kill "$dagster_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

uv venv --quiet --python 3.12 "$work/venv"
uv pip install --quiet --python "$work/venv/bin/python" "dagster${spec}" "dagster-webserver${spec}"
installed="$("$work/venv/bin/python" -c 'import dagster; print(dagster.__version__)')"
echo "Dagster ${installed} (spec '${spec:-latest}')"

go build -o "$work/exporter" "$root/cmd/exporter"

export DAGSTER_HOME="$work/home"
mkdir -p "$DAGSTER_HOME"
"$work/venv/bin/dagster-webserver" -f "$root/dev/compat/definitions.py" -p 3000 >"$work/dagster.log" 2>&1 &
dagster_pid=$!
for _ in $(seq 60); do
  curl -fs localhost:3000/server_info >/dev/null && break
  sleep 1
done
curl -fs localhost:3000/server_info >/dev/null || { cat "$work/dagster.log"; echo "Dagster did not start"; exit 1; }

ASSET_METADATA_VALUES='{"plain_asset":["row_count"]}' DAGSTER_GRAPHQL_ENDPOINT=http://localhost:3000/graphql PORT=9101 "$work/exporter" >"$work/exporter.log" 2>&1 &
exporter_pid=$!
sleep 25

metrics="$(curl -fs localhost:9101/metrics)"
results="$(grep '^dagster_exporter_last_scrape_success' <<<"$metrics" || true)"
echo "$results"
if [ -z "$results" ] || grep -qv ' 1$' <<<"$results"; then
  echo "A collector failed against Dagster ${installed}:"
  grep -i 'graphql error\|failed' "$work/exporter.log" || true
  exit 1
fi
echo "All collectors scraped without a GraphQL error on Dagster ${installed}"
