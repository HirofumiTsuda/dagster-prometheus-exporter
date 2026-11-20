#!/usr/bin/env bash
# Builds docs/dagster-dashboard.grafana-com.json from the dev dashboard, so a
# panel is changed in one place. The two differ only mechanically:
#
# - __inputs, __elements and __requires, which make Grafana's import wizard ask
#   for a data source;
# - a ${DS_PROMETHEUS} data source on every panel and every target.
#
# The dev file cannot have these: Grafana's file provisioning, which the
# docker compose stack uses, never resolves ${...} inputs, so every panel would
# break there. The generated file stays committed because the README links to it
# and grafana-dashboard.yml publishes it.
#
#   dev/grafana/build-grafana-com-dashboard.sh          write the file
#   dev/grafana/build-grafana-com-dashboard.sh --check  fail if the file is stale
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
src="$root/dev/grafana/dashboards/dagster-dashboard.json"
out="$root/docs/dagster-dashboard.grafana-com.json"

generate() {
  jq '
    def ds: {type: "prometheus", uid: "${DS_PROMETHEUS}"};
    # Names Grafana shows for the panel types a dashboard may use, in the order
    # the import wizard lists them. A type not listed here fails the build.
    def panel_names: {timeseries: "Time series", stat: "Stat", table: "Table"};
    ([.panels[].type] | unique) as $used
    | ($used - (panel_names | keys)) as $unknown
    | if ($unknown | length) > 0
      then error("panel type not known to build-grafana-com-dashboard.sh: \($unknown | join(", "))")
      else . end
    | {
        "__inputs": [{name: "DS_PROMETHEUS", label: "Prometheus", description: "", type: "datasource", pluginId: "prometheus", pluginName: "Prometheus"}],
        "__elements": {},
        "__requires": (
          [{type: "grafana", id: "grafana", name: "Grafana", version: "10.0.0"},
           {type: "datasource", id: "prometheus", name: "Prometheus", version: "1.0.0"}]
          + [panel_names | to_entries[] | select(.key as $k | $used | index($k)) | {type: "panel", id: .key, name: .value, version: ""}]
        )
      } + .
    | .panels |= map(
        (.targets // []) |= map(.datasource = ds)
        | .datasource = ds
      )
  ' "$src"
}

if [ "${1:-}" = "--check" ]; then
  if ! diff <(generate) "$out" >/dev/null; then
    echo "$out is out of date with $src. Run dev/grafana/build-grafana-com-dashboard.sh and commit the result." >&2
    diff <(generate) "$out" | head -20 >&2 || true
    exit 1
  fi
  exit 0
fi
generate > "$out"
