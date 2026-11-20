#!/usr/bin/env bash
# Renders the chart's alert rules with every rule enabled and runs
# alerts_test.yml against them with promtool (pinned in .tool-versions).
# Fails when a rule has no test, so a new rule cannot ship untested.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
chart="$here/../../charts/dagster-prometheus-exporter"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The rules that ship disabled are enabled here, so they are tested too.
disabled=$(awk '/^  rules:$/ {in_rules=1} in_rules && /^    [A-Za-z]+:$/ {name=$1} in_rules && /^      enabled: false/ {print name}' "$chart/values.yaml" | tr -d ':')
set_args=(--set alerts.enabled=true)
for rule in $disabled; do
  set_args+=(--set "alerts.rules.${rule}.enabled=true")
done

# spec.groups is the last key of the PrometheusRule, and a rules file is its content.
helm template "$chart" "${set_args[@]}" --show-only templates/prometheusrule.yaml \
  | sed -n '/^  groups:/,$p' | sed 's/^  //' > "$work/rules.yml"
cp "$here/alerts_test.yml" "$work/alerts_test.yml"

promtool check rules "$work/rules.yml"

untested=0
for alert in $(grep -E '^\s+- alert:' "$work/rules.yml" | sed -E 's/.*alert: //'); do
  if ! grep -q "alertname: $alert\$" "$here/alerts_test.yml"; then
    echo "no test for $alert in alerts_test.yml" >&2
    untested=1
  fi
done
[ "$untested" -eq 0 ]

cd "$work"
promtool test rules alerts_test.yml
