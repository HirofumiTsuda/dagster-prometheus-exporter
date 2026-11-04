#!/usr/bin/env bash
# Publishes a dashboard file to grafana.com as a new revision, unless the
# latest revision already holds the same JSON.
#
# usage: publish-grafana-dashboard.sh DASHBOARD_ID FILE
#   GRAFANA_COM_API_KEY  the key that may edit the dashboard. Without it the
#                        script only reads, and says whether it would publish.
set -euo pipefail

id="${1:?dashboard id}"
file="${2:?dashboard file}"
api="https://grafana.com/api/dashboards/${id}"

# Prints "<latest revision> <true|false: its JSON equals the file>". jq -e
# fails on an empty or non-JSON body, so a broken read is retried instead of
# being taken for a changed dashboard.
read_state() {
  curl -fsS --connect-timeout 10 --max-time 30 "${api}" |
    jq -er --slurpfile want "${file}" '"\(.revision|numbers) \((.json|objects) == $want[0])"'
}

state=""
for attempt in 1 2 3 4 5; do
  if state="$(read_state)"; then break; fi
  echo "read failed, attempt ${attempt}" >&2
  state=""
  sleep $((attempt * 10))
done
[ -n "${state}" ] || { echo "could not read ${api}" >&2; exit 1; }

read -r revision same <<<"${state}"
if [ "${same}" = true ]; then
  echo "revision ${revision} already matches ${file}"
  exit 0
fi

if [ -z "${GRAFANA_COM_API_KEY:-}" ]; then
  echo "revision ${revision} differs from ${file}; no GRAFANA_COM_API_KEY, so not publishing"
  exit 0
fi

# Never post twice. If this post times out it may still have landed, and
# posting again would add a duplicate revision. So after a failed post, only
# the moved revision number tells us it did land.
if new="$(curl -fsS --connect-timeout 10 --max-time 60 \
  -H "Authorization: Bearer ${GRAFANA_COM_API_KEY}" \
  -F "json=@${file};type=application/json" \
  "${api}/revisions" | jq -er '.revision|numbers')"; then
  echo "published revision ${new} from ${file}"
  exit 0
fi

sleep 30
if now="$(read_state)"; then
  read -r latest _ <<<"${now}"
  if [ "${latest}" != "${revision}" ]; then
    echo "the post timed out but revision ${latest} exists now" >&2
    exit 0
  fi
fi
echo "upload failed" >&2
exit 1
