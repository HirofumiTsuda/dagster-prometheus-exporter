---
name: verify-metric
description: Verify a Dagster exporter metric end to end against the real dev stack — create the condition in Dagster, confirm the series reaches /metrics and Prometheus, and confirm the Grafana panel actually renders it. Use whenever adding or changing a metric, or when a metric is suspected of not working. Unit tests with mocked GraphQL cannot catch a query Dagster answers differently than the mock does.
---

# Verify a metric end to end

Unit tests here mock Dagster's GraphQL, so they prove the Go code handles a
response — not that Dagster produces that response. Three separate bugs
(#69, #70, #85) were all invisible to a green CI. The worst was #85: the
schedule and sensor tick metrics were implemented, tested, documented, and
had Grafana panels, but had **never emitted a single series** on a real
instance, because `ticks(limit: 1)` needs a time bound that the mock didn't
model. It went unnoticed for weeks.

Finish the loop: create the condition in Dagster, then follow the data
through the exporter, Prometheus, and the dashboard panel.

## Steps

1. **Bring up the whole stack**, not just Dagster. The panel check at the end
   needs Prometheus and Grafana too.

   ```sh
   docker compose up -d --build
   ```

   Services: Dagster `:3000`, exporter `:9101`, Prometheus `:9090`, Grafana
   `:3001` (`root` / `passw0rd`, dashboard pre-provisioned).

2. **Wait for readiness** rather than sleeping a fixed amount:

   ```sh
   until curl -sf http://127.0.0.1:9090/-/ready >/dev/null \
      && curl -sf http://127.0.0.1:3001/api/health >/dev/null \
      && curl -sf http://127.0.0.1:9101/metrics >/dev/null; do sleep 5; done
   ```

3. **Create the condition the metric measures.** This is the step that gets
   skipped, and the reason a panel can sit empty for weeks without anyone
   noticing.

   Some conditions already exist in `dev/dagster_workspace/job.py`, running
   automatically:

   | Fixture | Produces |
   | --- | --- |
   | `quick_job_schedule` | schedule ticking `success` every minute |
   | `quick_job_sensor` | sensor ticking `skipped` every 30s |
   | `failing_sensor` | sensor ticking `failure` every 30s |
   | `broken_location` | code-location load error (opt-in: `dagster dev -w dev/workspace.yaml`) |

   Others have to be triggered. The run-queue backlog, for instance, needs
   more runs than the concurrency limit in `dev/dagster_home/dagster.yaml`
   allows (`heavy_limit`, limit 1), so launch three and two will queue:

   ```sh
   curl -s -X POST http://127.0.0.1:3000/graphql -H 'Content-Type: application/json' -d '{
     "query": "mutation($e: ExecutionParams!) { launchPipelineExecution(executionParams: $e) { __typename ... on LaunchRunSuccess { run { runId status } } ... on PythonError { message } } }",
     "variables": {"e": {"selector": {"repositoryLocationName": "dev-dagster-workspace", "repositoryName": "__repository__", "jobName": "heavy_job"}, "mode": "default"}}
   }' | jq -c '.data.launchPipelineExecution'
   ```

   If no fixture produces the condition, **add one**. A metric that can't be
   produced locally can't be verified locally, and its dashboard panel can't
   be checked by anyone.

4. **Confirm the series reaches `/metrics`.** Both scrape intervals are 15s
   (exporter → Dagster, Prometheus → exporter), so allow ~30s end to end.

   ```sh
   curl -s http://127.0.0.1:9101/metrics | grep '^dagster_<metric_name>'
   ```

   Absent here means the collector isn't producing it — check the exporter
   logs and query Dagster's GraphQL directly to see what it really returns.
   Do not assume the query is right because a unit test passes.

5. **Confirm Prometheus ingested it:**

   ```sh
   curl -s -G --data-urlencode 'query=dagster_<metric_name>' \
     http://127.0.0.1:9090/api/v1/query | jq '.data.result'
   ```

6. **Sweep every dashboard panel.** This is the check that would have caught
   #85 immediately, and it covers panels beyond the one just changed:

   ```sh
   jq -r '.panels[] | .title as $t | (.targets[]? | select(.expr) | "\($t)\t\(.expr)")' \
     dev/grafana/dashboards/dagster-dashboard.json \
   | while IFS=$'\t' read -r title expr; do
       q=$(printf '%s' "$expr" | sed 's/\$__rate_interval/5m/g; s/\$__interval/1m/g')
       r=$(curl -s -G --data-urlencode "query=$q" http://127.0.0.1:9090/api/v1/query)
       n=$(printf '%s' "$r" | jq '.data.result | length // 0')
       s=$(printf '%s' "$r" | jq -r '.status')
       if [ "$s" != "success" ]; then m="ERROR"; elif [ "$n" -eq 0 ]; then m="EMPTY"; else m="ok   "; fi
       printf '%s  %-36s %s series\n' "$m" "$title" "$n"
     done
   ```

   `EMPTY` is not automatically a bug — it means *find out why*. Either the
   condition wasn't created (step 3), or the metric is broken. Never leave an
   `EMPTY` unexplained; that is exactly the state #85 lived in.

7. **Check the panel has somewhere to render.** If the metric is new and no
   panel queries it, add one to
   `dev/grafana/dashboards/dagster-dashboard.json`. A metric nobody can see
   is a metric nobody will notice is broken. CI requires the file stay
   `jq`-formatted:

   ```sh
   diff <(jq . dev/grafana/dashboards/dagster-dashboard.json) dev/grafana/dashboards/dagster-dashboard.json
   ```

8. **Record what was observed in the PR.** Paste the actual `/metrics` lines
   and the panel sweep output. "Verified locally" without the output is not
   evidence — for #85 the unit tests were green the whole time.

9. **Tear down:**

   ```sh
   docker compose down --remove-orphans
   ```

## When a metric can't be produced locally

Some can't be, and that's worth stating rather than papering over. A
workspace-level `PythonError` needs a deliberately broken Dagster, so #69
was covered by mocks and the PR said so explicitly. That's fine — what isn't
fine is claiming end-to-end verification that didn't happen. Say which parts
were checked live and which were mocked.
