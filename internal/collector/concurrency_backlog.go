package collector

import "github.com/prometheus/client_golang/prometheus"

// concurrencyKeyTag is the run tag Dagster attaches when a run is subject
// to a tag-based run-queue concurrency limit (dagster.yaml's
// concurrency.runs.tag_concurrency_limits, granularity "run"). Note this is
// a different mechanism from Dagster's op/step "pool" concurrency limits —
// those are exposed via the instance.concurrencyLimits GraphQL query, but
// that query reads from a separate op-pool-specific store and does *not*
// reflect tag-based run-queue backlog at all (confirmed against Dagster's
// dagster_graphql/schema/instance.py: resolve_concurrencyLimits sources
// from event_log_storage.get_concurrency_info, which tracks op-pool claimed
// slots/pending steps, not queued runs). So the only way to see how many
// runs are backlogged behind a given concurrency key is to read it off each
// QUEUED run's own tags, which is what CollectActiveRuns does when it calls
// countConcurrencyKeyBacklog below.
const concurrencyKeyTag = "dagster/concurrency_key"

// countConcurrencyKeyBacklog increments backlog[value] if run is QUEUED and
// carries a dagster/concurrency_key tag. Folded into CollectActiveRuns
// (which already fetches every QUEUED/STARTING/STARTED run, tags included)
// rather than being its own collector with its own GraphQL query for the
// same data.
func countConcurrencyKeyBacklog(run Run, backlog map[string]int) {
	if run.Status != "QUEUED" {
		return
	}
	for _, tag := range run.Tags {
		if tag.Key == concurrencyKeyTag {
			backlog[tag.Value]++
		}
	}
}

func reflectConcurrencyKeyBacklog(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, count := range c.concurrencyKeyBacklog {
		ch <- prometheus.MustNewConstMetric(
			c.concurrencyKeyBacklogDesc,
			prometheus.GaugeValue,
			float64(count),
			key,
		)
	}
}
