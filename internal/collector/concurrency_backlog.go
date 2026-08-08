package collector

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
)

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
// QUEUED run's own tags, which is what this collector does.
const concurrencyKeyTag = "dagster/concurrency_key"

var queuedStatus = []string{"QUEUED"}

// CollectConcurrencyKeyBacklog counts, per concurrency_key tag value, how
// many runs are currently QUEUED because of that key's run-queue
// concurrency limit — i.e. how large the backlog behind it is right now.
func CollectConcurrencyKeyBacklog(ctx context.Context, c *DagsterCollector) error {
	backlog := make(map[string]int)

	err := fetchRunPages(ctx, queuedStatus, 0, c.dagsterGraphQLEndpoint, c.runsPageSize, func(page []Run) error {
		for _, run := range page {
			for _, tag := range run.Tags {
				if tag.Key == concurrencyKeyTag {
					backlog[tag.Value]++
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("failed to collect run queue concurrency-key backlog from dagster: %v", err)
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Zero-fill every concurrency key we've previously reported backlog
	// for, even if it has none right now, so its series doesn't just
	// disappear from Grafana — "0" is a meaningful, distinct signal from
	// "this key has never been seen." There's no upfront list of
	// configured concurrency keys to seed from (unlike knownJobs), so
	// "every key ever observed" is the best available substitute.
	for key := range c.concurrencyKeyBacklog {
		if _, ok := backlog[key]; !ok {
			backlog[key] = 0
		}
	}

	c.concurrencyKeyBacklog = backlog
	return nil
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
