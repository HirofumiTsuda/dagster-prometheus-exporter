package collector

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
)

// opPoolConcurrencyEntry is one op pool's most recently observed slot
// accounting.
//
// A step behind a pool passes through up to three states, not two: pending
// (waiting for a slot, unassigned) -> assigned (a slot has been reserved for
// it, but it hasn't started) -> active (the slot is claimed and the step is
// running). Dropping the "assigned" state would undercount how many steps
// are actually behind a pool, so all three are tracked separately rather
// than collapsing assigned into pending or active.
type opPoolConcurrencyEntry struct {
	activeSlots   int
	assignedSteps int
	pendingSteps  int
	// hasLimit gates emitting dagster_op_pool_concurrency_limit, the same
	// pattern reflectAssetStatus uses for lastMaterializationStatus == "":
	// limit is nullable in the schema (see GraphQLConcurrencyLimitsResponse),
	// so a pool with neither slots nor a configured default has none to
	// report.
	hasLimit          bool
	limit             int
	usingDefaultLimit bool
}

// CollectOpPoolConcurrency reports Dagster's op/step "pool" concurrency
// accounting: how many slots are claimed, how many steps are queued behind
// a pool, and its effective limit.
//
// This is a different mechanism from dagster_run_queue_concurrency_key_backlog
// (see concurrencyKeyTag's doc comment in collector.go), which tracks whole
// *runs* stuck behind a run-level dagster/concurrency_key tag.
// instance.concurrencyLimits is exactly the wrong field for that question —
// but it's exactly the right one for op-pool visibility, a genuinely
// separate gap (#111). The two mechanisms' labels are kept visibly distinct
// (concurrency_key vs pool below) so a reader doesn't conflate them despite
// the similar metric names.
//
// Unlike the run-queue backlog, no exporter-side zero-fill is needed here.
// instance.concurrencyLimits is sourced from Dagster's own event-log storage
// (SqlEventLogStorage.get_concurrency_keys, read against the pinned
// 1.13.15), which persistently remembers every pool that has ever claimed a
// slot and keeps returning it -- with zero counts -- once it goes idle. So
// CollectOpPoolConcurrency just replaces c.opPoolConcurrency wholesale on
// every scrape, the same pattern as CollectDaemonHealth/CollectAssetStatus:
// a pool that has never been used simply doesn't appear, which is the
// correct "never seen" signal without this exporter growing an unbounded
// map of its own the way #81 flagged for the run-queue backlog.
//
// instance and concurrencyLimits are both top-level, non-null fields -- not
// reachable via repositoriesOrError -- so this is its own query and its own
// collector, the same reasoning as CollectDaemonHealth.
func CollectOpPoolConcurrency(ctx context.Context, c *DagsterCollector) error {
	req := getConcurrencyLimitsRequest()

	resp, err := getConcurrencyLimits(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect op pool concurrency from dagster: %v", err)
		return err
	}

	limits := resp.Data.Instance.ConcurrencyLimits
	entries := make(map[string]opPoolConcurrencyEntry, len(limits))
	for _, l := range limits {
		entry := opPoolConcurrencyEntry{
			activeSlots:   l.ActiveSlotCount,
			assignedSteps: l.AssignedStepCount,
			pendingSteps:  l.PendingStepCount,
		}
		if l.Limit != nil {
			entry.hasLimit = true
			entry.limit = *l.Limit
			if l.UsingDefaultLimit != nil {
				entry.usingDefaultLimit = *l.UsingDefaultLimit
			}
		}
		entries[l.ConcurrencyKey] = entry
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.opPoolConcurrency = entries

	return nil
}

// reflectOpPoolConcurrency emits dagster_op_pool_concurrency_active_slots,
// _assigned_steps, _pending_steps, and _limit from a single locked pass, the
// same reasoning as reflectDaemonHealth: all four come from one entry per
// pool.
func reflectOpPoolConcurrency(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for pool, entry := range c.opPoolConcurrency {
		ch <- prometheus.MustNewConstMetric(
			c.opPoolActiveSlotsDesc,
			prometheus.GaugeValue,
			float64(entry.activeSlots),
			pool,
		)
		ch <- prometheus.MustNewConstMetric(
			c.opPoolAssignedStepsDesc,
			prometheus.GaugeValue,
			float64(entry.assignedSteps),
			pool,
		)
		ch <- prometheus.MustNewConstMetric(
			c.opPoolPendingStepsDesc,
			prometheus.GaugeValue,
			float64(entry.pendingSteps),
			pool,
		)

		if entry.hasLimit {
			usingDefaultLimit := "false"
			if entry.usingDefaultLimit {
				usingDefaultLimit = "true"
			}
			ch <- prometheus.MustNewConstMetric(
				c.opPoolLimitDesc,
				prometheus.GaugeValue,
				float64(entry.limit),
				pool,
				usingDefaultLimit,
			)
		}
	}
}
