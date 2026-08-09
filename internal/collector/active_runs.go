package collector

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	activeStatuses = []string{"QUEUED", "STARTING", "STARTED"}
)

type ActiveRunKey struct {
	JobName      string
	LocationName string
	Status       string
}

// activeRunAggregate rolls up every active run sharing an ActiveRunKey
// (job x location x status) into the two numbers dagster_active_runs and
// dagster_active_run_duration_seconds report: how many there are, and how
// long the longest-waiting/longest-running one among them has been in that
// state. Using run_id as a label instead would grow unbounded, so per-run
// detail is deliberately not kept past this aggregation.
type activeRunAggregate struct {
	count      int
	maxElapsed float64
}

// elapsedSince returns how long run has been in its current status, as of
// now. Dagster only bumps a run's updateTime on a run-level status-transition
// event (RUN_ENQUEUED/RUN_STARTING/RUN_START/...), not on step-level events
// (op start/success, asset materializations, ...) — see
// EVENT_TYPE_TO_PIPELINE_RUN_STATUS / SqlRunStorage.handle_run_event in
// Dagster's source. So updateTime is exactly "when this run entered its
// current status" for every active status, without needing to special-case
// STARTED with a separate startTime field.
func elapsedSince(run Run, now time.Time) float64 {
	return now.Sub(time.Unix(int64(run.UpdateTime), 0)).Seconds()
}

// CollectActiveRuns fetches every QUEUED/STARTING/STARTED run and folds
// each into both activeRunAggregates (dagster_active_runs/
// dagster_active_run_duration_seconds) and concurrencyKeyBacklog
// (dagster_run_queue_concurrency_key_backlog) from a single GraphQL fetch —
// the latter only needs QUEUED runs' tags, which this call already reads
// on every page, so giving it its own separate query would just re-fetch
// the same runs twice per scrape for no reason.
func CollectActiveRuns(ctx context.Context, c *DagsterCollector) error {
	aggregates := make(map[ActiveRunKey]activeRunAggregate)
	backlog := make(map[string]int)
	now := time.Now()

	err := fetchRunPages(ctx, activeStatuses, 0, c.dagsterGraphQLEndpoint, c.runsPageSize, func(page []Run) error {
		for _, run := range page {
			location := unknownLocationName
			if run.RepositoryOrigin != nil {
				location = run.RepositoryOrigin.RepositoryLocationName
			}
			key := ActiveRunKey{
				JobName:      run.JobName,
				LocationName: location,
				Status:       run.Status,
			}

			agg := aggregates[key]
			agg.count++
			if elapsed := elapsedSince(run, now); elapsed > agg.maxElapsed {
				agg.maxElapsed = elapsed
			}
			aggregates[key] = agg

			countConcurrencyKeyBacklog(run, backlog)
		}
		return nil
	})
	if err != nil {
		log.Printf("failed to collect active runs from dagster: %v", err)
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for jobKey := range c.knownJobs {
		for _, status := range activeStatuses {
			key := ActiveRunKey{
				JobName:      jobKey.JobName,
				LocationName: jobKey.LocationName,
				Status:       status,
			}
			if _, ok := aggregates[key]; !ok {
				aggregates[key] = activeRunAggregate{}
			}
		}
	}

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

	c.activeRunAggregates = aggregates
	c.concurrencyKeyBacklog = backlog
	return nil
}

// reflectActiveRuns emits both dagster_active_runs and
// dagster_active_run_duration_seconds from a single locked pass over
// c.activeRunAggregates — they're two views of the same aggregate, so
// iterating it twice (with two separate locks) would be pure overhead.
func reflectActiveRuns(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, agg := range c.activeRunAggregates {
		status := strings.ToLower(key.Status)
		ch <- prometheus.MustNewConstMetric(
			c.activeRunsDesc,
			prometheus.GaugeValue,
			float64(agg.count),
			key.JobName,
			key.LocationName,
			status,
		)
		ch <- prometheus.MustNewConstMetric(
			c.activeRunDurationDesc,
			prometheus.GaugeValue,
			agg.maxElapsed,
			key.JobName,
			key.LocationName,
			status,
		)
	}
}
