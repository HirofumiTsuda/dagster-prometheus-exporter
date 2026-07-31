package collector

import (
	"context"
	"log"
	"strings"
	"time"

	ttlcache "github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	completedStatuses = []string{"FAILURE", "SUCCESS"}
)

func getUpdatedAfter(base time.Time, lookbackWindow time.Duration) float64 {
	return float64(base.Add(-lookbackWindow).Unix())
}

func CollectCompletedRuns(ctx context.Context, c *DagsterCollector) error {
	c.mutex.Lock()
	lastSeenUpdateTime := c.lastSeenUpdateTime
	c.mutex.Unlock()

	updatedAfter := getUpdatedAfter(time.Now(), c.lookbackWindow)
	if lastSeenUpdateTime > 0 {
		updatedAfter = lastSeenUpdateTime - c.runsUpdatedAfterSafetyMargin.Seconds()
	}

	var maxUpdateTimeSeen float64

	err := fetchRunPages(ctx, completedStatuses, updatedAfter, c.dagsterGraphQLEndpoint, c.runsPageSize, func(page []Run) error {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		for _, result := range page {
			if result.UpdateTime > maxUpdateTimeSeen {
				maxUpdateTimeSeen = result.UpdateTime
			}

			location := unknownLocationName
			if result.RepositoryOrigin != nil {
				location = result.RepositoryOrigin.RepositoryLocationName
			}

			key := JobKey{JobName: result.JobName, LocationName: location}
			if current, ok := c.lastRunStatus[key]; !ok || result.EndTime > current.endTime {
				c.lastRunStatus[key] = lastRunEntry{status: result.Status, endTime: result.EndTime}
			}

			if item := c.processedRuns.Get(result.RunId); item != nil {
				continue
			}

			c.processedRuns.Set(result.RunId, struct{}{}, ttlcache.DefaultTTL)

			c.completedRunsCounter.WithLabelValues(result.JobName, location, strings.ToLower(result.Status)).Inc()
			c.trackedCompletedRunKeys[key] = struct{}{}
		}
		return nil
	})
	if err != nil {
		log.Printf("failed to collect completed runs from dagster: %v", err)
		return err
	}

	if maxUpdateTimeSeen > lastSeenUpdateTime {
		c.mutex.Lock()
		c.lastSeenUpdateTime = maxUpdateTimeSeen
		c.mutex.Unlock()
	}

	return nil
}

// lastRunEntry tracks the status of the most recently completed run seen so
// far for a job, so dagster_last_run_info keeps reporting it even after the
// run falls outside the completed-runs lookback window.
type lastRunEntry struct {
	status  string
	endTime float64
}

func reflectLastRunStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, entry := range c.lastRunStatus {
		ch <- prometheus.MustNewConstMetric(
			c.lastRunStatusDesc,
			prometheus.GaugeValue,
			1,
			key.JobName,
			key.LocationName,
			strings.ToLower(entry.status),
		)
	}
}

// pruneLastRunStatus deletes cached last-run status for jobs that no longer
// exist, so dagster_last_run_info doesn't keep reporting stale entries
// forever for removed jobs. Callers must hold c.mutex.
func pruneLastRunStatus(c *DagsterCollector, known map[JobKey]struct{}) {
	for key := range c.lastRunStatus {
		if _, ok := known[key]; !ok {
			delete(c.lastRunStatus, key)
		}
	}
}

// seedCompletedRunsCounter ensures every known job has a 0-valued series for
// each completed status, so jobs that have never run show 0 instead of no data.
// Callers must hold c.mutex.
func seedCompletedRunsCounter(c *DagsterCollector, known map[JobKey]struct{}) {
	for key := range known {
		for _, status := range completedStatuses {
			c.completedRunsCounter.WithLabelValues(key.JobName, key.LocationName, strings.ToLower(status)).Add(0)
		}
		c.trackedCompletedRunKeys[key] = struct{}{}
	}
}

// pruneCompletedRunsCounter deletes every tracked (job, location) series that
// isn't in known. This covers both jobs removed from Dagster and runs whose
// repositoryOrigin recorded a location name (e.g. an old, since-renamed
// auto-generated grpc location name) that never matches a live location, so
// those don't linger in dagster_completed_runs_total forever.
// Callers must hold c.mutex.
func pruneCompletedRunsCounter(c *DagsterCollector, known map[JobKey]struct{}) {
	for key := range c.trackedCompletedRunKeys {
		if _, ok := known[key]; ok {
			continue
		}
		for _, status := range completedStatuses {
			c.completedRunsCounter.DeleteLabelValues(key.JobName, key.LocationName, strings.ToLower(status))
		}
		delete(c.trackedCompletedRunKeys, key)
	}
}
