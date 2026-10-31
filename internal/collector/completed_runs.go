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
	completedStatuses = []string{"CANCELED", "FAILURE", "SUCCESS"}
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

	err := fetchRunPages(ctx, completedStatuses, updatedAfter, c.client, c.runsPageSize, func(page []Run) error {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		for _, result := range page {
			if result.UpdateTime > maxUpdateTimeSeen {
				maxUpdateTimeSeen = result.UpdateTime
			}

			location := result.location()
			key := JobKey{JobName: result.JobName, LocationName: location}
			updateLastRun(c, key, result)
			if result.Status == "SUCCESS" && result.EndTime > c.lastSuccessTime[key] {
				c.lastSuccessTime[key] = result.EndTime
			}

			if item := c.processedRuns.Get(result.RunId); item != nil {
				continue
			}

			// A run that finished before this scrape's window and is back only
			// because something touched it later is not a new completion.
			// Adding a tag bumps updateTime (and so brings the run back), and
			// once the run's processedRuns entry has expired it would be
			// counted a second time. EndTime 0 means Dagster recorded none, so
			// the run cannot be judged and is counted as before.
			if result.EndTime > 0 && result.EndTime < updatedAfter {
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

// lastRunEntry tracks the status and duration of the most recently completed
// run seen so far for a job, so dagster_last_run_info/dagster_last_run_duration_seconds
// keep reporting it even after the run falls outside the completed-runs
// lookback window.
type lastRunEntry struct {
	runID    string
	status   string
	endTime  float64
	duration float64
	// previous is the entry this one replaced, kept so updateLastRun can put
	// it back if this run turns out to be a failed attempt Dagster retries.
	previous *lastRunEntry
}

// willRetryTag is the tag Dagster puts on a failed run when run retries are
// enabled: "true" if it will launch another attempt, "false" on the last one.
// It is written right after the run's FAILURE status, as a separate update, so
// a scrape can see the failed run before its tag. Writing the tag bumps the
// run's updateTime, so the run comes back on a later scrape with it set.
const willRetryTag = "dagster/will_retry"

func (r Run) willRetry() bool {
	for _, tag := range r.Tags {
		if tag.Key == willRetryTag {
			return tag.Value == "true"
		}
	}
	return false
}

// updateLastRun records a completed run as its job's latest outcome, unless it
// is a failed attempt Dagster will retry. The retry's result is the outcome, so
// dagster_last_run_info doesn't flip to failure while a retry fixes it. If the
// attempt was already recorded, because a scrape saw it fail before its
// will_retry tag was written, the outcome it replaced is put back.
//
// One level of previous is enough: Dagster only launches the retry after the
// tag is written, so an attempt is undone before a later run of the job can be
// recorded on top of it.
//
// Callers must hold c.mutex.
func updateLastRun(c *DagsterCollector, key JobKey, run Run) {
	current, ok := c.lastRunStatus[key]
	if run.willRetry() {
		if ok && current.runID == run.RunId {
			if current.previous != nil {
				c.lastRunStatus[key] = *current.previous
			} else {
				delete(c.lastRunStatus, key)
			}
		}
		return
	}
	if ok && run.EndTime <= current.endTime {
		return
	}
	entry := lastRunEntry{
		runID:    run.RunId,
		status:   run.Status,
		endTime:  run.EndTime,
		duration: run.EndTime - run.CreationTime,
	}
	if ok {
		previous := current
		previous.previous = nil
		entry.previous = &previous
	}
	c.lastRunStatus[key] = entry
}

// reflectLastRun emits dagster_last_run_info, dagster_last_run_duration_seconds
// and dagster_last_run_end_timestamp_seconds from a single locked pass over
// c.lastRunStatus — they're views of the same underlying entry, so iterating
// it once per metric (with separate locks) would be pure overhead — and
// dagster_last_success_timestamp_seconds from c.lastSuccessTime.
func reflectLastRun(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, entry := range c.lastRunStatus {
		status := strings.ToLower(entry.status)
		ch <- prometheus.MustNewConstMetric(
			c.lastRunStatusDesc,
			prometheus.GaugeValue,
			1,
			key.JobName,
			key.LocationName,
			status,
		)
		ch <- prometheus.MustNewConstMetric(
			c.lastRunDurationDesc,
			prometheus.GaugeValue,
			entry.duration,
			key.JobName,
			key.LocationName,
			status,
		)
		// Not exported as 0 for a run without an end time: 0 reads as 1970,
		// which every time() - metric staleness alert would fire on.
		if entry.endTime > 0 {
			ch <- prometheus.MustNewConstMetric(
				c.lastRunEndTimestampDesc,
				prometheus.GaugeValue,
				entry.endTime,
				key.JobName,
				key.LocationName,
				status,
			)
		}
	}
	for key, endTime := range c.lastSuccessTime {
		ch <- prometheus.MustNewConstMetric(
			c.lastSuccessTimestampDesc,
			prometheus.GaugeValue,
			endTime,
			key.JobName,
			key.LocationName,
		)
	}
}

// pruneLastRunStatus deletes cached last-run status and last success time for
// jobs that no longer exist, so dagster_last_run_info doesn't keep reporting
// stale entries forever for removed jobs. retained is the roster plus the jobs
// whose absence from it isn't conclusive yet (see retainedJobs).
// Callers must hold c.mutex.
func pruneLastRunStatus(c *DagsterCollector, retained map[JobKey]struct{}) {
	for key := range c.lastRunStatus {
		if _, ok := retained[key]; !ok {
			delete(c.lastRunStatus, key)
		}
	}
	for key := range c.lastSuccessTime {
		if _, ok := retained[key]; !ok {
			delete(c.lastSuccessTime, key)
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
// isn't in retained (the roster plus the jobs whose absence from it isn't
// conclusive yet — see retainedJobs). This covers both jobs removed from
// Dagster and runs whose repositoryOrigin recorded a location name (e.g. an
// old, since-renamed auto-generated grpc location name) that never matches a
// live location, so those don't linger in dagster_completed_runs_total
// forever.
// Callers must hold c.mutex.
func pruneCompletedRunsCounter(c *DagsterCollector, retained map[JobKey]struct{}) {
	for key := range c.trackedCompletedRunKeys {
		if _, ok := retained[key]; ok {
			continue
		}
		for _, status := range completedStatuses {
			c.completedRunsCounter.DeleteLabelValues(key.JobName, key.LocationName, strings.ToLower(status))
		}
		delete(c.trackedCompletedRunKeys, key)
	}
}
