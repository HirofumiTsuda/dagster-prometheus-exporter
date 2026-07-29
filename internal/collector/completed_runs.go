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

func getCompletedRunsRequest(updatedAfter float64) *GraphQLRequest {
	variables := map[string]interface{}{
		"statuses":     completedStatuses,
		"updatedAfter": updatedAfter,
	}
	return &GraphQLRequest{
		Query:     runsQuery,
		Variables: variables,
	}
}

func getUpdatedAfter(base time.Time, lookbackWindow time.Duration) float64 {
	return float64(base.Add(-lookbackWindow).Unix())
}

func CollectCompletedRuns(ctx context.Context, c *DagsterCollector) {
	now := time.Now()
	updatedAfter := getUpdatedAfter(now, c.lookbackWindow)
	req := getCompletedRunsRequest(updatedAfter)

	resp, err := getRuns(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect active runs from dagster: %v", err)
		return
	}
	type latestRun struct {
		status  string
		endTime float64
	}
	latest := make(map[JobKey]latestRun)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, result := range resp.Data.RunsOrError.Results {
		location := unknownLocationName
		if result.RepositoryOrigin != nil {
			location = result.RepositoryOrigin.RepositoryLocationName
		}

		key := JobKey{JobName: result.JobName, LocationName: location}
		if current, ok := latest[key]; !ok || result.EndTime > current.endTime {
			latest[key] = latestRun{status: result.Status, endTime: result.EndTime}
		}

		if item := c.processedRuns.Get(result.RunId); item != nil {
			continue
		}

		c.processedRuns.Set(result.RunId, struct{}{}, ttlcache.DefaultTTL)

		c.completedRunsCounter.WithLabelValues(result.JobName, location, strings.ToLower(result.Status)).Inc()
	}

	lastRunStatus := make(map[JobKey]string, len(latest))
	for key, run := range latest {
		lastRunStatus[key] = run.status
	}
	c.lastRunStatus = lastRunStatus
}

func reflectLastRunStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, status := range c.lastRunStatus {
		ch <- prometheus.MustNewConstMetric(
			c.lastRunStatusDesc,
			prometheus.GaugeValue,
			1,
			key.JobName,
			key.LocationName,
			strings.ToLower(status),
		)
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
	}
}

// pruneCompletedRunsCounter deletes series for jobs that existed in previous
// but no longer exist in known, so metrics for removed jobs stop being reported.
// Callers must hold c.mutex.
func pruneCompletedRunsCounter(c *DagsterCollector, previous, known map[JobKey]struct{}) {
	for key := range previous {
		if _, ok := known[key]; ok {
			continue
		}
		for _, status := range completedStatuses {
			c.completedRunsCounter.DeleteLabelValues(key.JobName, key.LocationName, strings.ToLower(status))
		}
	}
}
