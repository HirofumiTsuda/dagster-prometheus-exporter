package collector

import (
	"context"
	"log"
	"strings"

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

func CollectActiveRuns(ctx context.Context, c *DagsterCollector) error {
	counts := make(map[ActiveRunKey]int)

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
			counts[key]++
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
			if _, ok := counts[key]; !ok {
				counts[key] = 0
			}
		}
	}

	c.activeRunsCounts = counts
	return nil
}

func reflectActiveRuns(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, count := range c.activeRunsCounts {
		ch <- prometheus.MustNewConstMetric(
			c.activeRunsDesc,
			prometheus.GaugeValue,
			float64(count),
			key.JobName,
			key.LocationName,
			strings.ToLower(key.Status),
		)
	}
}
