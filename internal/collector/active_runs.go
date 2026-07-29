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

func getActiveRunsRequest() *GraphQLRequest {
	variables := map[string]interface{}{
		"statuses":     activeStatuses,
		"updatedAfter": nil,
	}
	return &GraphQLRequest{
		Query:     runsQuery,
		Variables: variables,
	}
}

func CollectActiveRuns(ctx context.Context, c *DagsterCollector) {
	req := getActiveRunsRequest()

	resp, err := getRuns(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect active runs from dagster: %v", err)
		return
	}

	counts := make(map[ActiveRunKey]int)

	for _, run := range resp.Data.RunsOrError.Results {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.activeRunsCounts = counts
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
