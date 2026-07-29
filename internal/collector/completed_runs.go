package collector

import (
	"context"
	"log"
	"strings"
	"time"

	ttlcache "github.com/jellydator/ttlcache/v3"
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, result := range resp.Data.RunsOrError.Results {
		if item := c.processedRuns.Get(result.RunId); item != nil {
			continue
		}

		c.processedRuns.Set(result.RunId, struct{}{}, ttlcache.DefaultTTL)

		c.completedRunsCounter.WithLabelValues(result.JobName, strings.ToLower(result.Status)).Inc()
	}
}
