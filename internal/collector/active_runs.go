package collector

import (
	"context"
	"log"
)

var (
	activeStatuses = []string{"QUEUED", "STARTING", "STARTED"}
)

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

	statusCounts := make(map[string]int)
	for _, status := range activeStatuses {
		statusCounts[status] = 0
	}

	for _, run := range resp.Data.RunsOrError.Results {
		statusCounts[run.Status]++
	}

	for status, count := range statusCounts {
		c.activeRunsGauge.WithLabelValues(status).Set(float64(count))
	}
}
