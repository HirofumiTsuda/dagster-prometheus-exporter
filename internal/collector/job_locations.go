package collector

import (
	"context"
	"log"
)

func CollectJobLocations(ctx context.Context, c *DagsterCollector) {
	req := getJobLocationsRequest()

	resp, err := getJobLocations(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect job locations from dagster: %v", err)
		return
	}

	locations := make(map[string]string)
	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, job := range repo.Jobs {
			locations[job.Name] = repo.Location.Name
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.jobLocations = locations
}
