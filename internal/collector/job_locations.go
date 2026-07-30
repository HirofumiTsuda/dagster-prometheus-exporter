package collector

import (
	"context"
	"log"
)

type JobKey struct {
	JobName      string
	LocationName string
}

func CollectJobLocations(ctx context.Context, c *DagsterCollector) {
	req := getJobLocationsRequest()

	resp, err := getJobLocations(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect job locations from dagster: %v", err)
		return
	}

	known := make(map[JobKey]struct{})
	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, job := range repo.Jobs {
			known[JobKey{JobName: job.Name, LocationName: repo.Location.Name}] = struct{}{}
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.knownJobs = known

	pruneCompletedRunsCounter(c, known)
	seedCompletedRunsCounter(c, known)
	pruneLastRunStatus(c, known)
}
