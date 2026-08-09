package collector

import (
	"context"
	"log"
)

type JobKey struct {
	JobName      string
	LocationName string
}

// buildKnownJobs extracts the known-jobs set from a definitions-roster
// response. Pulled out of CollectDefinitionsRoster as its own pure function
// so it can be unit-tested directly, without an HTTP mock.
func buildKnownJobs(resp *GraphQLDefinitionsRosterResponse) map[JobKey]struct{} {
	known := make(map[JobKey]struct{})
	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, job := range repo.Jobs {
			known[JobKey{JobName: job.Name, LocationName: repo.Location.Name}] = struct{}{}
		}
	}
	return known
}

// CollectDefinitionsRoster fetches the full jobs+schedules roster in one
// GraphQL call (repositoriesOrError exposes both as sibling fields on
// Repository) and updates every piece of exporter state derived from "what
// currently exists": known jobs (for completed-run counter/last-run-status
// pruning/seeding, unchanged from before schedules were added), and known
// schedules with their current enabled/disabled status and most recent
// tick (see buildScheduleState in schedules.go).
func CollectDefinitionsRoster(ctx context.Context, c *DagsterCollector) error {
	req := getDefinitionsRosterRequest()

	resp, err := getDefinitionsRoster(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect definitions roster from dagster: %v", err)
		return err
	}

	knownJobs := buildKnownJobs(resp)
	scheduleStatus, scheduleTickStatus := buildScheduleState(resp)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.knownJobs = knownJobs
	c.scheduleStatus = scheduleStatus
	c.scheduleTickStatus = scheduleTickStatus

	pruneCompletedRunsCounter(c, knownJobs)
	seedCompletedRunsCounter(c, knownJobs)
	pruneLastRunStatus(c, knownJobs)

	return nil
}
