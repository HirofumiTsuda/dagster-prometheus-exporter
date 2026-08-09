package collector

import (
	"context"
	"log"
)

type JobKey struct {
	JobName      string
	LocationName string
}

// CollectDefinitionsRoster fetches the full jobs+schedules roster in one
// GraphQL call (repositoriesOrError exposes both as sibling fields on
// Repository) and updates every piece of exporter state derived from "what
// currently exists": known jobs (for completed-run counter/last-run-status
// pruning/seeding, unchanged from before schedules were added), and known
// schedules with their current enabled/disabled status and most recent
// tick.
func CollectDefinitionsRoster(ctx context.Context, c *DagsterCollector) error {
	req := getDefinitionsRosterRequest()

	resp, err := getDefinitionsRoster(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect definitions roster from dagster: %v", err)
		return err
	}

	knownJobs := make(map[JobKey]struct{})
	scheduleStatus := make(map[ScheduleKey]string)
	scheduleTickStatus := make(map[ScheduleKey]scheduleTickEntry)

	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, job := range repo.Jobs {
			knownJobs[JobKey{JobName: job.Name, LocationName: repo.Location.Name}] = struct{}{}
		}

		for _, schedule := range repo.Schedules {
			key := ScheduleKey{ScheduleName: schedule.Name, LocationName: repo.Location.Name}
			scheduleStatus[key] = schedule.ScheduleState.Status

			// ticks(limit: 1) returns the single most recent tick, newest
			// first; a schedule that has never fired yet returns an empty
			// list, and simply has no entry here (no seeded value — same
			// rationale as dagster_last_run_info for a job that's never run).
			if len(schedule.ScheduleState.Ticks) > 0 {
				tick := schedule.ScheduleState.Ticks[0]
				scheduleTickStatus[key] = scheduleTickEntry{status: tick.Status, timestamp: tick.Timestamp}
			}
		}
	}

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
