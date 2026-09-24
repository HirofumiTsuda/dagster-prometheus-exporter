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

// CollectDefinitionsRoster fetches the full jobs+schedules+sensors roster
// in one GraphQL call (repositoriesOrError exposes all three as sibling
// fields on Repository) and updates every piece of exporter state derived
// from "what currently exists": known jobs (for completed-run
// counter/last-run-status pruning/seeding, unchanged from before
// schedules/sensors were added), known schedules with their current
// enabled/disabled status and most recent tick (see buildScheduleState in
// schedules.go), and the same for sensors (see buildSensorState in
// sensors.go).
func CollectDefinitionsRoster(ctx context.Context, c *DagsterCollector) error {
	req := getDefinitionsRosterRequest()

	resp, err := getDefinitionsRoster(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect definitions roster from dagster: %v", err)
		return err
	}

	knownJobs := buildKnownJobs(resp)
	scheduleStatus, scheduleTickStatus := buildScheduleState(resp)
	sensorStatus, sensorTickStatus := buildSensorState(resp)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.knownJobs = knownJobs
	c.scheduleStatus = scheduleStatus
	c.scheduleTickStatus = scheduleTickStatus
	c.sensorStatus = sensorStatus
	c.sensorTickStatus = sensorTickStatus

	retained := retainedJobs(c, knownJobs)

	pruneCompletedRunsCounter(c, retained)
	seedCompletedRunsCounter(c, knownJobs)
	pruneLastRunStatus(c, retained)

	return nil
}

// jobAbsenceGraceScrapes is how many consecutive definitions-roster
// responses a job has to be missing from before its series are pruned.
const jobAbsenceGraceScrapes = 3

// retainedJobs returns the set of jobs whose series the pruners should keep:
// everything in the current roster, plus jobs that are absent from it but
// whose absence isn't (yet) evidence that the job is gone.
//
// repositoriesOrError omits a code location that fails to load instead of
// erroring out, so "deleted from Dagster" and "its code location is broken
// right now" look identical in the roster. Pruning on the first absence
// therefore resets dagster_completed_runs_total to 0 and drops
// dagster_last_run_info for every job in a location that a bad deploy broke
// — a counter reset that makes increase() over the outage overcount, and
// silenced last-run alerts for exactly the jobs that just broke.
//
// Two things hold a job back. A location that CollectCodeLocationStatus
// currently reports as failing to load (workspaceOrError still lists it,
// unlike repositoriesOrError) explains the absence outright, so its jobs are
// retained indefinitely. Everything else is retained for a few scrapes, which
// covers a location that's broken but whose load error hasn't landed in
// c.codeLocationLoadError yet — the two collectors run concurrently, so that
// map can be one cycle behind the roster.
//
// Callers must hold c.mutex.
func retainedJobs(c *DagsterCollector, known map[JobKey]struct{}) map[JobKey]struct{} {
	retained := make(map[JobKey]struct{}, len(known))
	for key := range known {
		retained[key] = struct{}{}
	}

	absence := make(map[JobKey]int)
	consider := func(key JobKey) {
		if _, ok := known[key]; ok {
			return
		}
		if _, ok := absence[key]; ok {
			return
		}
		if c.codeLocationLoadError[key.LocationName] {
			absence[key] = c.jobAbsenceStreak[key]
			retained[key] = struct{}{}
			return
		}
		streak := c.jobAbsenceStreak[key] + 1
		if streak >= jobAbsenceGraceScrapes {
			return
		}
		absence[key] = streak
		retained[key] = struct{}{}
	}

	for key := range c.trackedCompletedRunKeys {
		consider(key)
	}
	for key := range c.lastRunStatus {
		consider(key)
	}

	c.jobAbsenceStreak = absence

	return retained
}
