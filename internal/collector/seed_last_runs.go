package collector

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"
)

// seedRunsPerJob is how many of a job's latest completed runs a seed reads,
// so the newest one that isn't a retried attempt is among them.
const seedRunsPerJob = 5

// seedJobsPerQuery bounds how many jobs one seed request covers. Not every job
// in one request: on a deployment with hundreds of jobs that request can
// outlast the scrape timeout on every attempt, while batches each land on
// their own and the next scrape carries on from there.
const seedJobsPerQuery = 50

// repositoryLabelTag is the hidden tag Dagster puts on every run, with the
// value "<repository>@<location>". Filtering on pipelineName alone would mix
// up two jobs of the same name in different code locations.
const repositoryLabelTag = ".dagster/repository"

type seedTarget struct {
	key        JobKey
	repository string
}

// seedLastRuns reads each roster job's latest runs from Dagster, once per job,
// into the last-run state behind dagster_last_run_info and its siblings and
// dagster_last_success_timestamp_seconds.
//
// The runs go through the same updateLastRun as the incremental fetch, so a
// newer run already recorded wins and retried attempts are skipped. They don't
// touch the completed-runs counters: counting history again would make every
// restart look like a burst of completions.
func seedLastRuns(ctx context.Context, c *DagsterCollector) error {
	c.mutex.Lock()
	var fresh, retry []seedTarget
	for key, repository := range c.jobRepositories {
		if _, done := c.seededJobs[key]; done {
			continue
		}
		target := seedTarget{key: key, repository: repository}
		if _, skipped := c.seedSkippedJobs[key]; skipped {
			retry = append(retry, target)
		} else {
			fresh = append(fresh, target)
		}
	}
	c.mutex.Unlock()
	sortSeedTargets(fresh)
	sortSeedTargets(retry)

	for batch := range slices.Chunk(fresh, seedJobsPerQuery) {
		read, failed, err := seedBatch(ctx, c, batch)
		if err != nil {
			return fmt.Errorf("failed to seed last runs: %w", err)
		}
		// Some jobs were read and some were not: Dagster is up and cannot
		// answer for these. When none was read it looks like Dagster is
		// failing as a whole, which seedBatch reports as an error instead.
		if read > 0 {
			for _, f := range failed {
				skipSeed(c, f.target, f.err)
			}
		}
	}

	// A skipped job is asked for on its own, so one that Dagster still cannot
	// answer costs one request and does not hold up the others.
	for _, target := range retry {
		if _, _, err := seedBatch(ctx, c, []seedTarget{target}); err != nil && !isInternalServerError(err) {
			return fmt.Errorf("failed to seed last runs: %w", err)
		}
	}
	return nil
}

func sortSeedTargets(targets []seedTarget) {
	slices.SortFunc(targets, func(a, b seedTarget) int {
		return strings.Compare(a.key.LocationName+"\x00"+a.key.JobName, b.key.LocationName+"\x00"+b.key.JobName)
	})
}

type seedFailure struct {
	target seedTarget
	err    error
}

// seedBatch reads the batch in one request. When Dagster answers 500, which
// is how it fails to serialize one job's runs, the batch is halved until the
// jobs it cannot answer for are alone, so they do not take the others with
// them. It returns how many jobs were read and which were not. The error is
// for what halving cannot help: the request not reaching Dagster, any other
// status, or no job in the batch being readable.
func seedBatch(ctx context.Context, c *DagsterCollector, batch []seedTarget) (read int, failed []seedFailure, err error) {
	read, failed, err = seedBatchSplitting(ctx, c, batch)
	if err == nil && read == 0 && len(failed) > 0 {
		err = failed[0].err
	}
	if err != nil {
		return 0, nil, err
	}
	// A job read now is no longer skipped.
	c.mutex.Lock()
	for _, target := range batch {
		if _, done := c.seededJobs[target.key]; done {
			delete(c.seedSkippedJobs, target.key)
		}
	}
	c.mutex.Unlock()
	return read, failed, nil
}

func seedBatchSplitting(ctx context.Context, c *DagsterCollector, batch []seedTarget) (read int, failed []seedFailure, err error) {
	latest, successes, err := getSeedRuns(ctx, batch, c.client)
	if err == nil {
		applySeedRuns(c, batch, latest, successes)
		return len(batch), nil, nil
	}
	if !isInternalServerError(err) || ctx.Err() != nil {
		return 0, nil, err
	}
	if len(batch) == 1 {
		return 0, []seedFailure{{target: batch[0], err: err}}, nil
	}
	mid := len(batch) / 2
	for _, half := range [][]seedTarget{batch[:mid], batch[mid:]} {
		r, f, halfErr := seedBatchSplitting(ctx, c, half)
		if halfErr != nil {
			return 0, nil, halfErr
		}
		read += r
		failed = append(failed, f...)
	}
	return read, failed, nil
}

// isInternalServerError is true for Dagster's own 500. A 502, 503 or 504 comes
// from a proxy in front of a Dagster that is down or restarting, which
// halving would only repeat.
func isInternalServerError(err error) bool {
	var status *statusError
	return errors.As(err, &status) && status.code == http.StatusInternalServerError
}

// skipSeed remembers a job Dagster cannot answer for, and says so once. It is
// asked for again on every scrape, since the run that breaks the answer can age
// out of the latest few.
func skipSeed(c *DagsterCollector, target seedTarget, err error) {
	c.mutex.Lock()
	_, known := c.seedSkippedJobs[target.key]
	c.seedSkippedJobs[target.key] = struct{}{}
	c.mutex.Unlock()
	if !known {
		log.Printf("last runs of job %q in location %q were not read: %v. The other jobs were, and this one is asked for again on every scrape", target.key.JobName, target.key.LocationName, err)
	}
}

// applySeedRuns records what a seed read for the jobs of a batch.
func applySeedRuns(c *DagsterCollector, batch []seedTarget, latest, successes [][]Run) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, target := range batch {
		for _, run := range latest[i] {
			updateLastRun(c, target.key, run)
		}
		for _, run := range successes[i] {
			if run.EndTime > c.lastSuccessTime[target.key] {
				c.lastSuccessTime[target.key] = run.EndTime
			}
		}
		c.seededJobs[target.key] = struct{}{}
	}
}

//go:embed queries/seed_runs_fragment.graphql
var seedRunsFragment string

// getSeedRunsRequest asks, for every target, for its latest completed runs
// (alias rN) and its latest successful run (sN), as aliased runsOrError fields
// of one request.
func getSeedRunsRequest(targets []seedTarget) *GraphQLRequest {
	var params, fields strings.Builder
	variables := make(map[string]interface{}, 2*len(targets))
	for i, target := range targets {
		tags := []map[string]string{{"key": repositoryLabelTag, "value": target.repository + "@" + target.key.LocationName}}
		variables[fmt.Sprintf("r%d", i)] = map[string]interface{}{"pipelineName": target.key.JobName, "statuses": completedStatuses, "tags": tags}
		variables[fmt.Sprintf("s%d", i)] = map[string]interface{}{"pipelineName": target.key.JobName, "statuses": []string{"SUCCESS"}, "tags": tags}
		fmt.Fprintf(&params, "$r%d: RunsFilter, $s%d: RunsFilter, ", i, i)
		fmt.Fprintf(&fields, "  r%d: runsOrError(filter: $r%d, limit: %d) { ...seedRuns }\n  s%d: runsOrError(filter: $s%d, limit: 1) { ...seedRuns }\n",
			i, i, seedRunsPerJob, i, i)
	}
	return &GraphQLRequest{
		Query:     "query SeedLastRuns(" + strings.TrimSuffix(params.String(), ", ") + ") {\n" + fields.String() + "}\n" + seedRunsFragment,
		Variables: variables,
	}
}

type seedRunsResponse struct {
	Data map[string]struct {
		Typename string   `json:"__typename"`
		Results  []Run    `json:"results"`
		Message  string   `json:"message"`
		Stack    []string `json:"stack"`
	} `json:"data"`
	graphQLErrors
}

// getSeedRuns returns, per target and in the targets' order, its latest
// completed runs and its latest successful run.
func getSeedRuns(ctx context.Context, targets []seedTarget, client dagsterClient) (latest, successes [][]Run, err error) {
	var resp seedRunsResponse
	if err := doGraphQL(ctx, getSeedRunsRequest(targets), client, &resp); err != nil {
		return nil, nil, err
	}
	latest = make([][]Run, len(targets))
	successes = make([][]Run, len(targets))
	for i := range targets {
		for _, alias := range []string{fmt.Sprintf("r%d", i), fmt.Sprintf("s%d", i)} {
			if field := resp.Data[alias]; field.Typename != "Runs" {
				return nil, nil, unexpectedUnionMember("runsOrError", "Runs", field.Typename, field.Message, field.Stack)
			}
		}
		latest[i] = resp.Data[fmt.Sprintf("r%d", i)].Results
		successes[i] = resp.Data[fmt.Sprintf("s%d", i)].Results
	}
	return latest, successes, nil
}
