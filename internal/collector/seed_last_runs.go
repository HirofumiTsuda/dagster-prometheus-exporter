package collector

import (
	"context"
	_ "embed"
	"fmt"
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
	var pending []seedTarget
	for key, repository := range c.jobRepositories {
		if _, done := c.seededJobs[key]; !done {
			pending = append(pending, seedTarget{key: key, repository: repository})
		}
	}
	c.mutex.Unlock()
	slices.SortFunc(pending, func(a, b seedTarget) int {
		return strings.Compare(a.key.LocationName+"\x00"+a.key.JobName, b.key.LocationName+"\x00"+b.key.JobName)
	})

	for batch := range slices.Chunk(pending, seedJobsPerQuery) {
		latest, successes, err := getSeedRuns(ctx, batch, c.client)
		if err != nil {
			return fmt.Errorf("failed to seed last runs: %w", err)
		}
		c.mutex.Lock()
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
		c.mutex.Unlock()
	}
	return nil
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
