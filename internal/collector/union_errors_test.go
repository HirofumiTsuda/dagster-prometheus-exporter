package collector

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pythonErrorBody is what Dagster returns when a query fails on the Python
// side: HTTP 200, a well-formed body, no top-level "errors" array, and the
// union resolved to PythonError instead of the expected result type. The
// point of these tests is that this must not be mistaken for a successful
// query that happened to find nothing (issue #69).
func pythonErrorBody(field string) string {
	return `{"data": {"` + field + `": {
		"__typename": "PythonError",
		"message": "dagster exploded",
		"stack": ["Traceback (most recent call last):", "  raise Exception"]
	}}}`
}

// scriptedServer is a GraphQL stub whose response body can be swapped
// mid-test, so a collector can be brought to a known-good state first and
// only then be shown a failure — which is the only way to observe whether
// that earlier state survives the failure.
type scriptedServer struct {
	body atomic.Value
	url  string
}

func newScriptedServer(t *testing.T, body string) *scriptedServer {
	t.Helper()
	s := &scriptedServer{}
	s.body.Store(body)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(s.body.Load().(string)))
		assert.NoError(t, err)
	}))
	t.Cleanup(ts.Close)
	s.url = ts.URL
	return s
}

func (s *scriptedServer) serve(body string) { s.body.Store(body) }

func TestCollectDefinitionsRosterRejectsPythonErrorWithoutDiscardingState(t *testing.T) {
	const goodRoster = `{"data": {"repositoriesOrError": {
		"__typename": "RepositoryConnection",
		"nodes": [
			{
				"name": "repo", "location": {"name": "loc_a"},
				"jobs": [{"name": "job_a"}],
				"schedules": [{"name": "sched_a", "cronSchedule": "* * * * *", "scheduleState": {"status": "RUNNING", "ticks": []}}],
				"sensors": [{"name": "sensor_a", "sensorState": {"status": "RUNNING", "ticks": []}}]
			}
		]
	}}}`

	s := newScriptedServer(t, goodRoster)
	c := NewDagsterCollector(t.Context(), s.url, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectDefinitionsRoster(t.Context(), c))

	jobKey := JobKey{JobName: "job_a", LocationName: "loc_a"}
	require.Contains(t, c.knownJobs, jobKey)
	seededSeries := testutil.CollectAndCount(c.completedRunsCounter)
	require.Positive(t, seededSeries)

	// Accumulated across scrapes rather than refetched, so pruning it is
	// unrecoverable until the job next completes a run.
	lastRun := lastRunEntry{status: "SUCCESS", endTime: 100, duration: 42}
	c.lastRunStatus[jobKey] = lastRun

	s.serve(pythonErrorBody("repositoriesOrError"))

	err := CollectDefinitionsRoster(t.Context(), c)
	require.Error(t, err, "a PythonError must not be reported as a successful but empty roster")
	assert.Contains(t, err.Error(), "dagster exploded")

	assert.Contains(t, c.knownJobs, jobKey,
		"known jobs must survive a failed roster scrape")
	assert.Equal(t, seededSeries, testutil.CollectAndCount(c.completedRunsCounter),
		"dagster_completed_runs_total series must not be pruned by a failed roster scrape (pruning them looks like a counter reset to PromQL)")
	assert.Equal(t, lastRun, c.lastRunStatus[jobKey],
		"last-run state must not be pruned by a failed roster scrape")
	assert.Len(t, c.scheduleStatus, 1,
		"schedule status must not be replaced with an empty map by a failed roster scrape")
	assert.Len(t, c.sensorStatus, 1,
		"sensor status must not be replaced with an empty map by a failed roster scrape")
}

func TestRunCollectorsRejectRunsOrErrorFailures(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{
			name:     "python error",
			body:     pythonErrorBody("runsOrError"),
			contains: "dagster exploded",
		},
		{
			// Means the exporter's own filter is wrong, which is worth
			// surfacing loudly rather than reporting as "no runs".
			name:     "invalid filter",
			body:     `{"data": {"runsOrError": {"__typename": "InvalidPipelineRunsFilterError", "message": "bad filter"}}}`,
			contains: "bad filter",
		},
		{
			// Every query in queries/ selects __typename, so a response
			// without one isn't the shape this code was written against —
			// its empty result list can't be trusted either.
			name:     "missing typename",
			body:     `{"data": {"runsOrError": {"results": []}}}`,
			contains: "no __typename",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newScriptedServer(t, tc.body)
			c := NewDagsterCollector(t.Context(), s.url, time.Hour, time.Hour, 500, 5*time.Minute)

			activeErr := CollectActiveRuns(t.Context(), c)
			require.Error(t, activeErr, "CollectActiveRuns must not treat a failed runsOrError as zero active runs")
			assert.Contains(t, activeErr.Error(), tc.contains)

			completedErr := CollectCompletedRuns(t.Context(), c)
			require.Error(t, completedErr, "CollectCompletedRuns must not treat a failed runsOrError as zero completed runs")
			assert.Contains(t, completedErr.Error(), tc.contains)
		})
	}
}

func TestCollectActiveRunsRejectsPythonErrorWithoutDiscardingAggregates(t *testing.T) {
	const goodRuns = `{"data": {"runsOrError": {"__typename": "Runs", "results": [
		{
			"runId": "r1", "jobName": "job_a", "status": "STARTED",
			"creationTime": 100, "updateTime": 100,
			"repositoryOrigin": {"repositoryName": "repo", "repositoryLocationName": "loc_a"}
		}
	]}}}`

	s := newScriptedServer(t, goodRuns)
	c := NewDagsterCollector(t.Context(), s.url, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectActiveRuns(t.Context(), c))
	require.Len(t, c.activeRunAggregates, 1)

	s.serve(pythonErrorBody("runsOrError"))
	require.Error(t, CollectActiveRuns(t.Context(), c))

	assert.Len(t, c.activeRunAggregates, 1,
		"active-run aggregates are replaced wholesale on success, so a failed scrape must leave the previous ones in place rather than blanking them")
}

func TestCollectCodeLocationStatusRejectsPythonErrorWithoutDiscardingState(t *testing.T) {
	const goodWorkspace = `{"data": {"workspaceOrError": {
		"__typename": "Workspace",
		"locationEntries": [{"name": "loc_a", "locationOrLoadError": {"__typename": "RepositoryLocation"}}]
	}}}`

	s := newScriptedServer(t, goodWorkspace)
	c := NewDagsterCollector(t.Context(), s.url, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectCodeLocationStatus(t.Context(), c))
	require.Len(t, c.codeLocationLoadError, 1)

	s.serve(pythonErrorBody("workspaceOrError"))

	err := CollectCodeLocationStatus(t.Context(), c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dagster exploded")
	assert.Len(t, c.codeLocationLoadError, 1,
		"code location state must not be replaced with an empty map by a failed scrape")
}
