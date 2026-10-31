package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedRun is a completed run as a seed response returns it.
func seedRun(id, status string, end int64, willRetry string) string {
	tags := ""
	if willRetry != "" {
		tags = fmt.Sprintf(`{"key":"dagster/will_retry","value":%q}`, willRetry)
	}
	return fmt.Sprintf(`{"runId":%q,"jobName":"job_a","status":%q,"creationTime":%d,"endTime":%d,"updateTime":%d,"tags":[%s],"repositoryOrigin":{"repositoryName":"repo","repositoryLocationName":"loc"}}`,
		id, status, end-10, end, end, tags)
}

// seedServer answers every SeedLastRuns request with latest (newest first) for
// each rN alias and success for each sN alias, and records the requests.
type seedServer struct {
	latest, success []string
	requests        atomic.Int32
	lastRequest     GraphQLRequest
	fail            atomic.Bool
}

func (s *seedServer) start(t *testing.T) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		if s.fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req GraphQLRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		s.lastRequest = req
		fields := make([]string, 0, len(req.Variables))
		for name := range req.Variables {
			runs := s.latest
			if strings.HasPrefix(name, "s") {
				runs = s.success
			}
			fields = append(fields, fmt.Sprintf(`%q:{"__typename":"Runs","results":[%s]}`, name, strings.Join(runs, ",")))
		}
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"data":{` + strings.Join(fields, ",") + `}}`))
		assert.NoError(t, err)
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}

func seedCollector(t *testing.T, url string, jobs ...JobKey) *DagsterCollector {
	t.Helper()
	c := NewDagsterCollector(t.Context(), url, "", time.Hour, time.Hour, 500, 5*time.Minute)
	c.jobRepositories = make(map[JobKey]string)
	for _, key := range jobs {
		c.jobRepositories[key] = "repo"
	}
	return c
}

var seedKey = JobKey{JobName: "job_a", LocationName: "loc"}

func TestSeedLastRunsRestoresTheLastRunAndLastSuccessWithoutCounting(t *testing.T) {
	s := &seedServer{
		latest:  []string{seedRun("run_c", "FAILURE", 300, "false"), seedRun("run_b", "FAILURE", 200, "true")},
		success: []string{seedRun("run_a", "SUCCESS", 100, "")},
	}
	c := seedCollector(t, s.start(t), seedKey)

	require.NoError(t, seedLastRuns(t.Context(), c))

	assert.Equal(t, "run_c", c.lastRunStatus[seedKey].runID)
	assert.Equal(t, "FAILURE", c.lastRunStatus[seedKey].status)
	assert.Equal(t, float64(100), c.lastSuccessTime[seedKey])
	assert.Equal(t, float64(0), testutil.ToFloat64(c.completedRunsCounter.WithLabelValues("job_a", "loc", "failure")),
		"seeding reads history; it must not count it as new completions")
}

func TestSeedLastRunsSkipsAnAttemptThatWillBeRetried(t *testing.T) {
	s := &seedServer{
		latest:  []string{seedRun("run_b", "FAILURE", 200, "true"), seedRun("run_a", "SUCCESS", 100, "")},
		success: []string{seedRun("run_a", "SUCCESS", 100, "")},
	}
	c := seedCollector(t, s.start(t), seedKey)

	require.NoError(t, seedLastRuns(t.Context(), c))
	assert.Equal(t, "run_a", c.lastRunStatus[seedKey].runID)
}

func TestSeedLastRunsAsksForEachJobInItsOwnLocation(t *testing.T) {
	s := &seedServer{}
	c := seedCollector(t, s.start(t), seedKey)

	require.NoError(t, seedLastRuns(t.Context(), c))

	wantTags := []interface{}{map[string]interface{}{"key": ".dagster/repository", "value": "repo@loc"}}
	assert.Equal(t, map[string]interface{}{
		"pipelineName": "job_a", "statuses": []interface{}{"CANCELED", "FAILURE", "SUCCESS"}, "tags": wantTags,
	}, s.lastRequest.Variables["r0"])
	assert.Equal(t, map[string]interface{}{
		"pipelineName": "job_a", "statuses": []interface{}{"SUCCESS"}, "tags": wantTags,
	}, s.lastRequest.Variables["s0"])
}

func TestSeedLastRunsReadsEachJobOnceInBatches(t *testing.T) {
	jobs := make([]JobKey, 120)
	for i := range jobs {
		jobs[i] = JobKey{JobName: fmt.Sprintf("job_%03d", i), LocationName: "loc"}
	}
	s := &seedServer{}
	c := seedCollector(t, s.start(t), jobs...)

	require.NoError(t, seedLastRuns(t.Context(), c))
	assert.Equal(t, int32(3), s.requests.Load(), "120 jobs in batches of %d", seedJobsPerQuery)
	assert.Len(t, c.seededJobs, 120)

	require.NoError(t, seedLastRuns(t.Context(), c))
	assert.Equal(t, int32(3), s.requests.Load(), "jobs already seeded are not read again")

	c.jobRepositories[JobKey{JobName: "new_job", LocationName: "loc"}] = "repo"
	require.NoError(t, seedLastRuns(t.Context(), c))
	assert.Equal(t, int32(4), s.requests.Load(), "a job that appears later is read once")
}

func TestSeedLastRunsKeepsANewerRunAlreadyRecorded(t *testing.T) {
	s := &seedServer{latest: []string{seedRun("run_a", "FAILURE", 100, "")}}
	c := seedCollector(t, s.start(t), seedKey)
	c.lastRunStatus[seedKey] = lastRunEntry{runID: "run_b", status: "SUCCESS", endTime: 200}

	require.NoError(t, seedLastRuns(t.Context(), c))
	assert.Equal(t, "run_b", c.lastRunStatus[seedKey].runID)
}

func TestCollectCompletedRunsSeedsTheRosterJobs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		body := runsResponse()
		if strings.Contains(req.Query, "SeedLastRuns") {
			body = `{"data":{"r0":{"__typename":"Runs","results":[` + seedRun("run_a", "SUCCESS", 100, "") + `]},` +
				`"s0":{"__typename":"Runs","results":[` + seedRun("run_a", "SUCCESS", 100, "") + `]}}}`
		}
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(body))
		assert.NoError(t, err)
	}))
	defer ts.Close()
	c := seedCollector(t, ts.URL, seedKey)

	require.NoError(t, CollectCompletedRuns(t.Context(), c))
	assert.Equal(t, map[string]float64{"job_name=job_a,location=loc": 100},
		gaugeValues(t, c, "dagster_last_success_timestamp_seconds"))
}

func TestSeedLastRunsRetriesJobsAfterAFailedRequest(t *testing.T) {
	s := &seedServer{success: []string{seedRun("run_a", "SUCCESS", 100, "")}}
	c := seedCollector(t, s.start(t), seedKey)

	s.fail.Store(true)
	require.Error(t, seedLastRuns(t.Context(), c))
	assert.NotContains(t, c.seededJobs, seedKey)

	s.fail.Store(false)
	require.NoError(t, seedLastRuns(t.Context(), c))
	assert.Equal(t, float64(100), c.lastSuccessTime[seedKey])
}
