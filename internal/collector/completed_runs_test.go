package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectCompletedRunsUsesIncrementalUpdatedAfter(t *testing.T) {
	var seenUpdatedAfter []float64
	call := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		if v, ok := req.Variables["updatedAfter"]; ok && v != nil {
			seenUpdatedAfter = append(seenUpdatedAfter, v.(float64))
		} else {
			seenUpdatedAfter = append(seenUpdatedAfter, 0)
		}
		call++

		body := `{"data": {"runsOrError": {"__typename": "Runs", "results": []}}}`
		if call == 1 {
			body = `{
				"data": {
					"runsOrError": {
						"__typename": "Runs",
						"results": [
							{"runId": "run_1", "jobName": "job_a", "status": "SUCCESS", "endTime": 100, "updateTime": 1000}
						]
					}
				}
			}`
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(body))
		require.NoError(t, err)
	}))
	defer ts.Close()

	lookback := 2 * time.Hour
	safetyMargin := 5 * time.Minute
	c := NewDagsterCollector(t.Context(), ts.URL, lookback, time.Hour, 500, safetyMargin)

	require.NoError(t, CollectCompletedRuns(t.Context(), c))
	require.Len(t, seenUpdatedAfter, 1)
	expectedFirst := getUpdatedAfter(time.Now(), lookback)
	assert.InDelta(t, expectedFirst, seenUpdatedAfter[0], 5,
		"first scrape (no watermark yet) should fall back to the lookback window")
	assert.Equal(t, float64(1000), c.lastSeenUpdateTime, "watermark should advance to the max updateTime seen")

	require.NoError(t, CollectCompletedRuns(t.Context(), c))
	require.Len(t, seenUpdatedAfter, 2)
	assert.Equal(t, float64(1000)-safetyMargin.Seconds(), seenUpdatedAfter[1],
		"second scrape should use the watermark minus the safety margin, not the full lookback window again")
}

func TestCollectCompletedRunsTracksLastRunStatusAndDuration(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "job_a", "status": "FAILURE", "creationTime": 0, "endTime": 100, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}},
						{"runId": "run_2", "jobName": "job_a", "status": "SUCCESS", "creationTime": 150, "endTime": 200, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}}
					]
				}
			}
		}`))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectCompletedRuns(t.Context(), c))

	assert.Equal(t, "SUCCESS", c.lastRunStatus[JobKey{JobName: "job_a", LocationName: "loc_a"}].status)

	// job_never_ran has never had a completed run, so it has nothing to
	// report yet: no seeded 0 duration, no seeded "unknown" status.
	neverRanKey := JobKey{JobName: "job_never_ran", LocationName: "loc_a"}
	assert.NotContains(t, c.lastRunStatus, neverRanKey)

	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectLastRun(c, ch)
		close(ch)
	}()

	var infoMetric, durationMetric *dto.Metric
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		switch {
		case strings.Contains(m.Desc().String(), "dagster_last_run_info"):
			infoMetric = &dm
		case strings.Contains(m.Desc().String(), "dagster_last_run_duration_seconds"):
			durationMetric = &dm
		}
	}
	require.NotNil(t, infoMetric, "expected a dagster_last_run_info series")
	require.NotNil(t, durationMetric, "expected a dagster_last_run_duration_seconds series")

	assert.Equal(t, float64(1), infoMetric.GetGauge().GetValue())
	assert.Equal(t, float64(50), durationMetric.GetGauge().GetValue(),
		"duration should be endTime - creationTime of the most recent run (200-150), not the earlier one (100-0)")

	for _, m := range []*dto.Metric{infoMetric, durationMetric} {
		labels := make(map[string]string)
		for _, l := range m.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		assert.Equal(t, "job_a", labels["job_name"])
		assert.Equal(t, "loc_a", labels["location"])
		assert.Equal(t, "success", labels["status"])
	}
}

func TestLastRunStatusPersistsAfterFallingOutOfLookbackWindow(t *testing.T) {
	call := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		body := `{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "job_a", "status": "SUCCESS", "endTime": 100, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}}
					]
				}
			}
		}`
		if call > 1 {
			// Simulate the run aging out of the completed-runs lookback window:
			// the query now returns no results at all.
			body = `{
				"data": {
					"runsOrError": {
						"__typename": "Runs",
						"results": []
					}
				}
			}`
		}

		_, err := w.Write([]byte(body))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectCompletedRuns(t.Context(), c))
	require.Equal(t, "SUCCESS", c.lastRunStatus[JobKey{JobName: "job_a", LocationName: "loc_a"}].status)

	require.NoError(t, CollectCompletedRuns(t.Context(), c))
	assert.Equal(t, "SUCCESS", c.lastRunStatus[JobKey{JobName: "job_a", LocationName: "loc_a"}].status,
		"last known status should survive a scrape with no completed runs in range")
}

func TestPruneLastRunStatus(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour, 500, 5*time.Minute)

	goneKey := JobKey{JobName: "gone_job", LocationName: "loc_a"}
	stillHereKey := JobKey{JobName: "job_a", LocationName: "loc_a"}
	c.lastRunStatus[goneKey] = lastRunEntry{status: "SUCCESS", endTime: 100}
	c.lastRunStatus[stillHereKey] = lastRunEntry{status: "FAILURE", endTime: 100}

	pruneLastRunStatus(c, map[JobKey]struct{}{stillHereKey: {}})

	assert.NotContains(t, c.lastRunStatus, goneKey)
	assert.Contains(t, c.lastRunStatus, stillHereKey)
}

func TestSeedCompletedRunsCounter(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour, 500, 5*time.Minute)

	known := map[JobKey]struct{}{
		{JobName: "job_a", LocationName: "loc_a"}: {},
	}

	seedCompletedRunsCounter(c, known)

	for _, status := range completedStatuses {
		metric, err := c.completedRunsCounter.GetMetricWithLabelValues("job_a", "loc_a", strings.ToLower(status))
		require.NoError(t, err)
		assert.Equal(t, float64(0), testutil.ToFloat64(metric))
	}
}

func TestPruneCompletedRunsCounter(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour, 500, 5*time.Minute)

	previous := map[JobKey]struct{}{
		{JobName: "gone_job", LocationName: "loc_a"}: {},
	}
	known := map[JobKey]struct{}{}

	seedCompletedRunsCounter(c, previous)
	pruneCompletedRunsCounter(c, known)

	ch := make(chan prometheus.Metric, 8)
	go func() {
		c.completedRunsCounter.Collect(ch)
		close(ch)
	}()

	count := 0
	for range ch {
		count++
	}
	assert.Equal(t, 0, count, "expected no series to remain for a pruned job")
}

func TestPruneCompletedRunsCounterRemovesStaleLocationNotInRoster(t *testing.T) {
	// Regression test: a run whose repositoryOrigin recorded an old location
	// name (e.g. an auto-generated grpc name from before location_name was
	// pinned) must not linger in dagster_completed_runs_total forever just
	// because that (job, location) pair never appears in the live roster.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "job_a", "status": "SUCCESS", "endTime": 100, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "old.module.path"}}
					]
				}
			}
		}`))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectCompletedRuns(t.Context(), c))

	metric, err := c.completedRunsCounter.GetMetricWithLabelValues("job_a", "old.module.path", "success")
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(metric))

	// The live roster now reports "job_a" under a renamed, pinned location.
	known := map[JobKey]struct{}{
		{JobName: "job_a", LocationName: "current-location"}: {},
	}
	pruneCompletedRunsCounter(c, known)

	ch := make(chan prometheus.Metric, 8)
	go func() {
		c.completedRunsCounter.Collect(ch)
		close(ch)
	}()

	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		for _, l := range dm.GetLabel() {
			if l.GetName() == "location" {
				assert.NotEqual(t, "old.module.path", l.GetValue(), "stale location series should have been pruned")
			}
		}
	}
}

func TestCollectJobLocationsSeedsAndPrunes(t *testing.T) {
	call := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		body := `{
			"data": {
				"repositoriesOrError": {
					"__typename": "RepositoryConnection",
					"nodes": [
						{"name": "repo", "location": {"name": "loc_a"}, "jobs": [{"name": "job_a"}]}
					]
				}
			}
		}`
		if call > 1 {
			body = `{
				"data": {
					"repositoriesOrError": {
						"__typename": "RepositoryConnection",
						"nodes": []
					}
				}
			}`
		}

		_, err := w.Write([]byte(body))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectJobLocations(t.Context(), c))
	assert.Contains(t, c.knownJobs, JobKey{JobName: "job_a", LocationName: "loc_a"})

	metric, err := c.completedRunsCounter.GetMetricWithLabelValues("job_a", "loc_a", "success")
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(metric))

	require.NoError(t, CollectJobLocations(t.Context(), c))
	assert.NotContains(t, c.knownJobs, JobKey{JobName: "job_a", LocationName: "loc_a"})
}

func TestCollectCompletedRunsReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectCompletedRuns(t.Context(), c))
}

func TestCollectJobLocationsReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectJobLocations(t.Context(), c))
}
