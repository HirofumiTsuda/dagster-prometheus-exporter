package collector

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectActiveRunsLocationLabel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "job_a", "status": "STARTED", "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}},
						{"runId": "run_2", "jobName": "job_b", "status": "QUEUED", "repositoryOrigin": null}
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
	assert.NoError(t, CollectActiveRuns(t.Context(), c))

	assert.Equal(t, 1, c.activeRunAggregates[ActiveRunKey{JobName: "job_a", LocationName: "loc_a", Status: "STARTED"}].count)
	assert.Equal(t, 1, c.activeRunAggregates[ActiveRunKey{JobName: "job_b", LocationName: unknownLocationName, Status: "QUEUED"}].count)
}

func TestCollectActiveRunsZeroFillsKnownJobs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": []
				}
			}
		}`))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	c.knownJobs = map[JobKey]struct{}{
		{JobName: "never_run_job", LocationName: "loc_a"}: {},
	}

	assert.NoError(t, CollectActiveRuns(t.Context(), c))

	for _, status := range activeStatuses {
		agg := c.activeRunAggregates[ActiveRunKey{JobName: "never_run_job", LocationName: "loc_a", Status: status}]
		assert.Equal(t, 0, agg.count)
		assert.Equal(t, float64(0), agg.maxElapsed)
	}
}

func TestCollectActiveRunsReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectActiveRuns(t.Context(), c))
}

func TestCollectActiveRunsTracksMaxElapsedPerGroup(t *testing.T) {
	now := time.Now()
	// Two STARTED runs for job_a: one that entered STARTED 5 minutes ago,
	// one that entered STARTED 30 seconds ago (per updateTime, which
	// Dagster only bumps on a run-level status transition — see
	// elapsedSince). The group's elapsed time should reflect the older
	// (longer-running) one, not the newer one or their sum/average.
	olderUpdate := now.Add(-5 * time.Minute).Unix()
	newerUpdate := now.Add(-30 * time.Second).Unix()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		body := fmt.Sprintf(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_older", "jobName": "job_a", "status": "STARTED", "updateTime": %d, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}},
						{"runId": "run_newer", "jobName": "job_a", "status": "STARTED", "updateTime": %d, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}}
					]
				}
			}
		}`, olderUpdate, newerUpdate)
		_, err := w.Write([]byte(body))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectActiveRuns(t.Context(), c))

	agg := c.activeRunAggregates[ActiveRunKey{JobName: "job_a", LocationName: "loc_a", Status: "STARTED"}]
	assert.Equal(t, 2, agg.count)
	assert.InDelta(t, 300, agg.maxElapsed, 5, "should report the older run's elapsed time (~5m), not the newer one's (~30s)")

	ch := make(chan prometheus.Metric, 16)
	go func() {
		reflectActiveRuns(c, ch)
		close(ch)
	}()

	var sawCount, sawDuration bool
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		labels := make(map[string]string)
		for _, l := range dm.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if labels["job_name"] != "job_a" || labels["status"] != "started" {
			continue
		}
		desc := m.Desc().String()
		switch {
		case strings.Contains(desc, "dagster_active_run_duration_seconds"):
			sawDuration = true
			assert.InDelta(t, 300, dm.GetGauge().GetValue(), 5)
		case strings.Contains(desc, "dagster_active_runs"):
			sawCount = true
			assert.Equal(t, float64(2), dm.GetGauge().GetValue())
		}
	}
	assert.True(t, sawCount, "expected a dagster_active_runs series")
	assert.True(t, sawDuration, "expected a dagster_active_run_duration_seconds series")
}

func TestCollectActiveRunsAlsoTracksConcurrencyKeyBacklog(t *testing.T) {
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
						{"runId": "run_1", "jobName": "heavy_job", "status": "QUEUED", "tags": [{"key": "dagster/concurrency_key", "value": "heavy_limit"}]},
						{"runId": "run_2", "jobName": "heavy_job", "status": "QUEUED", "tags": [{"key": "dagster/concurrency_key", "value": "heavy_limit"}]},
						{"runId": "run_3", "jobName": "heavy_job", "status": "STARTED", "tags": [{"key": "dagster/concurrency_key", "value": "heavy_limit"}]}
					]
				}
			}
		}`
		if call > 1 {
			// The backlog has cleared: nothing queued anymore.
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
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectActiveRuns(t.Context(), c))
	// Only the two QUEUED runs count toward backlog; the STARTED one (past
	// the concurrency limit, actually running) doesn't.
	assert.Equal(t, 2, c.concurrencyKeyBacklog["heavy_limit"])

	require.NoError(t, CollectActiveRuns(t.Context(), c))
	assert.Equal(t, 0, c.concurrencyKeyBacklog["heavy_limit"],
		"a previously-seen key should be zero-filled, not dropped, once its backlog clears")
}

func TestRunLocationFallsBackWhenOriginIsAbsent(t *testing.T) {
	withOrigin := Run{RepositoryOrigin: &RunRepositoryOrigin{RepositoryLocationName: "loc_a"}}
	assert.Equal(t, "loc_a", withOrigin.location())

	assert.Equal(t, unknownLocationName, Run{}.location(),
		"a run with no repositoryOrigin should land in the placeholder location rather than an empty label")
}
