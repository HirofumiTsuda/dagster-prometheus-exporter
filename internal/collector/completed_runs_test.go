package collector

import (
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

func TestCollectCompletedRunsTracksLastRunStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "job_a", "status": "FAILURE", "endTime": 100, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}},
						{"runId": "run_2", "jobName": "job_a", "status": "SUCCESS", "endTime": 200, "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}}
					]
				}
			}
		}`))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour)
	CollectCompletedRuns(t.Context(), c)

	assert.Equal(t, "SUCCESS", c.lastRunStatus[JobKey{JobName: "job_a", LocationName: "loc_a"}])

	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectLastRunStatus(c, ch)
		close(ch)
	}()

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	require.Len(t, metrics, 1)

	var m dto.Metric
	require.NoError(t, metrics[0].Write(&m))
	assert.Equal(t, float64(1), m.GetGauge().GetValue())

	labels := make(map[string]string)
	for _, l := range m.GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	assert.Equal(t, "success", labels["status"])
}

func TestSeedCompletedRunsCounter(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour)

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
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour)

	previous := map[JobKey]struct{}{
		{JobName: "gone_job", LocationName: "loc_a"}: {},
	}
	known := map[JobKey]struct{}{}

	seedCompletedRunsCounter(c, previous)
	pruneCompletedRunsCounter(c, previous, known)

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

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour)

	CollectJobLocations(t.Context(), c)
	assert.Contains(t, c.knownJobs, JobKey{JobName: "job_a", LocationName: "loc_a"})

	metric, err := c.completedRunsCounter.GetMetricWithLabelValues("job_a", "loc_a", "success")
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(metric))

	CollectJobLocations(t.Context(), c)
	assert.NotContains(t, c.knownJobs, JobKey{JobName: "job_a", LocationName: "loc_a"})
}
