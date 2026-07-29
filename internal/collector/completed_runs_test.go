package collector

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
