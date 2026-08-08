package collector

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectConcurrencyKeyBacklogCountsQueuedRunsByTag(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "heavy_job", "status": "QUEUED", "tags": [{"key": "dagster/concurrency_key", "value": "heavy_limit"}]},
						{"runId": "run_2", "jobName": "heavy_job", "status": "QUEUED", "tags": [{"key": "dagster/concurrency_key", "value": "heavy_limit"}]},
						{"runId": "run_3", "jobName": "failing_job", "status": "QUEUED", "tags": [{"key": "dagster/concurrency_key", "value": "failing_limit"}]},
						{"runId": "run_4", "jobName": "other_job", "status": "QUEUED", "tags": [{"key": "some_other_tag", "value": "x"}]}
					]
				}
			}
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectConcurrencyKeyBacklog(t.Context(), c))

	assert.Equal(t, 2, c.concurrencyKeyBacklog["heavy_limit"])
	assert.Equal(t, 1, c.concurrencyKeyBacklog["failing_limit"])
	// run_4 has no dagster/concurrency_key tag, so it contributes to no key.
	assert.NotContains(t, c.concurrencyKeyBacklog, "")

	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectConcurrencyKeyBacklog(c, ch)
		close(ch)
	}()

	values := make(map[string]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		var key string
		for _, l := range dm.GetLabel() {
			if l.GetName() == "concurrency_key" {
				key = l.GetValue()
			}
		}
		values[key] = dm.GetGauge().GetValue()
	}
	assert.Equal(t, float64(2), values["heavy_limit"])
	assert.Equal(t, float64(1), values["failing_limit"])
}

func TestCollectConcurrencyKeyBacklogZeroFillsPreviouslySeenKeys(t *testing.T) {
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
						{"runId": "run_1", "jobName": "heavy_job", "status": "QUEUED", "tags": [{"key": "dagster/concurrency_key", "value": "heavy_limit"}]}
					]
				}
			}
		}`
		if call > 1 {
			// The backlog has cleared: no runs queued anymore.
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

	require.NoError(t, CollectConcurrencyKeyBacklog(t.Context(), c))
	assert.Equal(t, 1, c.concurrencyKeyBacklog["heavy_limit"])

	require.NoError(t, CollectConcurrencyKeyBacklog(t.Context(), c))
	assert.Equal(t, 0, c.concurrencyKeyBacklog["heavy_limit"],
		"a previously-seen key should be zero-filled, not dropped, once its backlog clears")
}

func TestCollectConcurrencyKeyBacklogReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectConcurrencyKeyBacklog(t.Context(), c))
}
