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

func TestCollectCodeLocationStatusReportsPerLocationErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"workspaceOrError": {
					"__typename": "Workspace",
					"locationEntries": [
						{"name": "loc_ok", "locationOrLoadError": {"__typename": "RepositoryLocation"}},
						{"name": "loc_broken", "locationOrLoadError": {"__typename": "PythonError", "message": "boom", "stack": ["line 1"]}}
					]
				}
			}
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectCodeLocationStatus(t.Context(), c))

	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectCodeLocationStatus(c, ch)
		close(ch)
	}()

	values := make(map[string]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		var location string
		for _, l := range dm.GetLabel() {
			if l.GetName() == "location" {
				location = l.GetValue()
			}
		}
		values[location] = dm.GetGauge().GetValue()
	}

	assert.Equal(t, float64(0), values["loc_ok"])
	assert.Equal(t, float64(1), values["loc_broken"])
}

func TestCollectCodeLocationStatusReturnsErrorOnWorkspaceLoadError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"workspaceOrError": {
					"__typename": "PythonError",
					"message": "workspace unreachable",
					"stack": ["line 1"]
				}
			}
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	err := CollectCodeLocationStatus(t.Context(), c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workspace unreachable")
}

func TestCollectCodeLocationStatusReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectCodeLocationStatus(t.Context(), c))
}
