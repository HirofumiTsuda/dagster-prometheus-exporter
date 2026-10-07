package collector

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

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

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	err := CollectCodeLocationStatus(t.Context(), c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workspace unreachable")
}

func TestCollectCodeLocationStatusReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectCodeLocationStatus(t.Context(), c))
}

func TestCollectCodeLocationStatusLogsStackTraceOnlyOnTransition(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	workspace := func(locationOrLoadError string) string {
		return fmt.Sprintf(`{"data": {"workspaceOrError": {
			"__typename": "Workspace",
			"locationEntries": [{"name": "loc_a", "locationOrLoadError": %s}]
		}}}`, locationOrLoadError)
	}
	const (
		loaded     = `{"__typename": "RepositoryLocation"}`
		brokenBoom = `{"__typename": "PythonError", "message": "boom", "stack": ["boom-frame"]}`
		brokenBang = `{"__typename": "PythonError", "message": "bang", "stack": ["bang-frame"]}`
	)

	s := newScriptedServer(t, workspace(loaded))
	c := NewDagsterCollector(t.Context(), s.url, "", time.Hour, time.Hour, 500, 5*time.Minute)

	scrape := func(body string) string {
		t.Helper()
		s.serve(workspace(body))
		buf.Reset()
		require.NoError(t, CollectCodeLocationStatus(t.Context(), c))
		return buf.String()
	}

	assert.Empty(t, scrape(loaded), "a healthy location should log nothing")

	out := scrape(brokenBoom)
	assert.Contains(t, out, `code location "loc_a" failed to load: boom`)
	assert.Contains(t, out, "boom-frame", "the stack trace should be logged when the location starts failing")

	assert.Empty(t, scrape(brokenBoom), "the same failure on the next scrape should not be logged again")
	assert.Empty(t, scrape(brokenBoom))

	out = scrape(brokenBang)
	assert.Contains(t, out, "failed to load: bang", "a changed error message should be logged")
	assert.Contains(t, out, "bang-frame")

	out = scrape(loaded)
	assert.Contains(t, out, `code location "loc_a" loaded successfully again`)
	assert.Equal(t, 1, strings.Count(strings.TrimSpace(out), "\n")+1, "recovery should be a single log line")

	assert.Empty(t, scrape(loaded), "a location that stays healthy should log nothing")

	out = scrape(brokenBang)
	assert.Contains(t, out, "bang-frame", "failing again after a recovery should log the stack trace again")
}

func TestCollectCodeLocationStatusLogsStackTraceOnFirstScrape(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	s := newScriptedServer(t, `{"data": {"workspaceOrError": {
		"__typename": "Workspace",
		"locationEntries": [{"name": "loc_a", "locationOrLoadError": {"__typename": "PythonError", "message": "boom", "stack": ["boom-frame"]}}]
	}}}`)
	c := NewDagsterCollector(t.Context(), s.url, "", time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectCodeLocationStatus(t.Context(), c))
	assert.Contains(t, buf.String(), "boom-frame",
		"a location that is already broken when the exporter starts should still be logged once")
	assert.NotContains(t, buf.String(), "loaded successfully again")
}
