package server

import (
	"context"
	"dagster-prometheus-exporter/internal/collector"
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

func TestScrapeDagsterHonorsContextTimeoutWhenRunningConcurrently(t *testing.T) {
	unblock := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Never respond in time. All three collectors hit this same
		// endpoint concurrently, so this also proves ctx cancellation is
		// honored by every one of them at once, not just the first.
		<-unblock
	}))
	// unblock must be closed before ts.Close(), since Close() waits for the
	// in-flight handlers above to return.
	defer ts.Close()
	defer close(unblock)

	c := collector.NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	const ctxTimeout = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), ctxTimeout)
	defer cancel()

	// scrapeDagster itself would hang forever if ctx cancellation weren't
	// honored, so it's called in its own goroutine and raced against a
	// bound here — a direct call + elapsed-time assertion can't catch that,
	// since a hang would never reach the assertion at all. The bound is a
	// generous multiple of ctxTimeout rather than an unrelated fixed value,
	// so it scales with whatever timeout the test above it uses.
	done := make(chan struct{})
	go func() {
		scrapeDagster(ctx, c)
		close(done)
	}()

	select {
	case <-done:
		// Expected: scrapeDagster returned once ctx timed out.
	case <-time.After(20 * ctxTimeout):
		t.Fatal("scrapeDagster did not return after its context timed out; a collector is not honoring ctx cancellation")
	}
}

func TestScrapeDagsterRecordsSelfHealthMetrics(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {"__typename": "Runs", "results": []},
				"repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": []}
			}
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := collector.NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	scrapeDagster(t.Context(), c)

	ch := make(chan prometheus.Metric, 16)
	go func() {
		c.Collect(ch)
		close(ch)
	}()

	success := map[string]float64{}
	for m := range ch {
		if !strings.Contains(m.Desc().String(), "dagster_exporter_last_scrape_success") {
			continue
		}
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		for _, l := range dm.GetLabel() {
			if l.GetName() == "collector" {
				success[l.GetValue()] = dm.GetGauge().GetValue()
			}
		}
	}

	for _, name := range []string{"job_locations", "active_runs", "completed_runs"} {
		assert.Equal(t, float64(1), success[name], "%s should report a successful scrape", name)
	}
}
