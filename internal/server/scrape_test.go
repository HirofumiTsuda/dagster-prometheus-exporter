package server

import (
	"dagster-prometheus-exporter/internal/collector"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestScrapeDagsterRunsCollectorsConcurrently(t *testing.T) {
	const perRequestDelay = 50 * time.Millisecond

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(perRequestDelay)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Valid enough for both GraphQLRunsResponse and
		// GraphQLJobLocationsResponse decoding, whichever collector hits it.
		_, _ = w.Write([]byte(`{
			"data": {
				"runsOrError": {"__typename": "Runs", "results": []},
				"repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": []}
			}
		}`))
	}))
	defer ts.Close()

	c := collector.NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour)

	start := time.Now()
	scrapeDagster(t.Context(), c)
	elapsed := time.Since(start)

	// Sequentially, three calls at perRequestDelay each would take ~150ms.
	// Running them concurrently should take roughly one delay's worth.
	assert.Less(t, elapsed, 2*perRequestDelay, "collectors should run concurrently, not sequentially")
}
