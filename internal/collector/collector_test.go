package collector

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestDagsterCollectorDescribeAndCollect(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour, 500, 5*time.Minute)

	descCh := make(chan *prometheus.Desc, 16)
	go func() {
		c.Describe(descCh)
		close(descCh)
	}()
	var descCount int
	for range descCh {
		descCount++
	}
	// activeRunsDesc, lastRunStatusDesc, scrapeDurationDesc, lastScrapeSuccessDesc,
	// codeLocationLoadErrorDesc, lastRunDurationDesc, activeRunDurationDesc,
	// plus one each from completedRunsCounter and scrapeErrorsCounter.
	assert.Equal(t, 9, descCount)

	c.RecordScrapeResult("active_runs", 10*time.Millisecond, nil)

	metricCh := make(chan prometheus.Metric, 16)
	go func() {
		c.Collect(metricCh)
		close(metricCh)
	}()
	var metricCount int
	for range metricCh {
		metricCount++
	}
	assert.Positive(t, metricCount)
}
