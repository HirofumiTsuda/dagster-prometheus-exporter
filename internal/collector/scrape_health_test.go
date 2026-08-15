package collector

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordScrapeResultSuccess(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour, 500, 5*time.Minute)

	c.RecordScrapeResult("active_runs", 250*time.Millisecond, nil)

	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectScrapeHealth(c, ch)
		close(ch)
	}()

	var sawDuration, sawSuccess bool
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))

		switch {
		case strings.Contains(m.Desc().String(), "dagster_exporter_scrape_duration_seconds"):
			sawDuration = true
			assert.InDelta(t, 0.25, dm.GetGauge().GetValue(), 0.001)
		case strings.Contains(m.Desc().String(), "dagster_exporter_last_scrape_success"):
			sawSuccess = true
			assert.Equal(t, float64(1), dm.GetGauge().GetValue())
		}
	}
	assert.True(t, sawDuration, "expected a scrape duration metric")
	assert.True(t, sawSuccess, "expected a last-scrape-success metric")

	errMetric, err := c.scrapeErrorsCounter.GetMetricWithLabelValues("active_runs")
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(errMetric))
}

func TestRecordScrapeResultFailureIncrementsErrorCounter(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://example.invalid", time.Hour, time.Hour, 500, 5*time.Minute)

	c.RecordScrapeResult("definitions_roster", 100*time.Millisecond, errors.New("boom"))
	c.RecordScrapeResult("definitions_roster", 100*time.Millisecond, errors.New("boom again"))

	errMetric, err := c.scrapeErrorsCounter.GetMetricWithLabelValues("definitions_roster")
	require.NoError(t, err)
	assert.Equal(t, float64(2), testutil.ToFloat64(errMetric),
		"error counter should accumulate across failed scrapes, not just reflect the latest one")

	c.mutex.Lock()
	success := c.scrapeResults["definitions_roster"].success
	c.mutex.Unlock()
	assert.False(t, success)
}
