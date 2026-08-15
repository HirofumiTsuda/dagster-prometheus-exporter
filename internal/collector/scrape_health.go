package collector

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// scrapeResult is one collector's most recent scrape outcome. Duration and
// success are always written together by RecordScrapeResult and always read
// together by reflectScrapeHealth, so keeping them in one map rather than
// two parallel ones removes the possibility of the two disagreeing about
// which collectors exist.
type scrapeResult struct {
	duration float64
	success  bool
}

// RecordScrapeResult records the outcome of one collector's most recent
// scrape, so it can be reported via dagster_exporter_scrape_duration_seconds,
// dagster_exporter_last_scrape_success, and dagster_exporter_scrape_errors_total.
func (c *DagsterCollector) RecordScrapeResult(collectorName string, duration time.Duration, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.scrapeResults[collectorName] = scrapeResult{
		duration: duration.Seconds(),
		success:  err == nil,
	}

	if err != nil {
		c.scrapeErrorsCounter.WithLabelValues(collectorName).Inc()
	}
}

func reflectScrapeHealth(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for name, result := range c.scrapeResults {
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDurationDesc,
			prometheus.GaugeValue,
			result.duration,
			name,
		)

		success := 0.0
		if result.success {
			success = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			c.lastScrapeSuccessDesc,
			prometheus.GaugeValue,
			success,
			name,
		)
	}
}
