package collector

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// RecordScrapeResult records the outcome of one collector's most recent
// scrape, so it can be reported via dagster_exporter_scrape_duration_seconds,
// dagster_exporter_last_scrape_success, and dagster_exporter_scrape_errors_total.
func (c *DagsterCollector) RecordScrapeResult(collectorName string, duration time.Duration, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.scrapeDuration[collectorName] = duration.Seconds()
	c.lastScrapeSuccess[collectorName] = err == nil

	if err != nil {
		c.scrapeErrorsCounter.WithLabelValues(collectorName).Inc()
	}
}

func reflectScrapeHealth(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for name, duration := range c.scrapeDuration {
		ch <- prometheus.MustNewConstMetric(
			c.scrapeDurationDesc,
			prometheus.GaugeValue,
			duration,
			name,
		)
	}

	for name, success := range c.lastScrapeSuccess {
		value := 0.0
		if success {
			value = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			c.lastScrapeSuccessDesc,
			prometheus.GaugeValue,
			value,
			name,
		)
	}
}
