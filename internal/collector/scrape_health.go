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
	// scraped is false for an entry that SeedScrapeHealth created and no
	// scrape has completed yet. A duration of 0 would be a made-up number, so
	// it is not emitted until a real one exists.
	scraped bool
}

// SeedScrapeHealth makes the self-health series exist from startup, for every
// collector in names, instead of appearing one by one as scrapes complete.
// last_scrape_success reads 0 and the error counter 0 until a scrape says
// otherwise.
//
// Leaving them absent would defeat the alert on last_scrape_success == 0: it
// cannot fire on a series that does not exist, so an exporter that never
// finishes its first scrape would look the same as a healthy one.
func (c *DagsterCollector) SeedScrapeHealth(names []string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, name := range names {
		if _, ok := c.scrapeResults[name]; !ok {
			c.scrapeResults[name] = scrapeResult{}
		}
		c.scrapeErrorsCounter.WithLabelValues(name).Add(0)
	}
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
		scraped:  true,
	}

	if err != nil {
		c.scrapeErrorsCounter.WithLabelValues(collectorName).Inc()
	}
}

func reflectScrapeHealth(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for name, result := range c.scrapeResults {
		if result.scraped {
			ch <- prometheus.MustNewConstMetric(
				c.scrapeDurationDesc,
				prometheus.GaugeValue,
				result.duration,
				name,
			)
		}

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
