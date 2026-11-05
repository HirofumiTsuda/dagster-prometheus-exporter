package collector

import (
	"context"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// backfillsLimit is how many backfills each of the two lists in one scrape
// reads. There is no cursor loop: paging back through every backfill ever
// launched would grow the request without bound.
const backfillsLimit = 100

// knownBackfillStatuses are the BulkActionStatus values, lowercased. They are
// always exported, so an alert on one of them does not depend on a series
// having appeared first.
var knownBackfillStatuses = []string{
	"requested", "completed", "failed", "canceled", "canceling",
	"completed_success", "completed_failed", "failing",
}

// activeBackfillStatuses are the statuses of a backfill that has not finished.
// Dagster has no separate "running" status: a backfill whose runs are
// executing stays REQUESTED.
var activeBackfillStatuses = map[string]bool{
	"requested": true,
	"canceling": true,
	"failing":   true,
}

// searchedActiveBackfillStatuses are the active statuses the second list asks
// for. FAILING is left out of the query on purpose: Dagster 1.9 has no such
// value in BulkActionStatus, and naming it makes that version reject the whole
// query. It is short-lived, so it is read from the newest list instead.
var searchedActiveBackfillStatuses = map[string]bool{
	"requested": true,
	"canceling": true,
}

// backfillsSummary is what the backfills collector keeps: counts and the
// oldest active backfill, not one entry per backfill. A backfill id as a label
// would add a series for every backfill ever launched.
type backfillsSummary struct {
	byStatus map[string]int
	// oldestActive is nil when no backfill is active, rather than 0, which
	// would place it at the Unix epoch.
	oldestActive *float64
}

func (s *backfillsSummary) noteIfOldestActive(status string, timestamp float64) {
	if activeBackfillStatuses[status] && (s.oldestActive == nil || timestamp < *s.oldestActive) {
		s.oldestActive = &timestamp
	}
}

// CollectBackfills reports the state of the newest backfills.
//
// COMPLETED_FAILED and FAILED are not merged: they are different statuses,
// and which one Dagster uses when is not something this collector can tell, so
// the label carries what Dagster said.
func CollectBackfills(ctx context.Context, c *DagsterCollector) error {
	resp, err := getBackfills(ctx, getBackfillsRequest(backfillsLimit), c.client)
	if err != nil {
		log.Printf("failed to collect backfills from dagster: %v", err)
		return err
	}

	summary := &backfillsSummary{byStatus: make(map[string]int, len(knownBackfillStatuses))}
	for _, status := range knownBackfillStatuses {
		summary.byStatus[status] = 0
	}
	// A status the second list asks for is counted from it, so its count and
	// the oldest active backfill agree with each other.
	for _, backfill := range resp.Data.Recent.Results {
		status := strings.ToLower(backfill.Status)
		if !searchedActiveBackfillStatuses[status] {
			summary.byStatus[status]++
		}
		summary.noteIfOldestActive(status, backfill.Timestamp)
	}
	for _, backfill := range resp.Data.Active.Results {
		status := strings.ToLower(backfill.Status)
		summary.byStatus[status]++
		summary.noteIfOldestActive(status, backfill.Timestamp)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.backfills = summary

	return nil
}

// reflectBackfills emits nothing before the first scrape, so a deployment the
// exporter has not read yet does not claim zero backfills.
func reflectBackfills(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.backfills == nil {
		return
	}

	for status, count := range c.backfills.byStatus {
		ch <- prometheus.MustNewConstMetric(c.backfillsDesc, prometheus.GaugeValue, float64(count), status)
	}
	if c.backfills.oldestActive != nil {
		ch <- prometheus.MustNewConstMetric(c.backfillOldestActiveDesc, prometheus.GaugeValue, *c.backfills.oldestActive)
	}
}
