package server

import (
	"context"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"log"
	"sync"
	"time"
)

const rosterCollector = "definitions_roster"

type collectFunc func(context.Context, *collector.DagsterCollector) error

// collectFuncs maps each name in config.CollectorNames to the function that
// scrapes it. Each one locks DagsterCollector's own mutex around its critical
// section, so running them in parallel is safe.
var collectFuncs = map[string]collectFunc{
	"definitions_roster":   collector.CollectDefinitionsRoster,
	"active_runs":          collector.CollectActiveRuns,
	"completed_runs":       collector.CollectCompletedRuns,
	"code_location_status": collector.CollectCodeLocationStatus,
	"daemon_health":        collector.CollectDaemonHealth,
	"asset_status":         collector.CollectAssetStatus,
	"op_pool_concurrency":  collector.CollectOpPoolConcurrency,
	"agents":               collector.CollectAgents,
}

// scrapeOne scrapes a single collector once and records the outcome for the
// self-health metrics.
func scrapeOne(ctx context.Context, c *collector.DagsterCollector, name string) {
	start := time.Now()
	err := collectFuncs[name](ctx, c)
	c.RecordScrapeResult(name, time.Since(start), err)
}

// scrapeDagster runs every collector once, concurrently, so the total
// latency is bounded by the slowest single call instead of their sum.
func scrapeDagster(ctx context.Context, c *collector.DagsterCollector) {
	var wg sync.WaitGroup
	for _, name := range config.CollectorNames {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scrapeOne(ctx, c, name)
		}()
	}
	wg.Wait()
}

// runCollector scrapes one collector on every tick of its own interval, each
// scrape bounded by its own timeout, after an immediate first scrape unless
// scrapeNow is false. Scrapes of one collector never overlap: the next one
// only starts after the current one finishes. Collectors don't wait for each
// other, so a slow one no longer holds back the cheap ones.
func runCollector(ctx context.Context, c *collector.DagsterCollector, name string, schedule config.CollectorSchedule, scrapeNow bool) {
	ticker := time.NewTicker(schedule.Interval)
	defer ticker.Stop()

	scrapeWithTimeout := func() {
		scrapeCtx, cancel := context.WithTimeout(ctx, schedule.Timeout)
		defer cancel()
		scrapeOne(scrapeCtx, c, name)
	}

	if scrapeNow {
		scrapeWithTimeout()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrapeWithTimeout()
		}
	}
}

// scheduledCollectors lists the collectors that have a schedule, in the order
// of config.CollectorNames and then the Dagster+ ones. The Dagster+ collectors
// have none unless the exporter was given a Dagster+ token.
func scheduledCollectors(schedules map[string]config.CollectorSchedule) []string {
	var names []string
	for _, name := range config.ActiveCollectorNames(true) {
		if _, ok := schedules[name]; ok {
			names = append(names, name)
		}
	}
	return names
}

// startScrape runs every collector on its own schedule until ctx is
// cancelled.
//
// The definitions roster is fetched once, to completion, before the other
// collectors start, so the first scrape of active_runs already knows every
// job and can report the idle ones as 0 instead of leaving them absent. It is
// only the first fetch that waits: afterwards the roster runs concurrently
// with the rest, because serializing every cycle would add a round trip to
// each scrape.
func startScrape(ctx context.Context, c *collector.DagsterCollector, schedules map[string]config.CollectorSchedule) {
	names := scheduledCollectors(schedules)
	log.Printf("Starting background scraper (%d collectors)...", len(names))

	for _, name := range names {
		schedule := schedules[name]
		log.Printf("  %s: interval %v, timeout %v", name, schedule.Interval, schedule.Timeout)
	}

	roster := schedules[rosterCollector]
	warmUpCtx, cancel := context.WithTimeout(ctx, roster.Timeout)
	scrapeOne(warmUpCtx, c, rosterCollector)
	cancel()

	var wg sync.WaitGroup
	for _, name := range names {
		schedule := schedules[name]
		wg.Add(1)
		go func() {
			defer wg.Done()
			runCollector(ctx, c, name, schedule, name != rosterCollector)
		}()
	}

	// Leave cleanly once a shutdown signal arrives.
	wg.Wait()
	log.Println("Stopping background scraper...")
}
