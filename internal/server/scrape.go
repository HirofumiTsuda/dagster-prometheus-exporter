package server

import (
	"context"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"log"
	"sync"
	"time"
)

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

// runCollector scrapes one collector immediately and then on every tick of its
// own interval, each scrape bounded by its own timeout. Scrapes of one
// collector never overlap: the next one only starts after the current one
// finishes. Collectors don't wait for each other, so a slow one no longer
// holds back the cheap ones.
func runCollector(ctx context.Context, c *collector.DagsterCollector, name string, schedule config.CollectorSchedule) {
	ticker := time.NewTicker(schedule.Interval)
	defer ticker.Stop()

	scrapeWithTimeout := func() {
		scrapeCtx, cancel := context.WithTimeout(ctx, schedule.Timeout)
		defer cancel()
		scrapeOne(scrapeCtx, c, name)
	}

	scrapeWithTimeout()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scrapeWithTimeout()
		}
	}
}

// startScrape runs every collector on its own schedule until ctx is
// cancelled.
func startScrape(ctx context.Context, c *collector.DagsterCollector, schedules map[string]config.CollectorSchedule) {
	log.Printf("Starting background scraper (%d collectors)...", len(config.CollectorNames))

	var wg sync.WaitGroup
	for _, name := range config.CollectorNames {
		schedule := schedules[name]
		log.Printf("  %s: interval %v, timeout %v", name, schedule.Interval, schedule.Timeout)
		wg.Add(1)
		go func() {
			defer wg.Done()
			runCollector(ctx, c, name, schedule)
		}()
	}

	// Leave cleanly once a shutdown signal arrives.
	wg.Wait()
	log.Println("Stopping background scraper...")
}
