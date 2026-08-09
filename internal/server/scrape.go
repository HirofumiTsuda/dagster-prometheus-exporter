package server

import (
	"context"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"log"
	"sync"
	"time"
)

// scrapeDagster runs the GraphQL-backed collectors concurrently. Each one
// locks DagsterCollector's own mutex around its critical section, so running
// them in parallel is safe; doing so bounds the total scrape latency by the
// slowest single call instead of their sum.
func scrapeDagster(ctx context.Context, c *collector.DagsterCollector) {
	var wg sync.WaitGroup

	spawn := func(name string, fn func(context.Context, *collector.DagsterCollector) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			err := fn(ctx, c)
			c.RecordScrapeResult(name, time.Since(start), err)
		}()
	}

	spawn("definitions_roster", collector.CollectDefinitionsRoster)
	spawn("active_runs", collector.CollectActiveRuns)
	spawn("completed_runs", collector.CollectCompletedRuns)
	spawn("code_location_status", collector.CollectCodeLocationStatus)

	wg.Wait()
}

func scrapeDagsterWithTimeout(ctx context.Context, c *collector.DagsterCollector, timeout time.Duration) {
	scrapeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	scrapeDagster(scrapeCtx, c)
}

func startScrape(ctx context.Context, c *collector.DagsterCollector, interval time.Duration, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Starting background scraper (interval: %v)...", interval)

	scrapeDagsterWithTimeout(ctx, c, timeout)

	for {
		select {
		case <-ctx.Done():
			// アプリ終了シグナルを受け取ったらループを安全に抜ける
			log.Println("Stopping background scraper...")
			return

		case <-ticker.C:
			scrapeDagsterWithTimeout(ctx, c, timeout)
		}
	}
}
