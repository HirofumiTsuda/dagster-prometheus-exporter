package server

import (
	"context"
	"dagster-prometheus-exporter/internal/collector"
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

	spawn := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}

	spawn(func() { collector.CollectJobLocations(ctx, c) })
	spawn(func() { collector.CollectActiveRuns(ctx, c) })
	spawn(func() { collector.CollectCompletedRuns(ctx, c) })

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
