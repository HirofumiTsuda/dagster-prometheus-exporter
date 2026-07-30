package server

import (
	"context"
	"dagster-prometheus-exporter/internal/collector"
	"log"
	"time"
)

func scrapeDagster(ctx context.Context, c *collector.DagsterCollector) {
	collector.CollectJobLocations(ctx, c)
	collector.CollectActiveRuns(ctx, c)
	collector.CollectCompletedRuns(ctx, c)
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
