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

func startScrape(ctx context.Context, c *collector.DagsterCollector, interval time.Duration, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Starting background scraper (interval: %v)...", interval)

	scrapeDagster(ctx, c)

	for {
		select {
		case <-ctx.Done():
			// アプリ終了シグナルを受け取ったらループを安全に抜ける
			log.Println("Stopping background scraper...")
			return

		case <-ticker.C:
			func() {
				scrapeCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				scrapeDagster(scrapeCtx, c)
			}()
		}
	}
}
