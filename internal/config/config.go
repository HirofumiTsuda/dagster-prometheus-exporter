package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port                         int
	DagsterGraphQLEndpoint       string
	LookbackWindow               time.Duration
	CacheTTL                     time.Duration
	DagsterScrapingInterval      time.Duration
	DagsterScrapingTimeout       time.Duration
	RunsPageSize                 int
	RunsUpdatedAfterSafetyMargin time.Duration
}

// Load は環境変数などから設定を読み込んで返す
func Load() (*Config, error) {
	port := 9101
	if portStr := os.Getenv("PORT"); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid PORT value: %w", err)
		}
		port = p
	}

	dagsterGraphQLEndpoint := os.Getenv("DAGSTER_GRAPHQL_ENDPOINT")
	if dagsterGraphQLEndpoint == "" {
		dagsterGraphQLEndpoint = "http://127.0.0.1:3000/graphql"
	}

	dagsterScrapingIntervalSeconds := 15
	if scrapingInterval_str := os.Getenv("DAGSTER_SCRAPING_INTERVAL_SECONDS"); scrapingInterval_str != "" {
		scrapingInterval, err := strconv.Atoi(scrapingInterval_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Dagster Scraping Interval Seconds value: %w", err)
		}
		dagsterScrapingIntervalSeconds = scrapingInterval
	}
	dagsterScrapingInterval := time.Duration(dagsterScrapingIntervalSeconds) * time.Second

	runsUpdatedAfterSafetyMarginMinutes := 5
	if margin_str := os.Getenv("RUNS_UPDATED_AFTER_SAFETY_MARGIN_MINUTES"); margin_str != "" {
		margin, err := strconv.Atoi(margin_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Runs Updated After Safety Margin Minutes value: %w", err)
		}
		runsUpdatedAfterSafetyMarginMinutes = margin
	}
	runsUpdatedAfterSafetyMargin := time.Duration(runsUpdatedAfterSafetyMarginMinutes) * time.Minute

	// processedRuns entries are touched (their TTL refreshed) on every
	// scrape that still finds a run relevant, so the TTL only needs to
	// survive consecutive missed/failed scrapes rather than any particular
	// data-related window. Defaulting it to a multiple of the scraping
	// interval — rather than an unrelated fixed value — means it tolerates
	// about the same span as runsUpdatedAfterSafetyMargin's default (20 x
	// 15s = 5m) worth of scrape failures before risking a double count.
	const cacheTTLScrapingIntervalMultiplier = 20
	cacheTTL := cacheTTLScrapingIntervalMultiplier * dagsterScrapingInterval
	if ttl_str := os.Getenv("CACHE_TTL_MINUTES"); ttl_str != "" {
		ttl, err := strconv.Atoi(ttl_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Cache TTL Minutes value: %w", err)
		}
		cacheTTL = time.Duration(ttl) * time.Minute
	}

	// Completed runs are fetched incrementally after the first scrape (see
	// CollectCompletedRuns), so LookbackWindow only matters for the initial
	// backfill on startup. Defaulting it to the scraping interval avoids
	// dumping a large batch of historical runs into the counters at t=0.
	lookbackWindow := dagsterScrapingInterval
	if lookback_window_str := os.Getenv("LOOKBACK_WINDOW_MINUTES"); lookback_window_str != "" {
		lookback, err := strconv.Atoi(lookback_window_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Lookback Window Minutes value: %w", err)
		}
		lookbackWindow = time.Duration(lookback) * time.Minute
	}

	runsPageSize := 500
	if pageSize_str := os.Getenv("RUNS_PAGE_SIZE"); pageSize_str != "" {
		pageSize, err := strconv.Atoi(pageSize_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Runs Page Size value: %w", err)
		}
		runsPageSize = pageSize
	}

	dagsterScrapingTimeoutSeconds := 10
	if scrapingTimeout_str := os.Getenv("DAGSTER_SCRAPING_TIMEOUT_SECONDS"); scrapingTimeout_str != "" {
		scrapingTimeout, err := strconv.Atoi(scrapingTimeout_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Dagster Scraping Timeout Seconds value: %w", err)
		}
		dagsterScrapingTimeoutSeconds = scrapingTimeout
	}
	dagsterScrapingTimeout := time.Duration(dagsterScrapingTimeoutSeconds) * time.Second

	return &Config{
		Port:                         port,
		DagsterGraphQLEndpoint:       dagsterGraphQLEndpoint,
		LookbackWindow:               lookbackWindow,
		CacheTTL:                     cacheTTL,
		DagsterScrapingInterval:      dagsterScrapingInterval,
		DagsterScrapingTimeout:       dagsterScrapingTimeout,
		RunsPageSize:                 runsPageSize,
		RunsUpdatedAfterSafetyMargin: runsUpdatedAfterSafetyMargin,
	}, nil
}
