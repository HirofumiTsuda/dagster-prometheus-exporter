package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port                    int
	DagsterURL              string
	LookbackWindow          time.Duration
	CacheTTL                time.Duration
	DagsterScrapingInterval time.Duration
	DagsterScrapingTimeout  time.Duration
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

	dagsterURL := os.Getenv("DAGSTER_URL")
	if dagsterURL == "" {
		dagsterURL = "http://127.0.0.1:3000/"
	}

	lookbackWindowMinutes := 12 * 60
	if lookback_window_str := os.Getenv("LOOKBACK_WINDOW_MINUTES"); lookback_window_str != "" {
		lookback, err := strconv.Atoi(lookback_window_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Lookback Window Minutes value: %w", err)
		}
		lookbackWindowMinutes = lookback
	}
	lookbackWindow := time.Duration(lookbackWindowMinutes) * time.Minute

	cacheTTLMinutes := 24 * 60
	if ttl_str := os.Getenv("CACHE_TTL_MINUTES"); ttl_str != "" {
		ttl, err := strconv.Atoi(ttl_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Cache TTL Minutes value: %w", err)
		}
		cacheTTLMinutes = ttl
	}
	cacheTTL := time.Duration(cacheTTLMinutes) * time.Minute

	dagsterScrapingIntervalSeconds := 15
	if scrapingInterval_str := os.Getenv("DAGSTER_SCRAPING_INTERVAL_SECONDS"); scrapingInterval_str != "" {
		scrapingInterval, err := strconv.Atoi(scrapingInterval_str)
		if err != nil {
			return nil, fmt.Errorf("invalid Dagster Scraping Interval Seconds value: %w", err)
		}
		dagsterScrapingIntervalSeconds = scrapingInterval
	}
	dagsterScrapingInterval := time.Duration(dagsterScrapingIntervalSeconds) * time.Second

	return &Config{
		Port:                    port,
		DagsterURL:              dagsterURL,
		LookbackWindow:          lookbackWindow,
		CacheTTL:                cacheTTL,
		DagsterScrapingInterval: dagsterScrapingInterval,
		DagsterScrapingTimeout:  10 * time.Second,
	}, nil
}
