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

// getEnvInt は環境変数 key を int として読み込む。未設定なら def を返す。
func getEnvInt(key string, def int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return def, nil
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %w", key, err)
	}
	return n, nil
}

// getEnvDuration は環境変数 key を unit 単位の int として読み込み time.Duration に変換する。
// 未設定なら def をそのまま返す(def は unit と異なる単位で組み立てられていてもよい)。
func getEnvDuration(key string, unit time.Duration, def time.Duration) (time.Duration, error) {
	val := os.Getenv(key)
	if val == "" {
		return def, nil
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %w", key, err)
	}
	return time.Duration(n) * unit, nil
}

// processedRuns entries are touched (their TTL refreshed) on every scrape
// that still finds a run relevant, so the TTL only needs to survive
// consecutive missed/failed scrapes rather than any particular data-related
// window. Defaulting it to a multiple of the scraping interval — rather than
// an unrelated fixed value — means it tolerates about the same span as
// RunsUpdatedAfterSafetyMargin's default (20 x 15s = 5m) worth of scrape
// failures before risking a double count.
const cacheTTLScrapingIntervalMultiplier = 20

// Load は環境変数などから設定を読み込んで返す
func Load() (*Config, error) {
	port, err := getEnvInt("PORT", 9101)
	if err != nil {
		return nil, err
	}

	dagsterGraphQLEndpoint := os.Getenv("DAGSTER_GRAPHQL_ENDPOINT")
	if dagsterGraphQLEndpoint == "" {
		dagsterGraphQLEndpoint = "http://127.0.0.1:3000/graphql"
	}

	dagsterScrapingInterval, err := getEnvDuration("DAGSTER_SCRAPING_INTERVAL_SECONDS", time.Second, 15*time.Second)
	if err != nil {
		return nil, err
	}

	runsUpdatedAfterSafetyMargin, err := getEnvDuration("RUNS_UPDATED_AFTER_SAFETY_MARGIN_MINUTES", time.Minute, 5*time.Minute)
	if err != nil {
		return nil, err
	}

	cacheTTL, err := getEnvDuration("CACHE_TTL_MINUTES", time.Minute, cacheTTLScrapingIntervalMultiplier*dagsterScrapingInterval)
	if err != nil {
		return nil, err
	}

	// Completed runs are fetched incrementally after the first scrape (see
	// CollectCompletedRuns), so LookbackWindow only matters for the initial
	// backfill on startup. Defaulting it to the scraping interval avoids
	// dumping a large batch of historical runs into the counters at t=0.
	lookbackWindow, err := getEnvDuration("LOOKBACK_WINDOW_MINUTES", time.Minute, dagsterScrapingInterval)
	if err != nil {
		return nil, err
	}

	runsPageSize, err := getEnvInt("RUNS_PAGE_SIZE", 500)
	if err != nil {
		return nil, err
	}

	dagsterScrapingTimeout, err := getEnvDuration("DAGSTER_SCRAPING_TIMEOUT_SECONDS", time.Second, 10*time.Second)
	if err != nil {
		return nil, err
	}

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
