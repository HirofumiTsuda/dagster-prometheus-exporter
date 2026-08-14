package config

import (
	"fmt"
	"log"
	"math"
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

// getEnvInt reads env var key as an int, returning def when it's unset, and
// rejects anything outside [minVal, maxVal].
//
// The range check is what keeps a bad value from turning into a confusing
// failure much later: RUNS_PAGE_SIZE=0 slips past fetchRunPages' end-of-
// pagination test and panics, and a PORT outside the valid range only shows
// up as a ListenAndServe failure that never names the env var responsible.
// Failing here means the error says which setting is wrong.
func getEnvInt(key string, def, minVal, maxVal int) (int, error) {
	val := os.Getenv(key)
	if val == "" {
		return def, nil
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %w", key, err)
	}
	if n < minVal || n > maxVal {
		return 0, fmt.Errorf("invalid %s value: %d is outside the allowed range [%d, %d]", key, n, minVal, maxVal)
	}
	return n, nil
}

// getEnvDuration reads env var key as an int count of unit and converts it
// to a time.Duration, returning def unchanged when the var is unset (def may
// be expressed in a different unit than unit).
//
// Zero and negative values are rejected, because every setting read through
// here breaks something downstream at those values:
// DAGSTER_SCRAPING_INTERVAL_SECONDS=0 panics time.NewTicker, and a negative
// LOOKBACK_WINDOW_MINUTES puts updatedAfter in the future so the initial
// backfill silently matches nothing.
func getEnvDuration(key string, unit time.Duration, def time.Duration) (time.Duration, error) {
	val := os.Getenv(key)
	if val == "" {
		return def, nil
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %w", key, err)
	}
	d := time.Duration(n) * unit
	if d <= 0 {
		return 0, fmt.Errorf("invalid %s value: %d (must be positive)", key, n)
	}
	return d, nil
}

// processedRuns entries are touched (their TTL refreshed) on every scrape
// that still finds a run relevant, so the TTL only needs to survive
// consecutive missed/failed scrapes rather than any particular data-related
// window. Defaulting it to a multiple of the scraping interval — rather than
// an unrelated fixed value — means it tolerates about the same span as
// RunsUpdatedAfterSafetyMargin's default (20 x 15s = 5m) worth of scrape
// failures before risking a double count.
const cacheTTLScrapingIntervalMultiplier = 20

// Load reads the exporter's configuration from environment variables,
// applying defaults for anything unset and rejecting values that can't work.
func Load() (*Config, error) {
	port, err := getEnvInt("PORT", 9101, 1, 65535)
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

	// The upper bound is MaxInt32 because GraphQL's Int is 32-bit: a larger
	// value can't be represented as the query's limit variable anyway.
	runsPageSize, err := getEnvInt("RUNS_PAGE_SIZE", 500, 1, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	dagsterScrapingTimeout, err := getEnvDuration("DAGSTER_SCRAPING_TIMEOUT_SECONDS", time.Second, 10*time.Second)
	if err != nil {
		return nil, err
	}

	// Scrapes run serially inside startScrape's loop, so a timeout longer
	// than the interval lets one slow scrape push the next tick back and
	// stretch the effective interval. That isn't invalid — waiting longer on
	// a temporarily slow Dagster is a reasonable thing to want — so this
	// warns rather than errors, just so it doesn't happen unnoticed.
	if dagsterScrapingTimeout > dagsterScrapingInterval {
		log.Printf("warning: DAGSTER_SCRAPING_TIMEOUT_SECONDS (%v) is longer than DAGSTER_SCRAPING_INTERVAL_SECONDS (%v); a slow scrape will stretch the effective interval",
			dagsterScrapingTimeout, dagsterScrapingInterval)
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
