package config

import (
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// CollectorNames lists every collector the exporter runs, in the names used
// for the collector label on the self-health metrics. Each one can override
// the global scraping interval and timeout through
// <NAME>_SCRAPING_INTERVAL_SECONDS / <NAME>_SCRAPING_TIMEOUT_SECONDS, where
// NAME is the collector name upper-cased (ASSET_STATUS, ...).
var CollectorNames = []string{
	"definitions_roster",
	"active_runs",
	"completed_runs",
	"code_location_status",
	"daemon_health",
	"asset_status",
	"op_pool_concurrency",
}

// CollectorSchedule is how often one collector scrapes and how long a single
// scrape of it may take.
type CollectorSchedule struct {
	Interval time.Duration
	Timeout  time.Duration
}

type Config struct {
	Port                         int
	DagsterGraphQLEndpoint       string
	DagsterCloudAPIToken         string
	LookbackWindow               time.Duration
	CacheTTL                     time.Duration
	DagsterScrapingInterval      time.Duration
	DagsterScrapingTimeout       time.Duration
	RunsPageSize                 int
	RunsUpdatedAfterSafetyMargin time.Duration
	// Collectors holds the effective schedule of every collector in
	// CollectorNames: its own override where set, else the global
	// DagsterScrapingInterval / DagsterScrapingTimeout.
	Collectors map[string]CollectorSchedule
	// Debug is set by LOG_LEVEL=debug and turns on per-call GraphQL timing
	// logs.
	Debug bool
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

	// Optional, and empty by default: OSS Dagster's GraphQL API is
	// unauthenticated, so the common case sets nothing here and no auth
	// header is sent (see doGraphQL). It's required only for Dagster+, whose
	// per-deployment GraphQL endpoint
	// (https://<org>.dagster.cloud/<deployment>/graphql) rejects every
	// request without a Dagster-Cloud-Api-Token header. Read from the
	// environment rather than a flag so the token isn't exposed in the
	// process's argv/command line.
	dagsterCloudAPIToken := os.Getenv("DAGSTER_CLOUD_API_TOKEN")

	dagsterScrapingInterval, err := getEnvDuration("DAGSTER_SCRAPING_INTERVAL_SECONDS", time.Second, 15*time.Second)
	if err != nil {
		return nil, err
	}

	dagsterScrapingTimeout, err := getEnvDuration("DAGSTER_SCRAPING_TIMEOUT_SECONDS", time.Second, 10*time.Second)
	if err != nil {
		return nil, err
	}

	collectors, err := loadCollectorSchedules(dagsterScrapingInterval, dagsterScrapingTimeout)
	if err != nil {
		return nil, err
	}

	// The settings below tune completed_runs, the only collector whose
	// correctness depends on how often it runs, so they follow its interval
	// rather than the global one.
	completedRunsInterval := collectors["completed_runs"].Interval

	runsUpdatedAfterSafetyMargin, err := getEnvDuration("RUNS_UPDATED_AFTER_SAFETY_MARGIN_MINUTES", time.Minute, 5*time.Minute)
	if err != nil {
		return nil, err
	}

	cacheTTL, err := getEnvDuration("CACHE_TTL_MINUTES", time.Minute, cacheTTLScrapingIntervalMultiplier*completedRunsInterval)
	if err != nil {
		return nil, err
	}
	// processedRuns refreshes TTL on hit, so a run is counted twice only when
	// scrapes fail for longer than the TTL. Require the TTL to cover at least
	// two intervals so a single missed scrape cannot expire the entry.
	if cacheTTL < 2*completedRunsInterval {
		return nil, fmt.Errorf("invalid CACHE_TTL_MINUTES value: %v is shorter than 2 * the completed_runs scraping interval (%v, from DAGSTER_SCRAPING_INTERVAL_SECONDS or COMPLETED_RUNS_SCRAPING_INTERVAL_SECONDS); a TTL that short can double-count completed runs after a missed scrape", cacheTTL, completedRunsInterval)
	}

	// Completed runs are fetched incrementally after the first scrape (see
	// CollectCompletedRuns), so LookbackWindow only matters for the initial
	// backfill on startup. Defaulting it to the scraping interval avoids
	// dumping a large batch of historical runs into the counters at t=0.
	lookbackWindow, err := getEnvDuration("LOOKBACK_WINDOW_MINUTES", time.Minute, completedRunsInterval)
	if err != nil {
		return nil, err
	}

	// The upper bound is MaxInt32 because GraphQL's Int is 32-bit: a larger
	// value can't be represented as the query's limit variable anyway.
	runsPageSize, err := getEnvInt("RUNS_PAGE_SIZE", 500, 1, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	debug, err := loadDebug()
	if err != nil {
		return nil, err
	}

	return &Config{
		Port:                         port,
		DagsterGraphQLEndpoint:       dagsterGraphQLEndpoint,
		DagsterCloudAPIToken:         dagsterCloudAPIToken,
		LookbackWindow:               lookbackWindow,
		CacheTTL:                     cacheTTL,
		DagsterScrapingInterval:      dagsterScrapingInterval,
		DagsterScrapingTimeout:       dagsterScrapingTimeout,
		RunsPageSize:                 runsPageSize,
		RunsUpdatedAfterSafetyMargin: runsUpdatedAfterSafetyMargin,
		Collectors:                   collectors,
		Debug:                        debug,
	}, nil
}

// loadCollectorSchedules resolves every collector's interval and timeout:
// its <NAME>_SCRAPING_*_SECONDS override if set, else the global value.
//
// A collector's scrapes run serially, so a timeout longer than its interval
// lets one slow scrape push the next tick back and stretch the effective
// interval. That isn't invalid -- waiting longer on a temporarily slow
// Dagster is a reasonable thing to want -- so this warns rather than errors,
// just so it doesn't happen unnoticed.
func loadCollectorSchedules(interval, timeout time.Duration) (map[string]CollectorSchedule, error) {
	// Collectors that inherit both global values share the global warning
	// below instead of each repeating it.
	if timeout > interval {
		log.Printf("warning: DAGSTER_SCRAPING_TIMEOUT_SECONDS (%v) is longer than DAGSTER_SCRAPING_INTERVAL_SECONDS (%v); a slow scrape will stretch the effective interval",
			timeout, interval)
	}

	schedules := make(map[string]CollectorSchedule, len(CollectorNames))
	for _, name := range CollectorNames {
		prefix := strings.ToUpper(name)

		collectorInterval, err := getEnvDuration(prefix+"_SCRAPING_INTERVAL_SECONDS", time.Second, interval)
		if err != nil {
			return nil, err
		}
		collectorTimeout, err := getEnvDuration(prefix+"_SCRAPING_TIMEOUT_SECONDS", time.Second, timeout)
		if err != nil {
			return nil, err
		}

		overridden := collectorInterval != interval || collectorTimeout != timeout
		if overridden && collectorTimeout > collectorInterval {
			log.Printf("warning: the %s scraping timeout (%v) is longer than its scraping interval (%v); a slow scrape will stretch the effective interval",
				name, collectorTimeout, collectorInterval)
		}
		schedules[name] = CollectorSchedule{Interval: collectorInterval, Timeout: collectorTimeout}
	}
	return schedules, nil
}

// loadDebug reads LOG_LEVEL, which is either "info" (the default) or "debug".
// Anything else is rejected rather than silently ignored, so a typo like
// LOG_LEVEL=degub doesn't leave the user wondering why nothing is logged.
func loadDebug() (bool, error) {
	switch level := strings.ToLower(os.Getenv("LOG_LEVEL")); level {
	case "", "info":
		return false, nil
	case "debug":
		return true, nil
	default:
		return false, fmt.Errorf("invalid LOG_LEVEL value: %q (must be info or debug)", level)
	}
}
