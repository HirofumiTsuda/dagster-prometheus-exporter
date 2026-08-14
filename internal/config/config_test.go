package config

import (
	"bytes"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// configEnvVars is every environment variable Load reads. Tests blank them
// all out first so a variable that happens to be set in the developer's own
// shell can't change the result — an empty value is treated as unset by
// getEnvInt/getEnvDuration, so this is equivalent to a clean environment.
var configEnvVars = []string{
	"PORT",
	"DAGSTER_GRAPHQL_ENDPOINT",
	"DAGSTER_SCRAPING_INTERVAL_SECONDS",
	"DAGSTER_SCRAPING_TIMEOUT_SECONDS",
	"RUNS_UPDATED_AFTER_SAFETY_MARGIN_MINUTES",
	"CACHE_TTL_MINUTES",
	"LOOKBACK_WINDOW_MINUTES",
	"RUNS_PAGE_SIZE",
}

func setEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, key := range configEnvVars {
		t.Setenv(key, "")
	}
	for key, value := range env {
		t.Setenv(key, value)
	}
}

func TestLoadDefaults(t *testing.T) {
	setEnv(t, nil)

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 9101, cfg.Port)
	assert.Equal(t, "http://127.0.0.1:3000/graphql", cfg.DagsterGraphQLEndpoint)
	assert.Equal(t, 15*time.Second, cfg.DagsterScrapingInterval)
	assert.Equal(t, 10*time.Second, cfg.DagsterScrapingTimeout)
	assert.Equal(t, 5*time.Minute, cfg.RunsUpdatedAfterSafetyMargin)
	assert.Equal(t, 500, cfg.RunsPageSize)

	// Both of these are derived from the scraping interval rather than being
	// fixed values, which is easy to break by accident — see the comments on
	// cacheTTLScrapingIntervalMultiplier and LOOKBACK_WINDOW_MINUTES.
	assert.Equal(t, cacheTTLScrapingIntervalMultiplier*15*time.Second, cfg.CacheTTL)
	assert.Equal(t, 15*time.Second, cfg.LookbackWindow)
}

func TestLoadDerivedDefaultsFollowTheScrapingInterval(t *testing.T) {
	setEnv(t, map[string]string{"DAGSTER_SCRAPING_INTERVAL_SECONDS": "30"})

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 30*time.Second, cfg.DagsterScrapingInterval)
	assert.Equal(t, 30*time.Second, cfg.LookbackWindow, "lookback window should default to the scraping interval")
	assert.Equal(t, 10*time.Minute, cfg.CacheTTL, "cache TTL should default to 20x the scraping interval")
}

func TestLoadExplicitValuesOverrideDerivedDefaults(t *testing.T) {
	setEnv(t, map[string]string{
		"PORT":                                     "8080",
		"DAGSTER_GRAPHQL_ENDPOINT":                 "http://dagster.example:3000/graphql",
		"DAGSTER_SCRAPING_INTERVAL_SECONDS":        "30",
		"DAGSTER_SCRAPING_TIMEOUT_SECONDS":         "20",
		"RUNS_UPDATED_AFTER_SAFETY_MARGIN_MINUTES": "2",
		"CACHE_TTL_MINUTES":                        "60",
		"LOOKBACK_WINDOW_MINUTES":                  "120",
		"RUNS_PAGE_SIZE":                           "250",
	})

	cfg, err := Load()
	require.NoError(t, err)

	assert.Equal(t, 8080, cfg.Port)
	assert.Equal(t, "http://dagster.example:3000/graphql", cfg.DagsterGraphQLEndpoint)
	assert.Equal(t, 30*time.Second, cfg.DagsterScrapingInterval)
	assert.Equal(t, 20*time.Second, cfg.DagsterScrapingTimeout)
	assert.Equal(t, 2*time.Minute, cfg.RunsUpdatedAfterSafetyMargin)
	assert.Equal(t, 60*time.Minute, cfg.CacheTTL)
	assert.Equal(t, 120*time.Minute, cfg.LookbackWindow)
	assert.Equal(t, 250, cfg.RunsPageSize)
}

func TestLoadRejectsUnusableValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"port zero", map[string]string{"PORT": "0"}},
		{"port above the valid range", map[string]string{"PORT": "65536"}},
		{"port negative", map[string]string{"PORT": "-1"}},
		{"port not a number", map[string]string{"PORT": "not-a-number"}},

		// Zero used to slip past fetchRunPages' end-of-pagination check and
		// panic on results[-1].
		{"page size zero", map[string]string{"RUNS_PAGE_SIZE": "0"}},
		{"page size negative", map[string]string{"RUNS_PAGE_SIZE": "-1"}},

		// Zero panics time.NewTicker in startScrape.
		{"scraping interval zero", map[string]string{"DAGSTER_SCRAPING_INTERVAL_SECONDS": "0"}},
		{"scraping interval negative", map[string]string{"DAGSTER_SCRAPING_INTERVAL_SECONDS": "-15"}},

		{"scraping timeout zero", map[string]string{"DAGSTER_SCRAPING_TIMEOUT_SECONDS": "0"}},

		// Negative puts updatedAfter in the future, so the initial backfill
		// silently matches nothing.
		{"lookback window negative", map[string]string{"LOOKBACK_WINDOW_MINUTES": "-5"}},
		{"lookback window zero", map[string]string{"LOOKBACK_WINDOW_MINUTES": "0"}},

		{"cache TTL zero", map[string]string{"CACHE_TTL_MINUTES": "0"}},
		{"safety margin zero", map[string]string{"RUNS_UPDATED_AFTER_SAFETY_MARGIN_MINUTES": "0"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			setEnv(t, tc.env)

			cfg, err := Load()

			require.Error(t, err)
			assert.Nil(t, cfg)
			for key := range tc.env {
				assert.Contains(t, err.Error(), key,
					"the error should name the environment variable at fault, so the operator knows what to fix")
			}
		})
	}
}

func TestLoadWarnsWhenTimeoutExceedsInterval(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	setEnv(t, map[string]string{
		"DAGSTER_SCRAPING_INTERVAL_SECONDS": "10",
		"DAGSTER_SCRAPING_TIMEOUT_SECONDS":  "30",
	})

	_, err := Load()
	require.NoError(t, err, "a timeout longer than the interval is unusual, not invalid")
	assert.Contains(t, buf.String(), "DAGSTER_SCRAPING_TIMEOUT_SECONDS")

	buf.Reset()
	setEnv(t, map[string]string{
		"DAGSTER_SCRAPING_INTERVAL_SECONDS": "30",
		"DAGSTER_SCRAPING_TIMEOUT_SECONDS":  "10",
	})

	_, err = Load()
	require.NoError(t, err)
	assert.Empty(t, buf.String(), "the usual timeout < interval case should be silent")
}
