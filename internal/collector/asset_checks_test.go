package collector

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assetChecksBody mirrors what Dagster 1.13 returned for five checks on two
// assets: one that passed, one that failed, one that failed with severity
// WARN, one that raised an exception, and one that has never run.
const assetChecksBody = `{"data": {"assetNodes": [
	{"assetKey": {"path": ["orders"]}, "assetChecksOrError": {"__typename": "AssetChecks", "checks": [
		{"name": "not_empty", "executionForLatestMaterialization": {"status": "SUCCEEDED", "evaluation": {"severity": "ERROR"}}},
		{"name": "no_nulls", "executionForLatestMaterialization": {"status": "FAILED", "evaluation": {"severity": "ERROR"}}},
		{"name": "fresh_enough", "executionForLatestMaterialization": {"status": "FAILED", "evaluation": {"severity": "WARN"}}}
	]}},
	{"assetKey": {"path": ["dbt", "customers"]}, "assetChecksOrError": {"__typename": "AssetChecks", "checks": [
		{"name": "crashes", "executionForLatestMaterialization": {"status": "EXECUTION_FAILED", "evaluation": null}},
		{"name": "never_ran", "executionForLatestMaterialization": null}
	]}}
]}}`

func assetCheckSeries(t *testing.T, c *DagsterCollector) map[[4]string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 16)
	go func() {
		reflectAssetChecks(c, ch)
		close(ch)
	}()

	series := make(map[[4]string]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		labels := make(map[string]string)
		for _, l := range dm.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		series[[4]string{labels["asset_key"], labels["check_name"], labels["status"], labels["severity"]}] = dm.GetGauge().GetValue()
	}
	return series
}

func newAssetChecksCollector(t *testing.T, body string) *DagsterCollector {
	t.Helper()
	ts := daemonHealthServer(t, body)
	return NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
}

func TestCollectAssetChecksReportsEachChecksLatestExecution(t *testing.T) {
	c := newAssetChecksCollector(t, assetChecksBody)

	require.NoError(t, CollectAssetChecks(t.Context(), c))

	assert.Equal(t, map[[4]string]float64{
		{"orders", "not_empty", "succeeded", "error"}:        1,
		{"orders", "no_nulls", "failed", "error"}:            1,
		{"orders", "fresh_enough", "failed", "warn"}:         1,
		{"dbt/customers", "crashes", "execution_failed", ""}: 1,
	}, assetCheckSeries(t, c),
		"a warning must be told apart from an error, and a check that could not run from one whose data is bad; a check that never ran has no series")
}

func TestCollectAssetChecksSkipsAnAssetWhoseChecksCannotBeRead(t *testing.T) {
	c := newAssetChecksCollector(t, `{"data": {"assetNodes": [
		{"assetKey": {"path": ["old"]}, "assetChecksOrError": {"__typename": "AssetCheckNeedsUserCodeUpgrade"}},
		{"assetKey": {"path": ["new"]}, "assetChecksOrError": {"__typename": "AssetChecks", "checks": [
			{"name": "ok", "executionForLatestMaterialization": {"status": "SUCCEEDED", "evaluation": {"severity": "ERROR"}}}
		]}}
	]}}`)

	require.NoError(t, CollectAssetChecks(t.Context(), c))

	assert.Equal(t, map[[4]string]float64{{"new", "ok", "succeeded", "error"}: 1}, assetCheckSeries(t, c),
		"one location that needs an upgrade must not hide the checks of the others")
}

func TestCollectAssetChecksKeepsTheLastStateWhenDagsterFails(t *testing.T) {
	s := newScriptedServer(t, assetChecksBody)
	c := NewDagsterCollector(t.Context(), s.url, "", time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectAssetChecks(t.Context(), c))

	s.serve(`{"errors": [{"message": "boom"}]}`)

	require.Error(t, CollectAssetChecks(t.Context(), c))
	assert.Len(t, assetCheckSeries(t, c), 4, "a failed scrape must not erase the checks that were read before")
}
