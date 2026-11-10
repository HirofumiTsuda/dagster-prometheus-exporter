package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assetMetadataBody mirrors what Dagster 1.13 returned for assets recording an
// int, a float, text, a URL, a boolean, NaN and an int past 32 bits.
const assetMetadataBody = `{"data": {"assetNodes": [
	{"assetKey": {"path": ["orders"]}, "assetMaterializations": [{"timestamp": "1793877398151", "metadataEntries": [
		{"label": "row_count", "intRepr": "12345"},
		{"label": "accuracy", "floatValue": 0.93},
		{"label": "note"},
		{"label": "link"},
		{"label": "is_ok"},
		{"label": "nan_value", "floatValue": null},
		{"label": "unlisted", "intRepr": "9"}
	]}]},
	{"assetKey": {"path": ["dbt", "customers"]}, "assetMaterializations": [{"timestamp": "1793877000000", "metadataEntries": [
		{"label": "row_count", "intRepr": "5000000000"}
	]}]},
	{"assetKey": {"path": ["never_ran"]}, "assetMaterializations": []},
	{"assetKey": {"path": ["daily"]}, "assetMaterializations": [{"timestamp": "1793877000000", "partition": "2026-01-02", "metadataEntries": [
		{"label": "row_count", "intRepr": "20"}
	]}]},
	{"assetKey": {"path": ["regional"]}, "assetMaterializations": [{"timestamp": "1793877100000", "partition": "2026-01-03|us", "metadataEntries": [
		{"label": "row_count", "intRepr": "1"}
	]}]}
]}}`

var assetMetadataAllowList = map[string][]string{
	"orders":        {"row_count", "accuracy", "note", "nan_value", "missing"},
	"dbt/customers": {"row_count"},
	"never_ran":     {"row_count"},
	"daily":         {"row_count"},
	"regional":      {"row_count"},
}

func newAssetMetadataCollector(t *testing.T, url string, allowList map[string][]string) *DagsterCollector {
	t.Helper()
	c := NewDagsterCollector(t.Context(), url, "", time.Hour, time.Hour, 500, 5*time.Minute)
	c.SetAssetMetadataValues(allowList)
	return c
}

// assetMetadataSeries collects what reflectAssetMetadata emits: the values by
// asset key, partition and label, and the timestamps by asset key and partition.
func assetMetadataSeries(t *testing.T, c *DagsterCollector) (values map[[3]string]float64, timestamps map[[2]string]float64) {
	t.Helper()
	ch := make(chan prometheus.Metric, 32)
	go func() {
		reflectAssetMetadata(c, ch)
		close(ch)
	}()

	values = make(map[[3]string]float64)
	timestamps = make(map[[2]string]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		labels := make(map[string]string)
		for _, l := range dm.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if label, ok := labels["label"]; ok {
			values[[3]string{labels["asset_key"], labels["partition"], label}] = dm.GetGauge().GetValue()
		} else {
			timestamps[[2]string{labels["asset_key"], labels["partition"]}] = dm.GetGauge().GetValue()
		}
	}
	return values, timestamps
}

func TestCollectAssetMetadataExportsOnlyListedNumericValues(t *testing.T) {
	c := newAssetMetadataCollector(t, daemonHealthServer(t, assetMetadataBody).URL, assetMetadataAllowList)

	require.NoError(t, CollectAssetMetadata(t.Context(), c))

	values, timestamps := assetMetadataSeries(t, c)
	assert.Equal(t, map[[3]string]float64{
		{"orders", "", "row_count"}:                12345,
		{"orders", "", "accuracy"}:                 0.93,
		{"dbt/customers", "", "row_count"}:         5e9,
		{"daily", "2026-01-02", "row_count"}:       20,
		{"regional", "2026-01-03|us", "row_count"}: 1,
	}, values, "text, NaN, a label that is not listed and one that is missing give no series; an int past 32 bits is kept")
	assert.Equal(t, map[[2]string]float64{
		{"orders", ""}:                1793877398.151,
		{"dbt/customers", ""}:         1793877000,
		{"daily", "2026-01-02"}:       1793877000,
		{"regional", "2026-01-03|us"}: 1793877100,
	}, timestamps, "the timestamp is in seconds, and an asset with no materialization has none")
}

func TestCollectAssetMetadataAsksOnlyForTheListedAssets(t *testing.T) {
	var got GraphQLRequest
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		_, err := w.Write([]byte(`{"data": {"assetNodes": []}}`))
		require.NoError(t, err)
	}))
	t.Cleanup(ts.Close)
	c := newAssetMetadataCollector(t, ts.URL, map[string][]string{"orders": {"row_count"}, "dbt/customers": {"row_count"}})

	require.NoError(t, CollectAssetMetadata(t.Context(), c))

	assert.Equal(t, []interface{}{
		map[string]interface{}{"path": []interface{}{"dbt", "customers"}},
		map[string]interface{}{"path": []interface{}{"orders"}},
	}, got.Variables["assetKeys"], "the cost grows with the assets asked for, so only the listed ones are")
}

func TestCollectAssetMetadataAsksNothingWithoutAnAllowList(t *testing.T) {
	var requests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
	t.Cleanup(ts.Close)
	c := newAssetMetadataCollector(t, ts.URL, nil)

	require.NoError(t, CollectAssetMetadata(t.Context(), c))

	assert.Zero(t, requests)
	values, _ := assetMetadataSeries(t, c)
	assert.Empty(t, values)
}

func TestCollectAssetMetadataKeepsTheLastStateWhenDagsterFails(t *testing.T) {
	s := newScriptedServer(t, assetMetadataBody)
	c := newAssetMetadataCollector(t, s.url, assetMetadataAllowList)
	require.NoError(t, CollectAssetMetadata(t.Context(), c))

	s.serve(`{"errors": [{"message": "boom"}]}`)

	require.Error(t, CollectAssetMetadata(t.Context(), c))
	values, _ := assetMetadataSeries(t, c)
	assert.Len(t, values, 5, "a failed scrape must not erase the values read before")
}

// A re-run of an old partition is the latest materialization, so the series
// moves to it and the old partition's series goes, rather than both staying.
func TestCollectAssetMetadataReplacesThePartitionWhenAnotherRuns(t *testing.T) {
	daily := func(partition, rows string) string {
		return `{"data": {"assetNodes": [{"assetKey": {"path": ["daily"]}, "assetMaterializations": [{"timestamp": "1793877000000", "partition": "` +
			partition + `", "metadataEntries": [{"label": "row_count", "intRepr": "` + rows + `"}]}]}]}}`
	}
	s := newScriptedServer(t, daily("2026-01-09", "9"))
	c := newAssetMetadataCollector(t, s.url, map[string][]string{"daily": {"row_count"}})
	require.NoError(t, CollectAssetMetadata(t.Context(), c))

	s.serve(daily("2026-01-02", "2"))
	require.NoError(t, CollectAssetMetadata(t.Context(), c))

	values, timestamps := assetMetadataSeries(t, c)
	assert.Equal(t, map[[3]string]float64{{"daily", "2026-01-02", "row_count"}: 2}, values)
	assert.Len(t, timestamps, 1)
}
