package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assetStatusServer routes the two GraphQL queries CollectAssetStatus issues
// (get_asset_nodes.graphql, then get_assets_latest_info.graphql) to separate
// canned response bodies, keyed off which field name the request's query
// text asks for — the same shape as a real Dagster endpoint, which answers
// both from a single /graphql URL.
func assetStatusServer(t *testing.T, nodesBody, latestInfoBody string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		switch {
		case strings.Contains(req.Query, "assetNodes"):
			_, err := w.Write([]byte(nodesBody))
			require.NoError(t, err)
		case strings.Contains(req.Query, "assetsLatestInfo"):
			_, err := w.Write([]byte(latestInfoBody))
			require.NoError(t, err)
		default:
			t.Fatalf("unexpected query: %s", req.Query)
		}
	}))
	t.Cleanup(ts.Close)
	return ts
}

// TestCollectAssetStatusDistinguishesNeverRunFromFailed exercises the reason
// this collector needs a second query at all: assetNodes.staleStatus is
// derived from assetMaterializations, which only records successful events,
// so a failed run and "never run" both show up there as MISSING. Only
// assetsLatestInfo.latestRun.status tells them apart (issue #56).
func TestCollectAssetStatusDistinguishesNeverRunFromFailed(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["good_asset"]}, "staleStatus": "FRESH"},
				{"assetKey": {"path": ["bad_asset"]}, "staleStatus": "MISSING"},
				{"assetKey": {"path": ["never_run_asset"]}, "staleStatus": "MISSING"}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["good_asset"]}, "latestRun": {"status": "SUCCESS"}},
				{"assetKey": {"path": ["bad_asset"]}, "latestRun": {"status": "FAILURE"}},
				{"assetKey": {"path": ["never_run_asset"]}, "latestRun": null}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Len(t, c.assetStatus, 3)
	assert.Equal(t, assetStatusEntry{staleStatus: "FRESH", lastMaterializationStatus: "SUCCESS"}, c.assetStatus["good_asset"])
	assert.Equal(t, assetStatusEntry{staleStatus: "MISSING", lastMaterializationStatus: "FAILURE"}, c.assetStatus["bad_asset"],
		"a failed run must be distinguishable from an asset that has never run, even though both look MISSING via staleStatus alone")
	assert.Equal(t, assetStatusEntry{staleStatus: "MISSING", lastMaterializationStatus: ""}, c.assetStatus["never_run_asset"])
}

// TestCollectAssetStatusJoinsMultiSegmentAssetKeys checks the "/"-joined
// label value for an asset key with more than one path segment (e.g. a dbt
// project and model name), the common case in practice.
func TestCollectAssetStatusJoinsMultiSegmentAssetKeys(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["my_dbt_project", "customers"]}, "staleStatus": "STALE"}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["my_dbt_project", "customers"]}, "latestRun": {"status": "SUCCESS"}}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Contains(t, c.assetStatus, "my_dbt_project/customers")
}

// TestCollectAssetStatusHandlesNullStaleStatus checks the nullable
// staleStatus field: an asset with no computable stale status must end up
// with an empty staleStatus, not a literal "null" or "<nil>" label value.
func TestCollectAssetStatusHandlesNullStaleStatus(t *testing.T) {
	nodesBody := `{
		"data": {
			"assetNodes": [
				{"assetKey": {"path": ["no_stale_status_asset"]}, "staleStatus": null}
			]
		}
	}`
	latestInfoBody := `{
		"data": {
			"assetsLatestInfo": [
				{"assetKey": {"path": ["no_stale_status_asset"]}, "latestRun": null}
			]
		}
	}`
	ts := assetStatusServer(t, nodesBody, latestInfoBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))

	require.Contains(t, c.assetStatus, "no_stale_status_asset")
	assert.Equal(t, "", c.assetStatus["no_stale_status_asset"].staleStatus)
}

// TestCollectAssetStatusSkipsLatestInfoQueryWhenNoAssetsDefined checks that
// an empty assetNodes result short-circuits before asking assetsLatestInfo
// for zero asset keys — a request the schema doesn't reject, but that's
// pure waste when the answer is already known to be empty.
func TestCollectAssetStatusSkipsLatestInfoQueryWhenNoAssetsDefined(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		if strings.Contains(req.Query, "assetsLatestInfo") {
			t.Fatal("assetsLatestInfo should not be queried when assetNodes returned nothing")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"data": {"assetNodes": []}}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectAssetStatus(t.Context(), c))
	assert.Empty(t, c.assetStatus)
}

// TestCollectAssetStatusReturnsErrorOnGraphQLError checks that a top-level
// GraphQL error from the first query is propagated rather than treated as
// "no assets".
func TestCollectAssetStatusReturnsErrorOnGraphQLError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"errors": [{"message": "boom"}]}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	err := CollectAssetStatus(t.Context(), c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
}

func TestReflectAssetStatus(t *testing.T) {
	c := NewDagsterCollector(t.Context(), "http://unused", time.Hour, time.Hour, 500, 5*time.Minute)
	c.assetStatus = map[string]assetStatusEntry{
		"good_asset":      {staleStatus: "FRESH", lastMaterializationStatus: "SUCCESS"},
		"never_run_asset": {staleStatus: "MISSING", lastMaterializationStatus: ""},
	}

	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectAssetStatus(c, ch)
		close(ch)
	}()

	type key struct {
		metric   string
		assetKey string
		status   string
	}
	seen := make(map[key]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))

		desc := m.Desc().String()
		var metricName string
		switch {
		case strings.Contains(desc, "dagster_asset_stale_status"):
			metricName = "dagster_asset_stale_status"
		case strings.Contains(desc, "dagster_asset_last_materialization_status"):
			metricName = "dagster_asset_last_materialization_status"
		default:
			t.Fatalf("unexpected metric desc: %s", desc)
		}

		var assetKey, status string
		for _, l := range dm.GetLabel() {
			switch l.GetName() {
			case "asset_key":
				assetKey = l.GetValue()
			case "status":
				status = l.GetValue()
			}
		}
		seen[key{metricName, assetKey, status}] = dm.GetGauge().GetValue()
	}

	assert.Equal(t, float64(1), seen[key{"dagster_asset_stale_status", "good_asset", "fresh"}])
	assert.Equal(t, float64(1), seen[key{"dagster_asset_last_materialization_status", "good_asset", "success"}])
	assert.Equal(t, float64(1), seen[key{"dagster_asset_stale_status", "never_run_asset", "missing"}])
	assert.Len(t, seen, 3,
		"never_run_asset must not emit a dagster_asset_last_materialization_status series: it has no run to report a status for")
}
