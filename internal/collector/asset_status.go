package collector

import (
	"context"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// assetStatusEntry is one asset's most recently observed state.
type assetStatusEntry struct {
	// staleStatus is "" when Dagster reports no stale status for this asset
	// (the field is nullable in the schema) — no dagster_asset_stale_status
	// series is emitted for it.
	staleStatus string
	// lastMaterializationStatus is "" when the asset has never had a run —
	// no dagster_asset_last_materialization_status series is emitted for it.
	lastMaterializationStatus string
}

// CollectAssetStatus reports each asset's staleness and the outcome of its
// most recent materializing run.
//
// This is two GraphQL round trips rather than one: assetNodes (every asset
// currently defined, across every code location, in one fetch — like
// repositoriesOrError but without a repositorySelector or per-location
// union) has to run first, because assetsLatestInfo needs the asset keys it
// returns as input. Neither field is reachable via repositoriesOrError, so
// this is its own collector — the same reasoning as
// CollectCodeLocationStatus and CollectDaemonHealth.
//
// assetNodes.staleStatus alone can't answer "did the last run succeed":
// staleStatus is derived from assetMaterializations, which only ever
// records successful events. A run that fails before producing a
// materialization leaves an asset looking exactly like one that has simply
// never run (see issue #56's investigation notes). Only
// assetsLatestInfo.latestRun.status distinguishes the two, hence the second
// query.
func CollectAssetStatus(ctx context.Context, c *DagsterCollector) error {
	nodesReq := getAssetNodesRequest()
	nodesResp, err := getAssetNodes(ctx, nodesReq, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect asset nodes from dagster: %v", err)
		return err
	}

	entries := make(map[string]assetStatusEntry, len(nodesResp.Data.AssetNodes))
	assetKeys := make([]AssetKeyPath, 0, len(nodesResp.Data.AssetNodes))
	for _, node := range nodesResp.Data.AssetNodes {
		var entry assetStatusEntry
		if node.StaleStatus != nil {
			entry.staleStatus = *node.StaleStatus
		}
		entries[assetKeyLabel(node.AssetKey.Path)] = entry
		assetKeys = append(assetKeys, node.AssetKey.Path)
	}

	// No assets defined anywhere: skip the second query rather than ask
	// assetsLatestInfo for an empty key list.
	if len(assetKeys) > 0 {
		infoReq := getAssetsLatestInfoRequest(assetKeys)
		infoResp, err := getAssetsLatestInfo(ctx, infoReq, c.dagsterGraphQLEndpoint)
		if err != nil {
			log.Printf("failed to collect assets latest info from dagster: %v", err)
			return err
		}

		for _, info := range infoResp.Data.AssetsLatestInfo {
			if info.LatestRun == nil {
				continue
			}
			key := assetKeyLabel(info.AssetKey.Path)
			entry := entries[key]
			entry.lastMaterializationStatus = info.LatestRun.Status
			entries[key] = entry
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.assetStatus = entries

	return nil
}

// assetKeyLabel joins an asset key's path segments into a single Prometheus
// label value, e.g. ["my_dbt_project", "customers"] becomes
// "my_dbt_project/customers" — the same separator Dagster's own UI uses to
// render an asset key.
func assetKeyLabel(path AssetKeyPath) string {
	return strings.Join(path, "/")
}

// reflectAssetStatus emits dagster_asset_stale_status and
// dagster_asset_last_materialization_status from a single locked pass over
// c.assetStatus, the same reasoning as reflectDaemonHealth: both come from
// one entry per asset.
func reflectAssetStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for assetKey, entry := range c.assetStatus {
		if entry.staleStatus != "" {
			ch <- prometheus.MustNewConstMetric(
				c.assetStaleStatusDesc,
				prometheus.GaugeValue,
				1,
				assetKey,
				strings.ToLower(entry.staleStatus),
			)
		}

		if entry.lastMaterializationStatus != "" {
			ch <- prometheus.MustNewConstMetric(
				c.assetLastMaterializationStatusDesc,
				prometheus.GaugeValue,
				1,
				assetKey,
				strings.ToLower(entry.lastMaterializationStatus),
			)
		}
	}
}
