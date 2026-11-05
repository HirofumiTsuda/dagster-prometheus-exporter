package collector

import (
	"context"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// assetCheckEntry is one check's latest execution. A check that has not run
// for the asset's latest materialization has no entry.
type assetCheckEntry struct {
	assetKey  string
	checkName string
	status    string
	// severity is "" when Dagster gives no evaluation, which is what a check
	// that raised an exception (EXECUTION_FAILED) looks like.
	severity string
}

// CollectAssetChecks reports the outcome of each asset check's latest
// execution.
//
// A failed check does not fail its run, so dagster_asset_last_materialization_status
// cannot show it. This is its own collector rather than part of CollectAssetStatus
// because the cost of assetChecksOrError is per asset and unknown on a large
// deployment, and a collector can be slowed down on its own
// (<NAME>_SCRAPING_INTERVAL_SECONDS).
//
// A check with no execution yet produces no entry instead of a status of its
// own, the same way an asset that never ran has no last-materialization series.
func CollectAssetChecks(ctx context.Context, c *DagsterCollector) error {
	resp, err := getAssetChecks(ctx, getAssetChecksRequest(), c.client)
	if err != nil {
		log.Printf("failed to collect asset checks from dagster: %v", err)
		return err
	}

	var entries []assetCheckEntry
	for _, node := range resp.Data.AssetNodes {
		if node.AssetChecksOrError.Typename != "AssetChecks" {
			log.Printf("asset checks of %s are unavailable: %s", assetKeyLabel(node.AssetKey.Path), node.AssetChecksOrError.Typename)
			continue
		}
		for _, check := range node.AssetChecksOrError.Checks {
			execution := check.ExecutionForLatestMaterialization
			if execution == nil {
				continue
			}
			entry := assetCheckEntry{
				assetKey:  assetKeyLabel(node.AssetKey.Path),
				checkName: check.Name,
				status:    strings.ToLower(execution.Status),
			}
			if execution.Evaluation != nil {
				entry.severity = strings.ToLower(execution.Evaluation.Severity)
			}
			entries = append(entries, entry)
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.assetChecks = entries

	return nil
}

func reflectAssetChecks(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, entry := range c.assetChecks {
		ch <- prometheus.MustNewConstMetric(
			c.assetCheckStatusDesc,
			prometheus.GaugeValue,
			1,
			entry.assetKey,
			entry.checkName,
			entry.status,
			entry.severity,
		)
	}
}
