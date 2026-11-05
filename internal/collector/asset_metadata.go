package collector

import (
	"context"
	"log"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// assetMetadataEntry is the numeric metadata read from one asset's latest
// materialization.
type assetMetadataEntry struct {
	assetKey string
	// timestamp is when the materialization happened, in seconds.
	timestamp float64
	values    map[string]float64
}

// SetAssetMetadataValues chooses the assets and metadata labels to export, as a
// map of asset key to labels (config.Load builds it from
// ASSET_METADATA_VALUES). Without it the asset_metadata collector asks Dagster
// nothing and exports nothing.
func (c *DagsterCollector) SetAssetMetadataValues(values map[string][]string) {
	c.assetMetadataValues = values
}

// CollectAssetMetadata reports the numeric metadata (a row count, a score) that
// an asset recorded on its latest materialization.
//
// Only the assets listed in ASSET_METADATA_VALUES are asked for, by key:
// Dagster reads each asset's latest event separately, so asking for all of
// them would cost in proportion to the number of assets, and exporting every
// label of every asset would grow the series count without a bound.
//
// A partitioned asset is skipped. Its latest materialization is whichever
// partition ran last, so the value would jump between partitions as they run,
// and one series per partition is the cardinality problem the allow list is
// there to avoid.
func CollectAssetMetadata(ctx context.Context, c *DagsterCollector) error {
	if len(c.assetMetadataValues) == 0 {
		return nil
	}

	assetKeys := make([]AssetKeyPath, 0, len(c.assetMetadataValues))
	for _, assetKey := range slices.Sorted(maps.Keys(c.assetMetadataValues)) {
		assetKeys = append(assetKeys, strings.Split(assetKey, "/"))
	}

	resp, err := getAssetMetadata(ctx, getAssetMetadataRequest(assetKeys), c.client)
	if err != nil {
		log.Printf("failed to collect asset metadata from dagster: %v", err)
		return err
	}

	var entries []assetMetadataEntry
	for _, node := range resp.Data.AssetNodes {
		assetKey := assetKeyLabel(node.AssetKey.Path)
		if node.IsPartitioned || len(node.AssetMaterializations) == 0 {
			continue
		}
		materialization := node.AssetMaterializations[0]
		timestampMillis, err := strconv.ParseFloat(materialization.Timestamp, 64)
		if err != nil {
			continue
		}

		wanted := c.assetMetadataValues[assetKey]
		values := make(map[string]float64)
		for _, entry := range materialization.MetadataEntries {
			if !slices.Contains(wanted, entry.Label) {
				continue
			}
			if entry.FloatValue != nil {
				values[entry.Label] = *entry.FloatValue
			} else if value, err := strconv.ParseFloat(entry.IntRepr, 64); err == nil {
				values[entry.Label] = value
			}
		}
		if len(values) == 0 {
			continue
		}
		entries = append(entries, assetMetadataEntry{assetKey: assetKey, timestamp: timestampMillis / 1000, values: values})
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.assetMetadata = entries

	return nil
}

func reflectAssetMetadata(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, entry := range c.assetMetadata {
		ch <- prometheus.MustNewConstMetric(c.assetMetadataTimestampDesc, prometheus.GaugeValue, entry.timestamp, entry.assetKey)
		for label, value := range entry.values {
			ch <- prometheus.MustNewConstMetric(c.assetMetadataValueDesc, prometheus.GaugeValue, value, entry.assetKey, label)
		}
	}
}
