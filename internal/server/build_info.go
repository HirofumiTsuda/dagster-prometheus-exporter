package server

import (
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/version"
	"github.com/prometheus/client_golang/prometheus"
)

// newBuildInfoGauge returns a dagster_exporter_build_info gauge, always 1,
// with the running binary's version/commit carried as labels — the same
// pattern as node_exporter's node_exporter_build_info. Separate from
// DagsterCollector since it isn't derived from scraping Dagster: it's a
// static fact about the process, set once and never updated.
func newBuildInfoGauge(ver, commit string) prometheus.Collector {
	g := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dagster_exporter_build_info",
			Help: "Always 1; version/commit of the running exporter binary are carried as labels",
		},
		[]string{"version", "commit"},
	)
	g.WithLabelValues(ver, commit).Set(1)
	return g
}

func registerBuildInfo() {
	prometheus.MustRegister(newBuildInfoGauge(version.Version, version.Commit))
}
