package server

import (
	"net/http"

	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// newRegistry returns a registry of its own holding c and everything else
// /metrics has always served. A new registry starts empty, so the Go runtime
// and process collectors that the default registry pre-registers are added
// explicitly: dropping them would remove every go_* and process_* series,
// which users' own queries may rely on.
func newRegistry(c prometheus.Collector) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		newBuildInfoGauge(version.Version, version.Commit),
		c,
	)
	return reg
}

// metricsHandler serves reg wrapped in InstrumentMetricHandler. HandlerFor
// alone would drop promhttp_metric_handler_requests_total and
// promhttp_metric_handler_requests_in_flight, which promhttp.Handler() adds
// on the default registry.
func metricsHandler(reg *prometheus.Registry) http.Handler {
	return promhttp.InstrumentMetricHandler(reg, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
}
