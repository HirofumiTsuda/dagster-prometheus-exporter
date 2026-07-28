package collector

import (
	"context"
	"sync"
	"time"

	ttlcache "github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
)

type DagsterCollector struct {
	dagsterGraphQLEndpoint string

	activeRunsGauge      *prometheus.GaugeVec
	completedRunsCounter *prometheus.CounterVec

	mu             sync.Mutex
	processedRuns  *ttlcache.Cache[string, struct{}]
	lookbackWindow time.Duration
}

func newDagsterCache(ctx context.Context, cacheTTL time.Duration) *ttlcache.Cache[string, struct{}] {
	cache := ttlcache.New[string, struct{}](
		ttlcache.WithTTL[string, struct{}](cacheTTL),
	)

	go cache.Start()

	go func() {
		<-ctx.Done()
		cache.Stop()
	}()

	return cache
}

func NewDagsterCollector(ctx context.Context, dagsterGraphQLEndpoint string, lookbackWindow time.Duration, cacheTTL time.Duration) *DagsterCollector {
	cache := newDagsterCache(ctx, cacheTTL)
	return &DagsterCollector{
		dagsterGraphQLEndpoint: dagsterGraphQLEndpoint,
		activeRunsGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dagster_active_runs",
				Help: "Number of active runs with each status in Dagster",
			},
			[]string{"status"},
		),
		completedRunsCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dagster_completed_runs_total",
				Help: "Total number of Dagster runs by status and job name",
			},
			[]string{"job_name", "status"}, // ラベルでジョブ名とステータスを分ける
		),
		processedRuns:  cache,
		lookbackWindow: lookbackWindow,
	}
}

func (c *DagsterCollector) Describe(ch chan<- *prometheus.Desc) {
	c.activeRunsGauge.Describe(ch)
	c.completedRunsCounter.Describe(ch)
}

func (c *DagsterCollector) Collect(ch chan<- prometheus.Metric) {
	c.activeRunsGauge.Collect(ch)
	c.completedRunsCounter.Collect(ch)
}
