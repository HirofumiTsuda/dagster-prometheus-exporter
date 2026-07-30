package collector

import (
	"context"
	"sync"
	"time"

	ttlcache "github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus"
)

const unknownLocationName = "unknown"

type DagsterCollector struct {
	dagsterGraphQLEndpoint string

	activeRunsDesc       *prometheus.Desc
	completedRunsCounter *prometheus.CounterVec
	lastRunStatusDesc    *prometheus.Desc

	mutex                   sync.Mutex
	activeRunsCounts        map[ActiveRunKey]int
	processedRuns           *ttlcache.Cache[string, struct{}]
	lookbackWindow          time.Duration
	knownJobs               map[JobKey]struct{}
	lastRunStatus           map[JobKey]lastRunEntry
	trackedCompletedRunKeys map[JobKey]struct{}
}

func newDagsterCache(ctx context.Context, cacheTTL time.Duration) *ttlcache.Cache[string, struct{}] {
	cache := ttlcache.New(
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
		activeRunsDesc: prometheus.NewDesc(
			"dagster_active_runs",
			"Number of active runs with each status in Dagster",
			[]string{"job_name", "location", "status"},
			nil,
		),
		completedRunsCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dagster_completed_runs_total",
				Help: "Total number of Dagster runs by status and job name",
			},
			[]string{"job_name", "location", "status"},
		),
		lastRunStatusDesc: prometheus.NewDesc(
			"dagster_last_run_info",
			"Status of the most recently completed run for a job (value is always 1; status is carried in the status label)",
			[]string{"job_name", "location", "status"},
			nil,
		),
		processedRuns:           cache,
		lookbackWindow:          lookbackWindow,
		lastRunStatus:           make(map[JobKey]lastRunEntry),
		trackedCompletedRunKeys: make(map[JobKey]struct{}),
	}
}

func (c *DagsterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeRunsDesc
	ch <- c.lastRunStatusDesc
	c.completedRunsCounter.Describe(ch)
}

func (c *DagsterCollector) Collect(ch chan<- prometheus.Metric) {
	reflectActiveRuns(c, ch)
	reflectLastRunStatus(c, ch)
	c.completedRunsCounter.Collect(ch)
}
