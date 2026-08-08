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

	activeRunsDesc            *prometheus.Desc
	completedRunsCounter      *prometheus.CounterVec
	lastRunStatusDesc         *prometheus.Desc
	scrapeDurationDesc        *prometheus.Desc
	lastScrapeSuccessDesc     *prometheus.Desc
	scrapeErrorsCounter       *prometheus.CounterVec
	codeLocationLoadErrorDesc *prometheus.Desc

	mutex                   sync.Mutex
	activeRunsCounts        map[ActiveRunKey]int
	processedRuns           *ttlcache.Cache[string, struct{}]
	lookbackWindow          time.Duration
	knownJobs               map[JobKey]struct{}
	lastRunStatus           map[JobKey]lastRunEntry
	trackedCompletedRunKeys map[JobKey]struct{}
	lastSeenUpdateTime      float64
	runsPageSize            int
	scrapeDuration          map[string]float64
	lastScrapeSuccess       map[string]bool
	codeLocationLoadError   map[string]bool
	// runsUpdatedAfterSafetyMargin is subtracted from the last-seen
	// updateTime watermark before it's used as the next scrape's
	// updatedAfter. A run's updateTime can be set slightly before its write
	// is actually committed and visible to us, so advancing the watermark
	// to the exact max we've observed risks permanently skipping a run
	// whose commit was delayed past that point. Re-fetching this small
	// overlap window each time is cheap and harmless: processedRuns and
	// lastRunStatus's newest-wins comparison already make reprocessing an
	// already-seen run a no-op.
	runsUpdatedAfterSafetyMargin time.Duration
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

func NewDagsterCollector(ctx context.Context, dagsterGraphQLEndpoint string, lookbackWindow time.Duration, cacheTTL time.Duration, runsPageSize int, runsUpdatedAfterSafetyMargin time.Duration) *DagsterCollector {
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
		scrapeDurationDesc: prometheus.NewDesc(
			"dagster_exporter_scrape_duration_seconds",
			"Duration of the most recent scrape, per collector",
			[]string{"collector"},
			nil,
		),
		lastScrapeSuccessDesc: prometheus.NewDesc(
			"dagster_exporter_last_scrape_success",
			"Whether the most recent scrape succeeded (1) or failed (0), per collector",
			[]string{"collector"},
			nil,
		),
		scrapeErrorsCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dagster_exporter_scrape_errors_total",
				Help: "Total number of failed scrapes, per collector",
			},
			[]string{"collector"},
		),
		codeLocationLoadErrorDesc: prometheus.NewDesc(
			"dagster_code_location_load_error",
			"Whether the code location most recently failed to load (1) or loaded successfully (0)",
			[]string{"location"},
			nil,
		),
		processedRuns:                cache,
		lookbackWindow:               lookbackWindow,
		lastRunStatus:                make(map[JobKey]lastRunEntry),
		trackedCompletedRunKeys:      make(map[JobKey]struct{}),
		runsPageSize:                 runsPageSize,
		runsUpdatedAfterSafetyMargin: runsUpdatedAfterSafetyMargin,
		scrapeDuration:               make(map[string]float64),
		lastScrapeSuccess:            make(map[string]bool),
	}
}

func (c *DagsterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeRunsDesc
	ch <- c.lastRunStatusDesc
	ch <- c.scrapeDurationDesc
	ch <- c.lastScrapeSuccessDesc
	ch <- c.codeLocationLoadErrorDesc
	c.completedRunsCounter.Describe(ch)
	c.scrapeErrorsCounter.Describe(ch)
}

func (c *DagsterCollector) Collect(ch chan<- prometheus.Metric) {
	reflectActiveRuns(c, ch)
	reflectLastRunStatus(c, ch)
	reflectScrapeHealth(c, ch)
	reflectCodeLocationStatus(c, ch)
	c.completedRunsCounter.Collect(ch)
	c.scrapeErrorsCounter.Collect(ch)
}
