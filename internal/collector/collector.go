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

	activeRunsDesc                     *prometheus.Desc
	completedRunsCounter               *prometheus.CounterVec
	lastRunStatusDesc                  *prometheus.Desc
	scrapeDurationDesc                 *prometheus.Desc
	lastScrapeSuccessDesc              *prometheus.Desc
	scrapeErrorsCounter                *prometheus.CounterVec
	codeLocationLoadErrorDesc          *prometheus.Desc
	lastRunDurationDesc                *prometheus.Desc
	activeRunDurationDesc              *prometheus.Desc
	concurrencyKeyBacklogDesc          *prometheus.Desc
	scheduleStatusDesc                 *prometheus.Desc
	scheduleTickStatusDesc             *prometheus.Desc
	scheduleTickTimestampDesc          *prometheus.Desc
	sensorStatusDesc                   *prometheus.Desc
	sensorTickStatusDesc               *prometheus.Desc
	sensorTickTimestampDesc            *prometheus.Desc
	daemonHealthyDesc                  *prometheus.Desc
	daemonLastHeartbeatDesc            *prometheus.Desc
	assetStaleStatusDesc               *prometheus.Desc
	assetLastMaterializationStatusDesc *prometheus.Desc

	mutex                   sync.Mutex
	activeRunAggregates     map[ActiveRunKey]activeRunAggregate
	concurrencyKeyBacklog   map[string]int
	scheduleStatus          map[ScheduleKey]string
	scheduleTickStatus      map[ScheduleKey]scheduleTickEntry
	sensorStatus            map[SensorKey]string
	sensorTickStatus        map[SensorKey]sensorTickEntry
	processedRuns           *ttlcache.Cache[string, struct{}]
	lookbackWindow          time.Duration
	knownJobs               map[JobKey]struct{}
	lastRunStatus           map[JobKey]lastRunEntry
	trackedCompletedRunKeys map[JobKey]struct{}
	lastSeenUpdateTime      float64
	runsPageSize            int
	scrapeResults           map[string]scrapeResult
	codeLocationLoadError   map[string]bool
	daemonHealth            map[string]daemonHealthEntry
	assetStatus             map[string]assetStatusEntry
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
		lastRunDurationDesc: prometheus.NewDesc(
			"dagster_last_run_duration_seconds",
			"Duration of the most recently completed run for a job (endTime - creationTime)",
			[]string{"job_name", "location", "status"},
			nil,
		),
		activeRunDurationDesc: prometheus.NewDesc(
			"dagster_active_run_duration_seconds",
			"Elapsed time (now - updateTime) of the longest-waiting/longest-running active run per job and status; 0 if there are no active runs in that group",
			[]string{"job_name", "location", "status"},
			nil,
		),
		concurrencyKeyBacklogDesc: prometheus.NewDesc(
			"dagster_run_queue_concurrency_key_backlog",
			"Number of runs currently QUEUED because of a tag-based run-queue concurrency limit, per concurrency_key",
			[]string{"concurrency_key"},
			nil,
		),
		scheduleStatusDesc: prometheus.NewDesc(
			"dagster_schedule_status",
			"Whether a schedule is currently turned on (value is always 1; status is carried in the status label, one of running/stopped)",
			[]string{"schedule_name", "location", "status"},
			nil,
		),
		scheduleTickStatusDesc: prometheus.NewDesc(
			"dagster_schedule_last_tick_status",
			"Status of a schedule's most recent tick (value is always 1; status is carried in the status label, one of started/skipped/success/failure)",
			[]string{"schedule_name", "location", "status"},
			nil,
		),
		scheduleTickTimestampDesc: prometheus.NewDesc(
			"dagster_schedule_last_tick_timestamp_seconds",
			"Unix timestamp of a schedule's most recent tick. Exported as a timestamp rather than an age so staleness is computed at query time (time() - metric), not frozen at scrape time",
			[]string{"schedule_name", "location"},
			nil,
		),
		sensorStatusDesc: prometheus.NewDesc(
			"dagster_sensor_status",
			"Whether a sensor is currently turned on (value is always 1; status is carried in the status label, one of running/stopped)",
			[]string{"sensor_name", "location", "status"},
			nil,
		),
		sensorTickStatusDesc: prometheus.NewDesc(
			"dagster_sensor_last_tick_status",
			"Status of a sensor's most recent tick (value is always 1; status is carried in the status label, one of started/skipped/success/failure)",
			[]string{"sensor_name", "location", "status"},
			nil,
		),
		sensorTickTimestampDesc: prometheus.NewDesc(
			"dagster_sensor_last_tick_timestamp_seconds",
			"Unix timestamp of a sensor's most recent tick. Exported as a timestamp rather than an age so staleness is computed at query time (time() - metric), not frozen at scrape time",
			[]string{"sensor_name", "location"},
			nil,
		),
		daemonHealthyDesc: prometheus.NewDesc(
			"dagster_daemon_healthy",
			"Whether a Dagster daemon is currently healthy (1) or not (0). required=false means the instance isn't configured to run it, so it being unhealthy is expected rather than an incident",
			[]string{"daemon_type", "required"},
			nil,
		),
		daemonLastHeartbeatDesc: prometheus.NewDesc(
			"dagster_daemon_last_heartbeat_timestamp_seconds",
			"Unix timestamp of a daemon's most recent heartbeat. Absent for a daemon that has never reported one. Exported as a timestamp rather than an age so staleness is computed at query time (time() - metric), not frozen at scrape time",
			[]string{"daemon_type"},
			nil,
		),
		assetStaleStatusDesc: prometheus.NewDesc(
			"dagster_asset_stale_status",
			"Whether an asset's data is missing, stale, or fresh (value is always 1; status is carried in the status label, one of missing/stale/fresh). Absent for an asset Dagster reports no computable stale status for",
			[]string{"asset_key", "status"},
			nil,
		),
		assetLastMaterializationStatusDesc: prometheus.NewDesc(
			"dagster_asset_last_materialization_status",
			"Status of an asset's most recent materializing run (value is always 1; status is carried in the status label). Absent for an asset that has never had a run — unlike stale status, this is derived from the run itself rather than assetMaterializations, so a run that fails before producing a materialization still shows up here (see docs/metrics.md)",
			[]string{"asset_key", "status"},
			nil,
		),
		processedRuns:                cache,
		lookbackWindow:               lookbackWindow,
		lastRunStatus:                make(map[JobKey]lastRunEntry),
		trackedCompletedRunKeys:      make(map[JobKey]struct{}),
		runsPageSize:                 runsPageSize,
		runsUpdatedAfterSafetyMargin: runsUpdatedAfterSafetyMargin,
		scrapeResults:                make(map[string]scrapeResult),
	}
}

func (c *DagsterCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.activeRunsDesc
	ch <- c.lastRunStatusDesc
	ch <- c.scrapeDurationDesc
	ch <- c.lastScrapeSuccessDesc
	ch <- c.codeLocationLoadErrorDesc
	ch <- c.lastRunDurationDesc
	ch <- c.activeRunDurationDesc
	ch <- c.concurrencyKeyBacklogDesc
	ch <- c.scheduleStatusDesc
	ch <- c.scheduleTickStatusDesc
	ch <- c.scheduleTickTimestampDesc
	ch <- c.sensorStatusDesc
	ch <- c.sensorTickStatusDesc
	ch <- c.sensorTickTimestampDesc
	ch <- c.daemonHealthyDesc
	ch <- c.daemonLastHeartbeatDesc
	ch <- c.assetStaleStatusDesc
	ch <- c.assetLastMaterializationStatusDesc
	c.completedRunsCounter.Describe(ch)
	c.scrapeErrorsCounter.Describe(ch)
}

func (c *DagsterCollector) Collect(ch chan<- prometheus.Metric) {
	reflectActiveRuns(c, ch)
	reflectLastRun(c, ch)
	reflectScrapeHealth(c, ch)
	reflectCodeLocationStatus(c, ch)
	reflectConcurrencyKeyBacklog(c, ch)
	reflectScheduleStatus(c, ch)
	reflectScheduleTickStatus(c, ch)
	reflectSensorStatus(c, ch)
	reflectSensorTickStatus(c, ch)
	reflectDaemonHealth(c, ch)
	reflectAssetStatus(c, ch)
	c.completedRunsCounter.Collect(ch)
	c.scrapeErrorsCounter.Collect(ch)
}
