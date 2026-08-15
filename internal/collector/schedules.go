package collector

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type ScheduleKey struct {
	ScheduleName string
	LocationName string
}

// scheduleTickEntry tracks the status of a schedule's most recently
// observed tick, for dagster_schedule_last_tick_status. timestamp isn't
// currently exposed as a metric itself, but is kept alongside status since
// it's what "most recent" is determined by (ticks(limit: 1, afterTimestamp: 1) already returns
// newest-first, so this is mostly for future use/debugging).
type scheduleTickEntry struct {
	status    string
	timestamp float64
}

// buildScheduleState extracts each schedule's enabled/disabled status and
// most recent tick from a definitions-roster response. Pulled out of
// CollectDefinitionsRoster as its own pure function so it can be
// unit-tested directly, without an HTTP mock.
func buildScheduleState(resp *GraphQLDefinitionsRosterResponse) (map[ScheduleKey]string, map[ScheduleKey]scheduleTickEntry) {
	status := make(map[ScheduleKey]string)
	tickStatus := make(map[ScheduleKey]scheduleTickEntry)

	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, schedule := range repo.Schedules {
			key := ScheduleKey{ScheduleName: schedule.Name, LocationName: repo.Location.Name}
			status[key] = schedule.ScheduleState.Status

			// ticks(limit: 1, afterTimestamp: 1) returns the single most recent tick, newest
			// first; a schedule that has never fired yet returns an empty
			// list, and simply has no entry here (no seeded value — same
			// rationale as dagster_last_run_info for a job that's never run).
			if len(schedule.ScheduleState.Ticks) > 0 {
				tick := schedule.ScheduleState.Ticks[0]
				tickStatus[key] = scheduleTickEntry{status: tick.Status, timestamp: tick.Timestamp}
			}
		}
	}

	return status, tickStatus
}

// reflectScheduleStatus emits dagster_schedule_status: whether a schedule
// is currently turned on (RUNNING) or off (STOPPED) in Dagster. This is
// refetched in full on every scrape (unlike dagster_last_run_info, which
// persists across scrapes to survive completed-runs' lookback window), so
// c.scheduleStatus is simply replaced wholesale each time rather than
// merged — a removed schedule just isn't in the map anymore.
func reflectScheduleStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, status := range c.scheduleStatus {
		ch <- prometheus.MustNewConstMetric(
			c.scheduleStatusDesc,
			prometheus.GaugeValue,
			1,
			key.ScheduleName,
			key.LocationName,
			strings.ToLower(status),
		)
	}
}

// reflectScheduleTickStatus emits dagster_schedule_last_tick_status for
// every schedule that has ticked at least once. Same "no seeded value
// until the first observation" behavior as dagster_last_run_info.
func reflectScheduleTickStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, entry := range c.scheduleTickStatus {
		ch <- prometheus.MustNewConstMetric(
			c.scheduleTickStatusDesc,
			prometheus.GaugeValue,
			1,
			key.ScheduleName,
			key.LocationName,
			strings.ToLower(entry.status),
		)
	}
}
