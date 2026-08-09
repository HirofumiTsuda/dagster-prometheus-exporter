package collector

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type SensorKey struct {
	SensorName   string
	LocationName string
}

// sensorTickEntry mirrors scheduleTickEntry: Sensor.sensorState is the same
// InstigationState type as Schedule.scheduleState under the hood, so
// sensors get an identical status/most-recent-tick tracking shape.
type sensorTickEntry struct {
	status    string
	timestamp float64
}

// buildSensorState extracts each sensor's enabled/disabled status and most
// recent tick from a definitions-roster response. Pulled out of
// CollectDefinitionsRoster as its own pure function so it can be
// unit-tested directly, without an HTTP mock — same pattern as
// buildScheduleState.
func buildSensorState(resp *GraphQLDefinitionsRosterResponse) (map[SensorKey]string, map[SensorKey]sensorTickEntry) {
	status := make(map[SensorKey]string)
	tickStatus := make(map[SensorKey]sensorTickEntry)

	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, sensor := range repo.Sensors {
			key := SensorKey{SensorName: sensor.Name, LocationName: repo.Location.Name}
			status[key] = sensor.SensorState.Status

			// ticks(limit: 1) returns the single most recent tick, newest
			// first; a sensor that has never fired yet returns an empty
			// list, and simply has no entry here (no seeded value — same
			// rationale as dagster_last_run_info for a job that's never run).
			if len(sensor.SensorState.Ticks) > 0 {
				tick := sensor.SensorState.Ticks[0]
				tickStatus[key] = sensorTickEntry{status: tick.Status, timestamp: tick.Timestamp}
			}
		}
	}

	return status, tickStatus
}

// reflectSensorStatus emits dagster_sensor_status: whether a sensor is
// currently turned on (RUNNING) or off (STOPPED) in Dagster. Refetched in
// full on every scrape, so c.sensorStatus is simply replaced wholesale each
// time rather than merged — a removed sensor just isn't in the map anymore.
func reflectSensorStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, status := range c.sensorStatus {
		ch <- prometheus.MustNewConstMetric(
			c.sensorStatusDesc,
			prometheus.GaugeValue,
			1,
			key.SensorName,
			key.LocationName,
			strings.ToLower(status),
		)
	}
}

// reflectSensorTickStatus emits dagster_sensor_last_tick_status for every
// sensor that has ticked at least once. Same "no seeded value until the
// first observation" behavior as dagster_last_run_info.
func reflectSensorTickStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, entry := range c.sensorTickStatus {
		ch <- prometheus.MustNewConstMetric(
			c.sensorTickStatusDesc,
			prometheus.GaugeValue,
			1,
			key.SensorName,
			key.LocationName,
			strings.ToLower(entry.status),
		)
	}
}
