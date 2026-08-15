package collector

import (
	"context"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// daemonHealthEntry is one Dagster daemon's most recently reported state.
//
// lastHeartbeat is a pointer because a daemon that has never reported one has
// no timestamp rather than a zero — emitting 0 would place its last heartbeat
// at the Unix epoch and make every staleness alert fire.
type daemonHealthEntry struct {
	required      bool
	healthy       bool
	lastHeartbeat *float64
}

// CollectDaemonHealth reports whether Dagster's daemons are alive.
//
// This is the one question none of the other collectors can answer. Everything
// else here describes Dagster's *work* — runs, schedules, sensors, code
// locations — and all of it is served by the webserver, which stays perfectly
// healthy when the daemon dies. A dead SCHEDULER just means schedules stop
// firing: dagster_schedule_status still reports running, the last tick status
// stays frozen at whatever it was, and dagster_exporter_last_scrape_success
// stays 1. Nothing in /metrics changes.
//
// It deliberately doesn't rely on schedule/sensor tick recency to infer this
// (see dagster_schedule_last_tick_timestamp_seconds, which is useful for a
// different question). Tick recency says nothing at all in a deployment that
// launches everything manually or through the API, and daemon liveness
// shouldn't depend on whether the user happens to have defined a sensor.
//
// instance.daemonHealth is a top-level field rather than something reachable
// from repositoriesOrError, so this is its own query and its own collector —
// the same reasoning as CollectCodeLocationStatus.
func CollectDaemonHealth(ctx context.Context, c *DagsterCollector) error {
	req := getDaemonHealthRequest()

	resp, err := getDaemonHealth(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect daemon health from dagster: %v", err)
		return err
	}

	statuses := resp.Data.Instance.DaemonHealth.AllDaemonStatuses
	entries := make(map[string]daemonHealthEntry, len(statuses))
	for _, status := range statuses {
		// healthy is nullable in the schema. Treat an absent value as
		// unhealthy rather than healthy: this metric exists to catch a daemon
		// that isn't reporting, and "no answer" is not an answer.
		healthy := status.Healthy != nil && *status.Healthy

		entries[status.DaemonType] = daemonHealthEntry{
			required:      status.Required,
			healthy:       healthy,
			lastHeartbeat: status.LastHeartbeatTime,
		}

		if len(status.LastHeartbeatErrors) > 0 {
			messages := make([]string, 0, len(status.LastHeartbeatErrors))
			for _, e := range status.LastHeartbeatErrors {
				messages = append(messages, e.Message)
			}
			// Logged rather than labeled, the same treatment
			// dagster_code_location_load_error gives its error message: the
			// text is unbounded and would blow up label cardinality.
			log.Printf("dagster daemon %q reported heartbeat errors: %s", status.DaemonType, strings.Join(messages, "; "))
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.daemonHealth = entries

	return nil
}

// reflectDaemonHealth emits dagster_daemon_healthy and
// dagster_daemon_last_heartbeat_timestamp_seconds from a single locked pass,
// the same reasoning as reflectLastRun: both come from one entry.
func reflectDaemonHealth(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for daemonType, entry := range c.daemonHealth {
		healthy := 0.0
		if entry.healthy {
			healthy = 1.0
		}
		required := "false"
		if entry.required {
			required = "true"
		}

		ch <- prometheus.MustNewConstMetric(
			c.daemonHealthyDesc,
			prometheus.GaugeValue,
			healthy,
			daemonType,
			required,
		)

		// A daemon that has never reported a heartbeat gets no series at all,
		// rather than one claiming a heartbeat at the epoch.
		if entry.lastHeartbeat != nil {
			ch <- prometheus.MustNewConstMetric(
				c.daemonLastHeartbeatDesc,
				prometheus.GaugeValue,
				*entry.lastHeartbeat,
				daemonType,
			)
		}
	}
}
