package collector

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	agentStatusRunning    = "RUNNING"
	agentStatusRunningTag = "running"
	agentStatusStopped    = "not_running"
)

// agentsSummary is what the agents collector keeps: counts and the newest
// heartbeat, not one entry per agent.
//
// Dagster+ keeps a stopped agent in the list, so a per-agent metric would hold
// a series at 0 for every agent that ever ran, and on Serverless an agent's id
// changes on every restart. The question an alert needs answered is whether
// any agent is running, which a count answers without growing.
type agentsSummary struct {
	running int
	stopped int
	// newestHeartbeat is nil when no agent has reported one, rather than 0,
	// which would place the heartbeat at the Unix epoch.
	newestHeartbeat *float64
}

// CollectAgents reports whether Dagster+ agents are running.
//
// The agent is what launches runs on Dagster+. When it stops the webserver
// stays up, and schedules, sensors and manual launches silently stop while runs
// sit in QUEUED or STARTING. dagster_daemon_healthy cannot see this: Dagster+
// runs its daemons itself, so the agent is the process that can be down.
//
// Only Dagster+ has the agents field, and the caller only starts this collector
// when DAGSTER_CLOUD_API_TOKEN is set. Sending it to open-source Dagster would
// fail every scrape and show up as an exporter error.
func CollectAgents(ctx context.Context, c *DagsterCollector) error {
	resp, err := getAgents(ctx, getAgentsRequest(), c.client)
	if err != nil {
		log.Printf("failed to collect agents from dagster: %v", err)
		return err
	}

	summary := &agentsSummary{}
	for _, agent := range resp.Data.Agents {
		// Anything but RUNNING counts as stopped, so a status Dagster+ adds
		// later reads as "not running" instead of being dropped: this metric
		// exists to catch an agent that isn't answering.
		if agent.Status == agentStatusRunning {
			summary.running++
		} else {
			summary.stopped++
		}
		if agent.LastHeartbeatTime != nil && (summary.newestHeartbeat == nil || *agent.LastHeartbeatTime > *summary.newestHeartbeat) {
			heartbeat := *agent.LastHeartbeatTime
			summary.newestHeartbeat = &heartbeat
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.agents = summary

	return nil
}

// reflectAgents emits dagster_agents for both statuses, and the newest
// heartbeat when there is one. It emits nothing before the first scrape, so a
// deployment the exporter hasn't read yet doesn't claim zero agents.
func reflectAgents(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.agents == nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(c.agentsDesc, prometheus.GaugeValue, float64(c.agents.running), agentStatusRunningTag)
	ch <- prometheus.MustNewConstMetric(c.agentsDesc, prometheus.GaugeValue, float64(c.agents.stopped), agentStatusStopped)

	if c.agents.newestHeartbeat != nil {
		ch <- prometheus.MustNewConstMetric(c.agentLastHeartbeatDesc, prometheus.GaugeValue, *c.agents.newestHeartbeat)
	}
}
