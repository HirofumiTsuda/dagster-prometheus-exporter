package collector

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// agentsBody mirrors what Dagster+ returned: one running agent, and one that
// stopped long ago and is still listed.
const agentsBody = `{"data": {"agents": [
	{"status": "RUNNING", "lastHeartbeatTime": 1793413716.3},
	{"status": "NOT_RUNNING", "lastHeartbeatTime": 1793298325.7}
]}}`

func newAgentsCollector(t *testing.T, body string) *DagsterCollector {
	t.Helper()
	ts := daemonHealthServer(t, body)
	return NewDagsterCollector(t.Context(), ts.URL, "token", time.Hour, time.Hour, 500, 5*time.Minute)
}

// agentMetrics collects what reflectAgents emits: the count per status, and
// the heartbeat if one was emitted.
func agentMetrics(t *testing.T, c *DagsterCollector) (counts map[string]float64, heartbeat *float64) {
	t.Helper()
	ch := make(chan prometheus.Metric, 8)
	go func() {
		reflectAgents(c, ch)
		close(ch)
	}()

	counts = make(map[string]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		if len(dm.GetLabel()) == 0 {
			value := dm.GetGauge().GetValue()
			heartbeat = &value
			continue
		}
		counts[dm.GetLabel()[0].GetValue()] = dm.GetGauge().GetValue()
	}
	return counts, heartbeat
}

func TestCollectAgentsCountsRunningAndStoppedAgents(t *testing.T) {
	c := newAgentsCollector(t, agentsBody)

	require.NoError(t, CollectAgents(t.Context(), c))

	counts, heartbeat := agentMetrics(t, c)
	assert.Equal(t, map[string]float64{"running": 1, "not_running": 1}, counts)
	require.NotNil(t, heartbeat)
	assert.InDelta(t, 1793413716.3, *heartbeat, 1e-6,
		"the heartbeat is the newest of all agents, so a stopped one cannot mask a running one")
}

func TestCollectAgentsReportsZeroRunningWhenNoAgentIsRegistered(t *testing.T) {
	c := newAgentsCollector(t, `{"data": {"agents": []}}`)

	require.NoError(t, CollectAgents(t.Context(), c))

	counts, heartbeat := agentMetrics(t, c)
	assert.Equal(t, map[string]float64{"running": 0, "not_running": 0}, counts,
		"running == 0 has to exist for an alert to fire when there is no agent at all")
	assert.Nil(t, heartbeat, "no agent means no heartbeat series, not one at the epoch")
}

func TestCollectAgentsCountsAnUnknownStatusAsStopped(t *testing.T) {
	c := newAgentsCollector(t, `{"data": {"agents": [{"status": "STARTING", "lastHeartbeatTime": null}]}}`)

	require.NoError(t, CollectAgents(t.Context(), c))

	counts, heartbeat := agentMetrics(t, c)
	assert.Equal(t, map[string]float64{"running": 0, "not_running": 1}, counts)
	assert.Nil(t, heartbeat, "a null heartbeat must stay absent")
}

func TestReflectAgentsEmitsNothingBeforeTheFirstScrape(t *testing.T) {
	c := newAgentsCollector(t, agentsBody)

	counts, heartbeat := agentMetrics(t, c)

	assert.Empty(t, counts, "an unread deployment must not claim zero agents")
	assert.Nil(t, heartbeat)
}

func TestCollectAgentsReturnsAnErrorWhenDagsterHasNoAgentsField(t *testing.T) {
	c := newAgentsCollector(t, `{"errors": [{"message": "Cannot query field \"agents\" on type \"Query\"."}]}`)

	require.Error(t, CollectAgents(t.Context(), c))

	counts, _ := agentMetrics(t, c)
	assert.Empty(t, counts, "a failed scrape must not report agents")
}
