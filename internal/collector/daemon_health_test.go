package collector

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// daemonHealthBody mirrors what a live Dagster 1.13.15 returns, including the
// nullable fields: a daemon that has never reported a heartbeat, and one
// whose healthy flag is absent entirely.
const daemonHealthBody = `{
	"data": {
		"instance": {
			"daemonHealth": {
				"allDaemonStatuses": [
					{"daemonType": "SCHEDULER", "required": true, "healthy": true, "lastHeartbeatTime": 1786766271.6, "lastHeartbeatErrors": []},
					{"daemonType": "SENSOR", "required": true, "healthy": false, "lastHeartbeatTime": 1786766200.1, "lastHeartbeatErrors": [{"message": "boom"}]},
					{"daemonType": "BACKFILL", "required": false, "healthy": null, "lastHeartbeatTime": null, "lastHeartbeatErrors": []}
				]
			}
		}
	}
}`

func daemonHealthServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(body))
		assert.NoError(t, err)
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestCollectDaemonHealth(t *testing.T) {
	ts := daemonHealthServer(t, daemonHealthBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectDaemonHealth(t.Context(), c))

	require.Len(t, c.daemonHealth, 3)
	assert.True(t, c.daemonHealth["SCHEDULER"].healthy)
	assert.False(t, c.daemonHealth["SENSOR"].healthy)
	assert.False(t, c.daemonHealth["BACKFILL"].healthy,
		"a null healthy flag means the daemon isn't answering, which is not the same as healthy")

	assert.True(t, c.daemonHealth["SCHEDULER"].required)
	assert.False(t, c.daemonHealth["BACKFILL"].required)

	require.NotNil(t, c.daemonHealth["SCHEDULER"].lastHeartbeat)
	assert.InDelta(t, 1786766271.6, *c.daemonHealth["SCHEDULER"].lastHeartbeat, 1e-6)
	assert.Nil(t, c.daemonHealth["BACKFILL"].lastHeartbeat,
		"a daemon that has never reported a heartbeat must stay nil, not become 0")
}

func TestReflectDaemonHealthMetrics(t *testing.T) {
	ts := daemonHealthServer(t, daemonHealthBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectDaemonHealth(t.Context(), c))

	ch := make(chan prometheus.Metric, 16)
	go func() {
		reflectDaemonHealth(c, ch)
		close(ch)
	}()

	healthy := make(map[string]float64)
	heartbeats := make(map[string]float64)
	required := make(map[string]string)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		labels := make(map[string]string)
		for _, l := range dm.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		if _, ok := labels["required"]; ok {
			healthy[labels["daemon_type"]] = dm.GetGauge().GetValue()
			required[labels["daemon_type"]] = labels["required"]
		} else {
			heartbeats[labels["daemon_type"]] = dm.GetGauge().GetValue()
		}
	}

	assert.Equal(t, float64(1), healthy["SCHEDULER"])
	assert.Equal(t, float64(0), healthy["SENSOR"])
	assert.Equal(t, float64(0), healthy["BACKFILL"])
	assert.Equal(t, "true", required["SCHEDULER"])
	assert.Equal(t, "false", required["BACKFILL"])

	assert.Len(t, heartbeats, 2, "the daemon with no heartbeat should produce no timestamp series at all")
	assert.NotContains(t, heartbeats, "BACKFILL")
	assert.InDelta(t, 1786766271.6, heartbeats["SCHEDULER"], 1e-6)
}

func TestCollectDaemonHealthReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	assert.Error(t, CollectDaemonHealth(t.Context(), c))
}
