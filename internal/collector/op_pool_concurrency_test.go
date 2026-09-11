package collector

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// concurrencyLimitsBody mirrors what a live Dagster 1.13.15 returns: one
// pool with a mix of active/assigned/pending steps and an explicit limit,
// one idle pool sitting on the instance-wide default limit, and one pool
// that has claimed slots but has no limit at all (the nullable case).
const concurrencyLimitsBody = `{
	"data": {
		"instance": {
			"concurrencyLimits": [
				{"concurrencyKey": "db_pool", "activeSlotCount": 2, "assignedStepCount": 1, "pendingStepCount": 3, "limit": 4, "usingDefaultLimit": false},
				{"concurrencyKey": "idle_pool", "activeSlotCount": 0, "assignedStepCount": 0, "pendingStepCount": 0, "limit": 1, "usingDefaultLimit": true},
				{"concurrencyKey": "no_limit_pool", "activeSlotCount": 1, "assignedStepCount": 0, "pendingStepCount": 0, "limit": null, "usingDefaultLimit": null}
			]
		}
	}
}`

func concurrencyLimitsServer(t *testing.T, body string) *httptest.Server {
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

func TestCollectOpPoolConcurrency(t *testing.T) {
	ts := concurrencyLimitsServer(t, concurrencyLimitsBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectOpPoolConcurrency(t.Context(), c))

	require.Len(t, c.opPoolConcurrency, 3)

	dbPool := c.opPoolConcurrency["db_pool"]
	assert.Equal(t, 2, dbPool.activeSlots)
	assert.Equal(t, 1, dbPool.assignedSteps)
	assert.Equal(t, 3, dbPool.pendingSteps)
	require.True(t, dbPool.hasLimit)
	assert.Equal(t, 4, dbPool.limit)
	assert.False(t, dbPool.usingDefaultLimit)

	idlePool := c.opPoolConcurrency["idle_pool"]
	assert.Equal(t, 0, idlePool.activeSlots)
	require.True(t, idlePool.hasLimit)
	assert.Equal(t, 1, idlePool.limit)
	assert.True(t, idlePool.usingDefaultLimit)

	noLimitPool := c.opPoolConcurrency["no_limit_pool"]
	assert.Equal(t, 1, noLimitPool.activeSlots)
	assert.False(t, noLimitPool.hasLimit,
		"a null limit must stay unset, not become 0")
}

func TestCollectOpPoolConcurrencyReplacesMapWholesale(t *testing.T) {
	// A pool that goes idle and stops appearing in a later response must
	// disappear from c.opPoolConcurrency too -- unlike the run-queue
	// backlog, this collector relies on Dagster's own storage to keep
	// reporting an idle pool at zero, and should never itself zero-fill or
	// retain a pool that Dagster has stopped returning.
	ts := concurrencyLimitsServer(t, concurrencyLimitsBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectOpPoolConcurrency(t.Context(), c))
	require.Contains(t, c.opPoolConcurrency, "db_pool")

	ts2 := concurrencyLimitsServer(t, `{"data": {"instance": {"concurrencyLimits": []}}}`)
	c.dagsterGraphQLEndpoint = ts2.URL
	require.NoError(t, CollectOpPoolConcurrency(t.Context(), c))

	assert.Empty(t, c.opPoolConcurrency)
}

func TestReflectOpPoolConcurrencyMetrics(t *testing.T) {
	ts := concurrencyLimitsServer(t, concurrencyLimitsBody)
	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectOpPoolConcurrency(t.Context(), c))

	ch := make(chan prometheus.Metric, 32)
	go func() {
		reflectOpPoolConcurrency(c, ch)
		close(ch)
	}()

	active := make(map[string]float64)
	assigned := make(map[string]float64)
	pending := make(map[string]float64)
	limit := make(map[string]float64)
	usingDefaultLimit := make(map[string]string)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		labels := make(map[string]string)
		for _, l := range dm.GetLabel() {
			labels[l.GetName()] = l.GetValue()
		}
		pool := labels["pool"]
		switch desc := m.Desc().String(); {
		case strings.Contains(desc, "dagster_op_pool_concurrency_active_slots"):
			active[pool] = dm.GetGauge().GetValue()
		case strings.Contains(desc, "dagster_op_pool_concurrency_assigned_steps"):
			assigned[pool] = dm.GetGauge().GetValue()
		case strings.Contains(desc, "dagster_op_pool_concurrency_pending_steps"):
			pending[pool] = dm.GetGauge().GetValue()
		case strings.Contains(desc, "dagster_op_pool_concurrency_limit"):
			limit[pool] = dm.GetGauge().GetValue()
			usingDefaultLimit[pool] = labels["using_default_limit"]
		}
	}

	assert.Equal(t, float64(2), active["db_pool"])
	assert.Equal(t, float64(1), assigned["db_pool"])
	assert.Equal(t, float64(3), pending["db_pool"])
	assert.Equal(t, float64(4), limit["db_pool"])
	assert.Equal(t, "false", usingDefaultLimit["db_pool"])

	assert.Equal(t, float64(0), active["idle_pool"])
	assert.Equal(t, float64(1), limit["idle_pool"])
	assert.Equal(t, "true", usingDefaultLimit["idle_pool"])

	assert.Equal(t, float64(1), active["no_limit_pool"])
	assert.NotContains(t, limit, "no_limit_pool",
		"a pool with no reported limit must produce no dagster_op_pool_concurrency_limit series at all")
}

func TestCollectOpPoolConcurrencyReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	assert.Error(t, CollectOpPoolConcurrency(t.Context(), c))
}
