package collector

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBackfillsCollector(t *testing.T, url string) *DagsterCollector {
	t.Helper()
	return NewDagsterCollector(t.Context(), url, "", time.Hour, time.Hour, 500, 5*time.Minute)
}

func backfillList(results string) string {
	return `{"__typename": "PartitionBackfills", "results": [` + results + `]}`
}

// backfillBody is the answer to both lists the collector asks for: the newest
// backfills of any status, and the newest ones that are still active.
func backfillBody(recent, active string) string {
	return `{"data": {"recent": ` + backfillList(recent) + `, "active": ` + backfillList(active) + `}}`
}

// backfillMetrics collects what reflectBackfills emits: the count per status,
// and the oldest active backfill if one was emitted.
func backfillMetrics(t *testing.T, c *DagsterCollector) (counts map[string]float64, oldestActive *float64) {
	t.Helper()
	ch := make(chan prometheus.Metric, 32)
	go func() {
		reflectBackfills(c, ch)
		close(ch)
	}()

	counts = make(map[string]float64)
	for m := range ch {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		if len(dm.GetLabel()) == 0 {
			value := dm.GetGauge().GetValue()
			oldestActive = &value
			continue
		}
		counts[dm.GetLabel()[0].GetValue()] = dm.GetGauge().GetValue()
	}
	return counts, oldestActive
}

func TestCollectBackfillsCountsByStatusAndFindsTheOldestActiveOne(t *testing.T) {
	c := newBackfillsCollector(t, daemonHealthServer(t, backfillBody(`
		{"status": "REQUESTED", "timestamp": 300},
		{"status": "COMPLETED_FAILED", "timestamp": 250},
		{"status": "CANCELING", "timestamp": 200},
		{"status": "COMPLETED_SUCCESS", "timestamp": 100},
		{"status": "REQUESTED", "timestamp": 150}`, `
		{"status": "REQUESTED", "timestamp": 300},
		{"status": "CANCELING", "timestamp": 200},
		{"status": "REQUESTED", "timestamp": 150}`)).URL)

	require.NoError(t, CollectBackfills(t.Context(), c))

	counts, oldestActive := backfillMetrics(t, c)
	assert.Equal(t, map[string]float64{
		"requested": 2, "completed": 0, "failed": 0, "canceled": 0, "canceling": 1,
		"completed_success": 1, "completed_failed": 1, "failing": 0,
	}, counts, "FAILED and COMPLETED_FAILED stay apart, and every status is present")
	require.NotNil(t, oldestActive)
	assert.Equal(t, 150.0, *oldestActive,
		"the oldest active backfill counts, and a finished one older than it does not")
}

func TestCollectBackfillsZeroFillsEveryStatusWhenThereAreNone(t *testing.T) {
	c := newBackfillsCollector(t, daemonHealthServer(t, backfillBody("", "")).URL)

	require.NoError(t, CollectBackfills(t.Context(), c))

	counts, oldestActive := backfillMetrics(t, c)
	assert.Len(t, counts, len(knownBackfillStatuses))
	for status, count := range counts {
		assert.Zero(t, count, status)
	}
	assert.Nil(t, oldestActive, "no active backfill means no series, not one at the epoch")
}

func TestCollectBackfillsKeepsAStatusDagsterAddsLater(t *testing.T) {
	c := newBackfillsCollector(t, daemonHealthServer(t, backfillBody(`{"status": "PAUSED", "timestamp": 100}`, "")).URL)

	require.NoError(t, CollectBackfills(t.Context(), c))

	counts, oldestActive := backfillMetrics(t, c)
	assert.Equal(t, 1.0, counts["paused"])
	assert.Nil(t, oldestActive, "an unknown status is not assumed to be active")
}

func TestCollectBackfillsRejectsPythonErrorWithoutDiscardingState(t *testing.T) {
	s := newScriptedServer(t, backfillBody(`{"status": "REQUESTED", "timestamp": 100}`, `{"status": "REQUESTED", "timestamp": 100}`))
	c := newBackfillsCollector(t, s.url)
	require.NoError(t, CollectBackfills(t.Context(), c))

	s.serve(`{"data": {"recent": ` + backfillList("") + `, "active": {"__typename": "PythonError", "message": "boom", "stack": []}}}`)

	require.Error(t, CollectBackfills(t.Context(), c),
		"a PythonError is not a successful query that found no backfills")
	counts, _ := backfillMetrics(t, c)
	assert.Equal(t, 1.0, counts["requested"], "a failed scrape must not erase what was read before")
}

func TestCollectBackfillsSeesAnActiveBackfillThatIsOlderThanTheRecentList(t *testing.T) {
	c := newBackfillsCollector(t, daemonHealthServer(t, backfillBody(
		`{"status": "COMPLETED_SUCCESS", "timestamp": 900}`,
		`{"status": "REQUESTED", "timestamp": 10}`)).URL)

	require.NoError(t, CollectBackfills(t.Context(), c))

	counts, oldestActive := backfillMetrics(t, c)
	assert.Equal(t, 1.0, counts["requested"],
		"a backfill that hangs must not drop out once newer ones push it past the recent list")
	require.NotNil(t, oldestActive)
	assert.Equal(t, 10.0, *oldestActive)
}

func TestCollectBackfillsReadsFailingFromTheRecentListBecauseTheQueryCannotNameIt(t *testing.T) {
	c := newBackfillsCollector(t, daemonHealthServer(t, backfillBody(`{"status": "FAILING", "timestamp": 50}`, "")).URL)

	require.NoError(t, CollectBackfills(t.Context(), c))

	counts, oldestActive := backfillMetrics(t, c)
	assert.Equal(t, 1.0, counts["failing"])
	require.NotNil(t, oldestActive)
	assert.Equal(t, 50.0, *oldestActive)
}

func TestBackfillsQueryDoesNotNameAStatusOlderDagsterLacks(t *testing.T) {
	assert.NotContains(t, backfillsQuery, "FAILING",
		"Dagster 1.9 has no FAILING in BulkActionStatus, and naming it makes that version reject the query")
}
