package collector

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectDefinitionsRosterSeedsAndPrunesJobs(t *testing.T) {
	call := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		body := `{
			"data": {
				"repositoriesOrError": {
					"__typename": "RepositoryConnection",
					"nodes": [
						{"name": "repo", "location": {"name": "loc_a"}, "jobs": [{"name": "job_a"}], "schedules": []}
					]
				}
			}
		}`
		if call > 1 {
			body = `{
				"data": {
					"repositoriesOrError": {
						"__typename": "RepositoryConnection",
						"nodes": []
					}
				}
			}`
		}

		_, err := w.Write([]byte(body))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	require.NoError(t, CollectDefinitionsRoster(t.Context(), c))
	assert.Contains(t, c.knownJobs, JobKey{JobName: "job_a", LocationName: "loc_a"})

	metric, err := c.completedRunsCounter.GetMetricWithLabelValues("job_a", "loc_a", "success")
	require.NoError(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(metric))

	require.NoError(t, CollectDefinitionsRoster(t.Context(), c))
	assert.NotContains(t, c.knownJobs, JobKey{JobName: "job_a", LocationName: "loc_a"})
}

func TestCollectDefinitionsRosterReturnsErrorOnServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)

	assert.Error(t, CollectDefinitionsRoster(t.Context(), c))
}

func TestCollectDefinitionsRosterTracksScheduleStatusAndLastTick(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"repositoriesOrError": {
					"__typename": "RepositoryConnection",
					"nodes": [
						{
							"name": "repo",
							"location": {"name": "loc_a"},
							"jobs": [],
							"schedules": [
								{
									"name": "my_schedule",
									"cronSchedule": "* * * * *",
									"scheduleState": {
										"status": "RUNNING",
										"ticks": [
											{"status": "SUCCESS", "timestamp": 200},
											{"status": "FAILURE", "timestamp": 100}
										]
									}
								},
								{
									"name": "never_ticked_schedule",
									"cronSchedule": "* * * * *",
									"scheduleState": {
										"status": "STOPPED",
										"ticks": []
									}
								}
							]
						}
					]
				}
			}
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectDefinitionsRoster(t.Context(), c))

	tickedKey := ScheduleKey{ScheduleName: "my_schedule", LocationName: "loc_a"}
	assert.Equal(t, "RUNNING", c.scheduleStatus[tickedKey])
	// ticks(limit: 1) already returns only the newest tick (SUCCESS@200),
	// not the older FAILURE@100 — this just checks that entry made it through.
	assert.Equal(t, "SUCCESS", c.scheduleTickStatus[tickedKey].status)

	neverTickedKey := ScheduleKey{ScheduleName: "never_ticked_schedule", LocationName: "loc_a"}
	assert.Equal(t, "STOPPED", c.scheduleStatus[neverTickedKey])
	assert.NotContains(t, c.scheduleTickStatus, neverTickedKey,
		"a schedule that has never ticked should have no tick-status entry, not a seeded zero value")

	statusCh := make(chan prometheus.Metric, 8)
	go func() {
		reflectScheduleStatus(c, statusCh)
		close(statusCh)
	}()
	statusValues := make(map[string]string)
	for m := range statusCh {
		var dm dto.Metric
		require.NoError(t, m.Write(&dm))
		var name, status string
		for _, l := range dm.GetLabel() {
			switch l.GetName() {
			case "schedule_name":
				name = l.GetValue()
			case "status":
				status = l.GetValue()
			}
		}
		statusValues[name] = status
		assert.Equal(t, float64(1), dm.GetGauge().GetValue())
	}
	assert.Equal(t, "running", statusValues["my_schedule"])
	assert.Equal(t, "stopped", statusValues["never_ticked_schedule"])

	tickCh := make(chan prometheus.Metric, 8)
	go func() {
		reflectScheduleTickStatus(c, tickCh)
		close(tickCh)
	}()
	var tickMetrics []prometheus.Metric
	for m := range tickCh {
		tickMetrics = append(tickMetrics, m)
	}
	require.Len(t, tickMetrics, 1, "only the schedule that has ticked should produce a series")

	var dm dto.Metric
	require.NoError(t, tickMetrics[0].Write(&dm))
	assert.Equal(t, float64(1), dm.GetGauge().GetValue())
	labels := make(map[string]string)
	for _, l := range dm.GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	assert.Equal(t, "my_schedule", labels["schedule_name"])
	assert.Equal(t, "loc_a", labels["location"])
	assert.Equal(t, "success", labels["status"])
}
