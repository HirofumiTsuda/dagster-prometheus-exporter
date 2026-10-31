package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const jobAutomationRoster = `{
	"data": {
		"repositoriesOrError": {
			"__typename": "RepositoryConnection",
			"nodes": [
				{
					"name": "repo", "location": {"name": "loc_a"},
					"jobs": [{"name": "scheduled_job"}, {"name": "paused_job"}, {"name": "sensed_job"}, {"name": "manual_job"}],
					"schedules": [
						{"name": "on", "pipelineName": "scheduled_job", "scheduleState": {"status": "RUNNING", "ticks": []}},
						{"name": "off", "pipelineName": "paused_job", "scheduleState": {"status": "STOPPED", "ticks": []}}
					],
					"sensors": [
						{"name": "on", "targets": [{"pipelineName": "sensed_job"}, {"pipelineName": "not_a_job"}], "sensorState": {"status": "RUNNING", "ticks": []}},
						{"name": "off", "targets": [{"pipelineName": "manual_job"}], "sensorState": {"status": "STOPPED", "ticks": []}}
					]
				},
				{
					"name": "repo", "location": {"name": "loc_b"},
					"jobs": [{"name": "scheduled_job"}],
					"schedules": [],
					"sensors": []
				}
			]
		}
	}
}`

func TestBuildJobAutomation(t *testing.T) {
	var resp GraphQLDefinitionsRosterResponse
	require.NoError(t, json.Unmarshal([]byte(jobAutomationRoster), &resp))

	assert.Equal(t, map[JobKey]bool{
		{JobName: "scheduled_job", LocationName: "loc_a"}: true,
		{JobName: "paused_job", LocationName: "loc_a"}:    false,
		{JobName: "sensed_job", LocationName: "loc_a"}:    true,
		{JobName: "manual_job", LocationName: "loc_a"}:    false,
		{JobName: "scheduled_job", LocationName: "loc_b"}: false,
	}, buildJobAutomation(&resp),
		"only a schedule or sensor that is on counts, a target that is not a job adds no series, and a schedule never marks a job of the same name in another location")
}

func TestJobAutomationMetric(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(jobAutomationRoster))
		assert.NoError(t, err)
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
	require.NoError(t, CollectDefinitionsRoster(t.Context(), c))

	assert.Equal(t, map[string]float64{
		"job_name=scheduled_job,location=loc_a": 1,
		"job_name=paused_job,location=loc_a":    0,
		"job_name=sensed_job,location=loc_a":    1,
		"job_name=manual_job,location=loc_a":    0,
		"job_name=scheduled_job,location=loc_b": 0,
	}, gaugeValues(t, c, "dagster_job_automation_running"))
}
