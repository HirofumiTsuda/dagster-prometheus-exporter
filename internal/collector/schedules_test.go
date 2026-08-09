package collector

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildScheduleState(t *testing.T) {
	var resp GraphQLDefinitionsRosterResponse
	require.NoError(t, json.Unmarshal([]byte(`{
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
								"name": "ticked_schedule",
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
								"scheduleState": {"status": "STOPPED", "ticks": []}
							}
						]
					}
				]
			}
		}
	}`), &resp))

	status, tickStatus := buildScheduleState(&resp)

	tickedKey := ScheduleKey{ScheduleName: "ticked_schedule", LocationName: "loc_a"}
	assert.Equal(t, "RUNNING", status[tickedKey])
	// ticks[0] (newest) should win, not ticks[1].
	assert.Equal(t, "SUCCESS", tickStatus[tickedKey].status)
	assert.Equal(t, float64(200), tickStatus[tickedKey].timestamp)

	neverTickedKey := ScheduleKey{ScheduleName: "never_ticked_schedule", LocationName: "loc_a"}
	assert.Equal(t, "STOPPED", status[neverTickedKey])
	assert.NotContains(t, tickStatus, neverTickedKey,
		"a schedule with an empty ticks list should have no tick-status entry, not a seeded zero value")
}
