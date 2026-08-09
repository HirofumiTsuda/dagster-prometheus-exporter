package collector

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSensorState(t *testing.T) {
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
						"sensors": [
							{
								"name": "ticked_sensor",
								"sensorState": {
									"status": "RUNNING",
									"ticks": [
										{"status": "SKIPPED", "timestamp": 200},
										{"status": "SUCCESS", "timestamp": 100}
									]
								}
							},
							{
								"name": "never_ticked_sensor",
								"sensorState": {"status": "STOPPED", "ticks": []}
							}
						]
					}
				]
			}
		}
	}`), &resp))

	status, tickStatus := buildSensorState(&resp)

	tickedKey := SensorKey{SensorName: "ticked_sensor", LocationName: "loc_a"}
	assert.Equal(t, "RUNNING", status[tickedKey])
	// ticks[0] (newest) should win, not ticks[1].
	assert.Equal(t, "SKIPPED", tickStatus[tickedKey].status)
	assert.Equal(t, float64(200), tickStatus[tickedKey].timestamp)

	neverTickedKey := SensorKey{SensorName: "never_ticked_sensor", LocationName: "loc_a"}
	assert.Equal(t, "STOPPED", status[neverTickedKey])
	assert.NotContains(t, tickStatus, neverTickedKey,
		"a sensor with an empty ticks list should have no tick-status entry, not a seeded zero value")
}
