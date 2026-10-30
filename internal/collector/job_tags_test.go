package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const jobTagsRoster = `{
	"data": {
		"repositoriesOrError": {
			"__typename": "RepositoryConnection",
			"nodes": [
				{
					"name": "repo", "location": {"name": "loc_a"},
					"jobs": [
						{"name": "tagged_job", "tags": [
							{"key": "owner", "value": "data-team"},
							{"key": "dagster/priority", "value": "5"},
							{"key": "unselected", "value": "x"}
						]},
						{"name": "untagged_job", "tags": []}
					],
					"schedules": []
				}
			]
		}
	}
}`

func TestBuildJobTags(t *testing.T) {
	var resp GraphQLDefinitionsRosterResponse
	require.NoError(t, json.Unmarshal([]byte(jobTagsRoster), &resp))

	jobTags := buildJobTags(&resp, []string{"dagster/priority", "owner"})

	assert.Equal(t, map[JobKey][]string{
		{JobName: "tagged_job", LocationName: "loc_a"}:   {"5", "data-team"},
		{JobName: "untagged_job", LocationName: "loc_a"}: {"", ""},
	}, jobTags, "values follow the order of keys; a tag the job doesn't have is empty")

	assert.Empty(t, buildJobTags(&resp, nil), "no keys configured means no series at all")
}

func TestJobTagsMetric(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(jobTagsRoster))
		assert.NoError(t, err)
	}))
	defer ts.Close()

	t.Run("exports the selected tags as labels", func(t *testing.T) {
		c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
		c.SetJobTagLabels(map[string]string{"owner": "tag_owner", "dagster/priority": "tag_dagster_priority"})
		require.NoError(t, CollectDefinitionsRoster(t.Context(), c))

		expected := `
# HELP dagster_job_tags Tags of a job definition selected by JOB_TAG_LABELS, one tag_<key> label each (value is always 1; a tag the job doesn't have is an empty label). Join it onto the other per-job metrics on (job_name, location) to filter or group them by tag
# TYPE dagster_job_tags gauge
dagster_job_tags{job_name="tagged_job",location="loc_a",tag_dagster_priority="5",tag_owner="data-team"} 1
dagster_job_tags{job_name="untagged_job",location="loc_a",tag_dagster_priority="",tag_owner=""} 1
`
		require.NoError(t, testutil.CollectAndCompare(c, strings.NewReader(expected), "dagster_job_tags"))
	})

	t.Run("has no series when no tags are selected", func(t *testing.T) {
		c := NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
		require.NoError(t, CollectDefinitionsRoster(t.Context(), c))

		assert.Equal(t, 0, testutil.CollectAndCount(c, "dagster_job_tags"))
	})
}
