package collector

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetActiveRunsRequest(t *testing.T) {
	reqBody := getActiveRunsRequest()
	if reqBody.Query == "" {
		t.Fatal("reqBody.Query must not be empty")
	}
	assert.Equal(t, activeStatuses, reqBody.Variables["statuses"])
}

func TestCollectActiveRunsLocationLabel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": [
						{"runId": "run_1", "jobName": "job_a", "status": "STARTED", "repositoryOrigin": {"repositoryName": "__repository__", "repositoryLocationName": "loc_a"}},
						{"runId": "run_2", "jobName": "job_b", "status": "QUEUED", "repositoryOrigin": null}
					]
				}
			}
		}`))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour)
	CollectActiveRuns(t.Context(), c)

	assert.Equal(t, 1, c.activeRunsCounts[ActiveRunKey{JobName: "job_a", LocationName: "loc_a", Status: "STARTED"}])
	assert.Equal(t, 1, c.activeRunsCounts[ActiveRunKey{JobName: "job_b", LocationName: unknownLocationName, Status: "QUEUED"}])
}

func TestCollectActiveRunsZeroFillsKnownJobs(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"data": {
				"runsOrError": {
					"__typename": "Runs",
					"results": []
				}
			}
		}`))
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer ts.Close()

	c := NewDagsterCollector(t.Context(), ts.URL, time.Hour, time.Hour)
	c.knownJobs = map[JobKey]struct{}{
		{JobName: "never_run_job", LocationName: "loc_a"}: {},
	}

	CollectActiveRuns(t.Context(), c)

	for _, status := range activeStatuses {
		assert.Equal(t, 0, c.activeRunsCounts[ActiveRunKey{JobName: "never_run_job", LocationName: "loc_a", Status: status}])
	}
}
