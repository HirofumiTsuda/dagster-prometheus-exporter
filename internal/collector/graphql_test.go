package collector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRunsRequest(t *testing.T) {
	tests := []struct {
		statuses             []string
		updatedAfter         float64
		expectedUpdatedAfter float64
	}{
		{
			statuses:             []string{"SUCCESS", "FAILED"},
			updatedAfter:         1000000,
			expectedUpdatedAfter: 1000000,
		},
	}

	for _, tc := range tests {
		req := getRunsRequest(tc.statuses, tc.updatedAfter)
		assert.Equal(t, req.Variables["statuses"], tc.statuses)
		assert.Equal(t, req.Variables["updatedAfter"], tc.expectedUpdatedAfter)
		assert.Contains(t, req.Query, "query")
	}
}

func TestGetRunsRequestWithNilUpdatedAfter(t *testing.T) {
	tests := []struct {
		statuses     []string
		updatedAfter float64
	}{
		{
			statuses:     []string{"QUEUED"},
			updatedAfter: 0.0,
		},
	}

	for _, tc := range tests {
		req := getRunsRequest(tc.statuses, tc.updatedAfter)
		assert.Equal(t, req.Variables["statuses"], tc.statuses)
		assert.Nil(t, req.Variables["updatedAfter"])
		assert.Contains(t, req.Query, "query")
	}
}

func TestGetRuns(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
		}
		mockResp := GraphQLRunsResponse{}
		mockResp.Data.RunsOrError.Typename = "Runs"
		mockResp.Data.RunsOrError.Results = []struct {
			RunId            string  `json:"runId"`
			JobName          string  `json:"jobName"`
			Status           string  `json:"status"`
			CreationTime     float64 `json:"creationTime"`
			UpdateTime       float64 `json:"updateTime"`
			EndTime          float64 `json:"endTime"`
			RepositoryOrigin *struct {
				RepositoryName         string `json:"repositoryName"`
				RepositoryLocationName string `json:"repositoryLocationName"`
			} `json:"repositoryOrigin"`
		}{
			{
				RunId:        "run_123",
				JobName:      "test_job",
				Status:       "STARTED",
				CreationTime: 1710000000.0,
				UpdateTime:   1720000000.0,
				EndTime:      1730000000.0,
				RepositoryOrigin: &struct {
					RepositoryName         string `json:"repositoryName"`
					RepositoryLocationName string `json:"repositoryLocationName"`
				}{
					RepositoryName:         "__repository__",
					RepositoryLocationName: "test_location",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		err := json.NewEncoder(w).Encode(mockResp)
		if err != nil {
			t.Fatalf("failed to encode response: %v", err)
		}
	}))
	defer ts.Close()

	req := getRunsRequest([]string{"STARTED"}, 0.0)

	resp, err := getRuns(t.Context(), req, ts.URL)

	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Len(t, resp.Data.RunsOrError.Results, 1)

	result := resp.Data.RunsOrError.Results[0]
	assert.Equal(t, "run_123", result.RunId)
	assert.Equal(t, "STARTED", result.Status)
	require.NotNil(t, result.RepositoryOrigin)
	assert.Equal(t, "test_location", result.RepositoryOrigin.RepositoryLocationName)
}

func TestGetRunsWithServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	req := getRunsRequest(nil, 0.0)
	response, err := getRuns(t.Context(), req, ts.URL)

	assert.Error(t, err)
	assert.Nil(t, response)
}
