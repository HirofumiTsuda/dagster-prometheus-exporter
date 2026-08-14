package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRunsRequest(t *testing.T) {
	tests := []struct {
		statuses             []string
		updatedAfter         float64
		cursor               string
		limit                int
		expectedUpdatedAfter float64
	}{
		{
			statuses:             []string{"SUCCESS", "FAILED"},
			updatedAfter:         1000000,
			cursor:               "run_abc",
			limit:                500,
			expectedUpdatedAfter: 1000000,
		},
	}

	for _, tc := range tests {
		req := getRunsRequest(tc.statuses, tc.updatedAfter, tc.cursor, tc.limit)
		assert.Equal(t, req.Variables["statuses"], tc.statuses)
		assert.Equal(t, req.Variables["updatedAfter"], tc.expectedUpdatedAfter)
		assert.Equal(t, req.Variables["cursor"], tc.cursor)
		assert.Equal(t, req.Variables["limit"], tc.limit)
		assert.Contains(t, req.Query, "query")
	}
}

func TestGetRunsRequestWithNilUpdatedAfterAndCursor(t *testing.T) {
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
		req := getRunsRequest(tc.statuses, tc.updatedAfter, "", 500)
		assert.Equal(t, req.Variables["statuses"], tc.statuses)
		assert.Nil(t, req.Variables["updatedAfter"])
		assert.Nil(t, req.Variables["cursor"])
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
		mockResp.Data.RunsOrError.Results = []Run{
			{
				RunId:        "run_123",
				JobName:      "test_job",
				Status:       "STARTED",
				CreationTime: 1710000000.0,
				UpdateTime:   1720000000.0,
				EndTime:      1730000000.0,
				RepositoryOrigin: &RunRepositoryOrigin{
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

	req := getRunsRequest([]string{"STARTED"}, 0.0, "", 500)

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

func TestGetRunsRespectsContextTimeout(t *testing.T) {
	unblock := make(chan struct{})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a Dagster instance that never responds in time. There is
		// no http.Client-level Timeout anymore (see graphql.go) — ctx must
		// be the only thing that can end this request.
		<-unblock
	}))
	// unblock must be closed before ts.Close(), since Close() waits for the
	// in-flight handler above to return.
	defer ts.Close()
	defer close(unblock)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	req := getRunsRequest(nil, 0.0, "", 500)

	start := time.Now()
	_, err := getRuns(ctx, req, ts.URL)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, 2*time.Second, "request should have been cancelled by the context deadline, not left to hang")
}

func TestGetRunsWithServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	req := getRunsRequest(nil, 0.0, "", 500)
	response, err := getRuns(t.Context(), req, ts.URL)

	assert.Error(t, err)
	assert.Nil(t, response)
}

// makeRunsPage builds n runs named run_<offset>..run_<offset+n-1>, for use
// as a canned page of results in the fake server below.
func makeRunsPage(offset, n int) []Run {
	runs := make([]Run, n)
	for i := range n {
		runs[i] = Run{RunId: fmt.Sprintf("run_%d", offset+i), JobName: "job_a", Status: "SUCCESS"}
	}
	return runs
}

func TestFetchRunPagesPaginatesUntilAShortPage(t *testing.T) {
	const pageSize = 3
	pages := [][]Run{
		makeRunsPage(0, pageSize), // full page: expect another request
		makeRunsPage(3, pageSize), // full page: expect another request
		makeRunsPage(6, 1),        // short page: this must be the last request
	}

	var seenCursors []string
	call := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req GraphQLRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		if cursor, ok := req.Variables["cursor"]; ok && cursor != nil {
			seenCursors = append(seenCursors, cursor.(string))
		} else {
			seenCursors = append(seenCursors, "")
		}

		require.Less(t, call, len(pages), "fetchRunPages made more requests than expected")
		mockResp := GraphQLRunsResponse{}
		mockResp.Data.RunsOrError.Typename = "Runs"
		mockResp.Data.RunsOrError.Results = pages[call]
		call++

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode(mockResp))
	}))
	defer ts.Close()

	var got []Run
	err := fetchRunPages(t.Context(), []string{"SUCCESS"}, 0, ts.URL, pageSize, func(page []Run) error {
		got = append(got, page...)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 3, call, "expected exactly 3 page requests (2 full pages + 1 short page)")
	assert.Len(t, got, 7)
	assert.Equal(t, []string{"", "run_2", "run_5"}, seenCursors,
		"each page's cursor should be the runId of the previous page's last result")
}

// A pageSize of zero or less makes "len(results) < pageSize" false for an
// empty page, which used to fall through to results[len(results)-1] and
// panic with index out of range [-1] — inside one of scrapeDagster's
// goroutines, so it took the whole process down rather than failing a
// scrape. config.Load rejects such a pageSize now, but fetchRunPages
// shouldn't depend on its caller to stay memory-safe.
func TestFetchRunPagesStopsOnAnEmptyPageWhateverThePageSize(t *testing.T) {
	for _, pageSize := range []int{0, -1} {
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Less(t, calls, 5, "fetchRunPages should have stopped after the first empty page")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"data": {"runsOrError": {"__typename": "Runs", "results": []}}}`))
				assert.NoError(t, err)
			}))
			defer ts.Close()

			pages := 0
			require.NotPanics(t, func() {
				err := fetchRunPages(t.Context(), []string{"SUCCESS"}, 0, ts.URL, pageSize, func(page []Run) error {
					pages++
					return nil
				})
				assert.NoError(t, err)
			})

			assert.Equal(t, 1, calls, "an empty first page should end pagination immediately")
			assert.Equal(t, 1, pages)
		})
	}
}

// The top-level "errors" array is carried by the embedded graphQLErrors
// struct, so it reaches Go through promoted fields rather than a field
// declared on each response type. Embedding is supposed to leave the wire
// format untouched — this checks that it actually does, for each response
// type, since nothing else in the suite exercises this channel.
func TestGraphQLTopLevelErrorsAreReported(t *testing.T) {
	const body = `{"data": {}, "errors": [{"message": "Cannot query field \"nope\""}]}`

	queries := map[string]func(ctx context.Context, endpoint string) error{
		"runsOrError": func(ctx context.Context, endpoint string) error {
			_, err := getRuns(ctx, getRunsRequest([]string{"SUCCESS"}, 0, "", 500), endpoint)
			return err
		},
		"repositoriesOrError": func(ctx context.Context, endpoint string) error {
			_, err := getDefinitionsRoster(ctx, getDefinitionsRosterRequest(), endpoint)
			return err
		},
		"workspaceOrError": func(ctx context.Context, endpoint string) error {
			_, err := getWorkspaceStatus(ctx, getWorkspaceStatusRequest(), endpoint)
			return err
		},
		"version": func(ctx context.Context, endpoint string) error {
			_, err := GetVersion(ctx, GetVersionRequest(), endpoint)
			return err
		},
	}

	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(body))
				assert.NoError(t, err)
			}))
			defer ts.Close()

			err := query(t.Context(), ts.URL)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "Cannot query field",
				"the GraphQL error message should survive decoding through the embedded struct")
		})
	}
}
