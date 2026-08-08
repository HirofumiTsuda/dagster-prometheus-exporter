package collector

import (
	"context"
	_ "embed"

	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// closeBody closes an HTTP response body, discarding the error. By this
// point the response has already been read (or the caller is bailing out),
// so there's nothing actionable to do with a close failure — this just
// keeps that intentional discard out of every call site.
func closeBody(body io.Closer) {
	_ = body.Close()
}

type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// RunRepositoryOrigin identifies the code location a Run was launched from,
// as recorded on the run itself (a launch-time snapshot, not a live lookup).
type RunRepositoryOrigin struct {
	RepositoryName         string `json:"repositoryName"`
	RepositoryLocationName string `json:"repositoryLocationName"`
}

type Run struct {
	RunId            string               `json:"runId"`
	JobName          string               `json:"jobName"`
	Status           string               `json:"status"`
	CreationTime     float64              `json:"creationTime"`
	UpdateTime       float64              `json:"updateTime"`
	EndTime          float64              `json:"endTime"`
	RepositoryOrigin *RunRepositoryOrigin `json:"repositoryOrigin"`
}

type GraphQLRunsResponse struct {
	Data struct {
		RunsOrError struct {
			Typename string   `json:"__typename"`
			Results  []Run    `json:"results"`
			Message  string   `json:"message"`
			Stack    []string `json:"stack"`
		} `json:"runsOrError"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

//go:embed queries/get_runs.graphql
var runsQuery string

func getRunsRequest(statuses []string, updateAfter float64, cursor string, limit int) *GraphQLRequest {

	var updatedAfter interface{} = updateAfter
	if updateAfter == 0.0 {
		updatedAfter = nil
	}
	var cursorVar interface{} = cursor
	if cursor == "" {
		cursorVar = nil
	}
	variables := map[string]interface{}{
		"statuses":     statuses,
		"updatedAfter": updatedAfter,
		"cursor":       cursorVar,
		"limit":        limit,
	}
	return &GraphQLRequest{
		Query:     runsQuery,
		Variables: variables,
	}
}

// fetchRunPages fetches every run matching statuses/updateAfter, paging
// through runsOrError via cursor and invoking onPage once per page, instead
// of accumulating every run into memory before returning. The caller is
// expected to fold each page into its own (much smaller) aggregate state.
//
// The cursor Dagster expects is simply the runId of the last run already
// seen (not an opaque token) — see
// https://github.com/dagster-io/dagster/issues/31024#issuecomment-5126177124.
func fetchRunPages(ctx context.Context, statuses []string, updateAfter float64, dagsterGraphQLEndpoint string, pageSize int, onPage func([]Run) error) error {
	cursor := ""

	for {
		req := getRunsRequest(statuses, updateAfter, cursor, pageSize)
		resp, err := getRuns(ctx, req, dagsterGraphQLEndpoint)
		if err != nil {
			return err
		}

		results := resp.Data.RunsOrError.Results
		if err := onPage(results); err != nil {
			return err
		}

		if len(results) < pageSize {
			return nil
		}
		cursor = results[len(results)-1].RunId
	}
}

func getRuns(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLRunsResponse, error) {
	jsonBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, dagsterGraphQLEndpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Timeout is intentionally not set here: the caller controls how long a
	// request may run via ctx (see http.NewRequestWithContext above).
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request: %w", err)
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var graphQLResp GraphQLRunsResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphQLResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(graphQLResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", graphQLResp.Errors[0].Message)
	}

	return &graphQLResp, nil
}

type GraphQLJobLocationsResponse struct {
	Data struct {
		RepositoriesOrError struct {
			Typename string `json:"__typename"`
			Nodes    []struct {
				Name     string `json:"name"`
				Location struct {
					Name string `json:"name"`
				} `json:"location"`
				Jobs []struct {
					Name string `json:"name"`
				} `json:"jobs"`
			} `json:"nodes"`
			Message string   `json:"message"`
			Stack   []string `json:"stack"`
		} `json:"repositoriesOrError"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

//go:embed queries/get_jobs_per_repository.graphql
var jobLocationsQuery string

func getJobLocationsRequest() *GraphQLRequest {
	return &GraphQLRequest{
		Query: jobLocationsQuery,
	}
}

func getJobLocations(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLJobLocationsResponse, error) {
	jsonBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, dagsterGraphQLEndpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Timeout is intentionally not set here: the caller controls how long a
	// request may run via ctx (see http.NewRequestWithContext above).
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request: %w", err)
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var graphQLResp GraphQLJobLocationsResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphQLResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(graphQLResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", graphQLResp.Errors[0].Message)
	}

	return &graphQLResp, nil
}

type GraphQLWorkspaceStatusResponse struct {
	Data struct {
		WorkspaceOrError struct {
			Typename        string `json:"__typename"`
			LocationEntries []struct {
				Name                string `json:"name"`
				LocationOrLoadError struct {
					Typename string   `json:"__typename"`
					Message  string   `json:"message"`
					Stack    []string `json:"stack"`
				} `json:"locationOrLoadError"`
			} `json:"locationEntries"`
			Message string   `json:"message"`
			Stack   []string `json:"stack"`
		} `json:"workspaceOrError"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

//go:embed queries/get_workspace_status.graphql
var workspaceStatusQuery string

func getWorkspaceStatusRequest() *GraphQLRequest {
	return &GraphQLRequest{
		Query: workspaceStatusQuery,
	}
}

func getWorkspaceStatus(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLWorkspaceStatusResponse, error) {
	jsonBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, dagsterGraphQLEndpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Timeout is intentionally not set here: the caller controls how long a
	// request may run via ctx (see http.NewRequestWithContext above).
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request: %w", err)
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var graphQLResp GraphQLWorkspaceStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphQLResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(graphQLResp.Errors) > 0 {
		return nil, fmt.Errorf("graphql error: %s", graphQLResp.Errors[0].Message)
	}

	return &graphQLResp, nil
}

//go:embed queries/get_version.graphql
var versionQuery string

type GraphQLVersionResponse struct {
	Data struct {
		Version string `json:"version"`
	} `json:"data"`
}

func GetVersionRequest() *GraphQLRequest {

	return &GraphQLRequest{
		Query:     versionQuery,
		Variables: nil,
	}
}

func GetVersion(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLVersionResponse, error) {
	jsonBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, dagsterGraphQLEndpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Timeout is intentionally not set here: the caller controls how long a
	// request may run via ctx (see http.NewRequestWithContext above).
	client := &http.Client{}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to execute http request: %w", err)
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var graphQLResp GraphQLVersionResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphQLResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if len(graphQLResp.Data.Version) == 0 {
		return nil, fmt.Errorf("graphql version response missing version")
	}

	return &graphQLResp, nil
}
