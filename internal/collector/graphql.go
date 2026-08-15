package collector

import (
	"context"
	_ "embed"

	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

// closeBody closes an HTTP response body, discarding the error. By this
// point the response has already been read (or the caller is bailing out),
// so there's nothing actionable to do with a close failure — this just
// keeps that intentional discard out of every call site.
func closeBody(body io.Closer) {
	_ = body.Close()
}

// graphQLClient is shared by every query. A zero-value http.Client already
// reuses http.DefaultTransport's connection pool, so this isn't about
// pooling — it's about having one place to set client-level policy, and one
// seam to swap in tests.
//
// Timeout is intentionally left unset: callers control how long a request
// may run through the context they pass (see http.NewRequestWithContext in
// doGraphQL). Setting it here would silently cap the per-scrape deadline
// that DAGSTER_SCRAPING_TIMEOUT_SECONDS is supposed to own.
var graphQLClient = &http.Client{}

// graphQLErrors is embedded in every response type to carry GraphQL's
// top-level "errors" array. Embedding keeps the field promoted to the top
// level of the JSON document, so the wire format is unchanged.
//
// Note this is a different failure channel from the unions handled by
// unexpectedUnionMember: Dagster reports most query-level problems through
// the union, and only protocol-level problems (a malformed query, an
// unknown field) show up here.
type graphQLErrors struct {
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (e *graphQLErrors) err() error {
	if len(e.Errors) > 0 {
		return fmt.Errorf("graphql error: %s", e.Errors[0].Message)
	}
	return nil
}

// graphQLResponse is implemented by every response type, via the
// graphQLErrors it embeds. Callers pass a pointer to their own response
// value, so the pointer-receiver err() is in the method set.
type graphQLResponse interface {
	err() error
}

// doGraphQL posts request to endpoint and decodes the reply into out, in the
// same shape as json.Unmarshal: the caller owns the destination value and
// passes a pointer to it.
//
// Every query goes through here so that transport-level policy — the
// context-controlled timeout, the status-code check, the top-level "errors"
// check — is defined once. It used to be copied into a separate 36-line
// function per query, and that duplication is exactly how the union checks
// in issue #69 came to exist in one copy but not the others.
func doGraphQL(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string, out graphQLResponse) error {
	jsonBytes, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal graphql request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, dagsterGraphQLEndpoint, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := graphQLClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to execute http request: %w", err)
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return out.err()
}

// unexpectedUnionMember reports that a GraphQL union resolved to something
// other than the success type the caller can actually read.
//
// Dagster signals query-level failures through the union itself
// (PythonError, InvalidPipelineRunsFilterError, ...) rather than through
// GraphQL's top-level "errors" array. A failed query therefore decodes into
// a perfectly well-formed response whose result list is simply empty, which
// is indistinguishable from "there is nothing there" unless __typename is
// checked. Treating that as success used to let one transient Dagster error
// prune every accumulated series — known jobs, completed-run counters,
// last-run status, schedules, sensors — while the collector still returned
// nil and dagster_exporter_last_scrape_success kept reporting 1 (issue #69).
//
// Anything that isn't the expected success type is therefore an error,
// including an absent __typename: every query in queries/ selects
// __typename, so its absence means the response isn't the shape this code
// was written against and its emptiness can't be trusted either.
//
// stack is logged rather than folded into the error: it's multi-line, only
// useful for debugging, and the error itself is logged as a single line by
// the calling collector.
func unexpectedUnionMember(field, want, got, message string, stack []string) error {
	if len(stack) > 0 {
		log.Printf("dagster returned %s for %s:\n%s", got, field, strings.Join(stack, "\n"))
	}
	if got == "" {
		got = "a response with no __typename"
	}
	if message == "" {
		message = "(no message provided)"
	}
	return fmt.Errorf("%s returned %s instead of %s: %s", field, got, want, message)
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

// RunTag is one Dagster run tag (key/value pair). Notably includes
// "dagster/concurrency_key" for runs subject to a tag-based run-queue
// concurrency limit (dagster.yaml's concurrency.runs.tag_concurrency_limits)
// — see CollectConcurrencyKeyBacklog.
type RunTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// unknownLocationName is the location label for runs whose repositoryOrigin
// is absent (launched outside a code location, or old enough to predate the
// field). The surrounding underscores keep it from colliding with a real
// code location name: a collision would sum "we don't know where this ran"
// and "it ran in the location called unknown" into one series, and would
// also defeat the pruning in pruneLastRunStatus/pruneCompletedRunsCounter,
// which relies on placeholder keys never matching a live location.
const unknownLocationName = "__unknown__"

// location returns the code location a run was launched from, falling back
// to unknownLocationName when repositoryOrigin is absent.
func (r Run) location() string {
	if r.RepositoryOrigin == nil {
		return unknownLocationName
	}
	return r.RepositoryOrigin.RepositoryLocationName
}

type Run struct {
	RunId            string               `json:"runId"`
	JobName          string               `json:"jobName"`
	Status           string               `json:"status"`
	CreationTime     float64              `json:"creationTime"`
	UpdateTime       float64              `json:"updateTime"`
	EndTime          float64              `json:"endTime"`
	Tags             []RunTag             `json:"tags"`
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
	graphQLErrors
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

		// An empty page always ends pagination, checked separately from the
		// short-page test below rather than folded into it: with a pageSize
		// of zero or less, "len(results) < pageSize" is false for an empty
		// page, and the cursor line would then index results[-1] and panic.
		// config.Load rejects such a pageSize, but this loop shouldn't
		// depend on its caller to stay memory-safe.
		if len(results) == 0 || len(results) < pageSize {
			return nil
		}
		cursor = results[len(results)-1].RunId
	}
}

func getRuns(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLRunsResponse, error) {
	var resp GraphQLRunsResponse
	if err := doGraphQL(ctx, request, dagsterGraphQLEndpoint, &resp); err != nil {
		return nil, err
	}

	if runsOrError := resp.Data.RunsOrError; runsOrError.Typename != "Runs" {
		return nil, unexpectedUnionMember("runsOrError", "Runs", runsOrError.Typename, runsOrError.Message, runsOrError.Stack)
	}

	return &resp, nil
}

// GraphQLDefinitionsRosterResponse is the shape of repositoriesOrError used
// to build the exporter's view of "what exists": known jobs (JobKey,
// pruning/seeding for completed-run counters and last-run status) and known
// schedules/sensors with their most recent tick, in one fetch. Dagster's
// Repository type exposes jobs, schedules, and sensors as independent
// sibling fields (not reachable only via jobs), so all three can be
// requested together without any per-job/per-schedule/per-sensor
// follow-up query. Schedule.scheduleState and Sensor.sensorState are both
// InstigationState under the hood, hence the identical status/ticks shape.
type GraphQLDefinitionsRosterResponse struct {
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
				Schedules []struct {
					Name          string `json:"name"`
					CronSchedule  string `json:"cronSchedule"`
					ScheduleState struct {
						Status string `json:"status"`
						Ticks  []struct {
							Status    string  `json:"status"`
							Timestamp float64 `json:"timestamp"`
						} `json:"ticks"`
					} `json:"scheduleState"`
				} `json:"schedules"`
				Sensors []struct {
					Name        string `json:"name"`
					SensorState struct {
						Status string `json:"status"`
						Ticks  []struct {
							Status    string  `json:"status"`
							Timestamp float64 `json:"timestamp"`
						} `json:"ticks"`
					} `json:"sensorState"`
				} `json:"sensors"`
			} `json:"nodes"`
			Message string   `json:"message"`
			Stack   []string `json:"stack"`
		} `json:"repositoriesOrError"`
	} `json:"data"`
	graphQLErrors
}

//go:embed queries/get_definitions_roster.graphql
var definitionsRosterQuery string

func getDefinitionsRosterRequest() *GraphQLRequest {
	return &GraphQLRequest{
		Query: definitionsRosterQuery,
	}
}

func getDefinitionsRoster(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLDefinitionsRosterResponse, error) {
	var resp GraphQLDefinitionsRosterResponse
	if err := doGraphQL(ctx, request, dagsterGraphQLEndpoint, &resp); err != nil {
		return nil, err
	}

	if repositoriesOrError := resp.Data.RepositoriesOrError; repositoriesOrError.Typename != "RepositoryConnection" {
		return nil, unexpectedUnionMember("repositoriesOrError", "RepositoryConnection", repositoriesOrError.Typename, repositoriesOrError.Message, repositoriesOrError.Stack)
	}

	return &resp, nil
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
	graphQLErrors
}

//go:embed queries/get_workspace_status.graphql
var workspaceStatusQuery string

func getWorkspaceStatusRequest() *GraphQLRequest {
	return &GraphQLRequest{
		Query: workspaceStatusQuery,
	}
}

func getWorkspaceStatus(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLWorkspaceStatusResponse, error) {
	var resp GraphQLWorkspaceStatusResponse
	if err := doGraphQL(ctx, request, dagsterGraphQLEndpoint, &resp); err != nil {
		return nil, err
	}

	if workspaceOrError := resp.Data.WorkspaceOrError; workspaceOrError.Typename != "Workspace" {
		return nil, unexpectedUnionMember("workspaceOrError", "Workspace", workspaceOrError.Typename, workspaceOrError.Message, workspaceOrError.Stack)
	}

	return &resp, nil
}

//go:embed queries/get_version.graphql
var versionQuery string

type GraphQLVersionResponse struct {
	Data struct {
		Version string `json:"version"`
	} `json:"data"`
	graphQLErrors
}

func GetVersionRequest() *GraphQLRequest {

	return &GraphQLRequest{
		Query:     versionQuery,
		Variables: nil,
	}
}

func GetVersion(ctx context.Context, request *GraphQLRequest, dagsterGraphQLEndpoint string) (*GraphQLVersionResponse, error) {
	var resp GraphQLVersionResponse
	if err := doGraphQL(ctx, request, dagsterGraphQLEndpoint, &resp); err != nil {
		return nil, err
	}

	// version isn't a union, so there's no __typename to check. An empty
	// value is the only signal that the reply didn't carry what /readyz
	// needs.
	if len(resp.Data.Version) == 0 {
		return nil, fmt.Errorf("graphql version response missing version")
	}

	return &resp, nil
}
