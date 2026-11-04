package server

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"github.com/prometheus/client_golang/prometheus"
)

// FuzzScrapeResponses serves arbitrary bodies for the three kinds of query a
// scrape makes, runs every collector twice, and gathers the metrics. The
// exporter reads whatever a GraphQL endpoint returns, from Dagster or from
// whatever sits in front of it, so no body may panic a collector or leave a
// registry that cannot be gathered. The second scrape matters: the first only
// fills the state, and pruning, the last-run undo and the seed read run on top
// of it.
//
// Under go test it replays the seeds below and anything saved in
// testdata/fuzz. To look for new failures, run it for a while:
//
//	go test ./internal/server -run '^$' -fuzz FuzzScrapeResponses -fuzztime 60s
func FuzzScrapeResponses(f *testing.F) {
	const (
		roster = `{"data":{"repositoriesOrError":{"__typename":"RepositoryConnection","nodes":[{"name":"repo","location":{"name":"loc"},` +
			`"jobs":[{"name":"job_a","tags":[{"key":"owner","value":"team"}]}],` +
			`"schedules":[{"name":"s","cronSchedule":"* * * * *","pipelineName":"job_a","scheduleState":{"status":"RUNNING","ticks":[{"status":"SUCCESS","timestamp":1700000000}]}}],` +
			`"sensors":[{"name":"n","targets":[{"pipelineName":"job_a"}],"sensorState":{"status":"RUNNING","ticks":[{"status":"FAILURE","timestamp":1700000000}]}}]}]}}}`
		runs = `{"data":{"runsOrError":{"__typename":"Runs","results":[` +
			`{"runId":"a","jobName":"job_a","status":"FAILURE","creationTime":1700000000,"updateTime":1700000050,"endTime":1700000040,"tags":[{"key":"dagster/will_retry","value":"true"}],"repositoryOrigin":{"repositoryName":"repo","repositoryLocationName":"loc"}},` +
			`{"runId":"b","jobName":"job_a","status":"STARTED","creationTime":1700000100,"updateTime":1700000100,"endTime":0,"tags":[],"repositoryOrigin":{"repositoryName":"repo","repositoryLocationName":"loc"}}]}}}`
		other = `{"data":{"agents":[{"status":"RUNNING","lastHeartbeatTime":1700000000}],` +
			`"instance":{"daemonHealth":{"allDaemonStatuses":[{"daemonType":"SCHEDULER","required":true,"healthy":true,"lastHeartbeatTime":1700000000,"lastHeartbeatErrors":[]}]},` +
			`"concurrencyLimits":[{"concurrencyKey":"p","activeSlotCount":1,"assignedStepCount":0,"pendingStepCount":2,"limit":2,"usingDefaultLimit":false}]},` +
			`"workspaceOrError":{"__typename":"Workspace","locationEntries":[{"name":"loc","locationOrLoadError":{"__typename":"PythonError","message":"boom","stack":[]}}]},` +
			`"assetsLatestInfo":[{"assetKey":{"path":["a"]},"latestRun":{"status":"SUCCESS","endTime":1700000000}}],` +
			`"assetNodes":[{"assetKey":{"path":["a"]},"isPartitioned":false,"staleStatus":"FRESH"}]}}`
		unionError = `{"data":{"repositoriesOrError":{"__typename":"PythonError","message":"x","stack":["y"]},"runsOrError":{"__typename":"InvalidPipelineRunsFilterError","message":"z"}}}`
	)
	f.Add(roster, runs, other)
	f.Add(unionError, unionError, unionError)
	f.Add(`{}`, `{}`, `{}`)
	f.Add(`{"data":null}`, `{"data":{"runsOrError":null}}`, `{"errors":[{"message":"a"},{"message":"b"}]}`)
	f.Add(`{"data":{"repositoriesOrError":{"__typename":"RepositoryConnection","nodes":[{"name":"","location":{"name":""},"jobs":[{"name":""}],"schedules":null,"sensors":null}]}}}`, runs, other)
	f.Add(roster, `{"data":{"runsOrError":{"__typename":"Runs","results":[{"runId":"a","jobName":"job_a","status":"SUCCESS","creationTime":-1,"updateTime":1e30,"endTime":-5,"tags":null}]}}}`, other)

	log.SetOutput(io.Discard)
	f.Cleanup(func() { log.SetOutput(io.Discard) })

	f.Fuzz(func(t *testing.T, rosterBody, runsBody, otherBody string) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query, _ := io.ReadAll(r.Body)
			body := otherBody
			switch {
			case strings.Contains(string(query), "repositoriesOrError"):
				body = rosterBody
			case strings.Contains(string(query), "runsOrError"):
				body = runsBody
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		defer ts.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := collector.NewDagsterCollector(ctx, ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
		c.SeedScrapeHealth(config.ActiveCollectorNames(true))
		c.SetJobTagLabels(map[string]string{"owner": "tag_owner"})

		// One after the other, on the test's goroutine: a panic in a goroutine of
		// scrapeDagster would kill the fuzzing worker with no stack trace and
		// no input worth saving. Running them concurrently is the race
		// detector's job.
		for range 2 {
			for _, name := range config.ActiveCollectorNames(true) {
				scrapeOne(ctx, c, name)
			}
		}

		reg := prometheus.NewPedanticRegistry()
		reg.MustRegister(c)
		if _, err := reg.Gather(); err != nil {
			t.Fatalf("metrics cannot be gathered: %v", err)
		}
	})
}
