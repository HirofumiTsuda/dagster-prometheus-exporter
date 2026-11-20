package server

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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
// The collectors run one after the other, on the test's goroutine: a panic in
// a goroutine would kill the fuzzing worker with no stack trace and no input
// worth saving.
//
// Under go test it replays the seeds below and anything saved in
// testdata/fuzz. To look for new failures, run it for a while:
//
//	go test ./internal/server -run '^$' -fuzz '^FuzzScrapeResponses$' -fuzztime 60s
func FuzzScrapeResponses(f *testing.F) { fuzzScrapeResponses(f, false) }

// FuzzScrapeResponsesConcurrent is the same with the collectors running in
// goroutines, as the exporter runs them, so the race detector and a deadlock
// see them against arbitrary bodies. A crash has no stack trace here, so
// reproduce one with FuzzScrapeResponses first.
//
//	go test -race ./internal/server -run '^$' -fuzz '^FuzzScrapeResponsesConcurrent$' -fuzztime 60s
func FuzzScrapeResponsesConcurrent(f *testing.F) { fuzzScrapeResponses(f, true) }

func fuzzScrapeResponses(f *testing.F, concurrent bool) {
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

	// One server for the whole run, not one per input. A fuzz worker runs
	// thousands of inputs a second and every input opens a connection for each
	// collector. With a new server each time, TCP connects stalled after a few
	// seconds, every collector waiting in connect() while the server waited in
	// accept(), until the coordinator killed the worker as hung (#280). The
	// collectors were not at fault: the same targets run for minutes on one server.
	var bodies atomic.Pointer[[3]string]
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, _ := io.ReadAll(r.Body)
		b := bodies.Load()
		body := b[2]
		switch {
		case strings.Contains(string(query), "repositoriesOrError"):
			body = b[0]
		case strings.Contains(string(query), "runsOrError"):
			body = b[1]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	f.Cleanup(ts.Close)

	log.SetOutput(io.Discard)
	f.Cleanup(func() { log.SetOutput(io.Discard) })

	f.Fuzz(func(t *testing.T, rosterBody, runsBody, otherBody string) {
		bodies.Store(&[3]string{rosterBody, runsBody, otherBody})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := collector.NewDagsterCollector(ctx, ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
		c.SeedScrapeHealth(config.ActiveCollectorNames(true))
		c.SetJobTagLabels(map[string]string{"owner": "tag_owner"})

		for range 2 {
			if !concurrent {
				for _, name := range config.ActiveCollectorNames(true) {
					scrapeOne(ctx, c, name)
				}
				continue
			}
			var wg sync.WaitGroup
			for _, name := range config.ActiveCollectorNames(true) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					scrapeOne(ctx, c, name)
				}()
			}
			wg.Wait()
		}

		reg := prometheus.NewPedanticRegistry()
		reg.MustRegister(c)
		if _, err := reg.Gather(); err != nil {
			t.Fatalf("metrics cannot be gathered: %v", err)
		}
	})
}
