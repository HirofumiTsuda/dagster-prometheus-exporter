package server

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDagster answers every query a full scrape makes with data that makes
// each metric family emit at least one series.
func fakeDagster(t *testing.T) *httptest.Server {
	t.Helper()
	now := time.Now().Unix()
	run := func(id, status, tags string) string {
		return fmt.Sprintf(`{"runId":%q,"jobName":"job_a","status":%q,"creationTime":%d,"updateTime":%d,"endTime":%d,"tags":[%s],"repositoryOrigin":{"repositoryName":"repo","repositoryLocationName":"loc_a"}}`,
			id, status, now-60, now-5, now-5, tags)
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		query := string(body)
		var out string
		switch {
		case strings.Contains(query, "repositoriesOrError"):
			out = fmt.Sprintf(`{"data":{"repositoriesOrError":{"__typename":"RepositoryConnection","nodes":[{"name":"repo","location":{"name":"loc_a"},"jobs":[{"name":"job_a","tags":[{"key":"owner","value":"team_a"}]}],
				"schedules":[{"name":"sched_a","cronSchedule":"* * * * *","scheduleState":{"status":"RUNNING","ticks":[{"status":"SUCCESS","timestamp":%d}]}}],
				"sensors":[{"name":"sens_a","sensorState":{"status":"RUNNING","ticks":[{"status":"SUCCESS","timestamp":%d}]}}]}]}}}`, now, now)
		case strings.Contains(query, "workspaceOrError"):
			out = `{"data":{"workspaceOrError":{"__typename":"Workspace","locationEntries":[
				{"name":"loc_a","locationOrLoadError":{"__typename":"RepositoryLocation"}},
				{"name":"loc_b","locationOrLoadError":{"__typename":"PythonError","message":"boom","stack":[]}}]}}}`
		case strings.Contains(query, "daemonHealth"):
			out = fmt.Sprintf(`{"data":{"instance":{"daemonHealth":{"allDaemonStatuses":[{"daemonType":"SCHEDULER","required":true,"healthy":true,"lastHeartbeatTime":%d,"lastHeartbeatErrors":[]}]}}}}`, now)
		case strings.Contains(query, "agents {"):
			out = fmt.Sprintf(`{"data":{"agents":[{"status":"RUNNING","lastHeartbeatTime":%d}]}}`, now)
		case strings.Contains(query, "concurrencyLimits"):
			out = `{"data":{"instance":{"concurrencyLimits":[{"concurrencyKey":"pool_a","activeSlotCount":1,"assignedStepCount":0,"pendingStepCount":2,"limit":2,"usingDefaultLimit":false}]}}}`
		case strings.Contains(query, "assetsLatestInfo"):
			out = fmt.Sprintf(`{"data":{"assetsLatestInfo":[{"assetKey":{"path":["asset_a"]},"latestRun":{"status":"SUCCESS","endTime":%d}}]}}`, now)
		case strings.Contains(query, "assetNodes"):
			out = `{"data":{"assetNodes":[{"assetKey":{"path":["asset_a"]},"isPartitioned":false,"staleStatus":"FRESH"}]}}`
		case strings.Contains(query, "runsOrError") && strings.Contains(query, `"SUCCESS"`):
			out = `{"data":{"runsOrError":{"__typename":"Runs","results":[` + run("done", "SUCCESS", "") + `]}}}`
		case strings.Contains(query, "runsOrError"):
			tag := `{"key":"dagster/concurrency_key","value":"pool_a"}`
			out = `{"data":{"runsOrError":{"__typename":"Runs","results":[` + run("active", "STARTED", "") + `,` + run("queued", "QUEUED", tag) + `]}}}`
		default:
			t.Errorf("unexpected query: %s", query)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(out))
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestMetricsPassPromlint(t *testing.T) {
	ts := fakeDagster(t)
	c := collector.NewDagsterCollector(t.Context(), ts.URL, "", time.Hour, time.Hour, 500, 5*time.Minute)
	c.SeedScrapeHealth(config.ActiveCollectorNames(true))
	// dagster_job_tags has no series until JOB_TAG_LABELS selects a tag.
	c.SetJobTagLabels(map[string]string{"owner": "tag_owner"})
	scrapeDagster(t.Context(), c)
	// scrapeDagster leaves out the Dagster+ collectors, which need a token.
	scrapeOne(t.Context(), c, "agents")

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c, newBuildInfoGauge("test-version", "test-commit"))

	// The linter only looks at families that emitted a series. Without this,
	// a family that never appears would pass it unnoticed.
	described := make(chan *prometheus.Desc, 64)
	go func() {
		c.Describe(described)
		newBuildInfoGauge("test-version", "test-commit").Describe(described)
		close(described)
	}()
	want := map[string]bool{}
	for desc := range described {
		want[descFqNameRe.FindStringSubmatch(desc.String())[1]] = true
	}
	families, err := reg.Gather()
	require.NoError(t, err)
	got := map[string]bool{}
	for _, family := range families {
		got[family.GetName()] = true
	}
	assert.Equal(t, want, got, "every metric family should have emitted a series, or the lint below skips it")

	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	for _, p := range problems {
		t.Errorf("promlint: %s: %s", p.Metric, p.Text)
	}
}
