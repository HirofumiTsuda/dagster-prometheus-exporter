package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsServesWhatTheDefaultRegistryUsedTo(t *testing.T) {
	reg := newRegistry(prometheus.NewGauge(prometheus.GaugeOpts{Name: "dagster_test_gauge", Help: "h"}))
	rec := httptest.NewRecorder()
	metricsHandler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	want := []string{
		"dagster_test_gauge",
		"dagster_exporter_build_info",
		"go_goroutines",
		"promhttp_metric_handler_requests_total",
		"promhttp_metric_handler_requests_in_flight",
	}
	if runtime.GOOS == "linux" {
		want = append(want, "process_cpu_seconds_total")
	}
	for _, name := range want {
		assert.Contains(t, body, "# TYPE "+name+" ", "/metrics must still serve %s", name)
	}
}

func TestRunServerCanRunTwiceInOneProcess(t *testing.T) {
	dagster := fakeDagster(t)
	t.Setenv("DAGSTER_GRAPHQL_ENDPOINT", dagster.URL)

	for range 2 {
		port := freePort(t)
		t.Setenv("PORT", fmt.Sprint(port))
		cfg, err := config.Load()
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- RunServer(ctx, cfg) }()
		t.Cleanup(cancel)

		metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
		require.Eventually(t, func() bool {
			resp, err := http.Get(metricsURL)
			if err != nil {
				return false
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return resp.StatusCode == http.StatusOK &&
				strings.Contains(string(body), "dagster_exporter_build_info") &&
				strings.Contains(string(body), "go_goroutines")
		}, 5*time.Second, 50*time.Millisecond, "RunServer on port %d should serve /metrics", port)

		cancel()
		select {
		case err := <-done:
			require.NoError(t, err, "a cancelled RunServer returns nil, and must not panic on the second run")
		case <-time.After(10 * time.Second):
			t.Fatal("RunServer did not return after its context was cancelled")
		}
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}
