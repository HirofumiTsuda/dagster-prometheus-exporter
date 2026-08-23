package server

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/prometheus/client_golang/prometheus"
)

// descFqNameRe pulls the metric name out of (*prometheus.Desc).String(),
// e.g. `Desc{fqName: "dagster_active_runs", ...}`. client_golang has no
// public accessor for a Desc's name, so this is the only way to read it
// back without reaching into the package's internals.
var descFqNameRe = regexp.MustCompile(`fqName: "([^"]+)"`)

// TestListMetricNames prints every metric family this exporter can ever
// produce on /metrics, one per line as "METRIC: <name>" — both
// DagsterCollector's families and dagster_exporter_build_info, registered
// the same way RunServer registers them.
//
// This is the single source of truth issue #90 asks for: helm-e2e.yml
// derives its expected-metrics list from this test's output
// (`go test -run TestListMetricNames -v`) instead of a hand-maintained
// list that silently drifts from the real collector. That drift is exactly
// how #85 went unnoticed for weeks — two entire metric families had never
// emitted a single series, and nothing checked for their absence.
//
// Describe() is used rather than Gather(): Describe unconditionally sends
// one Desc per metric family regardless of whether any data has been
// observed yet. Gather() only returns a family once Collect() has actually
// sent it a sample, so a freshly constructed, never-scraped
// DagsterCollector would report almost nothing through it — every
// in-memory map behind a per-asset/per-job/per-schedule metric starts
// empty.
func TestListMetricNames(t *testing.T) {
	c := collector.NewDagsterCollector(t.Context(), "http://unused", time.Hour, time.Hour, 500, 5*time.Minute)
	b := newBuildInfoGauge("test-version", "test-commit")

	ch := make(chan *prometheus.Desc, 32)
	go func() {
		c.Describe(ch)
		b.Describe(ch)
		close(ch)
	}()

	names := make(map[string]struct{})
	for desc := range ch {
		m := descFqNameRe.FindStringSubmatch(desc.String())
		if m == nil {
			t.Fatalf("could not extract a metric name from Desc: %s", desc.String())
		}
		names[m[1]] = struct{}{}
	}

	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	for _, name := range sorted {
		fmt.Println("METRIC:", name)
	}
}
