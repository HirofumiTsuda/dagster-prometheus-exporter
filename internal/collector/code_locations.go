package collector

import (
	"context"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// CollectCodeLocationStatus checks, independently of job/run collection,
// whether each code location in the workspace is currently loadable. A
// broken code location is a distinct failure mode from "this location has
// zero jobs" (see issue #38), so it gets its own query and its own metric
// rather than being inferred from the definitions roster's output.
func CollectCodeLocationStatus(ctx context.Context, c *DagsterCollector) error {
	req := getWorkspaceStatusRequest()

	resp, err := getWorkspaceStatus(ctx, req, c.dagsterGraphQLEndpoint)
	if err != nil {
		log.Printf("failed to collect code location status from dagster: %v", err)
		return err
	}

	// A workspace-level PythonError is already rejected by getWorkspaceStatus
	// (see unexpectedUnionMember), so reaching here means locationEntries is
	// trustworthy. The per-entry check below is a different thing entirely:
	// it's the metric itself, reporting which individual code locations
	// failed to load while the workspace query as a whole succeeded.
	loadErrors := make(map[string]bool, len(resp.Data.WorkspaceOrError.LocationEntries))
	for _, entry := range resp.Data.WorkspaceOrError.LocationEntries {
		failed := entry.LocationOrLoadError.Typename == "PythonError"
		loadErrors[entry.Name] = failed
		if failed {
			log.Printf("code location %q failed to load: %s\n%s", entry.Name, entry.LocationOrLoadError.Message, strings.Join(entry.LocationOrLoadError.Stack, "\n"))
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.codeLocationLoadError = loadErrors

	return nil
}

func reflectCodeLocationStatus(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for location, failed := range c.codeLocationLoadError {
		value := 0.0
		if failed {
			value = 1.0
		}
		ch <- prometheus.MustNewConstMetric(
			c.codeLocationLoadErrorDesc,
			prometheus.GaugeValue,
			value,
			location,
		)
	}
}
