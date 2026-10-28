package collector

import (
	"maps"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
)

func newJobTagsDesc(tagLabels []string) *prometheus.Desc {
	return prometheus.NewDesc(
		"dagster_job_tags",
		"Tags of a job definition selected by JOB_TAG_LABELS, one tag_<key> label each (value is always 1; a tag the job doesn't have is an empty label). Join it onto the other per-job metrics on (job_name, location) to filter or group them by tag",
		append([]string{"job_name", "location"}, tagLabels...),
		nil,
	)
}

// SetJobTagLabels makes dagster_job_tags export the given job tags, as a map
// of tag key to label name (config.Load builds it from JOB_TAG_LABELS). It
// changes the metric's label names, so it has to be called before the
// collector is registered. Without it, dagster_job_tags has no series.
func (c *DagsterCollector) SetJobTagLabels(labels map[string]string) {
	c.jobTagKeys = slices.Sorted(maps.Keys(labels))
	names := make([]string, len(c.jobTagKeys))
	for i, key := range c.jobTagKeys {
		names[i] = labels[key]
	}
	c.jobTagsDesc = newJobTagsDesc(names)
}

// buildJobTags extracts the value of each tag in keys, in that order, for
// every job in a definitions-roster response. A tag the job doesn't have
// is "". Pulled out of CollectDefinitionsRoster as its own pure function so
// it can be unit-tested directly, without an HTTP mock.
func buildJobTags(resp *GraphQLDefinitionsRosterResponse, keys []string) map[JobKey][]string {
	jobTags := make(map[JobKey][]string)
	if len(keys) == 0 {
		return jobTags
	}
	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		for _, job := range repo.Jobs {
			values := make([]string, len(keys))
			for _, tag := range job.Tags {
				if i := slices.Index(keys, tag.Key); i >= 0 {
					values[i] = tag.Value
				}
			}
			jobTags[JobKey{JobName: job.Name, LocationName: repo.Location.Name}] = values
		}
	}
	return jobTags
}

// reflectJobTags emits dagster_job_tags, one series per job. The map is
// rebuilt from every roster, like the schedule metrics, except that a job
// missing from the roster keeps its tags for as long as retainedJobs keeps
// its other series (see CollectDefinitionsRoster).
func reflectJobTags(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, values := range c.jobTags {
		ch <- prometheus.MustNewConstMetric(
			c.jobTagsDesc,
			prometheus.GaugeValue,
			1,
			append([]string{key.JobName, key.LocationName}, values...)...,
		)
	}
}
