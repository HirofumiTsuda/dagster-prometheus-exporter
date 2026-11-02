package collector

import "github.com/prometheus/client_golang/prometheus"

// buildJobAutomation reports, for every job in a definitions-roster response,
// whether a schedule or sensor that is turned on launches it. Pulled out of
// CollectDefinitionsRoster as its own pure function so it can be unit-tested
// directly, without an HTTP mock.
func buildJobAutomation(resp *GraphQLDefinitionsRosterResponse) map[JobKey]bool {
	automated := make(map[JobKey]bool)
	for _, repo := range resp.Data.RepositoriesOrError.Nodes {
		location := repo.Location.Name
		for _, job := range repo.Jobs {
			automated[JobKey{JobName: job.Name, LocationName: location}] = false
		}
		mark := func(jobName string) {
			key := JobKey{JobName: jobName, LocationName: location}
			if _, ok := automated[key]; ok {
				automated[key] = true
			}
		}
		for _, schedule := range repo.Schedules {
			if schedule.ScheduleState.Status == "RUNNING" {
				mark(schedule.PipelineName)
			}
		}
		for _, sensor := range repo.Sensors {
			if sensor.SensorState.Status == "RUNNING" {
				for _, target := range sensor.Targets {
					mark(target.PipelineName)
				}
			}
		}
	}
	return automated
}

// reflectJobAutomation emits dagster_job_automation_running, one series per job.
func reflectJobAutomation(c *DagsterCollector, ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for key, running := range c.jobAutomation {
		value := 0.0
		if running {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(
			c.jobAutomationDesc,
			prometheus.GaugeValue,
			value,
			key.JobName,
			key.LocationName,
		)
	}
}
