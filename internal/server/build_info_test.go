package server

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuildInfoGaugeReportsVersionAndCommit(t *testing.T) {
	g := newBuildInfoGauge("v1.2.3", "abc1234")

	ch := make(chan prometheus.Metric, 4)
	g.Collect(ch)
	close(ch)

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	require.Len(t, metrics, 1)

	var dm dto.Metric
	require.NoError(t, metrics[0].Write(&dm))
	assert.Equal(t, float64(1), dm.GetGauge().GetValue())

	labels := make(map[string]string)
	for _, l := range dm.GetLabel() {
		labels[l.GetName()] = l.GetValue()
	}
	assert.Equal(t, "v1.2.3", labels["version"])
	assert.Equal(t, "abc1234", labels["commit"])
}
