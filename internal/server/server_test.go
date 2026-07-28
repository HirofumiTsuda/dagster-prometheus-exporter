package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetDagsterGraphQLEndpoint(t *testing.T) {
	tests := []struct {
		url      string
		expected string
	}{
		{
			url:      "http://dagster-prometheus-exporter/test",
			expected: "http://dagster-prometheus-exporter/test/graphql",
		},
		{
			url:      "http://dagster-prometheus-exporter/trimcase/",
			expected: "http://dagster-prometheus-exporter/trimcase/graphql",
		},
	}
	for _, test := range tests {
		actual := getDagsterGraphQLEndpoint(test.url)
		assert.Equal(t, test.expected, actual)
	}
}
