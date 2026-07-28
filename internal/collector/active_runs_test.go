package collector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetActiveRunsRequest(t *testing.T) {
	reqBody := getActiveRunsRequest()
	if reqBody.Query == "" {
		t.Fatal("reqBody.Query must not be empty")
	}
	assert.Equal(t, activeStatuses, reqBody.Variables["statuses"])
}
