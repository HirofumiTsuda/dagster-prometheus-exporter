package collector

import "testing"

func TestCountConcurrencyKeyBacklog(t *testing.T) {
	tests := []struct {
		name   string
		run    Run
		wantAt map[string]int
	}{
		{
			name: "queued run with a concurrency key tag is counted",
			run: Run{
				Status: "QUEUED",
				Tags:   []RunTag{{Key: "dagster/concurrency_key", Value: "heavy_limit"}},
			},
			wantAt: map[string]int{"heavy_limit": 1},
		},
		{
			name: "non-queued run is ignored even if it has the tag",
			run: Run{
				Status: "STARTED",
				Tags:   []RunTag{{Key: "dagster/concurrency_key", Value: "heavy_limit"}},
			},
			wantAt: map[string]int{},
		},
		{
			name: "queued run with an unrelated tag contributes nothing",
			run: Run{
				Status: "QUEUED",
				Tags:   []RunTag{{Key: "some_other_tag", Value: "x"}},
			},
			wantAt: map[string]int{},
		},
		{
			name: "queued run with no tags contributes nothing",
			run: Run{
				Status: "QUEUED",
			},
			wantAt: map[string]int{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backlog := make(map[string]int)
			countConcurrencyKeyBacklog(tc.run, backlog)

			if len(backlog) != len(tc.wantAt) {
				t.Fatalf("backlog = %v, want %v", backlog, tc.wantAt)
			}
			for key, want := range tc.wantAt {
				if backlog[key] != want {
					t.Errorf("backlog[%q] = %d, want %d", key, backlog[key], want)
				}
			}
		})
	}
}
