// Package version holds the exporter's own version/commit, so they can be
// surfaced as the dagster_exporter_build_info metric (see
// internal/server/build_info.go) — the same idiom as node_exporter's
// node_exporter_build_info: an always-1 gauge carrying the values as
// labels, useful for spotting pods still running an old version after a
// fleet rollout (e.g. via the Helm chart).
package version

// Version and Commit default to these placeholders for a plain `go build`/
// `go test`/`go install` without the -ldflags below — that's expected for
// local development. The published container image sets them at build time
// via:
//
//	go build -ldflags "-X .../internal/version.Version=$VERSION -X .../internal/version.Commit=$COMMIT"
//
// (see docker/exporter.Dockerfile and .github/workflows/docker-publish.yml).
var (
	Version = "dev"
	Commit  = "unknown"
)
