package server

import (
	"context"
	"dagster-prometheus-exporter/internal/collector"
	"encoding/json"
	"log"
	"net/http"
)

func newReadyzHandler(ctx context.Context, dagsterGraphQLEndpoint string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := collector.GetVersionRequest()

		resp, err := collector.GetVersion(ctx, req, dagsterGraphQLEndpoint)
		if err != nil {
			log.Printf("failed to connect to dagster: %v", err)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)

			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "NOT_READY",
				"error":  err.Error(),
			})
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		respBody := struct {
			Status  string `json:"status"`
			Version string `json:"version"`
		}{
			Status:  "OK",
			Version: resp.Data.Version,
		}

		if err := json.NewEncoder(w).Encode(respBody); err != nil {
			log.Printf("failed to encode response: %v", err)
			return
		}
	})
}
