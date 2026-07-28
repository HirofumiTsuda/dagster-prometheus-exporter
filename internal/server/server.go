package server

import (
	"context"
	"dagster-prometheus-exporter/internal/collector"
	"dagster-prometheus-exporter/internal/config"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func getDagsterGraphQLEndpoint(dagsterURL string) string {
	cleanDagsterURL := strings.TrimRight(dagsterURL, "/")
	return fmt.Sprintf("%s/graphql", cleanDagsterURL)
}

func RunServer(ctx context.Context, config *config.Config) {
	dagsterGraphQLEndpoint := getDagsterGraphQLEndpoint(config.DagsterURL)
	portSuffix := fmt.Sprintf(":%d", config.Port)
	c := collector.NewDagsterCollector(ctx, dagsterGraphQLEndpoint, config.LookbackWindow, config.CacheTTL)
	prometheus.MustRegister(c)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", healthzHandler)
	mux.Handle("/readyz", newReadyzHandler(ctx, dagsterGraphQLEndpoint))

	srv := &http.Server{
		Addr:    portSuffix,
		Handler: mux,
	}

	go startScrape(ctx, c, config.DagsterScrapingInterval, config.DagsterScrapingTimeout)

	go func() {
		log.Printf("Starting Prometheus metrics server on port %d", config.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server ListenAndServe failed: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down HTTP server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server Shutdown error (forced close): %v", err)
	} else {
		log.Println("HTTP server stopped gracefully.")
	}

}
