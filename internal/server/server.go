package server

import (
	"context"
	"fmt"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunServer(ctx context.Context, config *config.Config) {
	portSuffix := fmt.Sprintf(":%d", config.Port)
	c := collector.NewDagsterCollector(ctx, config.DagsterGraphQLEndpoint, config.LookbackWindow, config.CacheTTL, config.RunsPageSize, config.RunsUpdatedAfterSafetyMargin)
	prometheus.MustRegister(c)
	registerBuildInfo()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", healthzHandler)
	mux.Handle("/readyz", newReadyzHandler(config.DagsterGraphQLEndpoint, config.DagsterScrapingTimeout))

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
