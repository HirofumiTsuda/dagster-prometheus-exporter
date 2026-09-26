package server

import (
	"context"
	"fmt"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/collector"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Every endpoint here is a bodiless GET answered from memory, except /readyz,
// which makes one GraphQL call bounded by DAGSTER_SCRAPING_TIMEOUT_SECONDS.
// So the read side can be tight, and the write side only needs to outlast
// that one call.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	// Added on top of the /readyz GraphQL timeout, so a readiness probe that
	// hits it still gets its 503 written instead of a dropped connection.
	writeTimeoutMargin = 10 * time.Second
	// Longer than any sensible Prometheus scrape_interval (default 1m), so a
	// scraper's keep-alive connection is reused rather than torn down between
	// scrapes.
	idleTimeout = 2 * time.Minute

	shutdownTimeout = 5 * time.Second
)

// RunServer serves /metrics, /healthz, and /readyz until ctx is cancelled
// (a clean shutdown, returns nil) or the server can't keep running, e.g.
// because the port is already taken (returns the error).
func RunServer(ctx context.Context, cfg *config.Config) error {
	// Listening before anything else makes the most common failure -- the
	// port is already taken -- fail fast, before a first scrape is spent
	// against Dagster for nothing.
	addr := fmt.Sprintf(":%d", cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("HTTP server failed to listen on %s: %w", addr, err)
	}

	// Stops the background scraper (and the collector's cache) on the error
	// path too, not only when the caller cancels ctx.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := collector.NewDagsterCollector(ctx, cfg.DagsterGraphQLEndpoint, cfg.LookbackWindow, cfg.CacheTTL, cfg.RunsPageSize, cfg.RunsUpdatedAfterSafetyMargin)
	prometheus.MustRegister(c)
	registerBuildInfo()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", healthzHandler)
	mux.Handle("/readyz", newReadyzHandler(cfg.DagsterGraphQLEndpoint, cfg.DagsterScrapingTimeout))

	srv := newHTTPServer(mux, cfg.DagsterScrapingTimeout)

	go startScrape(ctx, c, cfg.DagsterScrapingInterval, cfg.DagsterScrapingTimeout)

	return serve(ctx, srv, ln)
}

func newHTTPServer(handler http.Handler, readyzTimeout time.Duration) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      readyzTimeout + writeTimeoutMargin,
		IdleTimeout:       idleTimeout,
	}
}

// serve runs srv on ln until ctx is cancelled, then shuts it down gracefully.
// Unlike calling log.Fatalf from the serving goroutine, a failure to serve
// comes back as an error, so the caller's deferred cleanup still runs.
func serve(ctx context.Context, srv *http.Server, ln net.Listener) error {
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("Starting Prometheus metrics server on %s", ln.Addr())
		serveErr <- srv.Serve(ln)
	}()

	select {
	case err := <-serveErr:
		// Serve only returns http.ErrServerClosed after Shutdown, which
		// hasn't been called yet, so anything here is a real failure.
		return fmt.Errorf("HTTP server failed: %w", err)
	case <-ctx.Done():
	}

	log.Println("Shutting down HTTP server gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server Shutdown error (forced close): %v", err)
	} else {
		log.Println("HTTP server stopped gracefully.")
	}

	return nil
}
