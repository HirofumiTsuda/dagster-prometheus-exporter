package main

import (
	"context"
	"fmt"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/server"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}

	log.Println("Application completely stopped.")
}

// run holds everything main does, so its deferred cleanup always runs:
// main's log.Fatal (os.Exit) only happens after run has returned.
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.RunServer(ctx, cfg); err != nil {
		return fmt.Errorf("server failed: %w", err)
	}

	return nil
}
