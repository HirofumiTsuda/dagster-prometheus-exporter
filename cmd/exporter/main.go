package main

import (
	"context"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/config"
	"github.com/HirofumiTsuda/dagster-prometheus-exporter/internal/server"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.Load()

	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// stop() is called explicitly rather than deferred: log.Fatalf below
	// would skip a deferred call.
	err = server.RunServer(ctx, cfg)
	stop()
	if err != nil {
		log.Fatalf("Server failed: %v", err)
	}

	log.Println("Application completely stopped.")
}
