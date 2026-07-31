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
	config, err := config.Load()

	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server.RunServer(ctx, config)

	log.Println("Application completely stopped.")
}
