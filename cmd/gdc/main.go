package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/global-data-controller/gdc/internal/bootstrap"
	"github.com/global-data-controller/gdc/internal/logging"
	"github.com/global-data-controller/gdc/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

func main() {
	// Parse command line flags
	var configFile string
	flag.StringVar(&configFile, "config", "", "Path to configuration file")
	flag.Parse()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize bootstrap
	bs := bootstrap.New()
	if err := bs.Initialize(ctx, configFile); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize: %v\n", err)
		os.Exit(1)
	}

	logger := bs.GetLogger()
	logger.Info(ctx, "Global Data Controller starting",
		zap.String("version", "1.0.0"),
		zap.String("config_file", configFile))

	// Start components
	if err := bs.Start(ctx); err != nil {
		logger.Fatal(ctx, "Failed to start components", zap.Error(err))
	}

	// Demonstrate telemetry usage
	demonstrateTelemetry(ctx, logger)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info(ctx, "Global Data Controller is running. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	<-sigChan
	logger.Info(ctx, "Shutdown signal received, stopping gracefully...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop components
	if err := bs.Stop(shutdownCtx); err != nil {
		logger.Error(ctx, "Error during shutdown", zap.Error(err))
		os.Exit(1)
	}

	logger.Info(ctx, "Global Data Controller stopped successfully")
}

// demonstrateTelemetry shows how to use the telemetry system
func demonstrateTelemetry(ctx context.Context, logger logging.Logger) {
	// Start a span for demonstration
	spanCtx, span := telemetry.StartSpan(ctx, "demo_operation",
		// Add span attributes
	)
	defer span.End()

	logger.Info(spanCtx, "Demonstrating telemetry features")

	// Record some metrics
	if err := telemetry.IncrementCounter(spanCtx, "demo_counter_total",
		attribute.String("component", "main"),
		attribute.String("operation", "demo")); err != nil {
		logger.Error(spanCtx, "Failed to increment counter", zap.Error(err))
	}

	if err := telemetry.SetGauge(spanCtx, "demo_gauge",
		42.0,
		attribute.String("component", "main")); err != nil {
		logger.Error(spanCtx, "Failed to set gauge", zap.Error(err))
	}

	// Simulate some work and record duration
	start := time.Now()
	time.Sleep(100 * time.Millisecond)
	
	if err := telemetry.RecordDuration(spanCtx, "demo_work", start,
		attribute.String("component", "main"),
		attribute.String("work_type", "simulation")); err != nil {
		logger.Error(spanCtx, "Failed to record duration", zap.Error(err))
	}

	if err := telemetry.RecordHistogram(spanCtx, "demo_histogram",
		1.5,
		attribute.String("component", "main")); err != nil {
		logger.Error(spanCtx, "Failed to record histogram", zap.Error(err))
	}

	logger.Info(spanCtx, "Telemetry demonstration completed",
		zap.Duration("elapsed", time.Since(start)))
}