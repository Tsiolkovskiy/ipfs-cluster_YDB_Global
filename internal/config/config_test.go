package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	// Test loading default configuration
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load default config: %v", err)
	}

	// Verify default values
	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("Expected server host '0.0.0.0', got '%s'", cfg.Server.Host)
	}

	if cfg.Server.Port != 8080 {
		t.Errorf("Expected server port 8080, got %d", cfg.Server.Port)
	}

	if cfg.Server.GRPCPort != 9090 {
		t.Errorf("Expected gRPC port 9090, got %d", cfg.Server.GRPCPort)
	}

	if cfg.Database.Endpoint != "grpc://localhost:2136" {
		t.Errorf("Expected database endpoint 'grpc://localhost:2136', got '%s'", cfg.Database.Endpoint)
	}

	if cfg.EventBus.URL != "nats://localhost:4222" {
		t.Errorf("Expected event bus URL 'nats://localhost:4222', got '%s'", cfg.EventBus.URL)
	}

	if !cfg.Telemetry.Enabled {
		t.Error("Expected telemetry to be enabled by default")
	}

	if cfg.Telemetry.PrometheusPort != 9091 {
		t.Errorf("Expected Prometheus port 9091, got %d", cfg.Telemetry.PrometheusPort)
	}
}

func TestLoadWithEnvironmentVariables(t *testing.T) {
	// Set environment variables
	os.Setenv("GDC_SERVER_PORT", "9999")
	os.Setenv("GDC_DATABASE_ENDPOINT", "grpc://test:2136")
	os.Setenv("GDC_TELEMETRY_ENABLED", "false")
	os.Setenv("GDC_LOGGING_LEVEL", "debug")
	os.Setenv("GDC_LOGGING_FORMAT", "console")
	defer func() {
		os.Unsetenv("GDC_SERVER_PORT")
		os.Unsetenv("GDC_DATABASE_ENDPOINT")
		os.Unsetenv("GDC_TELEMETRY_ENABLED")
		os.Unsetenv("GDC_LOGGING_LEVEL")
		os.Unsetenv("GDC_LOGGING_FORMAT")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config with env vars: %v", err)
	}

	// Verify environment variables override defaults
	if cfg.Server.Port != 9999 {
		t.Errorf("Expected server port 9999 from env var, got %d", cfg.Server.Port)
	}

	if cfg.Database.Endpoint != "grpc://test:2136" {
		t.Errorf("Expected database endpoint 'grpc://test:2136' from env var, got '%s'", cfg.Database.Endpoint)
	}

	if cfg.Telemetry.Enabled {
		t.Error("Expected telemetry to be disabled from env var")
	}

	if cfg.Logging.Level != "debug" {
		t.Errorf("Expected logging level 'debug' from env var, got '%s'", cfg.Logging.Level)
	}

	if cfg.Logging.Format != "console" {
		t.Errorf("Expected logging format 'console' from env var, got '%s'", cfg.Logging.Format)
	}
}

func TestConfigTimeouts(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	expectedTimeout := 30 * time.Second
	if cfg.Server.ReadTimeout != expectedTimeout {
		t.Errorf("Expected read timeout %v, got %v", expectedTimeout, cfg.Server.ReadTimeout)
	}

	if cfg.Server.WriteTimeout != expectedTimeout {
		t.Errorf("Expected write timeout %v, got %v", expectedTimeout, cfg.Server.WriteTimeout)
	}
}

func TestTelemetryConfig(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify telemetry defaults
	if cfg.Telemetry.ServiceName != "global-data-controller" {
		t.Errorf("Expected service name 'global-data-controller', got '%s'", cfg.Telemetry.ServiceName)
	}

	if cfg.Telemetry.ServiceVersion != "1.0.0" {
		t.Errorf("Expected service version '1.0.0', got '%s'", cfg.Telemetry.ServiceVersion)
	}

	if cfg.Telemetry.SampleRate != 1.0 {
		t.Errorf("Expected sample rate 1.0, got %f", cfg.Telemetry.SampleRate)
	}
}

func TestLoggingConfig(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify logging defaults
	if cfg.Logging.Level != "info" {
		t.Errorf("Expected logging level 'info', got '%s'", cfg.Logging.Level)
	}

	if cfg.Logging.Format != "json" {
		t.Errorf("Expected logging format 'json', got '%s'", cfg.Logging.Format)
	}

	if cfg.Logging.OutputPath != "stdout" {
		t.Errorf("Expected output path 'stdout', got '%s'", cfg.Logging.OutputPath)
	}

	if cfg.Logging.ErrorPath != "stderr" {
		t.Errorf("Expected error path 'stderr', got '%s'", cfg.Logging.ErrorPath)
	}
}

func TestConfigWithTelemetryEnvironmentVariables(t *testing.T) {
	// Set telemetry environment variables
	os.Setenv("GDC_TELEMETRY_SERVICE_NAME", "test-gdc")
	os.Setenv("GDC_TELEMETRY_SERVICE_VERSION", "2.0.0")
	os.Setenv("GDC_TELEMETRY_SAMPLE_RATE", "0.5")
	os.Setenv("GDC_TELEMETRY_JAEGER_ENDPOINT", "http://localhost:14268/api/traces")
	defer func() {
		os.Unsetenv("GDC_TELEMETRY_SERVICE_NAME")
		os.Unsetenv("GDC_TELEMETRY_SERVICE_VERSION")
		os.Unsetenv("GDC_TELEMETRY_SAMPLE_RATE")
		os.Unsetenv("GDC_TELEMETRY_JAEGER_ENDPOINT")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Failed to load config with telemetry env vars: %v", err)
	}

	// Verify telemetry environment variables
	if cfg.Telemetry.ServiceName != "test-gdc" {
		t.Errorf("Expected service name 'test-gdc' from env var, got '%s'", cfg.Telemetry.ServiceName)
	}

	if cfg.Telemetry.ServiceVersion != "2.0.0" {
		t.Errorf("Expected service version '2.0.0' from env var, got '%s'", cfg.Telemetry.ServiceVersion)
	}

	if cfg.Telemetry.SampleRate != 0.5 {
		t.Errorf("Expected sample rate 0.5 from env var, got %f", cfg.Telemetry.SampleRate)
	}

	expectedEndpoint := "http://localhost:14268/api/traces"
	if cfg.Telemetry.JaegerEndpoint != expectedEndpoint {
		t.Errorf("Expected Jaeger endpoint '%s' from env var, got '%s'", expectedEndpoint, cfg.Telemetry.JaegerEndpoint)
	}
}