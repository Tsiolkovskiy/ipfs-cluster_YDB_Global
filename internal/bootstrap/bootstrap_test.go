package bootstrap

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestBootstrapLifecycle(t *testing.T) {
	bootstrap := New()
	ctx := context.Background()

	// Test initialization
	err := bootstrap.Initialize(ctx, "")
	if err != nil {
		t.Fatalf("Failed to initialize bootstrap: %v", err)
	}

	// Verify components are initialized
	if bootstrap.Config == nil {
		t.Error("Expected config to be initialized")
	}

	if bootstrap.Logger == nil {
		t.Error("Expected logger to be initialized")
	}

	if bootstrap.Telemetry == nil {
		t.Error("Expected telemetry to be initialized")
	}

	// Test start
	err = bootstrap.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start bootstrap: %v", err)
	}

	// Give components time to start
	time.Sleep(100 * time.Millisecond)

	// Test stop
	err = bootstrap.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop bootstrap: %v", err)
	}
}

func TestBootstrapWithConfigFile(t *testing.T) {
	// Create a temporary config file
	configContent := `
server:
  port: 8888
logging:
  level: debug
  format: console
telemetry:
  enabled: false
`
	tmpFile, err := os.CreateTemp("", "test-config-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp config file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}
	tmpFile.Close()

	bootstrap := New()
	ctx := context.Background()

	// Test initialization with config file
	err = bootstrap.Initialize(ctx, tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to initialize bootstrap with config file: %v", err)
	}

	// Verify config was loaded correctly
	if bootstrap.Config.Server.Port != 8888 {
		t.Errorf("Expected server port 8888, got %d", bootstrap.Config.Server.Port)
	}

	if bootstrap.Config.Logging.Level != "debug" {
		t.Errorf("Expected log level debug, got %s", bootstrap.Config.Logging.Level)
	}

	if bootstrap.Config.Telemetry.Enabled {
		t.Error("Expected telemetry to be disabled")
	}
}

func TestBootstrapWithEnvironmentVariables(t *testing.T) {
	// Set environment variables
	os.Setenv("GDC_SERVER_PORT", "7777")
	os.Setenv("GDC_LOGGING_LEVEL", "error")
	os.Setenv("GDC_TELEMETRY_ENABLED", "false")
	defer func() {
		os.Unsetenv("GDC_SERVER_PORT")
		os.Unsetenv("GDC_LOGGING_LEVEL")
		os.Unsetenv("GDC_TELEMETRY_ENABLED")
	}()

	bootstrap := New()
	ctx := context.Background()

	err := bootstrap.Initialize(ctx, "")
	if err != nil {
		t.Fatalf("Failed to initialize bootstrap: %v", err)
	}

	// Verify environment variables were applied
	if bootstrap.Config.Server.Port != 7777 {
		t.Errorf("Expected server port 7777 from env var, got %d", bootstrap.Config.Server.Port)
	}

	if bootstrap.Config.Logging.Level != "error" {
		t.Errorf("Expected log level error from env var, got %s", bootstrap.Config.Logging.Level)
	}

	if bootstrap.Config.Telemetry.Enabled {
		t.Error("Expected telemetry to be disabled from env var")
	}
}

func TestBootstrapGetters(t *testing.T) {
	bootstrap := New()
	ctx := context.Background()

	err := bootstrap.Initialize(ctx, "")
	if err != nil {
		t.Fatalf("Failed to initialize bootstrap: %v", err)
	}

	// Test getters
	config := bootstrap.GetConfig()
	if config == nil {
		t.Error("Expected GetConfig to return config")
	}

	logger := bootstrap.GetLogger()
	if logger == nil {
		t.Error("Expected GetLogger to return logger")
	}

	telemetry := bootstrap.GetTelemetry()
	if telemetry == nil {
		t.Error("Expected GetTelemetry to return telemetry")
	}
}

func TestBootstrapStopWithoutStart(t *testing.T) {
	bootstrap := New()
	ctx := context.Background()

	err := bootstrap.Initialize(ctx, "")
	if err != nil {
		t.Fatalf("Failed to initialize bootstrap: %v", err)
	}

	// Test stop without start (should not fail)
	err = bootstrap.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should not fail even without start: %v", err)
	}
}

func TestBootstrapStopWithoutInitialize(t *testing.T) {
	bootstrap := New()
	ctx := context.Background()

	// Test stop without initialize (should not fail)
	err := bootstrap.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should not fail even without initialize: %v", err)
	}
}