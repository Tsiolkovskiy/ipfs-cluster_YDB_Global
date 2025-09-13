package logging

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name   string
		config LoggingConfig
		valid  bool
	}{
		{
			name: "valid json config",
			config: LoggingConfig{
				Level:  "info",
				Format: "json",
			},
			valid: true,
		},
		{
			name: "valid console config",
			config: LoggingConfig{
				Level:  "debug",
				Format: "console",
			},
			valid: true,
		},
		{
			name: "invalid level",
			config: LoggingConfig{
				Level:  "invalid",
				Format: "json",
			},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewLogger(tt.config)
			if tt.valid {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if logger == nil {
					t.Error("Expected logger to be created")
				}
			} else {
				if err == nil {
					t.Error("Expected error for invalid config")
				}
			}
		})
	}
}

func TestLoggerWithTrace(t *testing.T) {
	logger, err := NewLogger(LoggingConfig{
		Level:  "debug",
		Format: "json",
	})
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create a tracer and span for testing
	tracer := otel.Tracer("test")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	// Test logging with trace context
	logger.Info(ctx, "test message", zap.String("key", "value"))
	logger.Debug(ctx, "debug message")
	logger.Warn(ctx, "warning message")
	logger.Error(ctx, "error message")

	// Test logger with additional fields
	childLogger := logger.With(zap.String("component", "test"))
	childLogger.Info(ctx, "child logger message")

	// Test logger with context
	contextLogger := logger.WithContext(ctx)
	contextLogger.Info(context.Background(), "context logger message")
}

func TestInitGlobalLogger(t *testing.T) {
	config := LoggingConfig{
		Level:  "info",
		Format: "json",
	}

	err := InitGlobalLogger(config)
	if err != nil {
		t.Fatalf("Failed to initialize global logger: %v", err)
	}

	logger := GetLogger()
	if logger == nil {
		t.Error("Expected global logger to be set")
	}

	// Test global convenience functions
	ctx := context.Background()
	Info(ctx, "global info message")
	Debug(ctx, "global debug message")
	Warn(ctx, "global warn message")
	Error(ctx, "global error message")
}

func TestExtractTraceFields(t *testing.T) {
	// Test with empty context
	fields := extractTraceFields(context.Background())
	if fields != nil {
		t.Error("Expected no fields for empty context")
	}

	// For this test, we'll skip the actual trace field extraction test
	// since it requires a full OpenTelemetry setup which is tested
	// in the telemetry package. The function is covered by integration tests.
	t.Skip("Trace field extraction requires full OpenTelemetry setup - covered by integration tests")
}

func TestGetWriteSyncer(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{"stdout", "stdout"},
		{"stderr", "stderr"},
		{"file", "/tmp/test.log"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer := getWriteSyncer(tt.path)
			if syncer == nil {
				t.Error("Expected WriteSyncer to be created")
			}
		})
	}
}