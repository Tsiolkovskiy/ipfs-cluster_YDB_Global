package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestNewTelemetry(t *testing.T) {
	tests := []struct {
		name   string
		config TelemetryConfig
		valid  bool
	}{
		{
			name: "disabled telemetry",
			config: TelemetryConfig{
				Enabled: false,
			},
			valid: true,
		},
		{
			name: "enabled telemetry with basic config",
			config: TelemetryConfig{
				Enabled:        true,
				ServiceName:    "test-service",
				ServiceVersion: "1.0.0",
				PrometheusPort: 9091,
				SampleRate:     1.0,
			},
			valid: true,
		},
		{
			name: "enabled telemetry with Jaeger",
			config: TelemetryConfig{
				Enabled:        true,
				ServiceName:    "test-service",
				ServiceVersion: "1.0.0",
				PrometheusPort: 9092,
				JaegerEndpoint: "http://localhost:14268/api/traces",
				SampleRate:     0.5,
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			telemetry, err := NewTelemetry(tt.config)
			if tt.valid {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if telemetry == nil {
					t.Error("Expected telemetry to be created")
				}
			} else {
				if err == nil {
					t.Error("Expected error for invalid config")
				}
			}
		})
	}
}

func TestTelemetryLifecycle(t *testing.T) {
	config := TelemetryConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		PrometheusPort: 9093,
		SampleRate:     1.0,
	}

	telemetry, err := NewTelemetry(config)
	if err != nil {
		t.Fatalf("Failed to create telemetry: %v", err)
	}

	ctx := context.Background()

	// Test start
	err = telemetry.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start telemetry: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test stop
	err = telemetry.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop telemetry: %v", err)
	}
}

func TestTelemetrySpans(t *testing.T) {
	config := TelemetryConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		SampleRate:     1.0,
	}

	telemetry, err := NewTelemetry(config)
	if err != nil {
		t.Fatalf("Failed to create telemetry: %v", err)
	}

	ctx := context.Background()

	// Test span creation
	spanCtx, span := telemetry.StartSpan(ctx, "test-operation")
	if span == nil {
		t.Error("Expected span to be created")
	}

	// Test nested span
	_, childSpan := telemetry.StartSpan(spanCtx, "child-operation")
	if childSpan == nil {
		t.Error("Expected child span to be created")
	}

	childSpan.End()
	span.End()
}

func TestTelemetryMetrics(t *testing.T) {
	config := TelemetryConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		SampleRate:     1.0,
	}

	telemetry, err := NewTelemetry(config)
	if err != nil {
		t.Fatalf("Failed to create telemetry: %v", err)
	}

	ctx := context.Background()

	// Test counter
	err = telemetry.IncrementCounter(ctx, "test_counter", 
		attribute.String("component", "test"))
	if err != nil {
		t.Errorf("Failed to increment counter: %v", err)
	}

	// Test gauge
	err = telemetry.SetGauge(ctx, "test_gauge", 42.0,
		attribute.String("component", "test"))
	if err != nil {
		t.Errorf("Failed to set gauge: %v", err)
	}

	// Test histogram
	err = telemetry.RecordHistogram(ctx, "test_histogram", 1.5,
		attribute.String("component", "test"))
	if err != nil {
		t.Errorf("Failed to record histogram: %v", err)
	}

	// Test duration recording
	start := time.Now()
	time.Sleep(10 * time.Millisecond)
	err = telemetry.RecordDuration(ctx, "test_operation", start,
		attribute.String("component", "test"))
	if err != nil {
		t.Errorf("Failed to record duration: %v", err)
	}
}

func TestGlobalTelemetry(t *testing.T) {
	config := TelemetryConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "1.0.0",
		SampleRate:     1.0,
	}

	err := InitGlobalTelemetry(config)
	if err != nil {
		t.Fatalf("Failed to initialize global telemetry: %v", err)
	}

	telemetry := GetGlobalTelemetry()
	if telemetry == nil {
		t.Error("Expected global telemetry to be set")
	}

	ctx := context.Background()

	// Test global convenience functions
	spanCtx, span := StartSpan(ctx, "global-test")
	span.End()

	err = IncrementCounter(spanCtx, "global_counter",
		attribute.String("test", "value"))
	if err != nil {
		t.Errorf("Failed to increment global counter: %v", err)
	}

	err = SetGauge(spanCtx, "global_gauge", 100.0,
		attribute.String("test", "value"))
	if err != nil {
		t.Errorf("Failed to set global gauge: %v", err)
	}

	err = RecordHistogram(spanCtx, "global_histogram", 2.5,
		attribute.String("test", "value"))
	if err != nil {
		t.Errorf("Failed to record global histogram: %v", err)
	}

	start := time.Now()
	time.Sleep(5 * time.Millisecond)
	err = RecordDuration(spanCtx, "global_operation", start,
		attribute.String("test", "value"))
	if err != nil {
		t.Errorf("Failed to record global duration: %v", err)
	}
}

func TestDisabledTelemetry(t *testing.T) {
	config := TelemetryConfig{
		Enabled: false,
	}

	telemetry, err := NewTelemetry(config)
	if err != nil {
		t.Fatalf("Failed to create disabled telemetry: %v", err)
	}

	ctx := context.Background()

	// Test that operations don't fail when disabled
	err = telemetry.Start(ctx)
	if err != nil {
		t.Errorf("Start should not fail for disabled telemetry: %v", err)
	}

	err = telemetry.Stop(ctx)
	if err != nil {
		t.Errorf("Stop should not fail for disabled telemetry: %v", err)
	}

	// Test that metrics operations don't fail
	err = telemetry.IncrementCounter(ctx, "test_counter")
	if err != nil {
		t.Errorf("IncrementCounter should not fail for disabled telemetry: %v", err)
	}

	err = telemetry.SetGauge(ctx, "test_gauge", 1.0)
	if err != nil {
		t.Errorf("SetGauge should not fail for disabled telemetry: %v", err)
	}

	err = telemetry.RecordHistogram(ctx, "test_histogram", 1.0)
	if err != nil {
		t.Errorf("RecordHistogram should not fail for disabled telemetry: %v", err)
	}
}