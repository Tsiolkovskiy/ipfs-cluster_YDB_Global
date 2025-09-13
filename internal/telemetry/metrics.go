package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// TelemetryConfig holds telemetry configuration
type TelemetryConfig struct {
	Enabled           bool   `mapstructure:"enabled"`
	ServiceName       string `mapstructure:"service_name"`
	ServiceVersion    string `mapstructure:"service_version"`
	PrometheusPort    int    `mapstructure:"prometheus_port"`
	JaegerEndpoint    string `mapstructure:"jaeger_endpoint"`
	SampleRate        float64 `mapstructure:"sample_rate"`
}

// Telemetry manages OpenTelemetry instrumentation
type Telemetry struct {
	config         TelemetryConfig
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	tracer         trace.Tracer
	meter          metric.Meter
	server         *http.Server
	mu             sync.RWMutex
	
	// Metrics instruments
	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
}

// NewTelemetry creates a new telemetry instance
func NewTelemetry(config TelemetryConfig) (*Telemetry, error) {
	if !config.Enabled {
		return &Telemetry{config: config}, nil
	}

	t := &Telemetry{
		config:     config,
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
	}

	if err := t.initTracing(); err != nil {
		return nil, fmt.Errorf("failed to initialize tracing: %w", err)
	}

	if err := t.initMetrics(); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	return t, nil
}

// initTracing initializes OpenTelemetry tracing
func (t *Telemetry) initTracing() error {
	// Create resource
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(t.config.ServiceName),
			semconv.ServiceVersion(t.config.ServiceVersion),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Create Jaeger exporter if endpoint is configured
	var exporter sdktrace.SpanExporter
	if t.config.JaegerEndpoint != "" {
		exporter, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(t.config.JaegerEndpoint)))
		if err != nil {
			return fmt.Errorf("failed to create Jaeger exporter: %w", err)
		}
	}

	// Create tracer provider
	var opts []sdktrace.TracerProviderOption
	opts = append(opts, sdktrace.WithResource(res))
	
	if exporter != nil {
		sampleRate := t.config.SampleRate
		if sampleRate == 0 {
			sampleRate = 1.0 // Default to 100% sampling
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
		opts = append(opts, sdktrace.WithSampler(sdktrace.TraceIDRatioBased(sampleRate)))
	}

	t.tracerProvider = sdktrace.NewTracerProvider(opts...)

	// Set global tracer provider
	otel.SetTracerProvider(t.tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Create tracer
	t.tracer = otel.Tracer(t.config.ServiceName)

	return nil
}

// initMetrics initializes OpenTelemetry metrics
func (t *Telemetry) initMetrics() error {
	// Create Prometheus exporter
	exporter, err := otelprom.New()
	if err != nil {
		return fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}

	// Create resource
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(t.config.ServiceName),
			semconv.ServiceVersion(t.config.ServiceVersion),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// Create meter provider
	t.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)

	// Set global meter provider
	otel.SetMeterProvider(t.meterProvider)

	// Create meter
	t.meter = otel.Meter(t.config.ServiceName)

	return nil
}

// Start starts the telemetry services
func (t *Telemetry) Start(ctx context.Context) error {
	if !t.config.Enabled {
		return nil
	}

	// Start Prometheus metrics server
	if t.config.PrometheusPort > 0 {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		
		t.server = &http.Server{
			Addr:    fmt.Sprintf(":%d", t.config.PrometheusPort),
			Handler: mux,
		}

		go func() {
			if err := t.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				// Log error but don't fail startup
				fmt.Printf("Prometheus server error: %v\n", err)
			}
		}()
	}

	return nil
}

// Stop stops the telemetry services
func (t *Telemetry) Stop(ctx context.Context) error {
	if !t.config.Enabled {
		return nil
	}

	// Stop HTTP server
	if t.server != nil {
		if err := t.server.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown Prometheus server: %w", err)
		}
	}

	// Shutdown tracer provider
	if t.tracerProvider != nil {
		if err := t.tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown tracer provider: %w", err)
		}
	}

	// Shutdown meter provider
	if t.meterProvider != nil {
		if err := t.meterProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown meter provider: %w", err)
		}
	}

	return nil
}

// GetTracer returns the OpenTelemetry tracer
func (t *Telemetry) GetTracer() trace.Tracer {
	return t.tracer
}

// GetMeter returns the OpenTelemetry meter
func (t *Telemetry) GetMeter() metric.Meter {
	return t.meter
}

// StartSpan starts a new span
func (t *Telemetry) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !t.config.Enabled || t.tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return t.tracer.Start(ctx, name, opts...)
}

// IncrementCounter increments a counter metric
func (t *Telemetry) IncrementCounter(ctx context.Context, name string, attrs ...attribute.KeyValue) error {
	if !t.config.Enabled {
		return nil
	}

	t.mu.Lock()
	counter, exists := t.counters[name]
	if !exists {
		var err error
		counter, err = t.meter.Int64Counter(name)
		if err != nil {
			t.mu.Unlock()
			return fmt.Errorf("failed to create counter %s: %w", name, err)
		}
		t.counters[name] = counter
	}
	t.mu.Unlock()

	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
	return nil
}

// SetGauge sets a gauge metric value
// Note: OpenTelemetry gauges are observable, so we use an UpDownCounter for direct recording
func (t *Telemetry) SetGauge(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) error {
	if !t.config.Enabled {
		return nil
	}

	// For simplicity, we'll use a histogram to record gauge-like values
	// In a production system, you'd want to use proper observable gauges with callbacks
	return t.RecordHistogram(ctx, name+"_gauge", value, attrs...)
}

// RecordHistogram records a value in a histogram
func (t *Telemetry) RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) error {
	if !t.config.Enabled {
		return nil
	}

	t.mu.Lock()
	histogram, exists := t.histograms[name]
	if !exists {
		var err error
		histogram, err = t.meter.Float64Histogram(name)
		if err != nil {
			t.mu.Unlock()
			return fmt.Errorf("failed to create histogram %s: %w", name, err)
		}
		t.histograms[name] = histogram
	}
	t.mu.Unlock()

	histogram.Record(ctx, value, metric.WithAttributes(attrs...))
	return nil
}

// RecordDuration records the duration of an operation
func (t *Telemetry) RecordDuration(ctx context.Context, name string, start time.Time, attrs ...attribute.KeyValue) error {
	duration := time.Since(start).Seconds()
	return t.RecordHistogram(ctx, name+"_duration_seconds", duration, attrs...)
}

// Global telemetry instance
var globalTelemetry *Telemetry

// InitGlobalTelemetry initializes the global telemetry instance
func InitGlobalTelemetry(config TelemetryConfig) error {
	telemetry, err := NewTelemetry(config)
	if err != nil {
		return err
	}
	globalTelemetry = telemetry
	return nil
}

// GetGlobalTelemetry returns the global telemetry instance
func GetGlobalTelemetry() *Telemetry {
	return globalTelemetry
}

// Convenience functions for global telemetry
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if globalTelemetry != nil {
		return globalTelemetry.StartSpan(ctx, name, opts...)
	}
	return ctx, trace.SpanFromContext(ctx)
}

func IncrementCounter(ctx context.Context, name string, attrs ...attribute.KeyValue) error {
	if globalTelemetry != nil {
		return globalTelemetry.IncrementCounter(ctx, name, attrs...)
	}
	return nil
}

func SetGauge(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) error {
	if globalTelemetry != nil {
		return globalTelemetry.SetGauge(ctx, name, value, attrs...)
	}
	return nil
}

func RecordHistogram(ctx context.Context, name string, value float64, attrs ...attribute.KeyValue) error {
	if globalTelemetry != nil {
		return globalTelemetry.RecordHistogram(ctx, name, value, attrs...)
	}
	return nil
}

func RecordDuration(ctx context.Context, name string, start time.Time, attrs ...attribute.KeyValue) error {
	if globalTelemetry != nil {
		return globalTelemetry.RecordDuration(ctx, name, start, attrs...)
	}
	return nil
}