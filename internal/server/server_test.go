package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/global-data-controller/gdc/internal/config"
	"go.uber.org/zap"
)

func TestNew(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:         "localhost",
			Port:         8080,
			GRPCPort:     9090,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Telemetry: config.TelemetryConfig{
			Enabled:        true,
			PrometheusPort: 9091,
			ServiceName:    "test-gdc",
			ServiceVersion: "1.0.0",
		},
	}

	logger := zap.NewNop()

	server, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server == nil {
		t.Fatal("Expected server instance, got nil")
	}

	if server.config != cfg {
		t.Error("Server config not set correctly")
	}

	if server.logger != logger {
		t.Error("Server logger not set correctly")
	}
}

func TestHealthHandler(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:         "localhost",
			Port:         8080,
			GRPCPort:     9090,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Telemetry: config.TelemetryConfig{
			Enabled:        false, // Disable for test
			ServiceName:    "test-gdc",
			ServiceVersion: "1.0.0",
		},
	}

	logger := zap.NewNop()
	server, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Test health handler directly
	req, err := http.NewRequest("GET", "/health", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	rr := &testResponseWriter{
		header: make(http.Header),
		body:   make([]byte, 0),
	}

	server.healthHandler(rr, req)

	if rr.statusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.statusCode)
	}

	expectedContentType := "application/json"
	if contentType := rr.header.Get("Content-Type"); contentType != expectedContentType {
		t.Errorf("Expected content type %s, got %s", expectedContentType, contentType)
	}

	expectedBody := `{"status":"healthy","service":"global-data-controller"}`
	if string(rr.body) != expectedBody {
		t.Errorf("Expected body %s, got %s", expectedBody, string(rr.body))
	}
}

func TestReadinessHandler(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host:         "localhost",
			Port:         8080,
			GRPCPort:     9090,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Telemetry: config.TelemetryConfig{
			Enabled:        false, // Disable for test
			ServiceName:    "test-gdc",
			ServiceVersion: "1.0.0",
		},
	}

	logger := zap.NewNop()
	server, err := New(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Test readiness handler directly
	req, err := http.NewRequest("GET", "/ready", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}

	rr := &testResponseWriter{
		header: make(http.Header),
		body:   make([]byte, 0),
	}

	server.readinessHandler(rr, req)

	if rr.statusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.statusCode)
	}

	expectedContentType := "application/json"
	if contentType := rr.header.Get("Content-Type"); contentType != expectedContentType {
		t.Errorf("Expected content type %s, got %s", expectedContentType, contentType)
	}

	expectedBody := `{"status":"ready","service":"global-data-controller"}`
	if string(rr.body) != expectedBody {
		t.Errorf("Expected body %s, got %s", expectedBody, string(rr.body))
	}
}

// testResponseWriter is a simple implementation of http.ResponseWriter for testing
type testResponseWriter struct {
	header     http.Header
	body       []byte
	statusCode int
}

func (w *testResponseWriter) Header() http.Header {
	return w.header
}

func (w *testResponseWriter) Write(data []byte) (int, error) {
	w.body = append(w.body, data...)
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	return len(data), nil
}

func (w *testResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}