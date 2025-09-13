package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/global-data-controller/gdc/internal/config"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server represents the main application server
type Server struct {
	config     *config.Config
	logger     *zap.Logger
	httpServer *http.Server
	grpcServer *grpc.Server
	wg         sync.WaitGroup
	shutdown   chan struct{}
}

// New creates a new server instance
func New(cfg *config.Config, logger *zap.Logger) (*Server, error) {
	return &Server{
		config:   cfg,
		logger:   logger,
		shutdown: make(chan struct{}),
	}, nil
}

// Start starts the server
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info("Starting Global Data Controller server",
		zap.String("version", s.config.Telemetry.ServiceVersion),
		zap.Int("http_port", s.config.Server.Port),
		zap.Int("grpc_port", s.config.Server.GRPCPort),
	)

	// Start HTTP server
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	// Start gRPC server
	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %w", err)
	}

	// Start metrics server if telemetry is enabled
	if s.config.Telemetry.Enabled {
		if err := s.startMetricsServer(); err != nil {
			return fmt.Errorf("failed to start metrics server: %w", err)
		}
	}

	s.logger.Info("Server started successfully")
	return nil
}

// Stop gracefully stops the server
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("Stopping server...")
	
	close(s.shutdown)

	// Stop HTTP server
	if s.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Failed to shutdown HTTP server", zap.Error(err))
		}
	}

	// Stop gRPC server
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// Wait for all goroutines to finish
	s.wg.Wait()

	s.logger.Info("Server stopped")
	return nil
}

// startHTTPServer starts the HTTP server
func (s *Server) startHTTPServer() error {
	mux := http.NewServeMux()
	
	// Health check endpoint
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readinessHandler)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port),
		Handler:      mux,
		ReadTimeout:  s.config.Server.ReadTimeout,
		WriteTimeout: s.config.Server.WriteTimeout,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// startGRPCServer starts the gRPC server
func (s *Server) startGRPCServer() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port: %w", err)
	}

	s.grpcServer = grpc.NewServer()
	
	// TODO: Register gRPC services here

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

// startMetricsServer starts the Prometheus metrics server
func (s *Server) startMetricsServer() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.Telemetry.PrometheusPort),
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Metrics server error", zap.Error(err))
		}
	}()

	return nil
}

// healthHandler handles health check requests
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","service":"global-data-controller"}`))
}

// readinessHandler handles readiness check requests
func (s *Server) readinessHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: Add actual readiness checks (database, event bus, etc.)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ready","service":"global-data-controller"}`))
}