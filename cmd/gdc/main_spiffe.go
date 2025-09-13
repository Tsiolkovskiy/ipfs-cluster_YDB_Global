package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/global-data-controller/gdc/internal/auth"
	"github.com/global-data-controller/gdc/internal/config"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Example main function showing SPIFFE/SPIRE integration
func mainWithSPIFFE() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	// Load configuration
	cfg := loadConfiguration()
	
	// Validate security configuration
	if err := cfg.Security.Validate(); err != nil {
		logger.Fatal("Invalid security configuration", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize SPIFFE authentication if enabled
	var authService *auth.SPIFFEAuthService
	if cfg.Security.IsSPIFFEEnabled() {
		logger.Info("Initializing SPIFFE authentication", 
			zap.String("socket_path", cfg.Security.SPIFFESocketPath),
			zap.String("trust_domain", cfg.Security.TrustDomain),
			zap.String("service_id", cfg.Security.ServiceID))

		authConfig := &auth.Config{
			SocketPath: cfg.Security.SPIFFESocketPath,
		}

		authService, err = auth.NewSPIFFEAuthService(authConfig, logger)
		if err != nil {
			logger.Fatal("Failed to create SPIFFE auth service", zap.Error(err))
		}

		// Initialize connection to SPIRE agent
		if err := authService.Initialize(ctx); err != nil {
			logger.Fatal("Failed to initialize SPIFFE auth service", zap.Error(err))
		}
		defer authService.Close()

		logger.Info("SPIFFE authentication initialized successfully")
	}

	// Start servers
	if cfg.Security.IsSPIFFEEnabled() {
		startServersWithSPIFFE(ctx, cfg, authService, logger)
	} else {
		startServersWithoutAuth(ctx, cfg, logger)
	}

	// Wait for shutdown signal
	waitForShutdown(logger)
}

// startServersWithSPIFFE starts HTTP and gRPC servers with SPIFFE authentication
func startServersWithSPIFFE(ctx context.Context, cfg *config.Config, authService *auth.SPIFFEAuthService, logger *zap.Logger) {
	// Create server configuration
	serverConfig := &auth.ServerConfig{
		TrustDomain:   cfg.Security.TrustDomain,
		ServiceID:     cfg.Security.ServiceID,
		AuthorizedIDs: cfg.Security.AuthorizedIDs,
	}

	authenticatedServer := auth.NewAuthenticatedServer(authService, serverConfig, logger)

	// Validate server configuration
	if err := authenticatedServer.ValidateConfig(); err != nil {
		logger.Fatal("Invalid server configuration", zap.Error(err))
	}

	// Start gRPC server with SPIFFE authentication
	go func() {
		if err := startGRPCServerWithSPIFFE(ctx, authenticatedServer, logger); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Start HTTP server with SPIFFE authentication
	go func() {
		if err := startHTTPServerWithSPIFFE(ctx, authenticatedServer, logger); err != nil {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	// Start client examples
	go func() {
		time.Sleep(2 * time.Second) // Wait for servers to start
		runClientExamples(ctx, authService, logger)
	}()
}

// startGRPCServerWithSPIFFE starts a gRPC server with SPIFFE mTLS
func startGRPCServerWithSPIFFE(ctx context.Context, authenticatedServer *auth.AuthenticatedServer, logger *zap.Logger) error {
	// Create gRPC server with SPIFFE authentication
	grpcServer, err := authenticatedServer.NewGRPCServer(ctx)
	if err != nil {
		return fmt.Errorf("failed to create gRPC server: %w", err)
	}

	// Register health service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Register other services here
	// pb.RegisterYourServiceServer(grpcServer, yourServiceImpl)

	logger.Info("Starting gRPC server with SPIFFE mTLS", zap.String("address", ":9443"))

	// Start server
	return authenticatedServer.ListenAndServeGRPC(ctx, ":9443", grpcServer)
}

// startHTTPServerWithSPIFFE starts an HTTP server with SPIFFE mTLS
func startHTTPServerWithSPIFFE(ctx context.Context, authenticatedServer *auth.AuthenticatedServer, logger *zap.Logger) error {
	// Create HTTP handler
	mux := http.NewServeMux()

	// Health endpoint (public, no authentication required)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Protected endpoint (requires SPIFFE authentication)
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		identity, err := auth.RequireIdentity(r.Context())
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		response := fmt.Sprintf(`{
			"status": "authenticated",
			"spiffe_id": "%s",
			"trust_domain": "%s",
			"timestamp": "%s"
		}`, identity.SPIFFEID.String(), identity.SPIFFEID.TrustDomain().String(), time.Now().Format(time.RFC3339))

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(response))
	})

	// Create HTTPS server with SPIFFE authentication
	httpServer, err := authenticatedServer.NewHTTPServer(ctx, mux)
	if err != nil {
		return fmt.Errorf("failed to create HTTP server: %w", err)
	}

	logger.Info("Starting HTTPS server with SPIFFE mTLS", zap.String("address", ":8443"))

	// Start server
	return authenticatedServer.ListenAndServeHTTPS(ctx, ":8443", httpServer)
}

// startServersWithoutAuth starts servers without authentication (fallback)
func startServersWithoutAuth(ctx context.Context, cfg *config.Config, logger *zap.Logger) {
	logger.Warn("Starting servers without authentication - not recommended for production")

	// Simple HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Info("Starting HTTP server", zap.String("address", ":8080"))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	// Shutdown server on context cancellation
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()
}

// runClientExamples demonstrates how to use SPIFFE clients
func runClientExamples(ctx context.Context, authService *auth.SPIFFEAuthService, logger *zap.Logger) {
	spiffeClient := auth.NewSPIFFEClient(authService, logger)

	// Example 1: HTTP client to another service
	targetServerID, err := spiffeid.FromString("spiffe://gdc.local/scheduler")
	if err != nil {
		logger.Error("Invalid target server ID", zap.Error(err))
		return
	}

	httpClient, err := spiffeClient.NewHTTPClient(ctx, targetServerID)
	if err != nil {
		logger.Error("Failed to create HTTP client", zap.Error(err))
		return
	}

	// Make a request (this will fail if scheduler is not running, but demonstrates the setup)
	resp, err := httpClient.Get("https://scheduler:8443/health")
	if err != nil {
		logger.Debug("HTTP request failed (expected if scheduler not running)", zap.Error(err))
	} else {
		resp.Body.Close()
		logger.Info("Successfully made authenticated HTTP request to scheduler")
	}

	// Example 2: gRPC client to another service
	conn, err := spiffeClient.NewGRPCConnection(ctx, "orchestrator:9443", 
		spiffeid.Must("spiffe://gdc.local/orchestrator"))
	if err != nil {
		logger.Debug("gRPC connection failed (expected if orchestrator not running)", zap.Error(err))
	} else {
		defer conn.Close()
		logger.Info("Successfully created authenticated gRPC connection to orchestrator")
	}

	// Example 3: HTTP client for any server in trust domain
	anyClient, err := spiffeClient.NewHTTPClientForAnyServer(ctx)
	if err != nil {
		logger.Error("Failed to create HTTP client for any server", zap.Error(err))
		return
	}

	_ = anyClient // Use the client as needed
	logger.Info("Successfully created HTTP client for any server in trust domain")
}

// loadConfiguration loads the application configuration
func loadConfiguration() *config.Config {
	cfg := &config.Config{
		Security: config.SecurityConfig{},
	}

	// Set defaults
	cfg.Security.SetDefaults()

	// Override with environment variables
	if socketPath := os.Getenv("GDC_SECURITY_SPIFFE_SOCKET_PATH"); socketPath != "" {
		cfg.Security.SPIFFESocketPath = socketPath
	}
	if trustDomain := os.Getenv("GDC_SECURITY_TRUST_DOMAIN"); trustDomain != "" {
		cfg.Security.TrustDomain = trustDomain
	}
	if serviceID := os.Getenv("GDC_SECURITY_SERVICE_ID"); serviceID != "" {
		cfg.Security.ServiceID = serviceID
	}
	if os.Getenv("GDC_SECURITY_TLS_ENABLED") == "true" {
		cfg.Security.TLSEnabled = true
		cfg.Security.AuthEnabled = true
		cfg.Security.AuthMethod = "spiffe"
	}

	return cfg
}

// waitForShutdown waits for shutdown signals
func waitForShutdown(logger *zap.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
}

// Example of how to use this in your actual main function:
/*
func main() {
	// Check if SPIFFE mode is enabled
	if os.Getenv("GDC_SECURITY_TLS_ENABLED") == "true" {
		mainWithSPIFFE()
	} else {
		// Your existing main function
		mainWithoutAuth()
	}
}
*/