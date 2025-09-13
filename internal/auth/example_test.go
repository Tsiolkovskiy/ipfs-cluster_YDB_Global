package auth_test

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/global-data-controller/gdc/internal/auth"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.uber.org/zap"
)

// Example demonstrates how to use SPIFFE authentication in a service
func ExampleSPIFFEAuthService() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Create SPIFFE auth service
	authConfig := &auth.Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}

	authService, err := auth.NewSPIFFEAuthService(authConfig, logger)
	if err != nil {
		log.Fatalf("Failed to create SPIFFE auth service: %v", err)
	}

	// Initialize the service (connects to SPIRE agent)
	ctx := context.Background()
	if err := authService.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize SPIFFE auth service: %v", err)
	}
	defer authService.Close()

	// Create server configuration
	serverConfig := &auth.ServerConfig{
		TrustDomain: "gdc.local",
		ServiceID:   "spiffe://gdc.local/gdc-server",
		AuthorizedIDs: []string{
			"spiffe://gdc.local/scheduler",
			"spiffe://gdc.local/orchestrator",
		},
	}

	// Create authenticated server helper
	authenticatedServer := auth.NewAuthenticatedServer(authService, serverConfig, logger)

	// Validate configuration
	if err := authenticatedServer.ValidateConfig(); err != nil {
		log.Fatalf("Invalid server configuration: %v", err)
	}

	// Create gRPC server with SPIFFE authentication
	grpcServer, err := authenticatedServer.NewGRPCServer(ctx)
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
	}

	// Register your gRPC services here
	// pb.RegisterYourServiceServer(grpcServer, yourServiceImpl)

	// Create HTTP server with SPIFFE authentication
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract identity from request context
		identity, err := auth.RequireIdentity(r.Context())
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		fmt.Fprintf(w, "Hello, %s!", identity.SPIFFEID.String())
	})

	httpServer, err := authenticatedServer.NewHTTPServer(ctx, httpHandler)
	if err != nil {
		log.Fatalf("Failed to create HTTP server: %v", err)
	}

	// Start servers
	go func() {
		if err := authenticatedServer.ListenAndServeGRPC(ctx, ":9090", grpcServer); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	go func() {
		if err := authenticatedServer.ListenAndServeHTTPS(ctx, ":8443", httpServer); err != nil {
			log.Printf("HTTPS server error: %v", err)
		}
	}()

	// Example of creating a client
	spiffeClient := auth.NewSPIFFEClient(authService, logger)

	// Create gRPC client connection to another service
	targetServerID, _ := spiffeid.FromString("spiffe://gdc.local/scheduler")
	conn, err := spiffeClient.NewGRPCConnection(ctx, "scheduler:9090", targetServerID)
	if err != nil {
		log.Printf("Failed to create gRPC connection: %v", err)
	} else {
		defer conn.Close()
		// Use the connection for gRPC calls
	}

	// Create HTTP client for another service
	httpClient, err := spiffeClient.NewHTTPClient(ctx, targetServerID)
	if err != nil {
		log.Printf("Failed to create HTTP client: %v", err)
	} else {
		// Use the client for HTTP requests
		resp, err := httpClient.Get("https://scheduler:8443/health")
		if err != nil {
			log.Printf("HTTP request failed: %v", err)
		} else {
			resp.Body.Close()
		}
	}

	fmt.Println("SPIFFE authentication example completed")
}

// Example_gRPCService demonstrates how to implement a gRPC service with SPIFFE authentication
func Example_gRPCService() {
	// This would be your actual gRPC service implementation
	type YourService struct {
		logger *zap.Logger
	}

	// Example gRPC method that uses SPIFFE identity
	yourServiceMethod := func(ctx context.Context, req interface{}) (interface{}, error) {
		// Extract identity from context
		identity, err := auth.RequireIdentity(ctx)
		if err != nil {
			return nil, fmt.Errorf("authentication required: %w", err)
		}

		// Log the authenticated identity
		log.Printf("Request from SPIFFE ID: %s", identity.SPIFFEID.String())

		// Your business logic here
		return "success", nil
	}

	_ = yourServiceMethod // Avoid unused variable warning

	fmt.Println("gRPC service with SPIFFE auth example")
}

// Example_httpMiddleware demonstrates how to use SPIFFE authentication middleware
func Example_httpMiddleware() {
	logger, _ := zap.NewProduction()

	authConfig := &auth.Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}

	authService, _ := auth.NewSPIFFEAuthService(authConfig, logger)

	// Create a handler that requires authentication
	protectedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := auth.RequireIdentity(r.Context())
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"authenticated_as": "%s"}`, identity.SPIFFEID.String())
	})

	// Wrap with authentication middleware
	authenticatedHandler := authService.HTTPAuthMiddleware()(protectedHandler)

	// Create HTTP server
	server := &http.Server{
		Addr:    ":8080",
		Handler: authenticatedHandler,
	}

	_ = server // Avoid unused variable warning

	fmt.Println("HTTP middleware example")
}