package auth

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// ServerConfig holds configuration for creating authenticated servers
type ServerConfig struct {
	TrustDomain   string   `json:"trust_domain"`
	ServiceID     string   `json:"service_id"`
	AuthorizedIDs []string `json:"authorized_ids"`
}

// AuthenticatedServer provides methods for creating servers with SPIFFE authentication
type AuthenticatedServer struct {
	authService *SPIFFEAuthService
	config      *ServerConfig
	logger      *zap.Logger
}

// NewAuthenticatedServer creates a new authenticated server helper
func NewAuthenticatedServer(authService *SPIFFEAuthService, config *ServerConfig, logger *zap.Logger) *AuthenticatedServer {
	return &AuthenticatedServer{
		authService: authService,
		config:      config,
		logger:      logger,
	}
}

// NewGRPCServer creates a new gRPC server with SPIFFE mTLS and authentication middleware
func (s *AuthenticatedServer) NewGRPCServer(ctx context.Context, opts ...grpc.ServerOption) (*grpc.Server, error) {
	// Parse authorized SPIFFE IDs
	var authorizedIDs []spiffeid.ID
	for _, idStr := range s.config.AuthorizedIDs {
		id, err := spiffeid.FromString(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid authorized SPIFFE ID %s: %w", idStr, err)
		}
		authorizedIDs = append(authorizedIDs, id)
	}

	// Get server TLS config
	tlsConfig, err := s.authService.GetServerTLSConfig(ctx, authorizedIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to get server TLS config: %w", err)
	}

	// Create gRPC credentials
	creds := credentials.NewTLS(tlsConfig)

	// Default server options with authentication
	defaultOpts := []grpc.ServerOption{
		grpc.Creds(creds),
		grpc.UnaryInterceptor(s.authService.GRPCAuthInterceptor()),
		grpc.StreamInterceptor(s.authService.GRPCStreamAuthInterceptor()),
	}

	// Combine with user-provided options
	allOpts := append(defaultOpts, opts...)

	server := grpc.NewServer(allOpts...)

	s.logger.Info("Created gRPC server with SPIFFE mTLS authentication",
		zap.String("service_id", s.config.ServiceID),
		zap.Strings("authorized_ids", s.config.AuthorizedIDs))

	return server, nil
}

// NewHTTPServer creates a new HTTP server with SPIFFE mTLS and authentication middleware
func (s *AuthenticatedServer) NewHTTPServer(ctx context.Context, handler http.Handler) (*http.Server, error) {
	// Parse authorized SPIFFE IDs
	var authorizedIDs []spiffeid.ID
	for _, idStr := range s.config.AuthorizedIDs {
		id, err := spiffeid.FromString(idStr)
		if err != nil {
			return nil, fmt.Errorf("invalid authorized SPIFFE ID %s: %w", idStr, err)
		}
		authorizedIDs = append(authorizedIDs, id)
	}

	// Get server TLS config
	tlsConfig, err := s.authService.GetServerTLSConfig(ctx, authorizedIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to get server TLS config: %w", err)
	}

	// Wrap handler with authentication middleware
	authenticatedHandler := s.authService.HTTPAuthMiddleware()(handler)

	server := &http.Server{
		Handler:   authenticatedHandler,
		TLSConfig: tlsConfig,
	}

	s.logger.Info("Created HTTP server with SPIFFE mTLS authentication",
		zap.String("service_id", s.config.ServiceID),
		zap.Strings("authorized_ids", s.config.AuthorizedIDs))

	return server, nil
}

// ListenAndServeGRPC starts a gRPC server with SPIFFE authentication on the specified address
func (s *AuthenticatedServer) ListenAndServeGRPC(ctx context.Context, addr string, server *grpc.Server) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s.logger.Info("Starting gRPC server with SPIFFE authentication", zap.String("address", addr))

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil {
			errChan <- fmt.Errorf("gRPC server error: %w", err)
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down gRPC server")
		server.GracefulStop()
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// ListenAndServeHTTPS starts an HTTPS server with SPIFFE authentication on the specified address
func (s *AuthenticatedServer) ListenAndServeHTTPS(ctx context.Context, addr string, server *http.Server) error {
	s.logger.Info("Starting HTTPS server with SPIFFE authentication", zap.String("address", addr))

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		// Use ListenAndServeTLS with empty cert/key files since TLS config is already set
		if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("HTTPS server error: %w", err)
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down HTTPS server")
		return server.Shutdown(context.Background())
	case err := <-errChan:
		return err
	}
}

// ValidateConfig validates the server configuration
func (s *AuthenticatedServer) ValidateConfig() error {
	if s.config.TrustDomain == "" {
		return fmt.Errorf("trust domain is required")
	}

	if s.config.ServiceID == "" {
		return fmt.Errorf("service ID is required")
	}

	// Validate service ID format
	_, err := spiffeid.FromString(s.config.ServiceID)
	if err != nil {
		return fmt.Errorf("invalid service ID format: %w", err)
	}

	// Validate authorized IDs
	for _, idStr := range s.config.AuthorizedIDs {
		_, err := spiffeid.FromString(idStr)
		if err != nil {
			return fmt.Errorf("invalid authorized SPIFFE ID %s: %w", idStr, err)
		}
	}

	return nil
}