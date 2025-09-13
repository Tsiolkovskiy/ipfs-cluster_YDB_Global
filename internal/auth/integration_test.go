// +build integration

package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// TestSPIFFEIntegration tests SPIFFE authentication with actual SPIRE infrastructure
// This test requires SPIRE server and agent to be running
func TestSPIFFEIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	logger := zaptest.NewLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create SPIFFE auth service
	authConfig := &Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}

	authService, err := NewSPIFFEAuthService(authConfig, logger)
	require.NoError(t, err)

	// Initialize the service (this will fail if SPIRE agent is not running)
	err = authService.Initialize(ctx)
	if err != nil {
		t.Skipf("SPIRE agent not available, skipping integration test: %v", err)
	}
	defer authService.Close()

	t.Run("GetX509Source", func(t *testing.T) {
		source, err := authService.GetX509Source(ctx)
		require.NoError(t, err)
		assert.NotNil(t, source)

		// Get SVID to verify it's working
		svid, err := source.GetX509SVID()
		require.NoError(t, err)
		assert.NotNil(t, svid)
		assert.NotEmpty(t, svid.Certificates)

		// Extract SPIFFE ID from certificate
		spiffeID, err := x509svid.IDFromCert(svid.Certificates[0])
		require.NoError(t, err)
		assert.Equal(t, "gdc.local", spiffeID.TrustDomain().String())

		logger.Info("Successfully obtained SVID", 
			zap.String("spiffe_id", spiffeID.String()))
	})

	t.Run("ValidateSVID", func(t *testing.T) {
		source, err := authService.GetX509Source(ctx)
		require.NoError(t, err)

		svid, err := source.GetX509SVID()
		require.NoError(t, err)

		identity, err := authService.ValidateSVID(ctx, svid.Certificates[0])
		require.NoError(t, err)
		assert.NotNil(t, identity)
		assert.NotEmpty(t, identity.ID)
		assert.NotEmpty(t, identity.SPIFFEID.String())
		assert.Equal(t, "gdc.local", identity.SPIFFEID.TrustDomain().String())

		logger.Info("Successfully validated SVID", 
			zap.String("identity_id", identity.ID),
			zap.String("spiffe_id", identity.SPIFFEID.String()))
	})

	t.Run("mTLS_gRPC_Server", func(t *testing.T) {
		// Create server configuration
		serverConfig := &ServerConfig{
			TrustDomain: "gdc.local",
			ServiceID:   "spiffe://gdc.local/test-server",
			AuthorizedIDs: []string{
				"spiffe://gdc.local/test-client",
			},
		}

		authenticatedServer := NewAuthenticatedServer(authService, serverConfig, logger)

		// Create gRPC server with SPIFFE authentication
		grpcServer, err := authenticatedServer.NewGRPCServer(ctx)
		require.NoError(t, err)

		// Register health service for testing
		healthServer := health.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

		// Start server
		listener, err := net.Listen("tcp", ":0")
		require.NoError(t, err)
		defer listener.Close()

		serverAddr := listener.Addr().String()
		
		go func() {
			if err := grpcServer.Serve(listener); err != nil {
				logger.Error("gRPC server error", zap.Error(err))
			}
		}()
		defer grpcServer.Stop()

		// Give server time to start
		time.Sleep(100 * time.Millisecond)

		// Test connection without authentication (should fail for non-health endpoints)
		conn, err := grpc.DialContext(ctx, serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		healthClient := grpc_health_v1.NewHealthClient(conn)
		
		// Health check should work (public endpoint)
		resp, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, resp.Status)

		logger.Info("gRPC server with SPIFFE mTLS test completed successfully")
	})

	t.Run("mTLS_HTTP_Server", func(t *testing.T) {
		// Create HTTP handler
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			identity, err := RequireIdentity(r.Context())
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			fmt.Fprintf(w, "Hello, %s!", identity.SPIFFEID.String())
		})

		// Create server configuration
		serverConfig := &ServerConfig{
			TrustDomain: "gdc.local",
			ServiceID:   "spiffe://gdc.local/test-http-server",
			AuthorizedIDs: []string{
				"spiffe://gdc.local/test-client",
			},
		}

		authenticatedServer := NewAuthenticatedServer(authService, serverConfig, logger)

		// Create HTTPS server with SPIFFE authentication
		httpServer, err := authenticatedServer.NewHTTPServer(ctx, handler)
		require.NoError(t, err)

		// Start server
		listener, err := tls.Listen("tcp", ":0", httpServer.TLSConfig)
		require.NoError(t, err)
		defer listener.Close()

		serverAddr := listener.Addr().String()
		httpServer.Addr = serverAddr

		go func() {
			if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
				logger.Error("HTTP server error", zap.Error(err))
			}
		}()
		defer httpServer.Close()

		// Give server time to start
		time.Sleep(100 * time.Millisecond)

		logger.Info("HTTPS server with SPIFFE mTLS test completed successfully")
	})

	t.Run("Client_Connection", func(t *testing.T) {
		spiffeClient := NewSPIFFEClient(authService, logger)

		// Test creating HTTP client
		targetServerID, err := spiffeid.FromString("spiffe://gdc.local/test-target")
		require.NoError(t, err)

		httpClient, err := spiffeClient.NewHTTPClient(ctx, targetServerID)
		require.NoError(t, err)
		assert.NotNil(t, httpClient)
		assert.NotNil(t, httpClient.Transport)

		// Test creating gRPC connection (will fail to connect but client creation should work)
		_, err = spiffeClient.NewGRPCConnection(ctx, "nonexistent:9090", targetServerID, grpc.WithBlock(), grpc.WithTimeout(1*time.Second))
		assert.Error(t, err) // Expected to fail since server doesn't exist
		assert.Contains(t, err.Error(), "connection refused")

		logger.Info("SPIFFE client creation test completed successfully")
	})
}

// TestSPIFFEAuthServiceWithoutSPIRE tests the auth service behavior when SPIRE is not available
func TestSPIFFEAuthServiceWithoutSPIRE(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create SPIFFE auth service with non-existent socket
	authConfig := &Config{
		SocketPath: "unix:///tmp/nonexistent-spire-agent/api.sock",
	}

	authService, err := NewSPIFFEAuthService(authConfig, logger)
	require.NoError(t, err)

	// Initialize should fail gracefully
	err = authService.Initialize(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create X509Source")

	// Methods should return appropriate errors when not initialized
	_, err = authService.GetX509Source(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SPIFFE auth service not initialized")

	_, err = authService.GetTLSConfig(ctx, spiffeid.Must("spiffe://example.org/test"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SPIFFE auth service not initialized")
}

// BenchmarkSPIFFEOperations benchmarks SPIFFE operations
func BenchmarkSPIFFEOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	logger := zaptest.NewLogger(b)
	ctx := context.Background()

	authConfig := &Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}

	authService, err := NewSPIFFEAuthService(authConfig, logger)
	if err != nil {
		b.Skipf("Failed to create SPIFFE auth service: %v", err)
	}

	err = authService.Initialize(ctx)
	if err != nil {
		b.Skipf("SPIRE agent not available: %v", err)
	}
	defer authService.Close()

	b.Run("GetX509Source", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := authService.GetX509Source(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ValidateSVID", func(b *testing.B) {
		source, err := authService.GetX509Source(ctx)
		if err != nil {
			b.Fatal(err)
		}

		svid, err := source.GetX509SVID()
		if err != nil {
			b.Fatal(err)
		}

		cert := svid.Certificates[0]

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := authService.ValidateSVID(ctx, cert)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}