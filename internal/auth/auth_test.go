package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestNewSPIFFEAuthService(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	cfg := &Config{
		SocketPath: "unix:///tmp/test-spire-agent/public/api.sock",
	}

	service, err := NewSPIFFEAuthService(cfg, logger)
	require.NoError(t, err)
	assert.NotNil(t, service)
	assert.Equal(t, cfg.SocketPath, service.socketPath)
}

func TestNewSPIFFEAuthService_DefaultSocketPath(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	cfg := &Config{} // Empty config should use default

	service, err := NewSPIFFEAuthService(cfg, logger)
	require.NoError(t, err)
	assert.NotNil(t, service)
	assert.Equal(t, "unix:///tmp/spire-agent/public/api.sock", service.socketPath)
}

func TestValidateSVID(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	service := &SPIFFEAuthService{
		logger: logger,
	}

	t.Run("nil certificate", func(t *testing.T) {
		identity, err := service.ValidateSVID(context.Background(), nil)
		assert.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "certificate is nil")
	})

	t.Run("certificate without SPIFFE ID", func(t *testing.T) {
		cert := createTestCertificate(t, "example.com", nil)
		
		identity, err := service.ValidateSVID(context.Background(), cert)
		assert.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "failed to extract SPIFFE ID")
	})

	t.Run("uninitialized service", func(t *testing.T) {
		spiffeURI, err := url.Parse("spiffe://example.org/workload")
		require.NoError(t, err)
		
		cert := createTestCertificate(t, "", []*url.URL{spiffeURI})
		
		identity, err := service.ValidateSVID(context.Background(), cert)
		assert.Error(t, err)
		assert.Nil(t, identity)
		assert.Contains(t, err.Error(), "SPIFFE auth service not initialized")
	})
}

func TestAuthenticate(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	service := &SPIFFEAuthService{
		logger: logger,
	}

	identity, err := service.Authenticate(context.Background(), "dummy-token")
	assert.Error(t, err)
	assert.Nil(t, identity)
	assert.Contains(t, err.Error(), "use ValidateSVID with client certificate")
}

func TestAuthorize(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	service := &SPIFFEAuthService{
		logger: logger,
	}

	spiffeID, err := spiffeid.FromString("spiffe://example.org/workload")
	require.NoError(t, err)

	identity := &Identity{
		ID:       "test-id",
		Subject:  "test-subject",
		SPIFFEID: spiffeID,
	}

	// For now, authorization should always succeed (placeholder implementation)
	err = service.Authorize(context.Background(), identity, "resource", "action")
	assert.NoError(t, err)
}

func TestGetTrustDomain_Uninitialized(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	service := &SPIFFEAuthService{
		logger: logger,
	}

	trustDomain, err := service.getTrustDomain()
	assert.Error(t, err)
	assert.Empty(t, trustDomain)
	assert.Contains(t, err.Error(), "SPIFFE auth service not initialized")
}

// Helper function to create test certificates
func createTestCertificate(t *testing.T, dnsName string, uris []*url.URL) *x509.Certificate {
	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  nil,
	}

	if dnsName != "" {
		template.DNSNames = []string{dnsName}
	}

	if len(uris) > 0 {
		template.URIs = uris
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(certDER)
	require.NoError(t, err)

	return cert
}

func TestIsPublicEndpoint(t *testing.T) {
	tests := []struct {
		method   string
		expected bool
	}{
		{"/grpc.health.v1.Health/Check", true},
		{"/grpc.health.v1.Health/Watch", true},
		{"/gdc.v1.HealthService/Check", true},
		{"/gdc.v1.PolicyService/CreatePolicy", false},
		{"/gdc.v1.SchedulerService/ComputePlacement", false},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			result := isPublicEndpoint(tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPublicHTTPEndpoint(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/health", true},
		{"/healthz", true},
		{"/ready", true},
		{"/metrics", true},
		{"/version", true},
		{"/api/v1/policies", false},
		{"/api/v1/scheduler/placement", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := isPublicHTTPEndpoint(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetIdentityFromContext(t *testing.T) {
	spiffeID, err := spiffeid.FromString("spiffe://example.org/workload")
	require.NoError(t, err)

	identity := &Identity{
		ID:       "test-id",
		Subject:  "test-subject",
		SPIFFEID: spiffeID,
	}

	t.Run("identity present", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), IdentityContextKey, identity)
		
		result, ok := GetIdentityFromContext(ctx)
		assert.True(t, ok)
		assert.Equal(t, identity, result)
	})

	t.Run("identity not present", func(t *testing.T) {
		ctx := context.Background()
		
		result, ok := GetIdentityFromContext(ctx)
		assert.False(t, ok)
		assert.Nil(t, result)
	})
}

func TestRequireIdentity(t *testing.T) {
	spiffeID, err := spiffeid.FromString("spiffe://example.org/workload")
	require.NoError(t, err)

	identity := &Identity{
		ID:       "test-id",
		Subject:  "test-subject",
		SPIFFEID: spiffeID,
	}

	t.Run("identity present", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), IdentityContextKey, identity)
		
		result, err := RequireIdentity(ctx)
		assert.NoError(t, err)
		assert.Equal(t, identity, result)
	})

	t.Run("identity not present", func(t *testing.T) {
		ctx := context.Background()
		
		result, err := RequireIdentity(ctx)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "no identity found in context")
	})
}