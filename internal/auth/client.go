package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// SPIFFEClient provides methods for creating authenticated clients
type SPIFFEClient struct {
	authService *SPIFFEAuthService
	logger      *zap.Logger
}

// NewSPIFFEClient creates a new SPIFFE client
func NewSPIFFEClient(authService *SPIFFEAuthService, logger *zap.Logger) *SPIFFEClient {
	return &SPIFFEClient{
		authService: authService,
		logger:      logger,
	}
}

// NewGRPCConnection creates a new gRPC connection with SPIFFE mTLS authentication
func (c *SPIFFEClient) NewGRPCConnection(ctx context.Context, target string, serverID spiffeid.ID, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// Get TLS config for mTLS with the target server
	tlsConfig, err := c.authService.GetTLSConfig(ctx, serverID)
	if err != nil {
		return nil, fmt.Errorf("failed to get TLS config: %w", err)
	}

	// Create gRPC credentials from TLS config
	creds := credentials.NewTLS(tlsConfig)

	// Default dial options
	defaultOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
		grpc.WithTimeout(30 * time.Second),
	}

	// Combine with user-provided options
	allOpts := append(defaultOpts, opts...)

	c.logger.Debug("Creating gRPC connection with SPIFFE mTLS", 
		zap.String("target", target),
		zap.String("server_id", serverID.String()))

	conn, err := grpc.DialContext(ctx, target, allOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC server: %w", err)
	}

	c.logger.Info("Successfully established gRPC connection with SPIFFE mTLS", 
		zap.String("target", target),
		zap.String("server_id", serverID.String()))

	return conn, nil
}

// NewHTTPClient creates a new HTTP client with SPIFFE mTLS authentication
func (c *SPIFFEClient) NewHTTPClient(ctx context.Context, serverID spiffeid.ID) (*http.Client, error) {
	// Get TLS config for mTLS with the target server
	tlsConfig, err := c.authService.GetTLSConfig(ctx, serverID)
	if err != nil {
		return nil, fmt.Errorf("failed to get TLS config: %w", err)
	}

	// Create HTTP transport with TLS config
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	c.logger.Debug("Created HTTP client with SPIFFE mTLS", 
		zap.String("server_id", serverID.String()))

	return client, nil
}

// NewHTTPClientForAnyServer creates an HTTP client that accepts any server from the same trust domain
func (c *SPIFFEClient) NewHTTPClientForAnyServer(ctx context.Context) (*http.Client, error) {
	x509Source, err := c.authService.GetX509Source(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get X509Source: %w", err)
	}

	// Get current SVID to determine trust domain
	svid, err := x509Source.GetX509SVID()
	if err != nil {
		return nil, fmt.Errorf("failed to get SVID: %w", err)
	}

	spiffeID, err := x509svid.IDFromCert(svid.Certificates[0])
	if err != nil {
		return nil, fmt.Errorf("failed to extract SPIFFE ID: %w", err)
	}

	// Get trust bundle
	bundle, err := x509Source.GetX509BundleForTrustDomain(spiffeID.TrustDomain())
	if err != nil {
		return nil, fmt.Errorf("failed to get trust bundle: %w", err)
	}

	// Convert certificates to DER format
	var certDER [][]byte
	for _, cert := range svid.Certificates {
		certDER = append(certDER, cert.Raw)
	}

	// Create certificate pool from trust bundle
	rootCAs := x509.NewCertPool()
	for _, cert := range bundle.X509Authorities() {
		rootCAs.AddCert(cert)
	}

	// Create TLS config that accepts any server from the same trust domain
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: certDER,
				PrivateKey:  svid.PrivateKey,
			},
		},
		RootCAs: rootCAs,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			// Custom verification to ensure peer is from same trust domain
			if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
				return fmt.Errorf("no verified certificate chains")
			}

			peerID, err := x509svid.IDFromCert(verifiedChains[0][0])
			if err != nil {
				return fmt.Errorf("failed to extract peer SPIFFE ID: %w", err)
			}

			if peerID.TrustDomain() != spiffeID.TrustDomain() {
				return fmt.Errorf("peer trust domain %s does not match expected %s", 
					peerID.TrustDomain(), spiffeID.TrustDomain())
			}

			return nil
		},
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	c.logger.Debug("Created HTTP client for any server in trust domain", 
		zap.String("trust_domain", spiffeID.TrustDomain().String()))

	return client, nil
}

// ValidateServerConnection validates that a connection is properly authenticated
func (c *SPIFFEClient) ValidateServerConnection(ctx context.Context, conn *grpc.ClientConn) error {
	// This is a placeholder for connection validation
	// In practice, you might want to make a test call to verify the connection
	c.logger.Debug("Validating server connection")
	return nil
}