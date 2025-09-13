package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"go.uber.org/zap"
)

// AuthService handles authentication and authorization
type AuthService interface {
	// Authentication
	Authenticate(ctx context.Context, token string) (*Identity, error)
	
	// Authorization
	Authorize(ctx context.Context, identity *Identity, resource string, action string) error
	AuthorizeWithContext(ctx context.Context, identity *Identity, resource string, action string, authzContext map[string]interface{}) error
	
	// SPIFFE specific methods
	GetX509Source(ctx context.Context) (*workloadapi.X509Source, error)
	GetTLSConfig(ctx context.Context, serverID spiffeid.ID) (*tls.Config, error)
	ValidateSVID(ctx context.Context, cert *x509.Certificate) (*Identity, error)
}

// Identity represents an authenticated identity
type Identity struct {
	ID       string            `json:"id"`
	Subject  string            `json:"subject"`
	Claims   map[string]string `json:"claims"`
	SPIFFEID spiffeid.ID       `json:"spiffe_id"`
}

// SPIFFEAuthService implements SPIFFE/SPIRE based authentication
type SPIFFEAuthService struct {
	socketPath string
	logger     *zap.Logger
	x509Source *workloadapi.X509Source
	authzService AuthorizationService
}

// Config holds SPIFFE authentication configuration
type Config struct {
	SocketPath string `json:"socket_path"`
}

// NewSPIFFEAuthService creates a new SPIFFE auth service
func NewSPIFFEAuthService(cfg *Config, logger *zap.Logger) (*SPIFFEAuthService, error) {
	if cfg.SocketPath == "" {
		cfg.SocketPath = "unix:///tmp/spire-agent/public/api.sock"
	}

	// Create OPA authorization service
	authzService := NewOPAAuthorizationService(logger)

	service := &SPIFFEAuthService{
		socketPath:   cfg.SocketPath,
		logger:       logger,
		authzService: authzService,
	}

	return service, nil
}

// Initialize initializes the SPIFFE auth service by connecting to SPIRE agent
func (s *SPIFFEAuthService) Initialize(ctx context.Context) error {
	s.logger.Info("Initializing SPIFFE auth service", zap.String("socket_path", s.socketPath))

	// Create X509Source to fetch SVIDs from SPIRE agent
	source, err := workloadapi.NewX509Source(ctx, workloadapi.WithClientOptions(workloadapi.WithAddr(s.socketPath)))
	if err != nil {
		return fmt.Errorf("failed to create X509Source: %w", err)
	}

	s.x509Source = source
	s.logger.Info("Successfully connected to SPIRE agent")

	// Initialize OPA authorization service
	if err := s.authzService.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize authorization service: %w", err)
	}

	return nil
}

// Close closes the SPIFFE auth service
func (s *SPIFFEAuthService) Close() error {
	if s.x509Source != nil {
		return s.x509Source.Close()
	}
	return nil
}

// GetX509Source returns the X509Source for accessing SVIDs
func (s *SPIFFEAuthService) GetX509Source(ctx context.Context) (*workloadapi.X509Source, error) {
	if s.x509Source == nil {
		return nil, fmt.Errorf("SPIFFE auth service not initialized")
	}
	return s.x509Source, nil
}

// GetTLSConfig returns a TLS config for mTLS with the specified server SPIFFE ID
func (s *SPIFFEAuthService) GetTLSConfig(ctx context.Context, serverID spiffeid.ID) (*tls.Config, error) {
	if s.x509Source == nil {
		return nil, fmt.Errorf("SPIFFE auth service not initialized")
	}

	// Create TLS config that validates the server's SPIFFE ID
	tlsConfig := tlsconfig.MTLSClientConfig(s.x509Source, s.x509Source, tlsconfig.AuthorizeID(serverID))
	
	return tlsConfig, nil
}

// GetServerTLSConfig returns a TLS config for server-side mTLS
func (s *SPIFFEAuthService) GetServerTLSConfig(ctx context.Context, authorizedIDs ...spiffeid.ID) (*tls.Config, error) {
	if s.x509Source == nil {
		return nil, fmt.Errorf("SPIFFE auth service not initialized")
	}

	var authorizer tlsconfig.Authorizer
	if len(authorizedIDs) > 0 {
		authorizer = tlsconfig.AuthorizeOneOf(authorizedIDs...)
	} else {
		// Allow any SPIFFE ID from the same trust domain
		trustDomain, err := s.getTrustDomain()
		if err != nil {
			return nil, fmt.Errorf("failed to get trust domain: %w", err)
		}
		authorizer = tlsconfig.AuthorizeMemberOf(trustDomain)
	}

	tlsConfig := tlsconfig.MTLSServerConfig(s.x509Source, s.x509Source, authorizer)
	
	return tlsConfig, nil
}

// ValidateSVID validates a SPIFFE SVID certificate and returns the identity
func (s *SPIFFEAuthService) ValidateSVID(ctx context.Context, cert *x509.Certificate) (*Identity, error) {
	if cert == nil {
		return nil, fmt.Errorf("certificate is nil")
	}

	// Extract SPIFFE ID from certificate
	spiffeID, err := x509svid.IDFromCert(cert)
	if err != nil {
		return nil, fmt.Errorf("failed to extract SPIFFE ID from certificate: %w", err)
	}

	// Validate certificate against trust bundle
	if s.x509Source == nil {
		return nil, fmt.Errorf("SPIFFE auth service not initialized")
	}

	bundle, err := s.x509Source.GetX509BundleForTrustDomain(spiffeID.TrustDomain())
	if err != nil || bundle == nil {
		return nil, fmt.Errorf("no trust bundle found for trust domain %s: %w", spiffeID.TrustDomain(), err)
	}

	// Verify certificate chain
	roots := x509.NewCertPool()
	for _, rootCA := range bundle.X509Authorities() {
		roots.AddCert(rootCA)
	}

	opts := x509.VerifyOptions{
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	_, err = cert.Verify(opts)
	if err != nil {
		return nil, fmt.Errorf("certificate verification failed: %w", err)
	}

	// Create identity from SPIFFE ID
	identity := &Identity{
		ID:       spiffeID.String(),
		Subject:  spiffeID.Path(),
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id":    spiffeID.String(),
			"trust_domain": spiffeID.TrustDomain().String(),
			"path":         spiffeID.Path(),
		},
	}

	return identity, nil
}

// Authenticate verifies SPIFFE SVID (for compatibility with AuthService interface)
func (s *SPIFFEAuthService) Authenticate(ctx context.Context, token string) (*Identity, error) {
	// In SPIFFE/SPIRE, authentication is typically done via mTLS
	// This method is kept for interface compatibility but should not be used directly
	// Instead, use ValidateSVID with the client certificate from mTLS connection
	return nil, fmt.Errorf("use ValidateSVID with client certificate for SPIFFE authentication")
}

// Authorize checks permissions using OPA/Rego
func (s *SPIFFEAuthService) Authorize(ctx context.Context, identity *Identity, resource string, action string) error {
	return s.AuthorizeWithContext(ctx, identity, resource, action, nil)
}

// AuthorizeWithContext checks permissions using OPA/Rego with additional context
func (s *SPIFFEAuthService) AuthorizeWithContext(ctx context.Context, identity *Identity, resource string, action string, authzContext map[string]interface{}) error {
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: resource,
		Action:   action,
		Context:  authzContext,
	}

	result, err := s.authzService.Authorize(ctx, request)
	if err != nil {
		s.logger.Error("Authorization evaluation failed", 
			zap.String("spiffe_id", identity.SPIFFEID.String()),
			zap.String("resource", resource),
			zap.String("action", action),
			zap.Error(err))
		return fmt.Errorf("authorization evaluation failed: %w", err)
	}

	if !result.Allowed {
		s.logger.Warn("Authorization denied", 
			zap.String("spiffe_id", identity.SPIFFEID.String()),
			zap.String("resource", resource),
			zap.String("action", action),
			zap.String("reason", result.Reason))
		return fmt.Errorf("access denied: %s", result.Reason)
	}

	s.logger.Debug("Authorization granted", 
		zap.String("spiffe_id", identity.SPIFFEID.String()),
		zap.String("resource", resource),
		zap.String("action", action),
		zap.String("reason", result.Reason))

	return nil
}

// getTrustDomain returns the trust domain from the current SVID
func (s *SPIFFEAuthService) getTrustDomain() (spiffeid.TrustDomain, error) {
	if s.x509Source == nil {
		return spiffeid.TrustDomain{}, fmt.Errorf("SPIFFE auth service not initialized")
	}

	svid, err := s.x509Source.GetX509SVID()
	if err != nil {
		return spiffeid.TrustDomain{}, fmt.Errorf("failed to get SVID: %w", err)
	}

	spiffeID, err := x509svid.IDFromCert(svid.Certificates[0])
	if err != nil {
		return spiffeid.TrustDomain{}, fmt.Errorf("failed to extract SPIFFE ID: %w", err)
	}

	return spiffeID.TrustDomain(), nil
}

// GetAuthorizationService returns the underlying authorization service
func (s *SPIFFEAuthService) GetAuthorizationService() AuthorizationService {
	return s.authzService
}