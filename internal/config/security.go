package config

import (
	"fmt"
	"strings"
)

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	// SPIFFE/SPIRE configuration
	SPIFFESocketPath string   `mapstructure:"spiffe_socket_path" json:"spiffe_socket_path"`
	TLSEnabled       bool     `mapstructure:"tls_enabled" json:"tls_enabled"`
	TrustDomain      string   `mapstructure:"trust_domain" json:"trust_domain"`
	ServiceID        string   `mapstructure:"service_id" json:"service_id"`
	AuthorizedIDs    []string `mapstructure:"authorized_ids" json:"authorized_ids"`

	// TLS certificate configuration (fallback if SPIFFE is not available)
	CertFile string `mapstructure:"cert_file" json:"cert_file"`
	KeyFile  string `mapstructure:"key_file" json:"key_file"`
	CAFile   string `mapstructure:"ca_file" json:"ca_file"`

	// Authentication settings
	AuthEnabled     bool   `mapstructure:"auth_enabled" json:"auth_enabled"`
	AuthMethod      string `mapstructure:"auth_method" json:"auth_method"` // "spiffe", "tls", "none"
	SessionTimeout  string `mapstructure:"session_timeout" json:"session_timeout"`
	TokenExpiration string `mapstructure:"token_expiration" json:"token_expiration"`

	// Authorization settings
	AuthzEnabled bool   `mapstructure:"authz_enabled" json:"authz_enabled"`
	AuthzMethod  string `mapstructure:"authz_method" json:"authz_method"` // "opa", "rbac", "none"
	OPAEndpoint  string `mapstructure:"opa_endpoint" json:"opa_endpoint"`

	// Audit settings
	AuditEnabled bool   `mapstructure:"audit_enabled" json:"audit_enabled"`
	AuditLogPath string `mapstructure:"audit_log_path" json:"audit_log_path"`
}

// Validate validates the security configuration
func (s *SecurityConfig) Validate() error {
	if s.TLSEnabled {
		if s.AuthMethod == "spiffe" {
			if s.SPIFFESocketPath == "" {
				return fmt.Errorf("spiffe_socket_path is required when auth_method is spiffe")
			}
			if s.TrustDomain == "" {
				return fmt.Errorf("trust_domain is required when auth_method is spiffe")
			}
			if s.ServiceID == "" {
				return fmt.Errorf("service_id is required when auth_method is spiffe")
			}
			if !strings.HasPrefix(s.ServiceID, "spiffe://") {
				return fmt.Errorf("service_id must be a valid SPIFFE ID (start with spiffe://)")
			}
			if !strings.Contains(s.ServiceID, s.TrustDomain) {
				return fmt.Errorf("service_id must belong to the configured trust_domain")
			}
		} else if s.AuthMethod == "tls" {
			if s.CertFile == "" || s.KeyFile == "" {
				return fmt.Errorf("cert_file and key_file are required when auth_method is tls")
			}
		}
	}

	if s.AuthzEnabled && s.AuthzMethod == "opa" {
		if s.OPAEndpoint == "" {
			return fmt.Errorf("opa_endpoint is required when authz_method is opa")
		}
	}

	return nil
}

// IsSPIFFEEnabled returns true if SPIFFE authentication is enabled
func (s *SecurityConfig) IsSPIFFEEnabled() bool {
	return s.TLSEnabled && s.AuthEnabled && s.AuthMethod == "spiffe"
}

// IsTLSEnabled returns true if TLS is enabled (either SPIFFE or traditional TLS)
func (s *SecurityConfig) IsTLSEnabled() bool {
	return s.TLSEnabled
}

// IsAuthEnabled returns true if authentication is enabled
func (s *SecurityConfig) IsAuthEnabled() bool {
	return s.AuthEnabled && s.AuthMethod != "none"
}

// IsAuthzEnabled returns true if authorization is enabled
func (s *SecurityConfig) IsAuthzEnabled() bool {
	return s.AuthzEnabled && s.AuthzMethod != "none"
}

// GetAuthorizedSPIFFEIDs returns the list of authorized SPIFFE IDs
func (s *SecurityConfig) GetAuthorizedSPIFFEIDs() []string {
	if !s.IsSPIFFEEnabled() {
		return nil
	}
	return s.AuthorizedIDs
}

// SetDefaults sets default values for security configuration
func (s *SecurityConfig) SetDefaults() {
	if s.SPIFFESocketPath == "" {
		s.SPIFFESocketPath = "unix:///tmp/spire-agent/public/api.sock"
	}
	if s.TrustDomain == "" {
		s.TrustDomain = "gdc.local"
	}
	if s.ServiceID == "" {
		s.ServiceID = "spiffe://gdc.local/gdc-server"
	}
	if s.AuthMethod == "" {
		if s.TLSEnabled {
			s.AuthMethod = "spiffe"
		} else {
			s.AuthMethod = "none"
		}
	}
	if s.AuthzMethod == "" {
		s.AuthzMethod = "none"
	}
	if s.SessionTimeout == "" {
		s.SessionTimeout = "1h"
	}
	if s.TokenExpiration == "" {
		s.TokenExpiration = "24h"
	}
}