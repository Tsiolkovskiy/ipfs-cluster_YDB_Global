package encryption

import (
	"time"
)

// Config holds encryption manager configuration
type Config struct {
	// Encryption settings
	DefaultAlgorithm string `json:"default_algorithm"` // aes-256-gcm, aes-128-gcm
	
	// Key provider configuration
	KeyProvider KeyProviderConfig `json:"key_provider"`
	
	// Rotation policy
	RotationPolicy *KeyRotationPolicy `json:"rotation_policy"`
	
	// Audit configuration
	AuditConfig *AuditConfig `json:"audit_config"`
	
	// ZFS integration settings
	ZFSIntegration ZFSIntegrationConfig `json:"zfs_integration"`
}

// KeyProviderConfig configures the key provider backend
type KeyProviderConfig struct {
	// Type of key provider (file, vault, hsm)
	Type string `json:"type"`
	
	// Configuration specific to the provider type
	Config map[string]interface{} `json:"config"`
}

// AuditConfig configures audit logging
type AuditConfig struct {
	// Enable audit logging
	Enabled bool `json:"enabled"`
	
	// Audit log file path
	LogFile string `json:"log_file"`
	
	// Log rotation settings
	MaxSize    int  `json:"max_size"`    // MB
	MaxBackups int  `json:"max_backups"`
	MaxAge     int  `json:"max_age"`     // days
	Compress   bool `json:"compress"`
	
	// Remote audit settings
	RemoteEndpoint string            `json:"remote_endpoint,omitempty"`
	RemoteHeaders  map[string]string `json:"remote_headers,omitempty"`
}

// ZFSIntegrationConfig configures ZFS-specific encryption settings
type ZFSIntegrationConfig struct {
	// Default encryption algorithm for new datasets
	DefaultEncryption string `json:"default_encryption"`
	
	// Key format (raw, passphrase, hex)
	KeyFormat string `json:"key_format"`
	
	// Automatic encryption for new datasets
	AutoEncrypt bool `json:"auto_encrypt"`
	
	// Encryption inheritance for child datasets
	InheritEncryption bool `json:"inherit_encryption"`
}

// DefaultConfig returns a default encryption configuration
func DefaultConfig() *Config {
	return &Config{
		DefaultAlgorithm: "aes-256-gcm",
		KeyProvider: KeyProviderConfig{
			Type: "file",
			Config: map[string]interface{}{
				"key_dir": "/var/lib/ipfs-cluster/keys",
			},
		},
		RotationPolicy: &KeyRotationPolicy{
			RotationInterval:     30 * 24 * time.Hour, // 30 days
			GracePeriod:         7 * 24 * time.Hour,   // 7 days
			MaxKeyAge:           90 * 24 * time.Hour,  // 90 days
			AutoRotate:          true,
			NotifyBeforeRotation: 24 * time.Hour,      // 1 day
		},
		AuditConfig: &AuditConfig{
			Enabled:    true,
			LogFile:    "/var/log/ipfs-cluster/encryption-audit.log",
			MaxSize:    100, // 100MB
			MaxBackups: 10,
			MaxAge:     30, // 30 days
			Compress:   true,
		},
		ZFSIntegration: ZFSIntegrationConfig{
			DefaultEncryption: "aes-256-gcm",
			KeyFormat:         "raw",
			AutoEncrypt:       true,
			InheritEncryption: true,
		},
	}
}

// Validate validates the encryption configuration
func (c *Config) Validate() error {
	if c.DefaultAlgorithm == "" {
		c.DefaultAlgorithm = "aes-256-gcm"
	}
	
	if c.RotationPolicy == nil {
		c.RotationPolicy = &KeyRotationPolicy{
			RotationInterval: 30 * 24 * time.Hour,
			GracePeriod:     7 * 24 * time.Hour,
			MaxKeyAge:       90 * 24 * time.Hour,
			AutoRotate:      true,
		}
	}
	
	if c.AuditConfig == nil {
		c.AuditConfig = &AuditConfig{
			Enabled: true,
			LogFile: "/var/log/ipfs-cluster/encryption-audit.log",
		}
	}
	
	return nil
}