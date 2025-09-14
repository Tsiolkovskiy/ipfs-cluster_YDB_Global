package access

import (
	"time"
)

// Config holds access controller configuration
type Config struct {
	// Permission storage configuration
	PermissionStorage PermissionStorageConfig `json:"permission_storage"`
	
	// ZFS integration settings
	ZFSIntegration ZFSIntegrationConfig `json:"zfs_integration"`
	
	// RPC integration settings
	RPCIntegration RPCIntegrationConfig `json:"rpc_integration"`
	
	// Default permissions
	DefaultPermissions []DefaultPermission `json:"default_permissions"`
	
	// Security settings
	Security SecurityConfig `json:"security"`
	
	// Audit settings
	Audit AuditConfig `json:"audit"`
}

// PermissionStorageConfig configures permission storage
type PermissionStorageConfig struct {
	// Type of storage (file, database, etc.)
	Type string `json:"type"`
	
	// Storage-specific configuration
	Config map[string]interface{} `json:"config"`
	
	// Cache settings
	CacheEnabled bool          `json:"cache_enabled"`
	CacheTTL     time.Duration `json:"cache_ttl"`
}

// ZFSIntegrationConfig configures ZFS delegation integration
type ZFSIntegrationConfig struct {
	// Enable ZFS delegation integration
	Enabled bool `json:"enabled"`
	
	// Default ZFS permissions for actions
	ActionPermissionMap map[string][]string `json:"action_permission_map"`
	
	// Enable recursive delegations by default
	DefaultRecursive bool `json:"default_recursive"`
	
	// ZFS command timeout
	CommandTimeout time.Duration `json:"command_timeout"`
}

// RPCIntegrationConfig configures RPC policy integration
type RPCIntegrationConfig struct {
	// Enable RPC policy integration
	Enabled bool `json:"enabled"`
	
	// Default RPC policy for new peers
	DefaultPolicy map[string]string `json:"default_policy"`
	
	// Enable fine-grained method permissions
	MethodPermissions bool `json:"method_permissions"`
	
	// RPC call timeout
	CallTimeout time.Duration `json:"call_timeout"`
}

// DefaultPermission represents a default permission to be granted
type DefaultPermission struct {
	Subject    string      `json:"subject"`
	Resource   string      `json:"resource"`
	Action     string      `json:"action"`
	Conditions []Condition `json:"conditions,omitempty"`
	ExpiresIn  *time.Duration `json:"expires_in,omitempty"`
}

// SecurityConfig configures security settings
type SecurityConfig struct {
	// Enable permission inheritance
	EnableInheritance bool `json:"enable_inheritance"`
	
	// Maximum permission lifetime
	MaxPermissionLifetime time.Duration `json:"max_permission_lifetime"`
	
	// Require explicit grants (no wildcards)
	RequireExplicitGrants bool `json:"require_explicit_grants"`
	
	// Enable permission validation
	EnableValidation bool `json:"enable_validation"`
	
	// Custom validation rules
	ValidationRules []ValidationRule `json:"validation_rules"`
}

// ValidationRule represents a custom validation rule
type ValidationRule struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Condition   string                 `json:"condition"`
	Action      string                 `json:"action"` // allow, deny, warn
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// AuditConfig configures access control audit settings
type AuditConfig struct {
	// Enable access audit logging
	Enabled bool `json:"enabled"`
	
	// Log all access attempts (not just violations)
	LogAllAttempts bool `json:"log_all_attempts"`
	
	// Log permission changes
	LogPermissionChanges bool `json:"log_permission_changes"`
	
	// Log ZFS delegation changes
	LogZFSDelegationChanges bool `json:"log_zfs_delegation_changes"`
	
	// Audit log retention period
	RetentionPeriod time.Duration `json:"retention_period"`
}

// DefaultConfig returns a default access control configuration
func DefaultConfig() *Config {
	return &Config{
		PermissionStorage: PermissionStorageConfig{
			Type: "file",
			Config: map[string]interface{}{
				"permissions_file": "/var/lib/ipfs-cluster/permissions.json",
			},
			CacheEnabled: true,
			CacheTTL:     5 * time.Minute,
		},
		ZFSIntegration: ZFSIntegrationConfig{
			Enabled: true,
			ActionPermissionMap: map[string][]string{
				"read":  {"send", "hold"},
				"write": {"create", "destroy", "mount", "receive"},
				"admin": {"create", "destroy", "mount", "receive", "send", "hold", "allow"},
			},
			DefaultRecursive: true,
			CommandTimeout:   30 * time.Second,
		},
		RPCIntegration: RPCIntegrationConfig{
			Enabled: true,
			DefaultPolicy: map[string]string{
				"Cluster.ID":           "allow",
				"Cluster.Peers":        "allow",
				"Cluster.PeerAdd":      "deny",
				"Cluster.PeerRemove":   "deny",
				"PinTracker.StatusAll": "allow",
			},
			MethodPermissions: true,
			CallTimeout:       10 * time.Second,
		},
		DefaultPermissions: []DefaultPermission{
			{
				Subject:  "admin",
				Resource: "*",
				Action:   "*",
			},
			{
				Subject:  "*",
				Resource: "rpc:Cluster.ID",
				Action:   "call",
			},
		},
		Security: SecurityConfig{
			EnableInheritance:     true,
			MaxPermissionLifetime: 365 * 24 * time.Hour, // 1 year
			RequireExplicitGrants: false,
			EnableValidation:      true,
		},
		Audit: AuditConfig{
			Enabled:                     true,
			LogAllAttempts:              false,
			LogPermissionChanges:        true,
			LogZFSDelegationChanges:     true,
			RetentionPeriod:            90 * 24 * time.Hour, // 90 days
		},
	}
}

// Validate validates the access control configuration
func (c *Config) Validate() error {
	// Set defaults for missing values
	if c.PermissionStorage.Type == "" {
		c.PermissionStorage.Type = "file"
	}
	
	if c.PermissionStorage.CacheTTL == 0 {
		c.PermissionStorage.CacheTTL = 5 * time.Minute
	}
	
	if c.ZFSIntegration.CommandTimeout == 0 {
		c.ZFSIntegration.CommandTimeout = 30 * time.Second
	}
	
	if c.RPCIntegration.CallTimeout == 0 {
		c.RPCIntegration.CallTimeout = 10 * time.Second
	}
	
	if c.Security.MaxPermissionLifetime == 0 {
		c.Security.MaxPermissionLifetime = 365 * 24 * time.Hour
	}
	
	if c.Audit.RetentionPeriod == 0 {
		c.Audit.RetentionPeriod = 90 * 24 * time.Hour
	}
	
	return nil
}