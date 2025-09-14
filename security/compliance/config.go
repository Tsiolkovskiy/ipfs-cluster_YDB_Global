package compliance

import (
	"time"
)

// Config holds compliance monitoring configuration
type Config struct {
	// Monitoring settings
	EvaluationInterval time.Duration `json:"evaluation_interval"`
	ReportingInterval  time.Duration `json:"reporting_interval"`
	
	// Violation settings
	AutoResolveThreshold time.Duration `json:"auto_resolve_threshold"`
	
	// Response configuration
	ResponseConfig *ResponseConfig `json:"response_config"`
	
	// Storage settings
	Storage StorageConfig `json:"storage"`
	
	// Notification settings
	Notifications NotificationConfig `json:"notifications"`
	
	// Compliance frameworks
	Frameworks []ComplianceFramework `json:"frameworks"`
}

// StorageConfig configures compliance data storage
type StorageConfig struct {
	// Type of storage (file, database, etc.)
	Type string `json:"type"`
	
	// Storage-specific configuration
	Config map[string]interface{} `json:"config"`
	
	// Data retention settings
	RetentionPeriod time.Duration `json:"retention_period"`
	
	// Backup settings
	BackupEnabled  bool          `json:"backup_enabled"`
	BackupInterval time.Duration `json:"backup_interval"`
	BackupPath     string        `json:"backup_path"`
}

// NotificationConfig configures compliance notifications
type NotificationConfig struct {
	// Enable notifications
	Enabled bool `json:"enabled"`
	
	// Notification channels
	Channels []NotificationChannel `json:"channels"`
	
	// Notification rules
	Rules []NotificationRule `json:"rules"`
}

// NotificationChannel represents a notification channel
type NotificationChannel struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`     // email, slack, webhook, etc.
	Config   map[string]interface{} `json:"config"`
	Enabled  bool                   `json:"enabled"`
}

// NotificationRule represents a notification rule
type NotificationRule struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Condition   string                 `json:"condition"`
	Channels    []string               `json:"channels"`
	Severity    *ViolationSeverity     `json:"severity,omitempty"`
	Categories  []PolicyCategory       `json:"categories,omitempty"`
	Throttle    *time.Duration         `json:"throttle,omitempty"`
	Enabled     bool                   `json:"enabled"`
}

// ComplianceFramework represents a compliance framework configuration
type ComplianceFramework struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Version     string                 `json:"version"`
	Description string                 `json:"description"`
	Controls    []ComplianceControl    `json:"controls"`
	Enabled     bool                   `json:"enabled"`
}

// ComplianceControl represents a compliance control
type ComplianceControl struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Category    string   `json:"category"`
	PolicyIDs   []string `json:"policy_ids"`
	Required    bool     `json:"required"`
}

// DefaultConfig returns a default compliance monitoring configuration
func DefaultConfig() *Config {
	return &Config{
		EvaluationInterval:   5 * time.Minute,
		ReportingInterval:    24 * time.Hour,
		AutoResolveThreshold: 7 * 24 * time.Hour, // 7 days
		
		ResponseConfig: &ResponseConfig{
			Enabled:                 true,
			MaxConcurrentExecutions: 10,
			DefaultTimeout:          30 * time.Second,
			MaxRetries:              3,
			RetryDelay:              5 * time.Second,
			DryRun:                  false,
		},
		
		Storage: StorageConfig{
			Type: "file",
			Config: map[string]interface{}{
				"violations_file": "/var/lib/ipfs-cluster/compliance/violations.json",
				"policies_file":   "/var/lib/ipfs-cluster/compliance/policies.json",
				"reports_dir":     "/var/lib/ipfs-cluster/compliance/reports",
			},
			RetentionPeriod: 365 * 24 * time.Hour, // 1 year
			BackupEnabled:   true,
			BackupInterval:  24 * time.Hour,
			BackupPath:      "/var/lib/ipfs-cluster/compliance/backups",
		},
		
		Notifications: NotificationConfig{
			Enabled: true,
			Channels: []NotificationChannel{
				{
					ID:   "security-team-email",
					Type: "email",
					Config: map[string]interface{}{
						"recipients": []string{"security@example.com"},
						"smtp_host":  "localhost",
						"smtp_port":  587,
					},
					Enabled: true,
				},
			},
			Rules: []NotificationRule{
				{
					ID:        "critical-violations",
					Name:      "Critical Security Violations",
					Condition: "severity == 'critical'",
					Channels:  []string{"security-team-email"},
					Severity:  &[]ViolationSeverity{ViolationSeverityCritical}[0],
					Enabled:   true,
				},
				{
					ID:        "high-violations",
					Name:      "High Severity Violations",
					Condition: "severity == 'high'",
					Channels:  []string{"security-team-email"},
					Severity:  &[]ViolationSeverity{ViolationSeverityHigh}[0],
					Throttle:  &[]time.Duration{1 * time.Hour}[0],
					Enabled:   true,
				},
			},
		},
		
		Frameworks: []ComplianceFramework{
			{
				ID:          "iso27001",
				Name:        "ISO 27001",
				Version:     "2013",
				Description: "Information Security Management System",
				Enabled:     true,
				Controls: []ComplianceControl{
					{
						ID:          "A.9.1.1",
						Name:        "Access Control Policy",
						Description: "An access control policy shall be established, documented and reviewed based on business and information security requirements.",
						Category:    "access_control",
						PolicyIDs:   []string{"access-control-policy"},
						Required:    true,
					},
					{
						ID:          "A.10.1.1",
						Name:        "Cryptographic Policy",
						Description: "A policy on the use of cryptographic controls for protection of information shall be developed and implemented.",
						Category:    "cryptography",
						PolicyIDs:   []string{"encryption-policy"},
						Required:    true,
					},
				},
			},
			{
				ID:          "gdpr",
				Name:        "GDPR",
				Version:     "2018",
				Description: "General Data Protection Regulation",
				Enabled:     true,
				Controls: []ComplianceControl{
					{
						ID:          "Art.32",
						Name:        "Security of Processing",
						Description: "Taking into account the state of the art, the costs of implementation and the nature, scope, context and purposes of processing as well as the risk of varying likelihood and severity for the rights and freedoms of natural persons, the controller and the processor shall implement appropriate technical and organisational measures to ensure a level of security appropriate to the risk.",
						Category:    "data_protection",
						PolicyIDs:   []string{"encryption-policy", "access-control-policy"},
						Required:    true,
					},
				},
			},
		},
	}
}

// Validate validates the compliance configuration
func (c *Config) Validate() error {
	// Set defaults for missing values
	if c.EvaluationInterval == 0 {
		c.EvaluationInterval = 5 * time.Minute
	}
	
	if c.ReportingInterval == 0 {
		c.ReportingInterval = 24 * time.Hour
	}
	
	if c.AutoResolveThreshold == 0 {
		c.AutoResolveThreshold = 7 * 24 * time.Hour
	}
	
	if c.ResponseConfig == nil {
		c.ResponseConfig = &ResponseConfig{
			Enabled:                 true,
			MaxConcurrentExecutions: 10,
			DefaultTimeout:          30 * time.Second,
			MaxRetries:              3,
			RetryDelay:              5 * time.Second,
		}
	}
	
	if c.Storage.Type == "" {
		c.Storage.Type = "file"
	}
	
	if c.Storage.RetentionPeriod == 0 {
		c.Storage.RetentionPeriod = 365 * 24 * time.Hour
	}
	
	return nil
}