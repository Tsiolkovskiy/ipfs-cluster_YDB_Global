// Package security provides integrated security services for IPFS Cluster ZFS integration
package security

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/security/access"
	"github.com/ipfs-cluster/ipfs-cluster/security/audit"
	"github.com/ipfs-cluster/ipfs-cluster/security/compliance"
	"github.com/ipfs-cluster/ipfs-cluster/security/encryption"

	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("security")

// SecurityManager integrates all security components
type SecurityManager struct {
	encryptionManager   *encryption.EncryptionManager
	accessController    *access.AccessController
	complianceMonitor   *compliance.ComplianceMonitor
	rpcPolicyIntegration *access.RPCPolicyIntegration
	
	config *Config
	
	// State management
	running bool
	mutex   sync.RWMutex
	
	// Context management
	ctx    context.Context
	cancel context.CancelFunc
}

// Config holds the integrated security configuration
type Config struct {
	Encryption *encryption.Config   `json:"encryption"`
	Access     *access.Config       `json:"access"`
	Compliance *compliance.Config   `json:"compliance"`
	Audit      *audit.Config        `json:"audit"`
}

// NewSecurityManager creates a new integrated security manager
func NewSecurityManager(config *Config) (*SecurityManager, error) {
	if config == nil {
		config = DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create shared audit logger
	auditLogger, err := audit.NewUnifiedAuditLogger(config.Audit)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create audit logger: %w", err)
	}

	// Create encryption manager
	keyProvider, err := encryption.NewFileKeyProvider(
		config.Encryption.KeyProvider.Config["key_dir"].(string),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create key provider: %w", err)
	}

	encryptionManager, err := encryption.NewEncryptionManager(config.Encryption, keyProvider)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create encryption manager: %w", err)
	}

	// Create access controller
	accessController, err := access.NewAccessController(config.Access, auditLogger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create access controller: %w", err)
	}

	// Create RPC policy integration
	rpcPolicyIntegration := access.NewRPCPolicyIntegration(
		accessController, 
		&config.Access.RPCIntegration, 
		auditLogger,
	)

	// Create compliance monitor with adapter
	complianceAuditLogger := &ComplianceAuditAdapter{auditLogger}
	complianceMonitor, err := compliance.NewComplianceMonitor(config.Compliance, complianceAuditLogger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create compliance monitor: %w", err)
	}

	sm := &SecurityManager{
		encryptionManager:    encryptionManager,
		accessController:     accessController,
		complianceMonitor:    complianceMonitor,
		rpcPolicyIntegration: rpcPolicyIntegration,
		config:               config,
		ctx:                  ctx,
		cancel:               cancel,
	}

	logger.Info("Security manager initialized successfully")
	return sm, nil
}

// Start starts all security services
func (sm *SecurityManager) Start() error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.running {
		return fmt.Errorf("security manager is already running")
	}

	// Start compliance monitoring
	if err := sm.complianceMonitor.StartMonitoring(); err != nil {
		return fmt.Errorf("failed to start compliance monitoring: %w", err)
	}

	// Start RPC policy cache cleanup
	go sm.rpcPolicyIntegration.StartCacheCleanup(sm.ctx)

	sm.running = true
	logger.Info("Security manager started")
	return nil
}

// Stop stops all security services
func (sm *SecurityManager) Stop() error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if !sm.running {
		return fmt.Errorf("security manager is not running")
	}

	// Stop compliance monitoring
	if err := sm.complianceMonitor.StopMonitoring(); err != nil {
		logger.Errorf("Error stopping compliance monitoring: %s", err)
	}

	// Cancel context to stop background tasks
	sm.cancel()

	sm.running = false
	logger.Info("Security manager stopped")
	return nil
}

// Shutdown gracefully shuts down all security services
func (sm *SecurityManager) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down security manager")

	// Stop services first
	if err := sm.Stop(); err != nil {
		logger.Warnf("Error stopping security manager: %s", err)
	}

	// Shutdown individual components
	if err := sm.encryptionManager.Shutdown(ctx); err != nil {
		logger.Errorf("Error shutting down encryption manager: %s", err)
	}

	if err := sm.accessController.Shutdown(ctx); err != nil {
		logger.Errorf("Error shutting down access controller: %s", err)
	}

	logger.Info("Security manager shutdown complete")
	return nil
}

// GetEncryptionManager returns the encryption manager
func (sm *SecurityManager) GetEncryptionManager() *encryption.EncryptionManager {
	return sm.encryptionManager
}

// GetAccessController returns the access controller
func (sm *SecurityManager) GetAccessController() *access.AccessController {
	return sm.accessController
}

// GetComplianceMonitor returns the compliance monitor
func (sm *SecurityManager) GetComplianceMonitor() *compliance.ComplianceMonitor {
	return sm.complianceMonitor
}

// GetRPCPolicyIntegration returns the RPC policy integration
func (sm *SecurityManager) GetRPCPolicyIntegration() *access.RPCPolicyIntegration {
	return sm.rpcPolicyIntegration
}

// EnableDatasetEncryption enables encryption for a ZFS dataset
func (sm *SecurityManager) EnableDatasetEncryption(dataset string, algorithm string) error {
	// Enable encryption
	if err := sm.encryptionManager.EnableDatasetEncryption(dataset, algorithm); err != nil {
		return fmt.Errorf("failed to enable dataset encryption: %w", err)
	}

	// Report compliance event
	violation := &compliance.SecurityViolation{
		PolicyID:    "encryption-policy",
		Severity:    compliance.ViolationSeverityLow,
		Title:       "Dataset Encryption Enabled",
		Description: fmt.Sprintf("Encryption enabled for dataset %s with algorithm %s", dataset, algorithm),
		Source:      "security_manager",
		Evidence: map[string]interface{}{
			"dataset":   dataset,
			"algorithm": algorithm,
		},
	}

	if err := sm.complianceMonitor.ReportViolation(violation); err != nil {
		logger.Warnf("Failed to report compliance event: %s", err)
	}

	return nil
}

// CheckDatasetAccess checks access to a ZFS dataset
func (sm *SecurityManager) CheckDatasetAccess(userID, dataset, action string, context map[string]interface{}) (bool, error) {
	request := &access.AccessRequest{
		Subject:   userID,
		Resource:  fmt.Sprintf("zfs:%s", dataset),
		Action:    action,
		Context:   context,
	}

	result, err := sm.accessController.CheckAccess(request)
	if err != nil {
		return false, fmt.Errorf("failed to check dataset access: %w", err)
	}

	// Report security violation if access is denied
	if !result.Allowed {
		violation := &compliance.SecurityViolation{
			PolicyID:    "access-control-policy",
			Severity:    compliance.ViolationSeverityMedium,
			Title:       "Unauthorized Dataset Access Attempt",
			Description: fmt.Sprintf("User %s attempted unauthorized %s access to dataset %s", userID, action, dataset),
			Source:      "security_manager",
			Target:      userID,
			Evidence: map[string]interface{}{
				"user_id": userID,
				"dataset": dataset,
				"action":  action,
				"reason":  result.Reason,
			},
		}

		if err := sm.complianceMonitor.ReportViolation(violation); err != nil {
			logger.Warnf("Failed to report security violation: %s", err)
		}
	}

	return result.Allowed, nil
}

// GenerateSecurityReport generates a comprehensive security report
func (sm *SecurityManager) GenerateSecurityReport() (*SecurityReport, error) {
	// Generate compliance report
	complianceReport, err := sm.complianceMonitor.GenerateComplianceReport(
		sm.ctx.Value("start_time").(time.Time),
		time.Now(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to generate compliance report: %w", err)
	}

	// Get encryption status for all datasets
	// This would typically iterate through all known datasets
	encryptionStatus := make(map[string]*encryption.EncryptionStatus)

	// Get access control statistics
	// This would typically aggregate access control data
	accessStats := &AccessControlStats{
		TotalPermissions: 0,
		ActiveUsers:      0,
		RecentDenials:    0,
	}

	report := &SecurityReport{
		GeneratedAt:       time.Now(),
		ComplianceReport:  complianceReport,
		EncryptionStatus:  encryptionStatus,
		AccessStats:       accessStats,
		SecurityScore:     sm.calculateSecurityScore(complianceReport),
	}

	return report, nil
}

// SecurityReport represents a comprehensive security report
type SecurityReport struct {
	GeneratedAt       time.Time                              `json:"generated_at"`
	ComplianceReport  *compliance.ComplianceReport           `json:"compliance_report"`
	EncryptionStatus  map[string]*encryption.EncryptionStatus `json:"encryption_status"`
	AccessStats       *AccessControlStats                    `json:"access_stats"`
	SecurityScore     float64                                `json:"security_score"`
}

// AccessControlStats represents access control statistics
type AccessControlStats struct {
	TotalPermissions int `json:"total_permissions"`
	ActiveUsers      int `json:"active_users"`
	RecentDenials    int `json:"recent_denials"`
}

// calculateSecurityScore calculates an overall security score
func (sm *SecurityManager) calculateSecurityScore(complianceReport *compliance.ComplianceReport) float64 {
	// This is a simplified security score calculation
	// In a real implementation, you would consider multiple factors
	
	baseScore := complianceReport.ComplianceScore
	
	// Adjust based on violation severity
	if complianceReport.Violations[compliance.ViolationSeverityCritical] > 0 {
		baseScore -= 20
	}
	if complianceReport.Violations[compliance.ViolationSeverityHigh] > 0 {
		baseScore -= 10
	}
	
	// Ensure score is between 0 and 100
	if baseScore < 0 {
		baseScore = 0
	}
	if baseScore > 100 {
		baseScore = 100
	}
	
	return baseScore
}

// ComplianceAuditAdapter adapts the unified audit logger for compliance interface
type ComplianceAuditAdapter struct {
	logger *audit.UnifiedAuditLogger
}

func (c *ComplianceAuditAdapter) LogViolation(violation *compliance.SecurityViolation) {
	c.logger.LogViolation(violation)
}

func (c *ComplianceAuditAdapter) LogPolicyChange(policyID, action string, details map[string]interface{}) {
	c.logger.LogPolicyChange(policyID, action, details)
}

func (c *ComplianceAuditAdapter) LogResponse(action *compliance.ResponseAction, success bool, details map[string]interface{}) {
	c.logger.LogResponse(action, success, details)
}

// DefaultConfig returns a default security configuration
func DefaultConfig() *Config {
	return &Config{
		Encryption: encryption.DefaultConfig(),
		Access:     access.DefaultConfig(),
		Compliance: compliance.DefaultConfig(),
		Audit:      audit.DefaultConfig(),
	}
}