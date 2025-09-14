package security

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/security/access"
	"github.com/ipfs-cluster/ipfs-cluster/security/audit"
	"github.com/ipfs-cluster/ipfs-cluster/security/compliance"
	"github.com/ipfs-cluster/ipfs-cluster/security/encryption"
)

func TestSecurityManagerIntegration(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "security-integration-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test configuration
	config := &Config{
		Encryption: &encryption.Config{
			DefaultAlgorithm: "aes-256-gcm",
			KeyProvider: encryption.KeyProviderConfig{
				Type: "file",
				Config: map[string]interface{}{
					"key_dir": filepath.Join(tempDir, "keys"),
				},
			},
			RotationPolicy: &encryption.KeyRotationPolicy{
				RotationInterval:     time.Hour,
				GracePeriod:         time.Minute,
				MaxKeyAge:           24 * time.Hour,
				AutoRotate:          false,
				NotifyBeforeRotation: time.Minute,
			},
			AuditConfig: &encryption.AuditConfig{
				Enabled: true,
				LogFile: filepath.Join(tempDir, "encryption-audit.log"),
			},
		},
		Access: &access.Config{
			PermissionStorage: access.PermissionStorageConfig{
				Type: "file",
				Config: map[string]interface{}{
					"permissions_file": filepath.Join(tempDir, "permissions.json"),
				},
				CacheEnabled: true,
				CacheTTL:     5 * time.Minute,
			},
			ZFSIntegration: access.ZFSIntegrationConfig{
				Enabled: false, // Disable for testing
			},
			RPCIntegration: access.RPCIntegrationConfig{
				Enabled: true,
				DefaultPolicy: map[string]string{
					"Cluster.ID": "allow",
				},
			},
		},
		Compliance: &compliance.Config{
			EvaluationInterval:   100 * time.Millisecond,
			ReportingInterval:    time.Hour,
			AutoResolveThreshold: time.Hour,
			ResponseConfig: &compliance.ResponseConfig{
				Enabled: true,
				DryRun:  true, // Enable dry run for testing
			},
		},
		Audit: &audit.Config{
			Enabled: true,
			LogFile: filepath.Join(tempDir, "security-audit.log"),
		},
	}

	// Create security manager
	sm, err := NewSecurityManager(config)
	if err != nil {
		t.Fatalf("Failed to create security manager: %v", err)
	}
	defer sm.Shutdown(context.Background())

	t.Run("StartAndStop", func(t *testing.T) {
		// Start security manager
		err := sm.Start()
		if err != nil {
			t.Fatalf("Failed to start security manager: %v", err)
		}

		// Verify it's running
		sm.mutex.RLock()
		running := sm.running
		sm.mutex.RUnlock()

		if !running {
			t.Errorf("Security manager should be running")
		}

		// Stop security manager
		err = sm.Stop()
		if err != nil {
			t.Fatalf("Failed to stop security manager: %v", err)
		}

		// Verify it's stopped
		sm.mutex.RLock()
		running = sm.running
		sm.mutex.RUnlock()

		if running {
			t.Errorf("Security manager should be stopped")
		}
	})

	t.Run("EnableDatasetEncryption", func(t *testing.T) {
		// Skip ZFS integration test in test environment
		t.Skip("Skipping ZFS integration test - requires ZFS to be installed")
	})

	t.Run("CheckDatasetAccess", func(t *testing.T) {
		userID := "test-user"
		dataset := "test/dataset2"
		action := "read"

		// First check should be denied (no permission)
		allowed, err := sm.CheckDatasetAccess(userID, dataset, action, nil)
		if err != nil {
			t.Errorf("Failed to check dataset access: %v", err)
		}

		if allowed {
			t.Errorf("Access should be denied for user without permission")
		}

		// Grant permission
		accessController := sm.GetAccessController()
		permission := &access.Permission{
			ID:        "test-permission",
			Subject:   userID,
			Resource:  "zfs:" + dataset,
			Action:    action,
			GrantedAt: time.Now(),
			GrantedBy: "admin",
		}

		err = accessController.GrantPermission(permission)
		if err != nil {
			t.Fatalf("Failed to grant permission: %v", err)
		}

		// Check access again (should be allowed now)
		allowed, err = sm.CheckDatasetAccess(userID, dataset, action, nil)
		if err != nil {
			t.Errorf("Failed to check dataset access: %v", err)
		}

		if !allowed {
			// Debug: Check the access request directly
			request := &access.AccessRequest{
				Subject:   userID,
				Resource:  "zfs:" + dataset,
				Action:    action,
				Timestamp: time.Now(),
			}
			
			result, _ := accessController.CheckAccess(request)
			t.Errorf("Access should be allowed after granting permission. Reason: %s", result.Reason)
			
			// Debug: List permissions to see what we have
			permissions, _ := accessController.ListPermissions(userID)
			t.Logf("Found %d permissions for user %s", len(permissions), userID)
			for _, perm := range permissions {
				t.Logf("Permission: %s -> %s:%s (Expires: %v)", perm.Subject, perm.Resource, perm.Action, perm.ExpiresAt)
			}
		}
	})

	t.Run("ComplianceIntegration", func(t *testing.T) {
		// Start monitoring
		err := sm.Start()
		if err != nil {
			t.Fatalf("Failed to start security manager: %v", err)
		}
		defer sm.Stop()

		complianceMonitor := sm.GetComplianceMonitor()

		// Add a test policy
		policy := &compliance.SecurityPolicy{
			ID:          "integration-test-policy",
			Name:        "Integration Test Policy",
			Description: "A policy for integration testing",
			Category:    compliance.PolicyCategoryAccess,
			Severity:    compliance.ViolationSeverityMedium,
			Enabled:     true,
			Rules: []compliance.PolicyRule{
				{
					ID:        "test-rule",
					Name:      "Test Rule",
					Condition: "test_condition == true",
					Enabled:   true,
				},
			},
			Actions: []compliance.ResponseAction{
				{
					Type:    compliance.ResponseActionTypeAlert,
					Target:  "security-team",
					Enabled: true,
				},
			},
		}

		err = complianceMonitor.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add test policy: %v", err)
		}

		// Report a test violation
		violation := &compliance.SecurityViolation{
			PolicyID:    policy.ID,
			RuleID:      "test-rule",
			Severity:    compliance.ViolationSeverityMedium,
			Title:       "Integration Test Violation",
			Description: "A violation for integration testing",
			Source:      "integration_test",
		}

		err = complianceMonitor.ReportViolation(violation)
		if err != nil {
			t.Fatalf("Failed to report violation: %v", err)
		}

		// Verify violation was recorded
		retrievedViolation, err := complianceMonitor.GetViolation(violation.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve violation: %v", err)
		}

		if retrievedViolation.Title != violation.Title {
			t.Errorf("Expected violation title %s, got %s", violation.Title, retrievedViolation.Title)
		}
	})

	t.Run("GenerateSecurityReport", func(t *testing.T) {
		// Set context with start time for report generation
		ctx := context.WithValue(context.Background(), "start_time", time.Now().Add(-time.Hour))
		sm.ctx = ctx

		report, err := sm.GenerateSecurityReport()
		if err != nil {
			t.Errorf("Failed to generate security report: %v", err)
		}

		if report.GeneratedAt.IsZero() {
			t.Errorf("Expected report generation timestamp to be set")
		}

		if report.ComplianceReport == nil {
			t.Errorf("Expected compliance report to be included")
		}

		if report.SecurityScore < 0 || report.SecurityScore > 100 {
			t.Errorf("Expected security score between 0 and 100, got %f", report.SecurityScore)
		}
	})

	t.Run("ComponentAccess", func(t *testing.T) {
		// Test that all components are accessible
		encManager := sm.GetEncryptionManager()
		if encManager == nil {
			t.Errorf("Encryption manager should not be nil")
		}

		accessController := sm.GetAccessController()
		if accessController == nil {
			t.Errorf("Access controller should not be nil")
		}

		complianceMonitor := sm.GetComplianceMonitor()
		if complianceMonitor == nil {
			t.Errorf("Compliance monitor should not be nil")
		}

		rpcIntegration := sm.GetRPCPolicyIntegration()
		if rpcIntegration == nil {
			t.Errorf("RPC policy integration should not be nil")
		}
	})
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config == nil {
		t.Fatalf("Default config should not be nil")
	}

	if config.Encryption == nil {
		t.Errorf("Default encryption config should not be nil")
	}

	if config.Access == nil {
		t.Errorf("Default access config should not be nil")
	}

	if config.Compliance == nil {
		t.Errorf("Default compliance config should not be nil")
	}

	if config.Audit == nil {
		t.Errorf("Default audit config should not be nil")
	}

	// Validate individual configs
	if err := config.Encryption.Validate(); err != nil {
		t.Errorf("Default encryption config should be valid: %v", err)
	}

	if err := config.Access.Validate(); err != nil {
		t.Errorf("Default access config should be valid: %v", err)
	}

	if err := config.Compliance.Validate(); err != nil {
		t.Errorf("Default compliance config should be valid: %v", err)
	}
}