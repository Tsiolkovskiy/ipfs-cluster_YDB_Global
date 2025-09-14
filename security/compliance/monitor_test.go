package compliance

import (
	"testing"
	"time"
)

// MockComplianceAuditLogger implements AuditLogger for testing
type MockComplianceAuditLogger struct {
	violations      []*SecurityViolation
	policyChanges   []map[string]interface{}
	responses       []map[string]interface{}
}

func (m *MockComplianceAuditLogger) LogViolation(violation *SecurityViolation) {
	m.violations = append(m.violations, violation)
}

func (m *MockComplianceAuditLogger) LogPolicyChange(policyID, action string, details map[string]interface{}) {
	change := map[string]interface{}{
		"policy_id": policyID,
		"action":    action,
		"details":   details,
	}
	m.policyChanges = append(m.policyChanges, change)
}

func (m *MockComplianceAuditLogger) LogResponse(action *ResponseAction, success bool, details map[string]interface{}) {
	response := map[string]interface{}{
		"action":  action,
		"success": success,
		"details": details,
	}
	m.responses = append(m.responses, response)
}

func TestComplianceMonitor(t *testing.T) {
	config := DefaultConfig()
	config.EvaluationInterval = 100 * time.Millisecond // Fast evaluation for testing
	config.ResponseConfig.DryRun = true // Enable dry run for testing
	
	auditLogger := &MockComplianceAuditLogger{}
	
	cm, err := NewComplianceMonitor(config, auditLogger)
	if err != nil {
		t.Fatalf("Failed to create compliance monitor: %v", err)
	}

	t.Run("AddPolicy", func(t *testing.T) {
		policy := &SecurityPolicy{
			ID:          "test-policy-1",
			Name:        "Test Policy",
			Description: "A test security policy",
			Category:    PolicyCategoryAccess,
			Severity:    ViolationSeverityMedium,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "test-rule-1",
					Name:      "Test Rule",
					Condition: "test_condition == true",
					Enabled:   true,
				},
			},
			Actions: []ResponseAction{
				{
					Type:    ResponseActionTypeAlert,
					Target:  "test-target",
					Enabled: true,
				},
			},
		}

		err := cm.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add policy: %v", err)
		}

		// Verify policy was added
		retrievedPolicy, err := cm.GetPolicy(policy.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve policy: %v", err)
		}

		if retrievedPolicy.Name != policy.Name {
			t.Errorf("Expected policy name %s, got %s", policy.Name, retrievedPolicy.Name)
		}

		// Verify audit log
		if len(auditLogger.policyChanges) == 0 {
			t.Errorf("Expected policy change to be logged")
		}
	})

	t.Run("UpdatePolicy", func(t *testing.T) {
		policy := &SecurityPolicy{
			ID:          "test-policy-2",
			Name:        "Test Policy 2",
			Description: "Another test policy",
			Category:    PolicyCategoryEncryption,
			Severity:    ViolationSeverityHigh,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "test-rule-2",
					Name:      "Test Rule 2",
					Condition: "encryption_enabled == false",
					Enabled:   true,
				},
			},
		}

		// Add policy first
		err := cm.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add policy: %v", err)
		}

		// Update policy
		policy.Description = "Updated description"
		err = cm.UpdatePolicy(policy)
		if err != nil {
			t.Fatalf("Failed to update policy: %v", err)
		}

		// Verify update
		retrievedPolicy, err := cm.GetPolicy(policy.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve updated policy: %v", err)
		}

		if retrievedPolicy.Description != "Updated description" {
			t.Errorf("Expected updated description, got %s", retrievedPolicy.Description)
		}
	})

	t.Run("RemovePolicy", func(t *testing.T) {
		policy := &SecurityPolicy{
			ID:          "test-policy-3",
			Name:        "Test Policy 3",
			Description: "Policy to be removed",
			Category:    PolicyCategoryAudit,
			Severity:    ViolationSeverityLow,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "test-rule-3",
					Name:      "Test Rule 3",
					Condition: "audit_enabled == false",
					Enabled:   true,
				},
			},
		}

		// Add policy first
		err := cm.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add policy: %v", err)
		}

		// Remove policy
		err = cm.RemovePolicy(policy.ID)
		if err != nil {
			t.Fatalf("Failed to remove policy: %v", err)
		}

		// Verify removal
		_, err = cm.GetPolicy(policy.ID)
		if err == nil {
			t.Errorf("Expected policy to be removed")
		}
	})

	t.Run("ReportViolation", func(t *testing.T) {
		violation := &SecurityViolation{
			PolicyID:    "test-policy-1",
			RuleID:      "test-rule-1",
			Severity:    ViolationSeverityHigh,
			Title:       "Test Violation",
			Description: "A test security violation",
			Source:      "test-source",
			Evidence: map[string]interface{}{
				"test_data": "test_value",
			},
		}

		err := cm.ReportViolation(violation)
		if err != nil {
			t.Fatalf("Failed to report violation: %v", err)
		}

		// Verify violation was recorded
		retrievedViolation, err := cm.GetViolation(violation.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve violation: %v", err)
		}

		if retrievedViolation.Title != violation.Title {
			t.Errorf("Expected violation title %s, got %s", violation.Title, retrievedViolation.Title)
		}

		if retrievedViolation.Status != ViolationStatusOpen {
			t.Errorf("Expected violation status to be open, got %s", retrievedViolation.Status)
		}

		// Verify audit log
		if len(auditLogger.violations) == 0 {
			t.Errorf("Expected violation to be logged")
		}
	})

	t.Run("ResolveViolation", func(t *testing.T) {
		violation := &SecurityViolation{
			PolicyID:    "test-policy-1",
			RuleID:      "test-rule-1",
			Severity:    ViolationSeverityMedium,
			Title:       "Resolvable Violation",
			Description: "A violation that will be resolved",
			Source:      "test-source",
		}

		err := cm.ReportViolation(violation)
		if err != nil {
			t.Fatalf("Failed to report violation: %v", err)
		}

		// Resolve violation
		err = cm.ResolveViolation(violation.ID, "Manual resolution for testing")
		if err != nil {
			t.Fatalf("Failed to resolve violation: %v", err)
		}

		// Verify resolution
		retrievedViolation, err := cm.GetViolation(violation.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve resolved violation: %v", err)
		}

		if retrievedViolation.Status != ViolationStatusResolved {
			t.Errorf("Expected violation status to be resolved, got %s", retrievedViolation.Status)
		}

		if retrievedViolation.ResolvedAt == nil {
			t.Errorf("Expected resolved timestamp to be set")
		}
	})

	t.Run("ListViolations", func(t *testing.T) {
		// Add multiple violations
		violations := []*SecurityViolation{
			{
				PolicyID:    "test-policy-1",
				RuleID:      "test-rule-1",
				Severity:    ViolationSeverityHigh,
				Title:       "High Severity Violation",
				Description: "A high severity violation",
				Source:      "test-source",
			},
			{
				PolicyID:    "test-policy-1",
				RuleID:      "test-rule-1",
				Severity:    ViolationSeverityLow,
				Title:       "Low Severity Violation",
				Description: "A low severity violation",
				Source:      "test-source",
			},
		}

		for _, violation := range violations {
			err := cm.ReportViolation(violation)
			if err != nil {
				t.Fatalf("Failed to report violation: %v", err)
			}
		}

		// List all violations
		allViolations := cm.ListViolations(nil)
		if len(allViolations) < 2 {
			t.Errorf("Expected at least 2 violations, got %d", len(allViolations))
		}

		// List high severity violations only
		highSeverity := ViolationSeverityHigh
		filter := &ViolationFilter{
			Severity: &highSeverity,
		}
		highViolations := cm.ListViolations(filter)
		
		for _, violation := range highViolations {
			if violation.Severity != ViolationSeverityHigh {
				t.Errorf("Expected only high severity violations, got %s", violation.Severity)
			}
		}
	})

	t.Run("GenerateComplianceReport", func(t *testing.T) {
		endTime := time.Now()
		startTime := endTime.Add(-24 * time.Hour)

		report, err := cm.GenerateComplianceReport(startTime, endTime)
		if err != nil {
			t.Fatalf("Failed to generate compliance report: %v", err)
		}

		if report.GeneratedAt.IsZero() {
			t.Errorf("Expected report generation timestamp to be set")
		}

		if report.TotalPolicies == 0 {
			t.Errorf("Expected some policies in the report")
		}

		// Compliance score should be between 0 and 100
		if report.ComplianceScore < 0 || report.ComplianceScore > 100 {
			t.Errorf("Expected compliance score between 0 and 100, got %f", report.ComplianceScore)
		}
	})

	t.Run("ListPolicies", func(t *testing.T) {
		policies := cm.ListPolicies()
		
		// Should have at least the default policies plus any added in tests
		if len(policies) < 2 {
			t.Errorf("Expected at least 2 policies (default policies), got %d", len(policies))
		}

		// Verify default policies are present
		foundAccessPolicy := false
		foundEncryptionPolicy := false
		
		for _, policy := range policies {
			if policy.ID == "access-control-policy" {
				foundAccessPolicy = true
			}
			if policy.ID == "encryption-policy" {
				foundEncryptionPolicy = true
			}
		}

		if !foundAccessPolicy {
			t.Errorf("Expected to find default access control policy")
		}

		if !foundEncryptionPolicy {
			t.Errorf("Expected to find default encryption policy")
		}
	})
}

func TestResponseEngine(t *testing.T) {
	config := &ResponseConfig{
		Enabled:                 true,
		MaxConcurrentExecutions: 5,
		DefaultTimeout:          5 * time.Second,
		MaxRetries:              2,
		RetryDelay:              100 * time.Millisecond,
		DryRun:                  true, // Enable dry run for testing
	}

	auditLogger := &MockComplianceAuditLogger{}

	re, err := NewResponseEngine(config, auditLogger)
	if err != nil {
		t.Fatalf("Failed to create response engine: %v", err)
	}

	t.Run("ExecuteResponse", func(t *testing.T) {
		violation := &SecurityViolation{
			ID:          "test-violation-1",
			PolicyID:    "test-policy-1",
			RuleID:      "test-rule-1",
			Severity:    ViolationSeverityHigh,
			Title:       "Test Violation for Response",
			Description: "A violation to test response execution",
			Source:      "test-source",
		}

		actions := []ResponseAction{
			{
				Type:    ResponseActionTypeAlert,
				Target:  "security-team",
				Enabled: true,
			},
			{
				Type:    ResponseActionTypeLog,
				Target:  "audit-log",
				Enabled: true,
			},
		}

		// Execute response (should complete quickly in dry run mode)
		re.ExecuteResponse(violation, actions)

		// Wait a bit for async execution
		time.Sleep(200 * time.Millisecond)

		// Verify executions were logged
		executions := re.ListExecutions(nil)
		if len(executions) < 2 {
			t.Errorf("Expected at least 2 executions, got %d", len(executions))
		}

		// Verify audit logs
		if len(auditLogger.responses) < 2 {
			t.Errorf("Expected at least 2 response logs, got %d", len(auditLogger.responses))
		}
	})

	t.Run("ListExecutions", func(t *testing.T) {
		// List all executions
		allExecutions := re.ListExecutions(nil)
		if len(allExecutions) == 0 {
			t.Errorf("Expected some executions from previous test")
		}

		// Filter by status
		completedStatus := ExecutionStatusCompleted
		filter := &ExecutionFilter{
			Status: &completedStatus,
		}
		completedExecutions := re.ListExecutions(filter)

		for _, execution := range completedExecutions {
			if execution.Status != ExecutionStatusCompleted {
				t.Errorf("Expected only completed executions, got %s", execution.Status)
			}
		}
	})
}

func TestPolicyValidation(t *testing.T) {
	config := DefaultConfig()
	auditLogger := &MockComplianceAuditLogger{}
	
	cm, err := NewComplianceMonitor(config, auditLogger)
	if err != nil {
		t.Fatalf("Failed to create compliance monitor: %v", err)
	}

	t.Run("ValidPolicy", func(t *testing.T) {
		policy := &SecurityPolicy{
			ID:          "valid-policy",
			Name:        "Valid Policy",
			Description: "A valid security policy",
			Category:    PolicyCategoryAccess,
			Severity:    ViolationSeverityMedium,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "valid-rule",
					Name:      "Valid Rule",
					Condition: "valid_condition == true",
					Enabled:   true,
				},
			},
		}

		err := cm.AddPolicy(policy)
		if err != nil {
			t.Errorf("Valid policy should be accepted, got error: %v", err)
		}
	})

	t.Run("InvalidPolicyEmptyID", func(t *testing.T) {
		policy := &SecurityPolicy{
			ID:          "", // Empty ID should be invalid
			Name:        "Invalid Policy",
			Description: "A policy with empty ID",
			Category:    PolicyCategoryAccess,
			Severity:    ViolationSeverityMedium,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "rule-1",
					Name:      "Rule 1",
					Condition: "condition == true",
					Enabled:   true,
				},
			},
		}

		err := cm.AddPolicy(policy)
		if err == nil {
			t.Errorf("Policy with empty ID should be rejected")
		}
	})

	t.Run("InvalidPolicyNoRules", func(t *testing.T) {
		policy := &SecurityPolicy{
			ID:          "no-rules-policy",
			Name:        "No Rules Policy",
			Description: "A policy with no rules",
			Category:    PolicyCategoryAccess,
			Severity:    ViolationSeverityMedium,
			Enabled:     true,
			Rules:       []PolicyRule{}, // No rules should be invalid
		}

		err := cm.AddPolicy(policy)
		if err == nil {
			t.Errorf("Policy with no rules should be rejected")
		}
	})
}