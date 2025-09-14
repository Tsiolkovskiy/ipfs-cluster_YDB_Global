// Package compliance provides security compliance monitoring and automated threat response
package compliance

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("compliance")

// ComplianceMonitor monitors security compliance and detects violations
type ComplianceMonitor struct {
	config          *Config
	policies        map[string]*SecurityPolicy
	violations      map[string]*SecurityViolation
	responseEngine  *ResponseEngine
	auditLogger     AuditLogger
	
	// Monitoring state
	monitoring      bool
	monitoringMutex sync.RWMutex
	
	// Context management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SecurityPolicy represents a security compliance policy
type SecurityPolicy struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Category    PolicyCategory         `json:"category"`
	Severity    ViolationSeverity      `json:"severity"`
	Rules       []PolicyRule           `json:"rules"`
	Actions     []ResponseAction       `json:"actions"`
	Enabled     bool                   `json:"enabled"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// PolicyCategory represents the category of a security policy
type PolicyCategory string

const (
	PolicyCategoryAccess      PolicyCategory = "access"
	PolicyCategoryEncryption  PolicyCategory = "encryption"
	PolicyCategoryAudit       PolicyCategory = "audit"
	PolicyCategoryData        PolicyCategory = "data"
	PolicyCategoryNetwork     PolicyCategory = "network"
	PolicyCategoryCompliance  PolicyCategory = "compliance"
)

// PolicyRule represents a rule within a security policy
type PolicyRule struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Condition   string                 `json:"condition"`   // Expression to evaluate
	Threshold   *Threshold             `json:"threshold,omitempty"`
	TimeWindow  *time.Duration         `json:"time_window,omitempty"`
	Enabled     bool                   `json:"enabled"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// Threshold represents a threshold for rule evaluation
type Threshold struct {
	Value    float64         `json:"value"`
	Operator ThresholdOperator `json:"operator"`
	Unit     string          `json:"unit,omitempty"`
}

// ThresholdOperator represents threshold comparison operators
type ThresholdOperator string

const (
	ThresholdOperatorGreater      ThresholdOperator = ">"
	ThresholdOperatorGreaterEqual ThresholdOperator = ">="
	ThresholdOperatorLess         ThresholdOperator = "<"
	ThresholdOperatorLessEqual    ThresholdOperator = "<="
	ThresholdOperatorEqual        ThresholdOperator = "=="
	ThresholdOperatorNotEqual     ThresholdOperator = "!="
)

// SecurityViolation represents a detected security violation
type SecurityViolation struct {
	ID          string                 `json:"id"`
	PolicyID    string                 `json:"policy_id"`
	RuleID      string                 `json:"rule_id"`
	Severity    ViolationSeverity      `json:"severity"`
	Title       string                 `json:"title"`
	Description string                 `json:"description"`
	Source      string                 `json:"source"`
	Target      string                 `json:"target,omitempty"`
	DetectedAt  time.Time              `json:"detected_at"`
	ResolvedAt  *time.Time             `json:"resolved_at,omitempty"`
	Status      ViolationStatus        `json:"status"`
	Evidence    map[string]interface{} `json:"evidence"`
	Response    []ResponseAction       `json:"response"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ViolationSeverity represents the severity of a security violation
type ViolationSeverity string

const (
	ViolationSeverityLow      ViolationSeverity = "low"
	ViolationSeverityMedium   ViolationSeverity = "medium"
	ViolationSeverityHigh     ViolationSeverity = "high"
	ViolationSeverityCritical ViolationSeverity = "critical"
)

// ViolationStatus represents the status of a security violation
type ViolationStatus string

const (
	ViolationStatusOpen       ViolationStatus = "open"
	ViolationStatusInProgress ViolationStatus = "in_progress"
	ViolationStatusResolved   ViolationStatus = "resolved"
	ViolationStatusIgnored    ViolationStatus = "ignored"
)

// ResponseAction represents an automated response action
type ResponseAction struct {
	Type        ResponseActionType     `json:"type"`
	Target      string                 `json:"target"`
	Parameters  map[string]interface{} `json:"parameters"`
	Timeout     time.Duration          `json:"timeout"`
	RetryCount  int                    `json:"retry_count"`
	Enabled     bool                   `json:"enabled"`
}

// ResponseActionType represents the type of response action
type ResponseActionType string

const (
	ResponseActionTypeAlert         ResponseActionType = "alert"
	ResponseActionTypeBlock         ResponseActionType = "block"
	ResponseActionTypeQuarantine    ResponseActionType = "quarantine"
	ResponseActionTypeRevoke        ResponseActionType = "revoke"
	ResponseActionTypeRotateKey     ResponseActionType = "rotate_key"
	ResponseActionTypeNotify        ResponseActionType = "notify"
	ResponseActionTypeLog           ResponseActionType = "log"
	ResponseActionTypeCustom        ResponseActionType = "custom"
)

// AuditLogger interface for compliance audit logging
type AuditLogger interface {
	LogViolation(violation *SecurityViolation)
	LogPolicyChange(policyID, action string, details map[string]interface{})
	LogResponse(action *ResponseAction, success bool, details map[string]interface{})
}

// NewComplianceMonitor creates a new compliance monitor
func NewComplianceMonitor(config *Config, auditLogger AuditLogger) (*ComplianceMonitor, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	responseEngine, err := NewResponseEngine(config.ResponseConfig, auditLogger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create response engine: %w", err)
	}

	cm := &ComplianceMonitor{
		config:         config,
		policies:       make(map[string]*SecurityPolicy),
		violations:     make(map[string]*SecurityViolation),
		responseEngine: responseEngine,
		auditLogger:    auditLogger,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Load default policies
	if err := cm.loadDefaultPolicies(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load default policies: %w", err)
	}

	logger.Info("Compliance monitor initialized successfully")
	return cm, nil
}

// StartMonitoring starts the compliance monitoring
func (cm *ComplianceMonitor) StartMonitoring() error {
	cm.monitoringMutex.Lock()
	defer cm.monitoringMutex.Unlock()

	if cm.monitoring {
		return fmt.Errorf("monitoring is already running")
	}

	cm.monitoring = true

	// Start monitoring goroutines
	cm.wg.Add(3)
	go cm.policyEvaluationLoop()
	go cm.violationProcessingLoop()
	go cm.complianceReportingLoop()

	logger.Info("Compliance monitoring started")
	return nil
}

// StopMonitoring stops the compliance monitoring
func (cm *ComplianceMonitor) StopMonitoring() error {
	cm.monitoringMutex.Lock()
	defer cm.monitoringMutex.Unlock()

	if !cm.monitoring {
		return fmt.Errorf("monitoring is not running")
	}

	cm.monitoring = false
	cm.cancel()

	// Wait for goroutines to finish
	done := make(chan struct{})
	go func() {
		cm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Compliance monitoring stopped")
	case <-time.After(30 * time.Second):
		logger.Warn("Timeout waiting for monitoring goroutines to stop")
	}

	return nil
}

// AddPolicy adds a new security policy
func (cm *ComplianceMonitor) AddPolicy(policy *SecurityPolicy) error {
	if err := cm.validatePolicy(policy); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}

	policy.CreatedAt = time.Now()
	policy.UpdatedAt = time.Now()

	cm.policies[policy.ID] = policy

	cm.auditLogger.LogPolicyChange(policy.ID, "added", map[string]interface{}{
		"name":        policy.Name,
		"category":    policy.Category,
		"severity":    policy.Severity,
		"rules_count": len(policy.Rules),
	})

	logger.Infof("Added security policy: %s (%s)", policy.Name, policy.ID)
	return nil
}

// UpdatePolicy updates an existing security policy
func (cm *ComplianceMonitor) UpdatePolicy(policy *SecurityPolicy) error {
	if _, exists := cm.policies[policy.ID]; !exists {
		return fmt.Errorf("policy not found: %s", policy.ID)
	}

	if err := cm.validatePolicy(policy); err != nil {
		return fmt.Errorf("invalid policy: %w", err)
	}

	policy.UpdatedAt = time.Now()
	cm.policies[policy.ID] = policy

	cm.auditLogger.LogPolicyChange(policy.ID, "updated", map[string]interface{}{
		"name":     policy.Name,
		"category": policy.Category,
		"severity": policy.Severity,
	})

	logger.Infof("Updated security policy: %s (%s)", policy.Name, policy.ID)
	return nil
}

// RemovePolicy removes a security policy
func (cm *ComplianceMonitor) RemovePolicy(policyID string) error {
	policy, exists := cm.policies[policyID]
	if !exists {
		return fmt.Errorf("policy not found: %s", policyID)
	}

	delete(cm.policies, policyID)

	cm.auditLogger.LogPolicyChange(policyID, "removed", map[string]interface{}{
		"name": policy.Name,
	})

	logger.Infof("Removed security policy: %s (%s)", policy.Name, policyID)
	return nil
}

// GetPolicy retrieves a security policy by ID
func (cm *ComplianceMonitor) GetPolicy(policyID string) (*SecurityPolicy, error) {
	policy, exists := cm.policies[policyID]
	if !exists {
		return nil, fmt.Errorf("policy not found: %s", policyID)
	}

	return policy, nil
}

// ListPolicies returns all security policies
func (cm *ComplianceMonitor) ListPolicies() []*SecurityPolicy {
	policies := make([]*SecurityPolicy, 0, len(cm.policies))
	for _, policy := range cm.policies {
		policies = append(policies, policy)
	}
	return policies
}

// ReportViolation reports a security violation
func (cm *ComplianceMonitor) ReportViolation(violation *SecurityViolation) error {
	if violation.ID == "" {
		violation.ID = fmt.Sprintf("violation-%d", time.Now().UnixNano())
	}

	if violation.DetectedAt.IsZero() {
		violation.DetectedAt = time.Now()
	}

	violation.Status = ViolationStatusOpen

	cm.violations[violation.ID] = violation

	// Log the violation
	cm.auditLogger.LogViolation(violation)

	// Trigger automated response
	if policy, exists := cm.policies[violation.PolicyID]; exists {
		go cm.responseEngine.ExecuteResponse(violation, policy.Actions)
	}

	logger.Warnf("Security violation detected: %s (Severity: %s)", violation.Title, violation.Severity)
	return nil
}

// GetViolation retrieves a security violation by ID
func (cm *ComplianceMonitor) GetViolation(violationID string) (*SecurityViolation, error) {
	violation, exists := cm.violations[violationID]
	if !exists {
		return nil, fmt.Errorf("violation not found: %s", violationID)
	}

	return violation, nil
}

// ListViolations returns violations with optional filtering
func (cm *ComplianceMonitor) ListViolations(filter *ViolationFilter) []*SecurityViolation {
	violations := make([]*SecurityViolation, 0)

	for _, violation := range cm.violations {
		if filter == nil || cm.matchesFilter(violation, filter) {
			violations = append(violations, violation)
		}
	}

	return violations
}

// ResolveViolation marks a violation as resolved
func (cm *ComplianceMonitor) ResolveViolation(violationID string, resolution string) error {
	violation, exists := cm.violations[violationID]
	if !exists {
		return fmt.Errorf("violation not found: %s", violationID)
	}

	now := time.Now()
	violation.Status = ViolationStatusResolved
	violation.ResolvedAt = &now

	if violation.Metadata == nil {
		violation.Metadata = make(map[string]interface{})
	}
	violation.Metadata["resolution"] = resolution

	logger.Infof("Resolved security violation: %s", violationID)
	return nil
}

// GenerateComplianceReport generates a compliance report for a time period
func (cm *ComplianceMonitor) GenerateComplianceReport(startTime, endTime time.Time) (*ComplianceReport, error) {
	report := &ComplianceReport{
		StartTime:   startTime,
		EndTime:     endTime,
		GeneratedAt: time.Now(),
		Violations:  make(map[ViolationSeverity]int),
		Policies:    make(map[PolicyCategory]int),
		Trends:      make(map[string]interface{}),
	}

	// Count violations by severity
	for _, violation := range cm.violations {
		if violation.DetectedAt.After(startTime) && violation.DetectedAt.Before(endTime) {
			report.TotalViolations++
			report.Violations[violation.Severity]++

			if violation.Status == ViolationStatusResolved {
				report.ResolvedViolations++
			}
		}
	}

	// Count policies by category
	for _, policy := range cm.policies {
		report.TotalPolicies++
		report.Policies[policy.Category]++

		if policy.Enabled {
			report.ActivePolicies++
		}
	}

	// Calculate compliance score (simplified)
	if report.TotalViolations > 0 {
		report.ComplianceScore = float64(report.ResolvedViolations) / float64(report.TotalViolations) * 100
	} else {
		report.ComplianceScore = 100.0
	}

	return report, nil
}

// ViolationFilter represents filters for listing violations
type ViolationFilter struct {
	Severity   *ViolationSeverity `json:"severity,omitempty"`
	Status     *ViolationStatus   `json:"status,omitempty"`
	PolicyID   *string            `json:"policy_id,omitempty"`
	StartTime  *time.Time         `json:"start_time,omitempty"`
	EndTime    *time.Time         `json:"end_time,omitempty"`
}

// ComplianceReport represents a compliance report
type ComplianceReport struct {
	StartTime          time.Time                      `json:"start_time"`
	EndTime            time.Time                      `json:"end_time"`
	GeneratedAt        time.Time                      `json:"generated_at"`
	TotalViolations    int                            `json:"total_violations"`
	ResolvedViolations int                            `json:"resolved_violations"`
	Violations         map[ViolationSeverity]int      `json:"violations_by_severity"`
	TotalPolicies      int                            `json:"total_policies"`
	ActivePolicies     int                            `json:"active_policies"`
	Policies           map[PolicyCategory]int         `json:"policies_by_category"`
	ComplianceScore    float64                        `json:"compliance_score"`
	Trends             map[string]interface{}         `json:"trends"`
}

// policyEvaluationLoop continuously evaluates security policies
func (cm *ComplianceMonitor) policyEvaluationLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.evaluatePolicies()
		case <-cm.ctx.Done():
			return
		}
	}
}

// violationProcessingLoop processes detected violations
func (cm *ComplianceMonitor) violationProcessingLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.processViolations()
		case <-cm.ctx.Done():
			return
		}
	}
}

// complianceReportingLoop generates periodic compliance reports
func (cm *ComplianceMonitor) complianceReportingLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(cm.config.ReportingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.generatePeriodicReport()
		case <-cm.ctx.Done():
			return
		}
	}
}

// evaluatePolicies evaluates all active security policies
func (cm *ComplianceMonitor) evaluatePolicies() {
	for _, policy := range cm.policies {
		if !policy.Enabled {
			continue
		}

		for _, rule := range policy.Rules {
			if !rule.Enabled {
				continue
			}

			if violation := cm.evaluateRule(policy, &rule); violation != nil {
				cm.ReportViolation(violation)
			}
		}
	}
}

// evaluateRule evaluates a single policy rule
func (cm *ComplianceMonitor) evaluateRule(policy *SecurityPolicy, rule *PolicyRule) *SecurityViolation {
	// This is a simplified implementation
	// In a real system, you would implement a proper rule evaluation engine
	
	// For demonstration, we'll create a sample violation for certain conditions
	if rule.Condition == "failed_login_attempts > 5" {
		// This would check actual failed login attempts
		return &SecurityViolation{
			PolicyID:    policy.ID,
			RuleID:      rule.ID,
			Severity:    policy.Severity,
			Title:       "Excessive Failed Login Attempts",
			Description: "Multiple failed login attempts detected",
			Source:      "authentication_system",
			Evidence: map[string]interface{}{
				"attempts": 6,
				"rule":     rule.Condition,
			},
		}
	}

	return nil
}

// processViolations processes open violations
func (cm *ComplianceMonitor) processViolations() {
	for _, violation := range cm.violations {
		if violation.Status == ViolationStatusOpen {
			// Check if violation should be auto-resolved
			if cm.shouldAutoResolve(violation) {
				cm.ResolveViolation(violation.ID, "auto-resolved")
			}
		}
	}
}

// generatePeriodicReport generates a periodic compliance report
func (cm *ComplianceMonitor) generatePeriodicReport() {
	endTime := time.Now()
	startTime := endTime.Add(-cm.config.ReportingInterval)

	report, err := cm.GenerateComplianceReport(startTime, endTime)
	if err != nil {
		logger.Errorf("Failed to generate compliance report: %s", err)
		return
	}

	logger.Infof("Compliance report: Score=%.1f%%, Violations=%d, Resolved=%d",
		report.ComplianceScore, report.TotalViolations, report.ResolvedViolations)
}

// matchesFilter checks if a violation matches the given filter
func (cm *ComplianceMonitor) matchesFilter(violation *SecurityViolation, filter *ViolationFilter) bool {
	if filter.Severity != nil && violation.Severity != *filter.Severity {
		return false
	}

	if filter.Status != nil && violation.Status != *filter.Status {
		return false
	}

	if filter.PolicyID != nil && violation.PolicyID != *filter.PolicyID {
		return false
	}

	if filter.StartTime != nil && violation.DetectedAt.Before(*filter.StartTime) {
		return false
	}

	if filter.EndTime != nil && violation.DetectedAt.After(*filter.EndTime) {
		return false
	}

	return true
}

// shouldAutoResolve determines if a violation should be automatically resolved
func (cm *ComplianceMonitor) shouldAutoResolve(violation *SecurityViolation) bool {
	// Auto-resolve violations older than configured threshold
	if time.Since(violation.DetectedAt) > cm.config.AutoResolveThreshold {
		return true
	}

	// Auto-resolve low severity violations after 24 hours
	if violation.Severity == ViolationSeverityLow && time.Since(violation.DetectedAt) > 24*time.Hour {
		return true
	}

	return false
}

// validatePolicy validates a security policy
func (cm *ComplianceMonitor) validatePolicy(policy *SecurityPolicy) error {
	if policy.ID == "" {
		return fmt.Errorf("policy ID cannot be empty")
	}

	if policy.Name == "" {
		return fmt.Errorf("policy name cannot be empty")
	}

	if len(policy.Rules) == 0 {
		return fmt.Errorf("policy must have at least one rule")
	}

	for i, rule := range policy.Rules {
		if rule.ID == "" {
			return fmt.Errorf("rule %d ID cannot be empty", i)
		}

		if rule.Condition == "" {
			return fmt.Errorf("rule %d condition cannot be empty", i)
		}
	}

	return nil
}

// loadDefaultPolicies loads default security policies
func (cm *ComplianceMonitor) loadDefaultPolicies() error {
	defaultPolicies := []*SecurityPolicy{
		{
			ID:          "access-control-policy",
			Name:        "Access Control Policy",
			Description: "Monitors access control violations",
			Category:    PolicyCategoryAccess,
			Severity:    ViolationSeverityHigh,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "failed-auth-rule",
					Name:      "Failed Authentication Attempts",
					Condition: "failed_login_attempts > 5",
					Enabled:   true,
					Threshold: &Threshold{
						Value:    5,
						Operator: ThresholdOperatorGreater,
					},
				},
			},
			Actions: []ResponseAction{
				{
					Type:    ResponseActionTypeAlert,
					Target:  "security-team",
					Enabled: true,
				},
			},
		},
		{
			ID:          "encryption-policy",
			Name:        "Encryption Policy",
			Description: "Monitors encryption compliance",
			Category:    PolicyCategoryEncryption,
			Severity:    ViolationSeverityCritical,
			Enabled:     true,
			Rules: []PolicyRule{
				{
					ID:        "unencrypted-data-rule",
					Name:      "Unencrypted Data Detection",
					Condition: "unencrypted_datasets > 0",
					Enabled:   true,
				},
			},
			Actions: []ResponseAction{
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
			},
		},
	}

	for _, policy := range defaultPolicies {
		if err := cm.AddPolicy(policy); err != nil {
			return fmt.Errorf("failed to add default policy %s: %w", policy.ID, err)
		}
	}

	return nil
}