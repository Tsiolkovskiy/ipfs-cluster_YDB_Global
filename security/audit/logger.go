// Package audit provides unified audit logging for all security components
package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// UnifiedAuditLogger provides audit logging for all security components
type UnifiedAuditLogger struct {
	config *Config
	logger *lumberjack.Logger
	mutex  sync.Mutex
}

// Config configures the unified audit logger
type Config struct {
	Enabled     bool   `json:"enabled"`
	LogFile     string `json:"log_file"`
	MaxSize     int    `json:"max_size"`
	MaxBackups  int    `json:"max_backups"`
	MaxAge      int    `json:"max_age"`
	Compress    bool   `json:"compress"`
}

// AuditEvent represents a unified audit event
type AuditEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	EventType   string                 `json:"event_type"`
	Component   string                 `json:"component"`
	Operation   string                 `json:"operation"`
	Success     bool                   `json:"success"`
	UserID      string                 `json:"user_id,omitempty"`
	SourceIP    string                 `json:"source_ip,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

// NewUnifiedAuditLogger creates a new unified audit logger
func NewUnifiedAuditLogger(config *Config) (*UnifiedAuditLogger, error) {
	if config == nil || !config.Enabled {
		return &UnifiedAuditLogger{config: config}, nil
	}

	// Create log directory if it doesn't exist
	logDir := filepath.Dir(config.LogFile)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create audit log directory: %w", err)
	}

	logger := &lumberjack.Logger{
		Filename:   config.LogFile,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAge,
		Compress:   config.Compress,
	}

	al := &UnifiedAuditLogger{
		config: config,
		logger: logger,
	}

	// Log initialization
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "system",
		Component: "audit",
		Operation: "logger_initialized",
		Success:   true,
		Details: map[string]interface{}{
			"log_file": config.LogFile,
		},
	})

	return al, nil
}

// LogOperation logs a successful operation (encryption interface)
func (al *UnifiedAuditLogger) LogOperation(operation string, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "operation",
		Component: "encryption",
		Operation: operation,
		Success:   true,
		Details:   details,
	})
}

// LogError logs an error event (encryption interface)
func (al *UnifiedAuditLogger) LogError(operation string, err error, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "error",
		Component: "encryption",
		Operation: operation,
		Success:   false,
		Error:     err.Error(),
		Details:   details,
	})
}

// LogAccess logs an access event (access control interface)
func (al *UnifiedAuditLogger) LogAccess(operation string, userID, sourceIP string, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "access",
		Component: "access_control",
		Operation: operation,
		Success:   true,
		UserID:    userID,
		SourceIP:  sourceIP,
		Details:   details,
	})
}

// LogSecurityViolation logs a security violation (access control interface)
func (al *UnifiedAuditLogger) LogSecurityViolation(operation string, userID, sourceIP string, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "security_violation",
		Component: "access_control",
		Operation: operation,
		Success:   false,
		UserID:    userID,
		SourceIP:  sourceIP,
		Details:   details,
	})
}

// LogViolation logs a compliance violation (compliance interface)
func (al *UnifiedAuditLogger) LogViolation(violation interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "compliance_violation",
		Component: "compliance",
		Operation: "violation_detected",
		Success:   false,
		Details: map[string]interface{}{
			"violation": violation,
		},
	})
}

// LogPolicyChange logs a policy change (compliance interface)
func (al *UnifiedAuditLogger) LogPolicyChange(policyID, action string, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "policy_change",
		Component: "compliance",
		Operation: action,
		Success:   true,
		Details: map[string]interface{}{
			"policy_id": policyID,
			"details":   details,
		},
	})
}

// LogResponse logs a response action (compliance interface)
func (al *UnifiedAuditLogger) LogResponse(action interface{}, success bool, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "response_action",
		Component: "compliance",
		Operation: "response_executed",
		Success:   success,
		Details: map[string]interface{}{
			"action":  action,
			"details": details,
		},
	})
}

// LogKeyOperation logs key-related operations (encryption interface)
func (al *UnifiedAuditLogger) LogKeyOperation(operation, keyID, dataset string, success bool, details map[string]interface{}) {
	al.logEvent(&AuditEvent{
		Timestamp: time.Now(),
		EventType: "key_operation",
		Component: "encryption",
		Operation: operation,
		Success:   success,
		Details: map[string]interface{}{
			"key_id":  keyID,
			"dataset": dataset,
			"details": details,
		},
	})
}

// logEvent writes an audit event to the log
func (al *UnifiedAuditLogger) logEvent(event *AuditEvent) {
	if al.config == nil || !al.config.Enabled || al.logger == nil {
		return
	}

	al.mutex.Lock()
	defer al.mutex.Unlock()

	// Marshal event to JSON
	eventData, err := json.Marshal(event)
	if err != nil {
		fmt.Fprintf(al.logger, "AUDIT_ERROR: Failed to marshal event: %v\n", err)
		return
	}

	// Write to log file
	fmt.Fprintf(al.logger, "%s\n", eventData)
}

// Close closes the audit logger
func (al *UnifiedAuditLogger) Close() error {
	if al.logger != nil {
		return al.logger.Close()
	}
	return nil
}

// DefaultConfig returns a default audit configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:    true,
		LogFile:    "/var/log/ipfs-cluster/security-audit.log",
		MaxSize:    100, // 100MB
		MaxBackups: 10,
		MaxAge:     30, // 30 days
		Compress:   true,
	}
}