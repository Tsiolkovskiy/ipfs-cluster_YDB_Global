package encryption

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// AuditLogger handles security audit logging for encryption operations
type AuditLogger struct {
	config *AuditConfig
	logger *lumberjack.Logger
	mutex  sync.Mutex
}

// AuditEvent represents a security audit event
type AuditEvent struct {
	Timestamp   time.Time              `json:"timestamp"`
	EventType   string                 `json:"event_type"`
	Operation   string                 `json:"operation"`
	Success     bool                   `json:"success"`
	UserID      string                 `json:"user_id,omitempty"`
	Dataset     string                 `json:"dataset,omitempty"`
	KeyID       string                 `json:"key_id,omitempty"`
	SourceIP    string                 `json:"source_ip,omitempty"`
	Details     map[string]interface{} `json:"details,omitempty"`
	Error       string                 `json:"error,omitempty"`
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(config *AuditConfig) (*AuditLogger, error) {
	if config == nil || !config.Enabled {
		return &AuditLogger{config: config}, nil
	}

	// Create log directory if it doesn't exist
	if err := os.MkdirAll(config.LogFile[:len(config.LogFile)-len(filepath.Base(config.LogFile))], 0755); err != nil {
		return nil, fmt.Errorf("failed to create audit log directory: %w", err)
	}

	logger := &lumberjack.Logger{
		Filename:   config.LogFile,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAge,
		Compress:   config.Compress,
	}

	al := &AuditLogger{
		config: config,
		logger: logger,
	}

	// Log audit logger initialization
	al.LogOperation("audit_logger_initialized", map[string]interface{}{
		"log_file": config.LogFile,
	})

	return al, nil
}

// LogOperation logs a successful operation
func (al *AuditLogger) LogOperation(operation string, details map[string]interface{}) {
	if al.config == nil || !al.config.Enabled {
		return
	}

	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: "operation",
		Operation: operation,
		Success:   true,
		Details:   details,
	}

	al.logEvent(event)
}

// LogError logs an error event
func (al *AuditLogger) LogError(operation string, err error, details map[string]interface{}) {
	if al.config == nil || !al.config.Enabled {
		return
	}

	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: "error",
		Operation: operation,
		Success:   false,
		Error:     err.Error(),
		Details:   details,
	}

	al.logEvent(event)
}

// LogAccess logs an access event
func (al *AuditLogger) LogAccess(operation string, userID, sourceIP string, details map[string]interface{}) {
	if al.config == nil || !al.config.Enabled {
		return
	}

	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: "access",
		Operation: operation,
		Success:   true,
		UserID:    userID,
		SourceIP:  sourceIP,
		Details:   details,
	}

	al.logEvent(event)
}

// LogSecurityViolation logs a security violation
func (al *AuditLogger) LogSecurityViolation(operation string, userID, sourceIP string, details map[string]interface{}) {
	if al.config == nil || !al.config.Enabled {
		return
	}

	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: "security_violation",
		Operation: operation,
		Success:   false,
		UserID:    userID,
		SourceIP:  sourceIP,
		Details:   details,
	}

	al.logEvent(event)
}

// LogKeyOperation logs key-related operations
func (al *AuditLogger) LogKeyOperation(operation, keyID, dataset string, success bool, details map[string]interface{}) {
	if al.config == nil || !al.config.Enabled {
		return
	}

	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: "key_operation",
		Operation: operation,
		Success:   success,
		KeyID:     keyID,
		Dataset:   dataset,
		Details:   details,
	}

	al.logEvent(event)
}

// logEvent writes an audit event to the log
func (al *AuditLogger) logEvent(event *AuditEvent) {
	al.mutex.Lock()
	defer al.mutex.Unlock()

	if al.logger == nil {
		return
	}

	// Marshal event to JSON
	eventData, err := json.Marshal(event)
	if err != nil {
		// Fallback to basic logging if JSON marshaling fails
		fmt.Fprintf(al.logger, "AUDIT_ERROR: Failed to marshal event: %v\n", err)
		return
	}

	// Write to log file
	fmt.Fprintf(al.logger, "%s\n", eventData)

	// Send to remote endpoint if configured
	if al.config.RemoteEndpoint != "" {
		go al.sendToRemote(event)
	}
}

// sendToRemote sends audit events to a remote endpoint
func (al *AuditLogger) sendToRemote(event *AuditEvent) {
	// This would implement sending to a remote SIEM or audit system
	// For now, we'll just log that we would send it
	logger.Debugf("Would send audit event to remote endpoint: %s", al.config.RemoteEndpoint)
}

// Close closes the audit logger
func (al *AuditLogger) Close() error {
	if al.logger != nil {
		return al.logger.Close()
	}
	return nil
}

// GetAuditEvents retrieves audit events from the log file (for compliance reporting)
func (al *AuditLogger) GetAuditEvents(startTime, endTime time.Time) ([]*AuditEvent, error) {
	if al.config == nil || !al.config.Enabled {
		return nil, fmt.Errorf("audit logging is not enabled")
	}

	// This is a simplified implementation
	// In a production system, you might want to use a database or structured log format
	events := make([]*AuditEvent, 0)

	// Read log file and parse events
	// This would need to be implemented based on your specific requirements
	// For now, return empty slice
	
	return events, nil
}

// GenerateComplianceReport generates a compliance report for a time period
func (al *AuditLogger) GenerateComplianceReport(startTime, endTime time.Time) (*ComplianceReport, error) {
	events, err := al.GetAuditEvents(startTime, endTime)
	if err != nil {
		return nil, err
	}

	report := &ComplianceReport{
		StartTime:        startTime,
		EndTime:          endTime,
		GeneratedAt:      time.Now(),
		TotalEvents:      len(events),
		OperationCounts:  make(map[string]int),
		ErrorCounts:      make(map[string]int),
		SecurityViolations: 0,
	}

	// Analyze events
	for _, event := range events {
		report.OperationCounts[event.Operation]++
		
		if !event.Success {
			report.ErrorCounts[event.Operation]++
		}
		
		if event.EventType == "security_violation" {
			report.SecurityViolations++
		}
	}

	return report, nil
}

// ComplianceReport represents a compliance audit report
type ComplianceReport struct {
	StartTime          time.Time         `json:"start_time"`
	EndTime            time.Time         `json:"end_time"`
	GeneratedAt        time.Time         `json:"generated_at"`
	TotalEvents        int               `json:"total_events"`
	OperationCounts    map[string]int    `json:"operation_counts"`
	ErrorCounts        map[string]int    `json:"error_counts"`
	SecurityViolations int               `json:"security_violations"`
}