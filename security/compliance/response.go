package compliance

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var responseLogger = logging.Logger("compliance-response")

// ResponseEngine handles automated responses to security violations
type ResponseEngine struct {
	config      *ResponseConfig
	handlers    map[ResponseActionType]ResponseHandler
	auditLogger AuditLogger
	
	// Execution tracking
	executions map[string]*ResponseExecution
	execMutex  sync.RWMutex
	
	// Context management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// ResponseHandler interface for implementing response actions
type ResponseHandler interface {
	Execute(ctx context.Context, action *ResponseAction, violation *SecurityViolation) error
	Validate(action *ResponseAction) error
	GetType() ResponseActionType
}

// ResponseExecution tracks the execution of a response action
type ResponseExecution struct {
	ID          string                 `json:"id"`
	ViolationID string                 `json:"violation_id"`
	Action      *ResponseAction        `json:"action"`
	Status      ExecutionStatus        `json:"status"`
	StartedAt   time.Time              `json:"started_at"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Error       string                 `json:"error,omitempty"`
	RetryCount  int                    `json:"retry_count"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// ExecutionStatus represents the status of a response execution
type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
	ExecutionStatusRetrying  ExecutionStatus = "retrying"
)

// ResponseConfig configures the response engine
type ResponseConfig struct {
	// Enable automated responses
	Enabled bool `json:"enabled"`
	
	// Maximum concurrent executions
	MaxConcurrentExecutions int `json:"max_concurrent_executions"`
	
	// Default timeout for response actions
	DefaultTimeout time.Duration `json:"default_timeout"`
	
	// Maximum retry attempts
	MaxRetries int `json:"max_retries"`
	
	// Retry delay
	RetryDelay time.Duration `json:"retry_delay"`
	
	// Enable dry run mode (log actions without executing)
	DryRun bool `json:"dry_run"`
}

// NewResponseEngine creates a new response engine
func NewResponseEngine(config *ResponseConfig, auditLogger AuditLogger) (*ResponseEngine, error) {
	if config == nil {
		config = &ResponseConfig{
			Enabled:                 true,
			MaxConcurrentExecutions: 10,
			DefaultTimeout:          30 * time.Second,
			MaxRetries:              3,
			RetryDelay:              5 * time.Second,
			DryRun:                  false,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	re := &ResponseEngine{
		config:      config,
		handlers:    make(map[ResponseActionType]ResponseHandler),
		auditLogger: auditLogger,
		executions:  make(map[string]*ResponseExecution),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Register default handlers
	re.registerDefaultHandlers()

	responseLogger.Info("Response engine initialized")
	return re, nil
}

// RegisterHandler registers a response handler
func (re *ResponseEngine) RegisterHandler(handler ResponseHandler) {
	re.handlers[handler.GetType()] = handler
	responseLogger.Infof("Registered response handler: %s", handler.GetType())
}

// ExecuteResponse executes response actions for a violation
func (re *ResponseEngine) ExecuteResponse(violation *SecurityViolation, actions []ResponseAction) {
	if !re.config.Enabled {
		responseLogger.Debug("Response engine is disabled, skipping execution")
		return
	}

	for _, action := range actions {
		if !action.Enabled {
			continue
		}

		re.wg.Add(1)
		go func(a ResponseAction) {
			defer re.wg.Done()
			re.executeAction(violation, &a)
		}(action)
	}
}

// executeAction executes a single response action
func (re *ResponseEngine) executeAction(violation *SecurityViolation, action *ResponseAction) {
	executionID := fmt.Sprintf("exec-%s-%d", violation.ID, time.Now().UnixNano())
	
	execution := &ResponseExecution{
		ID:          executionID,
		ViolationID: violation.ID,
		Action:      action,
		Status:      ExecutionStatusPending,
		StartedAt:   time.Now(),
		RetryCount:  0,
	}

	re.execMutex.Lock()
	re.executions[executionID] = execution
	re.execMutex.Unlock()

	responseLogger.Infof("Starting response execution: %s (Action: %s, Target: %s)", 
		executionID, action.Type, action.Target)

	// Execute with retries
	var err error
	for attempt := 0; attempt <= re.config.MaxRetries; attempt++ {
		if attempt > 0 {
			execution.Status = ExecutionStatusRetrying
			execution.RetryCount = attempt
			responseLogger.Infof("Retrying response execution: %s (Attempt: %d)", executionID, attempt)
			time.Sleep(re.config.RetryDelay)
		} else {
			execution.Status = ExecutionStatusRunning
		}

		err = re.performAction(violation, action)
		if err == nil {
			break
		}

		responseLogger.Warnf("Response execution failed: %s (Attempt: %d, Error: %s)", 
			executionID, attempt+1, err.Error())
	}

	// Update execution status
	now := time.Now()
	execution.CompletedAt = &now

	if err != nil {
		execution.Status = ExecutionStatusFailed
		execution.Error = err.Error()
		responseLogger.Errorf("Response execution failed permanently: %s (Error: %s)", executionID, err.Error())
	} else {
		execution.Status = ExecutionStatusCompleted
		responseLogger.Infof("Response execution completed successfully: %s", executionID)
	}

	// Log to audit
	re.auditLogger.LogResponse(action, err == nil, map[string]interface{}{
		"execution_id":  executionID,
		"violation_id":  violation.ID,
		"retry_count":   execution.RetryCount,
		"duration":      execution.CompletedAt.Sub(execution.StartedAt).String(),
		"error":         execution.Error,
	})
}

// performAction performs the actual response action
func (re *ResponseEngine) performAction(violation *SecurityViolation, action *ResponseAction) error {
	if re.config.DryRun {
		responseLogger.Infof("DRY RUN: Would execute %s action on %s", action.Type, action.Target)
		return nil
	}

	handler, exists := re.handlers[action.Type]
	if !exists {
		return fmt.Errorf("no handler registered for action type: %s", action.Type)
	}

	// Validate action
	if err := handler.Validate(action); err != nil {
		return fmt.Errorf("action validation failed: %w", err)
	}

	// Set timeout
	timeout := action.Timeout
	if timeout == 0 {
		timeout = re.config.DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(re.ctx, timeout)
	defer cancel()

	// Execute action
	return handler.Execute(ctx, action, violation)
}

// GetExecution retrieves a response execution by ID
func (re *ResponseEngine) GetExecution(executionID string) (*ResponseExecution, error) {
	re.execMutex.RLock()
	defer re.execMutex.RUnlock()

	execution, exists := re.executions[executionID]
	if !exists {
		return nil, fmt.Errorf("execution not found: %s", executionID)
	}

	return execution, nil
}

// ListExecutions returns all response executions with optional filtering
func (re *ResponseEngine) ListExecutions(filter *ExecutionFilter) []*ResponseExecution {
	re.execMutex.RLock()
	defer re.execMutex.RUnlock()

	executions := make([]*ResponseExecution, 0)
	for _, execution := range re.executions {
		if filter == nil || re.matchesExecutionFilter(execution, filter) {
			executions = append(executions, execution)
		}
	}

	return executions
}

// ExecutionFilter represents filters for listing executions
type ExecutionFilter struct {
	Status      *ExecutionStatus `json:"status,omitempty"`
	ActionType  *ResponseActionType `json:"action_type,omitempty"`
	ViolationID *string          `json:"violation_id,omitempty"`
	StartTime   *time.Time       `json:"start_time,omitempty"`
	EndTime     *time.Time       `json:"end_time,omitempty"`
}

// matchesExecutionFilter checks if an execution matches the filter
func (re *ResponseEngine) matchesExecutionFilter(execution *ResponseExecution, filter *ExecutionFilter) bool {
	if filter.Status != nil && execution.Status != *filter.Status {
		return false
	}

	if filter.ActionType != nil && execution.Action.Type != *filter.ActionType {
		return false
	}

	if filter.ViolationID != nil && execution.ViolationID != *filter.ViolationID {
		return false
	}

	if filter.StartTime != nil && execution.StartedAt.Before(*filter.StartTime) {
		return false
	}

	if filter.EndTime != nil && execution.StartedAt.After(*filter.EndTime) {
		return false
	}

	return true
}

// Shutdown gracefully shuts down the response engine
func (re *ResponseEngine) Shutdown(ctx context.Context) error {
	responseLogger.Info("Shutting down response engine")
	
	re.cancel()
	
	// Wait for all executions to complete
	done := make(chan struct{})
	go func() {
		re.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		responseLogger.Info("All response executions completed")
	case <-ctx.Done():
		responseLogger.Warn("Shutdown timeout reached, some executions may be incomplete")
	}
	
	return nil
}

// registerDefaultHandlers registers default response handlers
func (re *ResponseEngine) registerDefaultHandlers() {
	re.RegisterHandler(&AlertHandler{})
	re.RegisterHandler(&LogHandler{})
	re.RegisterHandler(&BlockHandler{})
	re.RegisterHandler(&NotifyHandler{})
}

// AlertHandler handles alert response actions
type AlertHandler struct{}

func (h *AlertHandler) GetType() ResponseActionType {
	return ResponseActionTypeAlert
}

func (h *AlertHandler) Validate(action *ResponseAction) error {
	if action.Target == "" {
		return fmt.Errorf("alert target cannot be empty")
	}
	return nil
}

func (h *AlertHandler) Execute(ctx context.Context, action *ResponseAction, violation *SecurityViolation) error {
	responseLogger.Infof("ALERT: %s - %s (Severity: %s, Target: %s)", 
		violation.Title, violation.Description, violation.Severity, action.Target)
	
	// In a real implementation, this would send alerts via email, Slack, PagerDuty, etc.
	return nil
}

// LogHandler handles log response actions
type LogHandler struct{}

func (h *LogHandler) GetType() ResponseActionType {
	return ResponseActionTypeLog
}

func (h *LogHandler) Validate(action *ResponseAction) error {
	return nil
}

func (h *LogHandler) Execute(ctx context.Context, action *ResponseAction, violation *SecurityViolation) error {
	responseLogger.Infof("SECURITY LOG: Violation %s - %s (Severity: %s)", 
		violation.ID, violation.Title, violation.Severity)
	
	// In a real implementation, this would write to security logs, SIEM, etc.
	return nil
}

// BlockHandler handles block response actions
type BlockHandler struct{}

func (h *BlockHandler) GetType() ResponseActionType {
	return ResponseActionTypeBlock
}

func (h *BlockHandler) Validate(action *ResponseAction) error {
	if action.Target == "" {
		return fmt.Errorf("block target cannot be empty")
	}
	return nil
}

func (h *BlockHandler) Execute(ctx context.Context, action *ResponseAction, violation *SecurityViolation) error {
	responseLogger.Infof("BLOCKING: Target %s due to violation %s", action.Target, violation.ID)
	
	// In a real implementation, this would block IP addresses, users, etc.
	// For example, updating firewall rules, revoking access tokens, etc.
	return nil
}

// NotifyHandler handles notification response actions
type NotifyHandler struct{}

func (h *NotifyHandler) GetType() ResponseActionType {
	return ResponseActionTypeNotify
}

func (h *NotifyHandler) Validate(action *ResponseAction) error {
	if action.Target == "" {
		return fmt.Errorf("notification target cannot be empty")
	}
	return nil
}

func (h *NotifyHandler) Execute(ctx context.Context, action *ResponseAction, violation *SecurityViolation) error {
	responseLogger.Infof("NOTIFICATION: Sending to %s about violation %s", action.Target, violation.ID)
	
	// In a real implementation, this would send notifications via various channels
	return nil
}