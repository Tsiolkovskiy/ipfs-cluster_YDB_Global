package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Built-in step executors for common operations

// DelayStepExecutor executes a delay step (useful for testing and workflows)
type DelayStepExecutor struct {
	logger *slog.Logger
}

// NewDelayStepExecutor creates a new delay step executor
func NewDelayStepExecutor(logger *slog.Logger) *DelayStepExecutor {
	return &DelayStepExecutor{logger: logger}
}

// Execute executes a delay step
func (d *DelayStepExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	// Get delay duration from parameters
	delayParam, exists := step.Parameters["delay"]
	if !exists {
		return nil, fmt.Errorf("delay parameter is required")
	}
	
	var delay time.Duration
	var err error
	
	switch v := delayParam.(type) {
	case string:
		delay, err = time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid delay duration: %w", err)
		}
	case float64:
		delay = time.Duration(v) * time.Second
	case int:
		delay = time.Duration(v) * time.Second
	default:
		return nil, fmt.Errorf("delay parameter must be a duration string or number of seconds")
	}
	
	d.logger.Info("Executing delay step", 
		"execution_id", execution.ID, "step_id", step.ID, "delay", delay)
	
	// Wait for the specified duration or until context is cancelled
	select {
	case <-time.After(delay):
		return &StepResult{
			Success: true,
			Data: map[string]interface{}{
				"delayed_for": delay.String(),
				"completed_at": time.Now().UTC(),
			},
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetStepType returns the step type this executor handles
func (d *DelayStepExecutor) GetStepType() string {
	return "delay"
}

// LogStepExecutor executes a log step (useful for debugging workflows)
type LogStepExecutor struct {
	logger *slog.Logger
}

// NewLogStepExecutor creates a new log step executor
func NewLogStepExecutor(logger *slog.Logger) *LogStepExecutor {
	return &LogStepExecutor{logger: logger}
}

// Execute executes a log step
func (l *LogStepExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	// Get message from parameters
	messageParam, exists := step.Parameters["message"]
	if !exists {
		return nil, fmt.Errorf("message parameter is required")
	}
	
	message, ok := messageParam.(string)
	if !ok {
		return nil, fmt.Errorf("message parameter must be a string")
	}
	
	// Get log level from parameters (default to info)
	levelParam, exists := step.Parameters["level"]
	level := "info"
	if exists {
		if levelStr, ok := levelParam.(string); ok {
			level = levelStr
		}
	}
	
	// Log the message at the specified level
	switch level {
	case "debug":
		l.logger.Debug(message, "execution_id", execution.ID, "step_id", step.ID)
	case "info":
		l.logger.Info(message, "execution_id", execution.ID, "step_id", step.ID)
	case "warn":
		l.logger.Warn(message, "execution_id", execution.ID, "step_id", step.ID)
	case "error":
		l.logger.Error(message, "execution_id", execution.ID, "step_id", step.ID)
	default:
		l.logger.Info(message, "execution_id", execution.ID, "step_id", step.ID)
	}
	
	return &StepResult{
		Success: true,
		Data: map[string]interface{}{
			"message": message,
			"level":   level,
			"logged_at": time.Now().UTC(),
		},
	}, nil
}

// GetStepType returns the step type this executor handles
func (l *LogStepExecutor) GetStepType() string {
	return "log"
}

// ConditionalStepExecutor executes a conditional step
type ConditionalStepExecutor struct {
	logger *slog.Logger
}

// NewConditionalStepExecutor creates a new conditional step executor
func NewConditionalStepExecutor(logger *slog.Logger) *ConditionalStepExecutor {
	return &ConditionalStepExecutor{logger: logger}
}

// Execute executes a conditional step
func (c *ConditionalStepExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	// Get condition from parameters
	conditionParam, exists := step.Parameters["condition"]
	if !exists {
		return nil, fmt.Errorf("condition parameter is required")
	}
	
	condition, ok := conditionParam.(string)
	if !ok {
		return nil, fmt.Errorf("condition parameter must be a string")
	}
	
	// Simple condition evaluation (in a real implementation, you might use a more sophisticated expression evaluator)
	result := c.evaluateCondition(condition, execution)
	
	c.logger.Info("Executing conditional step", 
		"execution_id", execution.ID, "step_id", step.ID, 
		"condition", condition, "result", result)
	
	return &StepResult{
		Success: true,
		Data: map[string]interface{}{
			"condition": condition,
			"result":    result,
			"evaluated_at": time.Now().UTC(),
		},
	}, nil
}

// GetStepType returns the step type this executor handles
func (c *ConditionalStepExecutor) GetStepType() string {
	return "conditional"
}

// evaluateCondition evaluates a simple condition against the execution context
func (c *ConditionalStepExecutor) evaluateCondition(condition string, execution *WorkflowExecution) bool {
	// This is a very simple implementation. In a real system, you'd want to use
	// a proper expression evaluator like govaluate or similar.
	
	switch condition {
	case "always_true":
		return true
	case "always_false":
		return false
	default:
		// Check if condition exists as a key in execution context
		if value, exists := execution.Context[condition]; exists {
			if boolValue, ok := value.(bool); ok {
				return boolValue
			}
		}
		return false
	}
}

// FailStepExecutor executes a step that always fails (useful for testing error handling)
type FailStepExecutor struct {
	logger *slog.Logger
}

// NewFailStepExecutor creates a new fail step executor
func NewFailStepExecutor(logger *slog.Logger) *FailStepExecutor {
	return &FailStepExecutor{logger: logger}
}

// Execute executes a fail step
func (f *FailStepExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	// Get error message from parameters
	messageParam, exists := step.Parameters["message"]
	message := "Step intentionally failed"
	if exists {
		if msgStr, ok := messageParam.(string); ok {
			message = msgStr
		}
	}
	
	f.logger.Info("Executing fail step", 
		"execution_id", execution.ID, "step_id", step.ID, "message", message)
	
	return &StepResult{
		Success: false,
		Error:   message,
	}, fmt.Errorf("%s", message)
}

// GetStepType returns the step type this executor handles
func (f *FailStepExecutor) GetStepType() string {
	return "fail"
}

// NoOpCompensationHandler provides a no-op compensation handler
type NoOpCompensationHandler struct {
	stepType string
	logger   *slog.Logger
}

// NewNoOpCompensationHandler creates a new no-op compensation handler
func NewNoOpCompensationHandler(stepType string, logger *slog.Logger) *NoOpCompensationHandler {
	return &NoOpCompensationHandler{
		stepType: stepType,
		logger:   logger,
	}
}

// Compensate performs no-op compensation
func (n *NoOpCompensationHandler) Compensate(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) error {
	n.logger.Info("Executing no-op compensation", 
		"execution_id", execution.ID, "step_id", step.ID, "step_type", step.Type)
	return nil
}

// GetStepType returns the step type this handler compensates
func (n *NoOpCompensationHandler) GetStepType() string {
	return n.stepType
}

// LogCompensationHandler provides a compensation handler that logs compensation actions
type LogCompensationHandler struct {
	logger *slog.Logger
}

// NewLogCompensationHandler creates a new log compensation handler
func NewLogCompensationHandler(logger *slog.Logger) *LogCompensationHandler {
	return &LogCompensationHandler{logger: logger}
}

// Compensate performs log compensation (logs that compensation occurred)
func (l *LogCompensationHandler) Compensate(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) error {
	// Find the saga transaction for this step
	var transaction *SagaTransaction
	for _, t := range execution.SagaTransactions {
		if t.StepID == step.ID {
			transaction = t
			break
		}
	}
	
	compensationMessage := "Compensating log step"
	if transaction != nil && transaction.CompensationData != nil {
		if msg, exists := transaction.CompensationData["original_message"]; exists {
			compensationMessage = fmt.Sprintf("Compensating log step with original message: %v", msg)
		}
	}
	
	l.logger.Warn(compensationMessage, 
		"execution_id", execution.ID, "step_id", step.ID, 
		"compensated_at", time.Now().UTC())
	
	return nil
}

// GetStepType returns the step type this handler compensates
func (l *LogCompensationHandler) GetStepType() string {
	return "log"
}