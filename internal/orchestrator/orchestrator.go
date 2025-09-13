package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/global-data-controller/gdc/internal/eventbus"
)

// Orchestrator defines the workflow orchestration interface
type Orchestrator interface {
	// Workflow management
	StartWorkflow(ctx context.Context, workflow *WorkflowDefinition) (*WorkflowExecution, error)
	GetWorkflowStatus(ctx context.Context, id string) (*WorkflowStatus, error)
	CancelWorkflow(ctx context.Context, id string) error
	
	// Monitoring
	ListActiveWorkflows(ctx context.Context) ([]*WorkflowExecution, error)
	GetWorkflowHistory(ctx context.Context, filter *HistoryFilter) ([]*WorkflowExecution, error)
	
	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// WorkflowEngine implements the Orchestrator interface with saga pattern support
type WorkflowEngine struct {
	mu                sync.RWMutex
	executions        map[string]*WorkflowExecution
	stepExecutors     map[string]StepExecutor
	compensators      map[string]CompensationHandler
	eventBus          eventbus.EventBus
	logger            *slog.Logger
	checkpointStore   CheckpointStore
	retryConfig       *RetryConfig
	
	// Control channels
	stopCh            chan struct{}
	executorPool      chan struct{} // Semaphore for concurrent executions
	
	// Metrics
	activeWorkflows   int64
	completedWorkflows int64
	failedWorkflows   int64
}

// StepExecutor defines the interface for executing workflow steps
type StepExecutor interface {
	Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error)
	GetStepType() string
}

// CompensationHandler defines the interface for handling step compensation
type CompensationHandler interface {
	Compensate(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) error
	GetStepType() string
}

// CheckpointStore defines the interface for persisting workflow state
type CheckpointStore interface {
	SaveCheckpoint(ctx context.Context, execution *WorkflowExecution) error
	LoadCheckpoint(ctx context.Context, executionID string) (*WorkflowExecution, error)
	DeleteCheckpoint(ctx context.Context, executionID string) error
	ListCheckpoints(ctx context.Context, filter *HistoryFilter) ([]*WorkflowExecution, error)
}

// RetryConfig defines retry behavior
type RetryConfig struct {
	MaxAttempts     int           `json:"max_attempts"`
	InitialDelay    time.Duration `json:"initial_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	BackoffFactor   float64       `json:"backoff_factor"`
	Jitter          bool          `json:"jitter"`
}

// StepResult represents the result of step execution
type StepResult struct {
	Success     bool                   `json:"success"`
	Data        map[string]interface{} `json:"data,omitempty"`
	Error       string                 `json:"error,omitempty"`
	Checkpoint  map[string]interface{} `json:"checkpoint,omitempty"`
}

// SagaTransaction represents a compensatable transaction
type SagaTransaction struct {
	StepID          string                 `json:"step_id"`
	CompensationData map[string]interface{} `json:"compensation_data"`
	ExecutedAt      time.Time              `json:"executed_at"`
}

// WorkflowDefinition represents a workflow definition
type WorkflowDefinition struct {
	Type        string                 `json:"type"`
	Steps       []*WorkflowStep        `json:"steps"`
	Retries     int                    `json:"retries"`
	Timeout     time.Duration          `json:"timeout"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// WorkflowStep represents a single step in a workflow
type WorkflowStep struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	Parameters   map[string]interface{} `json:"parameters"`
	Dependencies []string               `json:"dependencies,omitempty"`
	Retries      int                    `json:"retries"`
	Timeout      time.Duration          `json:"timeout"`
}

// WorkflowExecution represents an executing workflow
type WorkflowExecution struct {
	ID              string                 `json:"id"`
	Definition      *WorkflowDefinition    `json:"definition"`
	Status          WorkflowExecutionStatus `json:"status"`
	Steps           []*StepExecution       `json:"steps"`
	StartedAt       time.Time              `json:"started_at"`
	CompletedAt     *time.Time             `json:"completed_at,omitempty"`
	Error           string                 `json:"error,omitempty"`
	TraceID         string                 `json:"trace_id,omitempty"`
	
	// Saga-specific fields
	SagaTransactions []*SagaTransaction     `json:"saga_transactions,omitempty"`
	CompensationMode bool                   `json:"compensation_mode"`
	
	// Execution context
	Context         map[string]interface{} `json:"context,omitempty"`
	
	// Internal fields (not serialized)
	cancelFunc      context.CancelFunc     `json:"-"`
	mu              sync.RWMutex           `json:"-"`
}

// WorkflowExecutionStatus represents the status of a workflow execution
type WorkflowExecutionStatus string

const (
	WorkflowStatusPending   WorkflowExecutionStatus = "pending"
	WorkflowStatusRunning   WorkflowExecutionStatus = "running"
	WorkflowStatusCompleted WorkflowExecutionStatus = "completed"
	WorkflowStatusFailed    WorkflowExecutionStatus = "failed"
	WorkflowStatusCancelled WorkflowExecutionStatus = "cancelled"
)

// StepExecution represents the execution of a workflow step
type StepExecution struct {
	ID          string              `json:"id"`
	Status      StepExecutionStatus `json:"status"`
	StartedAt   *time.Time          `json:"started_at,omitempty"`
	CompletedAt *time.Time          `json:"completed_at,omitempty"`
	Error       string              `json:"error,omitempty"`
	Retries     int                 `json:"retries"`
}

// StepExecutionStatus represents the status of a step execution
type StepExecutionStatus string

const (
	StepStatusPending   StepExecutionStatus = "pending"
	StepStatusRunning   StepExecutionStatus = "running"
	StepStatusCompleted StepExecutionStatus = "completed"
	StepStatusFailed    StepExecutionStatus = "failed"
	StepStatusSkipped   StepExecutionStatus = "skipped"
)

// WorkflowStatus represents the current status of a workflow
type WorkflowStatus struct {
	ID       string                  `json:"id"`
	Status   WorkflowExecutionStatus `json:"status"`
	Progress float64                 `json:"progress"` // 0.0 to 1.0
	Message  string                  `json:"message,omitempty"`
}

// HistoryFilter represents filtering options for workflow history
type HistoryFilter struct {
	Status    WorkflowExecutionStatus `json:"status,omitempty"`
	Type      string                  `json:"type,omitempty"`
	StartTime *time.Time              `json:"start_time,omitempty"`
	EndTime   *time.Time              `json:"end_time,omitempty"`
	Limit     int                     `json:"limit,omitempty"`
	Offset    int                     `json:"offset,omitempty"`
}

// NewWorkflowEngine creates a new workflow engine
func NewWorkflowEngine(eventBus eventbus.EventBus, checkpointStore CheckpointStore, logger *slog.Logger) *WorkflowEngine {
	return &WorkflowEngine{
		executions:      make(map[string]*WorkflowExecution),
		stepExecutors:   make(map[string]StepExecutor),
		compensators:    make(map[string]CompensationHandler),
		eventBus:        eventBus,
		logger:          logger,
		checkpointStore: checkpointStore,
		retryConfig: &RetryConfig{
			MaxAttempts:   3,
			InitialDelay:  time.Second,
			MaxDelay:      time.Minute * 5,
			BackoffFactor: 2.0,
			Jitter:        true,
		},
		stopCh:       make(chan struct{}),
		executorPool: make(chan struct{}, 100), // Max 100 concurrent workflows
	}
}

// RegisterStepExecutor registers a step executor for a specific step type
func (we *WorkflowEngine) RegisterStepExecutor(executor StepExecutor) {
	we.mu.Lock()
	defer we.mu.Unlock()
	we.stepExecutors[executor.GetStepType()] = executor
}

// RegisterCompensationHandler registers a compensation handler for a specific step type
func (we *WorkflowEngine) RegisterCompensationHandler(handler CompensationHandler) {
	we.mu.Lock()
	defer we.mu.Unlock()
	we.compensators[handler.GetStepType()] = handler
}

// Start starts the workflow engine
func (we *WorkflowEngine) Start(ctx context.Context) error {
	we.logger.Info("Starting workflow engine")
	
	// Load existing checkpoints and resume workflows
	if err := we.loadAndResumeWorkflows(ctx); err != nil {
		return fmt.Errorf("failed to load and resume workflows: %w", err)
	}
	
	we.logger.Info("Workflow engine started")
	return nil
}

// Stop stops the workflow engine gracefully
func (we *WorkflowEngine) Stop(ctx context.Context) error {
	we.logger.Info("Stopping workflow engine")
	
	close(we.stopCh)
	
	// Wait for all active workflows to complete or timeout
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout.C:
			we.logger.Warn("Timeout waiting for workflows to complete, forcing shutdown")
			return nil
		case <-ticker.C:
			we.mu.RLock()
			activeCount := len(we.executions)
			we.mu.RUnlock()
			
			if activeCount == 0 {
				we.logger.Info("All workflows completed, engine stopped")
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// StartWorkflow starts a new workflow execution
func (we *WorkflowEngine) StartWorkflow(ctx context.Context, workflow *WorkflowDefinition) (*WorkflowExecution, error) {
	if workflow == nil {
		return nil, fmt.Errorf("workflow definition cannot be nil")
	}
	
	if err := we.validateWorkflowDefinition(workflow); err != nil {
		return nil, fmt.Errorf("invalid workflow definition: %w", err)
	}
	
	execution := &WorkflowExecution{
		ID:         uuid.New().String(),
		Definition: workflow,
		Status:     WorkflowStatusPending,
		Steps:      make([]*StepExecution, len(workflow.Steps)),
		StartedAt:  time.Now().UTC(),
		Context:    make(map[string]interface{}),
		TraceID:    extractTraceID(ctx),
	}
	
	// Initialize step executions
	for i, step := range workflow.Steps {
		execution.Steps[i] = &StepExecution{
			ID:      step.ID,
			Status:  StepStatusPending,
			Retries: 0,
		}
	}
	
	// Store execution
	we.mu.Lock()
	we.executions[execution.ID] = execution
	we.activeWorkflows++
	we.mu.Unlock()
	
	// Save initial checkpoint
	if err := we.checkpointStore.SaveCheckpoint(ctx, execution); err != nil {
		we.logger.Error("Failed to save initial checkpoint", "execution_id", execution.ID, "error", err)
	}
	
	// Start execution asynchronously
	go we.executeWorkflow(ctx, execution)
	
	we.logger.Info("Started workflow", "execution_id", execution.ID, "type", workflow.Type)
	
	return execution, nil
}

// GetWorkflowStatus returns the current status of a workflow
func (we *WorkflowEngine) GetWorkflowStatus(ctx context.Context, id string) (*WorkflowStatus, error) {
	we.mu.RLock()
	execution, exists := we.executions[id]
	we.mu.RUnlock()
	
	if !exists {
		// Try to load from checkpoint store
		var err error
		execution, err = we.checkpointStore.LoadCheckpoint(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("workflow not found: %s", id)
		}
	}
	
	execution.mu.RLock()
	defer execution.mu.RUnlock()
	
	progress := we.calculateProgress(execution)
	message := we.getStatusMessage(execution)
	
	return &WorkflowStatus{
		ID:       execution.ID,
		Status:   execution.Status,
		Progress: progress,
		Message:  message,
	}, nil
}

// CancelWorkflow cancels a running workflow
func (we *WorkflowEngine) CancelWorkflow(ctx context.Context, id string) error {
	we.mu.RLock()
	execution, exists := we.executions[id]
	we.mu.RUnlock()
	
	if !exists {
		return fmt.Errorf("workflow not found: %s", id)
	}
	
	execution.mu.Lock()
	defer execution.mu.Unlock()
	
	if execution.Status == WorkflowStatusCompleted || execution.Status == WorkflowStatusFailed || execution.Status == WorkflowStatusCancelled {
		return fmt.Errorf("workflow %s is already in terminal state: %s", id, execution.Status)
	}
	
	execution.Status = WorkflowStatusCancelled
	if execution.cancelFunc != nil {
		execution.cancelFunc()
	}
	
	// Save checkpoint
	if err := we.checkpointStore.SaveCheckpoint(ctx, execution); err != nil {
		we.logger.Error("Failed to save checkpoint after cancellation", "execution_id", id, "error", err)
	}
	
	// Publish cancellation event
	we.publishWorkflowEvent(ctx, execution, "cancelled", nil)
	
	we.logger.Info("Cancelled workflow", "execution_id", id)
	
	return nil
}

// ListActiveWorkflows returns all currently active workflows
func (we *WorkflowEngine) ListActiveWorkflows(ctx context.Context) ([]*WorkflowExecution, error) {
	we.mu.RLock()
	defer we.mu.RUnlock()
	
	var active []*WorkflowExecution
	for _, execution := range we.executions {
		execution.mu.RLock()
		if execution.Status == WorkflowStatusPending || execution.Status == WorkflowStatusRunning {
			active = append(active, execution)
		}
		execution.mu.RUnlock()
	}
	
	return active, nil
}

// GetWorkflowHistory returns workflow history based on filter
func (we *WorkflowEngine) GetWorkflowHistory(ctx context.Context, filter *HistoryFilter) ([]*WorkflowExecution, error) {
	return we.checkpointStore.ListCheckpoints(ctx, filter)
}

// executeWorkflow executes a workflow with saga pattern support
func (we *WorkflowEngine) executeWorkflow(parentCtx context.Context, execution *WorkflowExecution) {
	// Acquire execution slot
	select {
	case we.executorPool <- struct{}{}:
		defer func() { <-we.executorPool }()
	case <-we.stopCh:
		return
	}
	
	// Create cancellable context
	ctx, cancel := context.WithCancel(parentCtx)
	execution.mu.Lock()
	execution.cancelFunc = cancel
	execution.Status = WorkflowStatusRunning
	execution.mu.Unlock()
	
	defer func() {
		cancel()
		we.cleanupExecution(execution)
	}()
	
	// Apply workflow timeout if specified
	if execution.Definition.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, execution.Definition.Timeout)
		defer cancel()
	}
	
	we.logger.Info("Executing workflow", "execution_id", execution.ID, "type", execution.Definition.Type)
	
	// Execute steps in order
	var lastError error
	for i, step := range execution.Definition.Steps {
		select {
		case <-ctx.Done():
			we.handleWorkflowCancellation(parentCtx, execution, ctx.Err())
			return
		case <-we.stopCh:
			we.handleWorkflowCancellation(parentCtx, execution, fmt.Errorf("engine shutting down"))
			return
		default:
		}
		
		// Check dependencies
		if !we.areDependenciesSatisfied(execution, step) {
			we.logger.Warn("Step dependencies not satisfied, skipping", 
				"execution_id", execution.ID, "step_id", step.ID)
			execution.Steps[i].Status = StepStatusSkipped
			continue
		}
		
		// Execute step with retry logic
		stepResult, err := we.executeStepWithRetry(ctx, step, execution)
		if err != nil {
			lastError = err
			execution.Steps[i].Status = StepStatusFailed
			execution.Steps[i].Error = err.Error()
			
			we.logger.Error("Step execution failed", 
				"execution_id", execution.ID, "step_id", step.ID, "error", err)
			
			// Start compensation if this is a saga workflow
			if we.isSagaWorkflow(execution.Definition) {
				we.logger.Info("Starting saga compensation", "execution_id", execution.ID)
				if compensationErr := we.compensateWorkflow(ctx, execution); compensationErr != nil {
					we.logger.Error("Saga compensation failed", 
						"execution_id", execution.ID, "error", compensationErr)
				}
			}
			
			break
		}
		
		// Mark step as completed
		execution.Steps[i].Status = StepStatusCompleted
		now := time.Now().UTC()
		execution.Steps[i].CompletedAt = &now
		
		// Record saga transaction if successful
		if we.isSagaWorkflow(execution.Definition) && stepResult.Success {
			sagaTransaction := &SagaTransaction{
				StepID:          step.ID,
				CompensationData: stepResult.Checkpoint,
				ExecutedAt:      now,
			}
			execution.SagaTransactions = append(execution.SagaTransactions, sagaTransaction)
		}
		
		// Update execution context with step results
		if stepResult.Data != nil {
			for key, value := range stepResult.Data {
				execution.Context[key] = value
			}
		}
		
		// Save checkpoint after each step
		if err := we.checkpointStore.SaveCheckpoint(ctx, execution); err != nil {
			we.logger.Error("Failed to save checkpoint", "execution_id", execution.ID, "error", err)
		}
		
		// Publish progress event
		we.publishStepProgressEvent(ctx, execution, step.ID, "completed", 1.0)
	}
	
	// Determine final status
	execution.mu.Lock()
	if lastError != nil {
		execution.Status = WorkflowStatusFailed
		execution.Error = lastError.Error()
	} else {
		execution.Status = WorkflowStatusCompleted
	}
	now := time.Now().UTC()
	execution.CompletedAt = &now
	execution.mu.Unlock()
	
	// Save final checkpoint
	if err := we.checkpointStore.SaveCheckpoint(parentCtx, execution); err != nil {
		we.logger.Error("Failed to save final checkpoint", "execution_id", execution.ID, "error", err)
	}
	
	// Publish completion event
	if lastError != nil {
		we.publishWorkflowEvent(parentCtx, execution, "failed", map[string]interface{}{
			"error": lastError.Error(),
		})
		we.failedWorkflows++
	} else {
		we.publishWorkflowEvent(parentCtx, execution, "completed", execution.Context)
		we.completedWorkflows++
	}
	
	we.logger.Info("Workflow execution completed", 
		"execution_id", execution.ID, "status", execution.Status)
}

// executeStepWithRetry executes a step with exponential backoff retry logic
func (we *WorkflowEngine) executeStepWithRetry(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	executor, exists := we.stepExecutors[step.Type]
	if !exists {
		return nil, fmt.Errorf("no executor found for step type: %s", step.Type)
	}
	
	maxRetries := step.Retries
	if maxRetries == 0 {
		maxRetries = we.retryConfig.MaxAttempts
	}
	
	var lastError error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Update step execution info
		stepExec := we.findStepExecution(execution, step.ID)
		if stepExec != nil {
			stepExec.Retries = attempt
			if attempt == 0 {
				now := time.Now().UTC()
				stepExec.StartedAt = &now
			}
		}
		
		// Publish progress event
		we.publishStepProgressEvent(ctx, execution, step.ID, "running", 0.0)
		
		// Execute step
		result, err := executor.Execute(ctx, step, execution)
		if err == nil && result != nil && result.Success {
			return result, nil
		}
		
		lastError = err
		if result != nil && result.Error != "" {
			lastError = fmt.Errorf("%s", result.Error)
		}
		
		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		
		// Don't retry if this is the last attempt
		if attempt >= maxRetries {
			break
		}
		
		// Calculate retry delay
		delay := we.calculateRetryDelay(attempt)
		we.logger.Warn("Step execution failed, retrying", 
			"execution_id", execution.ID, "step_id", step.ID, 
			"attempt", attempt+1, "max_retries", maxRetries+1, 
			"delay", delay, "error", lastError)
		
		// Wait before retry
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	
	return nil, fmt.Errorf("step failed after %d attempts: %w", maxRetries+1, lastError)
}

// compensateWorkflow performs saga compensation by executing compensation handlers in reverse order
func (we *WorkflowEngine) compensateWorkflow(ctx context.Context, execution *WorkflowExecution) error {
	execution.mu.Lock()
	execution.CompensationMode = true
	execution.mu.Unlock()
	
	we.logger.Info("Starting workflow compensation", "execution_id", execution.ID)
	
	// Execute compensations in reverse order
	for i := len(execution.SagaTransactions) - 1; i >= 0; i-- {
		transaction := execution.SagaTransactions[i]
		
		// Find the step definition
		var step *WorkflowStep
		for _, s := range execution.Definition.Steps {
			if s.ID == transaction.StepID {
				step = s
				break
			}
		}
		
		if step == nil {
			we.logger.Error("Step not found for compensation", 
				"execution_id", execution.ID, "step_id", transaction.StepID)
			continue
		}
		
		// Find compensation handler
		handler, exists := we.compensators[step.Type]
		if !exists {
			we.logger.Warn("No compensation handler found for step type", 
				"execution_id", execution.ID, "step_id", step.ID, "step_type", step.Type)
			continue
		}
		
		// Execute compensation
		we.logger.Info("Executing compensation", 
			"execution_id", execution.ID, "step_id", step.ID)
		
		if err := handler.Compensate(ctx, step, execution); err != nil {
			we.logger.Error("Compensation failed", 
				"execution_id", execution.ID, "step_id", step.ID, "error", err)
			// Continue with other compensations even if one fails
		} else {
			we.logger.Info("Compensation completed", 
				"execution_id", execution.ID, "step_id", step.ID)
		}
	}
	
	we.logger.Info("Workflow compensation completed", "execution_id", execution.ID)
	return nil
}

// Helper methods

func (we *WorkflowEngine) validateWorkflowDefinition(workflow *WorkflowDefinition) error {
	if workflow.Type == "" {
		return fmt.Errorf("workflow type is required")
	}
	
	if len(workflow.Steps) == 0 {
		return fmt.Errorf("workflow must have at least one step")
	}
	
	stepIDs := make(map[string]bool)
	for _, step := range workflow.Steps {
		if step.ID == "" {
			return fmt.Errorf("step ID is required")
		}
		
		if stepIDs[step.ID] {
			return fmt.Errorf("duplicate step ID: %s", step.ID)
		}
		stepIDs[step.ID] = true
		
		if step.Type == "" {
			return fmt.Errorf("step type is required for step %s", step.ID)
		}
		
		// Validate dependencies exist
		for _, depID := range step.Dependencies {
			if !stepIDs[depID] {
				return fmt.Errorf("step %s depends on non-existent step %s", step.ID, depID)
			}
		}
	}
	
	return nil
}

func (we *WorkflowEngine) areDependenciesSatisfied(execution *WorkflowExecution, step *WorkflowStep) bool {
	for _, depID := range step.Dependencies {
		stepExec := we.findStepExecution(execution, depID)
		if stepExec == nil || stepExec.Status != StepStatusCompleted {
			return false
		}
	}
	return true
}

func (we *WorkflowEngine) findStepExecution(execution *WorkflowExecution, stepID string) *StepExecution {
	for _, stepExec := range execution.Steps {
		if stepExec.ID == stepID {
			return stepExec
		}
	}
	return nil
}

func (we *WorkflowEngine) isSagaWorkflow(definition *WorkflowDefinition) bool {
	// Check if workflow metadata indicates it's a saga
	if sagaFlag, exists := definition.Metadata["saga"]; exists {
		if saga, ok := sagaFlag.(bool); ok {
			return saga
		}
	}
	return false
}

func (we *WorkflowEngine) calculateRetryDelay(attempt int) time.Duration {
	delay := time.Duration(float64(we.retryConfig.InitialDelay) * math.Pow(we.retryConfig.BackoffFactor, float64(attempt)))
	
	if delay > we.retryConfig.MaxDelay {
		delay = we.retryConfig.MaxDelay
	}
	
	if we.retryConfig.Jitter {
		jitter := time.Duration(float64(delay) * (0.5 + rand.Float64()*0.5))
		delay = jitter
	}
	
	return delay
}

func (we *WorkflowEngine) calculateProgress(execution *WorkflowExecution) float64 {
	if len(execution.Steps) == 0 {
		return 0.0
	}
	
	completed := 0
	for _, step := range execution.Steps {
		if step.Status == StepStatusCompleted || step.Status == StepStatusSkipped {
			completed++
		}
	}
	
	return float64(completed) / float64(len(execution.Steps))
}

func (we *WorkflowEngine) getStatusMessage(execution *WorkflowExecution) string {
	switch execution.Status {
	case WorkflowStatusPending:
		return "Workflow is pending execution"
	case WorkflowStatusRunning:
		return fmt.Sprintf("Executing step %d of %d", we.getCurrentStepIndex(execution)+1, len(execution.Steps))
	case WorkflowStatusCompleted:
		return "Workflow completed successfully"
	case WorkflowStatusFailed:
		return fmt.Sprintf("Workflow failed: %s", execution.Error)
	case WorkflowStatusCancelled:
		return "Workflow was cancelled"
	default:
		return "Unknown status"
	}
}

func (we *WorkflowEngine) getCurrentStepIndex(execution *WorkflowExecution) int {
	for i, step := range execution.Steps {
		if step.Status == StepStatusRunning || step.Status == StepStatusPending {
			return i
		}
	}
	return len(execution.Steps) - 1
}

func (we *WorkflowEngine) handleWorkflowCancellation(ctx context.Context, execution *WorkflowExecution, reason error) {
	execution.mu.Lock()
	execution.Status = WorkflowStatusCancelled
	execution.Error = reason.Error()
	now := time.Now().UTC()
	execution.CompletedAt = &now
	execution.mu.Unlock()
	
	// Save checkpoint
	if err := we.checkpointStore.SaveCheckpoint(ctx, execution); err != nil {
		we.logger.Error("Failed to save checkpoint after cancellation", "execution_id", execution.ID, "error", err)
	}
	
	// Publish cancellation event
	we.publishWorkflowEvent(ctx, execution, "cancelled", map[string]interface{}{
		"reason": reason.Error(),
	})
}

func (we *WorkflowEngine) cleanupExecution(execution *WorkflowExecution) {
	we.mu.Lock()
	delete(we.executions, execution.ID)
	we.activeWorkflows--
	we.mu.Unlock()
}

func (we *WorkflowEngine) loadAndResumeWorkflows(ctx context.Context) error {
	// Load pending/running workflows from checkpoint store
	filter := &HistoryFilter{
		Status: WorkflowStatusRunning,
		Limit:  1000,
	}
	
	executions, err := we.checkpointStore.ListCheckpoints(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to load checkpoints: %w", err)
	}
	
	for _, execution := range executions {
		we.mu.Lock()
		we.executions[execution.ID] = execution
		we.activeWorkflows++
		we.mu.Unlock()
		
		// Resume execution
		go we.executeWorkflow(ctx, execution)
		
		we.logger.Info("Resumed workflow execution", "execution_id", execution.ID)
	}
	
	we.logger.Info("Loaded and resumed workflows", "count", len(executions))
	return nil
}

func (we *WorkflowEngine) publishWorkflowEvent(ctx context.Context, execution *WorkflowExecution, eventType string, data map[string]interface{}) {
	if we.eventBus == nil {
		return
	}
	
	eventData := &eventbus.ExecProgressEvent{
		WorkflowID: execution.ID,
		Status:     eventType,
		Progress:   we.calculateProgress(execution),
		Message:    we.getStatusMessage(execution),
	}
	
	if execution.CompletedAt != nil {
		eventData.CompletedAt = execution.CompletedAt
	}
	
	event := eventbus.NewExecProgressEvent("orchestrator", eventData, execution.TraceID)
	
	if err := we.eventBus.PublishEventAsync(ctx, event); err != nil {
		we.logger.Error("Failed to publish workflow event", "execution_id", execution.ID, "error", err)
	}
}

func (we *WorkflowEngine) publishStepProgressEvent(ctx context.Context, execution *WorkflowExecution, stepID, status string, progress float64) {
	if we.eventBus == nil {
		return
	}
	
	eventData := &eventbus.ExecProgressEvent{
		WorkflowID: execution.ID,
		StepID:     stepID,
		Status:     status,
		Progress:   progress,
	}
	
	event := eventbus.NewExecProgressEvent("orchestrator", eventData, execution.TraceID)
	
	if err := we.eventBus.PublishEventAsync(ctx, event); err != nil {
		we.logger.Error("Failed to publish step progress event", 
			"execution_id", execution.ID, "step_id", stepID, "error", err)
	}
}

func extractTraceID(ctx context.Context) string {
	// Extract trace ID from context - implementation depends on tracing library used
	// For now, return empty string
	return ""
}