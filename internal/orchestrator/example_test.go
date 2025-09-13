package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// Example demonstrates how to use the Orchestrator for a pin operation workflow
func Example_pinWorkflow() {
	// Create logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	
	// Create checkpoint store
	checkpointStore := NewMemoryCheckpointStore()
	
	// Create workflow engine
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register built-in executors
	engine.RegisterStepExecutor(NewLogStepExecutor(logger))
	engine.RegisterStepExecutor(NewDelayStepExecutor(logger))
	engine.RegisterStepExecutor(NewConditionalStepExecutor(logger))
	
	// Register compensation handlers for saga support
	engine.RegisterCompensationHandler(NewLogCompensationHandler(logger))
	engine.RegisterCompensationHandler(NewNoOpCompensationHandler("delay", logger))
	
	// Start the engine
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		logger.Error("Failed to start workflow engine", "error", err)
		return
	}
	defer engine.Stop(ctx)
	
	// Define a pin operation workflow
	pinWorkflow := &WorkflowDefinition{
		Type: "pin_operation",
		Steps: []*WorkflowStep{
			{
				ID:   "validate_request",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Validating pin request",
					"level":   "info",
				},
				Retries: 2,
				Timeout: 30 * time.Second,
			},
			{
				ID:   "evaluate_policies",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Evaluating placement policies",
					"level":   "info",
				},
				Dependencies: []string{"validate_request"},
				Retries:      2,
				Timeout:      30 * time.Second,
			},
			{
				ID:   "create_placement_plan",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Creating placement plan",
					"level":   "info",
				},
				Dependencies: []string{"evaluate_policies"},
				Retries:      3,
				Timeout:      60 * time.Second,
			},
			{
				ID:   "execute_pins",
				Type: "delay",
				Parameters: map[string]interface{}{
					"delay": "2s", // Simulate pin execution time
				},
				Dependencies: []string{"create_placement_plan"},
				Retries:      5,
				Timeout:      300 * time.Second,
			},
			{
				ID:   "verify_pins",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Verifying pin completion",
					"level":   "info",
				},
				Dependencies: []string{"execute_pins"},
				Retries:      3,
				Timeout:      60 * time.Second,
			},
			{
				ID:   "update_metadata",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Updating global metadata",
					"level":   "info",
				},
				Dependencies: []string{"verify_pins"},
				Retries:      2,
				Timeout:      30 * time.Second,
			},
		},
		Retries: 1,
		Timeout: 10 * time.Minute,
		Metadata: map[string]interface{}{
			"saga":        true,
			"cid":         "QmExampleCID123",
			"replication": 4,
			"zones":       []string{"msk", "nn"},
		},
	}
	
	// Start the workflow
	execution, err := engine.StartWorkflow(ctx, pinWorkflow)
	if err != nil {
		logger.Error("Failed to start pin workflow", "error", err)
		return
	}
	
	logger.Info("Started pin workflow", "execution_id", execution.ID)
	
	// Monitor workflow progress
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	
	timeout := time.After(30 * time.Second)
	
	for {
		select {
		case <-timeout:
			logger.Error("Workflow timeout")
			return
		case <-ticker.C:
			status, err := engine.GetWorkflowStatus(ctx, execution.ID)
			if err != nil {
				logger.Error("Failed to get workflow status", "error", err)
				return
			}
			
			logger.Info("Workflow progress", 
				"execution_id", execution.ID,
				"status", status.Status,
				"progress", fmt.Sprintf("%.1f%%", status.Progress*100),
				"message", status.Message)
			
			if status.Status == WorkflowStatusCompleted {
				logger.Info("Pin workflow completed successfully")
				return
			} else if status.Status == WorkflowStatusFailed {
				logger.Error("Pin workflow failed", "message", status.Message)
				return
			} else if status.Status == WorkflowStatusCancelled {
				logger.Info("Pin workflow was cancelled")
				return
			}
		}
	}
}

// Example demonstrates how to create a custom step executor
func Example_customExecutor() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	
	// Create a custom pin executor
	pinExecutor := &PinStepExecutor{
		logger: logger,
	}
	
	// Create workflow engine and register the custom executor
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	engine.RegisterStepExecutor(pinExecutor)
	
	// Also register a compensation handler for the pin executor
	pinCompensator := &PinCompensationHandler{
		logger: logger,
	}
	engine.RegisterCompensationHandler(pinCompensator)
	
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		logger.Error("Failed to start workflow engine", "error", err)
		return
	}
	defer engine.Stop(ctx)
	
	// Create a workflow using the custom executor
	workflow := &WorkflowDefinition{
		Type: "custom_pin_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "pin_content",
				Type: "pin",
				Parameters: map[string]interface{}{
					"cid":      "QmExampleCID123",
					"clusters": []string{"cluster1", "cluster2"},
					"rf":       3,
				},
				Retries: 3,
				Timeout: 5 * time.Minute,
			},
		},
		Metadata: map[string]interface{}{
			"saga": true, // Enable saga compensation
		},
		Timeout: 10 * time.Minute,
	}
	
	execution, err := engine.StartWorkflow(ctx, workflow)
	if err != nil {
		logger.Error("Failed to start custom workflow", "error", err)
		return
	}
	
	logger.Info("Started custom pin workflow", "execution_id", execution.ID)
	
	// Wait for completion (simplified for example)
	time.Sleep(2 * time.Second)
	
	status, err := engine.GetWorkflowStatus(ctx, execution.ID)
	if err != nil {
		logger.Error("Failed to get workflow status", "error", err)
		return
	}
	
	logger.Info("Final workflow status", 
		"execution_id", execution.ID,
		"status", status.Status,
		"progress", fmt.Sprintf("%.1f%%", status.Progress*100))
}

// PinStepExecutor is a custom executor for pin operations
type PinStepExecutor struct {
	logger *slog.Logger
}

func (p *PinStepExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	// Extract parameters
	cid, _ := step.Parameters["cid"].(string)
	clusters, _ := step.Parameters["clusters"].([]string)
	rf, _ := step.Parameters["rf"].(int)
	
	p.logger.Info("Executing pin operation", 
		"execution_id", execution.ID,
		"step_id", step.ID,
		"cid", cid,
		"clusters", clusters,
		"replication_factor", rf)
	
	// Simulate pin operation
	time.Sleep(1 * time.Second)
	
	// In a real implementation, this would:
	// 1. Connect to IPFS clusters
	// 2. Send pin requests
	// 3. Monitor pin status
	// 4. Handle failures and retries
	
	// For this example, we'll simulate success
	result := &StepResult{
		Success: true,
		Data: map[string]interface{}{
			"pinned_cid":     cid,
			"pinned_clusters": clusters,
			"pin_time":       time.Now().UTC(),
		},
		Checkpoint: map[string]interface{}{
			"cid":      cid,
			"clusters": clusters,
			"rf":       rf,
		},
	}
	
	p.logger.Info("Pin operation completed", 
		"execution_id", execution.ID,
		"step_id", step.ID,
		"cid", cid)
	
	return result, nil
}

func (p *PinStepExecutor) GetStepType() string {
	return "pin"
}

// PinCompensationHandler handles compensation for pin operations
type PinCompensationHandler struct {
	logger *slog.Logger
}

func (p *PinCompensationHandler) Compensate(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) error {
	// Find the saga transaction for this step
	var transaction *SagaTransaction
	for _, t := range execution.SagaTransactions {
		if t.StepID == step.ID {
			transaction = t
			break
		}
	}
	
	if transaction == nil {
		return fmt.Errorf("no saga transaction found for step %s", step.ID)
	}
	
	// Extract compensation data
	cid, _ := transaction.CompensationData["cid"].(string)
	clusters, _ := transaction.CompensationData["clusters"].([]interface{})
	
	p.logger.Info("Compensating pin operation (unpinning)", 
		"execution_id", execution.ID,
		"step_id", step.ID,
		"cid", cid,
		"clusters", clusters)
	
	// Simulate unpin operation
	time.Sleep(500 * time.Millisecond)
	
	// In a real implementation, this would:
	// 1. Connect to IPFS clusters
	// 2. Send unpin requests
	// 3. Verify unpin completion
	
	p.logger.Info("Pin compensation completed", 
		"execution_id", execution.ID,
		"step_id", step.ID,
		"cid", cid)
	
	return nil
}

func (p *PinCompensationHandler) GetStepType() string {
	return "pin"
}

// Example demonstrates workflow with complex dependencies and parallel execution
func Example_parallelWorkflow() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register executors
	engine.RegisterStepExecutor(NewLogStepExecutor(logger))
	engine.RegisterStepExecutor(NewDelayStepExecutor(logger))
	engine.RegisterStepExecutor(NewConditionalStepExecutor(logger))
	
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		logger.Error("Failed to start workflow engine", "error", err)
		return
	}
	defer engine.Stop(ctx)
	
	// Complex workflow with parallel branches
	workflow := &WorkflowDefinition{
		Type: "complex_replication_workflow",
		Steps: []*WorkflowStep{
			// Initial validation
			{
				ID:   "validate_content",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Validating content for replication",
				},
			},
			
			// Parallel policy evaluation
			{
				ID:   "evaluate_rf_policy",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Evaluating replication factor policy",
				},
				Dependencies: []string{"validate_content"},
			},
			{
				ID:   "evaluate_placement_policy",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Evaluating placement policy",
				},
				Dependencies: []string{"validate_content"},
			},
			{
				ID:   "evaluate_cost_policy",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Evaluating cost policy",
				},
				Dependencies: []string{"validate_content"},
			},
			
			// Merge policy results
			{
				ID:   "merge_policies",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Merging policy evaluation results",
				},
				Dependencies: []string{"evaluate_rf_policy", "evaluate_placement_policy", "evaluate_cost_policy"},
			},
			
			// Parallel zone preparation
			{
				ID:   "prepare_msk_zone",
				Type: "delay",
				Parameters: map[string]interface{}{
					"delay": "1s",
				},
				Dependencies: []string{"merge_policies"},
			},
			{
				ID:   "prepare_nn_zone",
				Type: "delay",
				Parameters: map[string]interface{}{
					"delay": "1s",
				},
				Dependencies: []string{"merge_policies"},
			},
			
			// Final replication
			{
				ID:   "execute_replication",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Executing cross-zone replication",
				},
				Dependencies: []string{"prepare_msk_zone", "prepare_nn_zone"},
			},
		},
		Timeout: 5 * time.Minute,
	}
	
	execution, err := engine.StartWorkflow(ctx, workflow)
	if err != nil {
		logger.Error("Failed to start complex workflow", "error", err)
		return
	}
	
	logger.Info("Started complex replication workflow", "execution_id", execution.ID)
	
	// Monitor until completion
	for {
		time.Sleep(200 * time.Millisecond)
		
		status, err := engine.GetWorkflowStatus(ctx, execution.ID)
		if err != nil {
			logger.Error("Failed to get workflow status", "error", err)
			return
		}
		
		if status.Status == WorkflowStatusCompleted {
			logger.Info("Complex workflow completed successfully")
			break
		} else if status.Status == WorkflowStatusFailed {
			logger.Error("Complex workflow failed", "message", status.Message)
			break
		}
	}
}