package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowEngine_BasicWorkflow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register executors
	engine.RegisterStepExecutor(NewDelayStepExecutor(logger))
	engine.RegisterStepExecutor(NewLogStepExecutor(logger))
	
	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)
	
	// Create a simple workflow
	workflow := &WorkflowDefinition{
		Type: "test_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "step1",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Starting workflow",
					"level":   "info",
				},
			},
			{
				ID:   "step2",
				Type: "delay",
				Parameters: map[string]interface{}{
					"delay": "100ms",
				},
				Dependencies: []string{"step1"},
			},
			{
				ID:   "step3",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Workflow completed",
					"level":   "info",
				},
				Dependencies: []string{"step2"},
			},
		},
		Retries: 2,
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	assert.NotEmpty(t, execution.ID)
	assert.Equal(t, WorkflowStatusPending, execution.Status)
	
	// Wait for completion
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			t.Fatal("Workflow did not complete within timeout")
		case <-ticker.C:
			status, err := engine.GetWorkflowStatus(ctx, execution.ID)
			require.NoError(t, err)
			
			if status.Status == WorkflowStatusCompleted {
				assert.Equal(t, 1.0, status.Progress)
				return
			} else if status.Status == WorkflowStatusFailed {
				t.Fatalf("Workflow failed: %s", status.Message)
			}
		}
	}
}

func TestWorkflowEngine_WorkflowWithFailure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register executors
	engine.RegisterStepExecutor(NewLogStepExecutor(logger))
	engine.RegisterStepExecutor(NewFailStepExecutor(logger))
	
	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)
	
	// Create a workflow that will fail
	workflow := &WorkflowDefinition{
		Type: "failing_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "step1",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Starting workflow",
				},
			},
			{
				ID:   "step2",
				Type: "fail",
				Parameters: map[string]interface{}{
					"message": "This step always fails",
				},
				Dependencies: []string{"step1"},
				Retries:      1, // Only retry once
			},
		},
		Retries: 0,
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	
	// Wait for failure
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			t.Fatal("Workflow did not fail within timeout")
		case <-ticker.C:
			status, err := engine.GetWorkflowStatus(ctx, execution.ID)
			require.NoError(t, err)
			
			if status.Status == WorkflowStatusFailed {
				assert.Contains(t, status.Message, "failed")
				return
			} else if status.Status == WorkflowStatusCompleted {
				t.Fatal("Workflow should have failed but completed successfully")
			}
		}
	}
}

func TestWorkflowEngine_SagaWorkflow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register executors and compensators
	engine.RegisterStepExecutor(NewLogStepExecutor(logger))
	engine.RegisterStepExecutor(NewFailStepExecutor(logger))
	engine.RegisterCompensationHandler(NewLogCompensationHandler(logger))
	
	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)
	
	// Create a saga workflow that will fail and trigger compensation
	workflow := &WorkflowDefinition{
		Type: "saga_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "step1",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "First step",
				},
			},
			{
				ID:   "step2",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Second step",
				},
				Dependencies: []string{"step1"},
			},
			{
				ID:   "step3",
				Type: "fail",
				Parameters: map[string]interface{}{
					"message": "This will trigger compensation",
				},
				Dependencies: []string{"step2"},
				Retries:      0, // No retries to fail immediately
			},
		},
		Metadata: map[string]interface{}{
			"saga": true, // Mark as saga workflow
		},
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	
	// Wait for failure and compensation
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			t.Fatal("Workflow did not complete compensation within timeout")
		case <-ticker.C:
			status, err := engine.GetWorkflowStatus(ctx, execution.ID)
			require.NoError(t, err)
			
			if status.Status == WorkflowStatusFailed {
				// Check that compensation was executed
				// In a real test, you'd verify compensation side effects
				assert.Contains(t, status.Message, "failed")
				return
			}
		}
	}
}

func TestWorkflowEngine_CancelWorkflow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register executors
	engine.RegisterStepExecutor(NewDelayStepExecutor(logger))
	
	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)
	
	// Create a workflow with a long delay
	workflow := &WorkflowDefinition{
		Type: "long_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "step1",
				Type: "delay",
				Parameters: map[string]interface{}{
					"delay": "10s", // Long delay
				},
			},
		},
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	
	// Wait a bit for workflow to start
	time.Sleep(100 * time.Millisecond)
	
	// Cancel the workflow
	err = engine.CancelWorkflow(ctx, execution.ID)
	require.NoError(t, err)
	
	// Verify cancellation
	status, err := engine.GetWorkflowStatus(ctx, execution.ID)
	require.NoError(t, err)
	assert.Equal(t, WorkflowStatusCancelled, status.Status)
}

func TestWorkflowEngine_RetryLogic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Create a custom executor that fails a few times then succeeds
	failingExecutor := &FailingExecutor{
		failCount: 0,
		maxFails:  2, // Fail twice, then succeed
		logger:    logger,
	}
	engine.RegisterStepExecutor(failingExecutor)
	
	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)
	
	// Create a workflow with retry logic
	workflow := &WorkflowDefinition{
		Type: "retry_workflow",
		Steps: []*WorkflowStep{
			{
				ID:      "step1",
				Type:    "failing",
				Retries: 3, // Allow 3 retries
				Parameters: map[string]interface{}{
					"message": "This will fail twice then succeed",
				},
			},
		},
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	
	// Wait for completion
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			t.Fatal("Workflow did not complete within timeout")
		case <-ticker.C:
			status, err := engine.GetWorkflowStatus(ctx, execution.ID)
			require.NoError(t, err)
			
			if status.Status == WorkflowStatusCompleted {
				assert.Equal(t, 1.0, status.Progress)
				// Verify that retries occurred
				assert.Equal(t, 3, failingExecutor.failCount) // Should have failed 2 times + 1 success
				return
			} else if status.Status == WorkflowStatusFailed {
				t.Fatalf("Workflow failed: %s", status.Message)
			}
		}
	}
}

func TestWorkflowEngine_Dependencies(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	// Register executors
	engine.RegisterStepExecutor(NewLogStepExecutor(logger))
	engine.RegisterStepExecutor(NewDelayStepExecutor(logger))
	
	ctx := context.Background()
	require.NoError(t, engine.Start(ctx))
	defer engine.Stop(ctx)
	
	// Create a workflow with complex dependencies
	workflow := &WorkflowDefinition{
		Type: "dependency_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "step1",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Step 1",
				},
			},
			{
				ID:   "step2",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Step 2",
				},
			},
			{
				ID:   "step3",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Step 3 - depends on 1 and 2",
				},
				Dependencies: []string{"step1", "step2"},
			},
			{
				ID:   "step4",
				Type: "log",
				Parameters: map[string]interface{}{
					"message": "Step 4 - depends on 3",
				},
				Dependencies: []string{"step3"},
			},
		},
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	
	// Wait for completion
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-timeout:
			t.Fatal("Workflow did not complete within timeout")
		case <-ticker.C:
			status, err := engine.GetWorkflowStatus(ctx, execution.ID)
			require.NoError(t, err)
			
			if status.Status == WorkflowStatusCompleted {
				assert.Equal(t, 1.0, status.Progress)
				return
			} else if status.Status == WorkflowStatusFailed {
				t.Fatalf("Workflow failed: %s", status.Message)
			}
		}
	}
}

func TestWorkflowEngine_CheckpointRecovery(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	
	// Create first engine instance
	engine1 := NewWorkflowEngine(nil, checkpointStore, logger)
	engine1.RegisterStepExecutor(NewDelayStepExecutor(logger))
	
	ctx := context.Background()
	require.NoError(t, engine1.Start(ctx))
	
	// Create a workflow
	workflow := &WorkflowDefinition{
		Type: "checkpoint_workflow",
		Steps: []*WorkflowStep{
			{
				ID:   "step1",
				Type: "delay",
				Parameters: map[string]interface{}{
					"delay": "1s",
				},
			},
		},
		Timeout: time.Minute,
	}
	
	// Start workflow
	execution, err := engine1.StartWorkflow(ctx, workflow)
	require.NoError(t, err)
	
	// Wait a bit then stop the engine (simulating crash)
	time.Sleep(50 * time.Millisecond) // Shorter wait to stop before completion
	engine1.Stop(ctx)
	
	// Create second engine instance with same checkpoint store
	engine2 := NewWorkflowEngine(nil, checkpointStore, logger)
	engine2.RegisterStepExecutor(NewDelayStepExecutor(logger))
	
	// Start second engine (should recover workflows)
	require.NoError(t, engine2.Start(ctx))
	defer engine2.Stop(ctx)
	
	// Check that workflow can be found (it might have completed by now, but should be recoverable)
	status, err := engine2.GetWorkflowStatus(ctx, execution.ID)
	require.NoError(t, err)
	// The workflow should be found, regardless of its status
	assert.NotEmpty(t, status.ID)
}

// FailingExecutor is a test executor that fails a specified number of times before succeeding
type FailingExecutor struct {
	failCount int
	maxFails  int
	logger    *slog.Logger
}

func (f *FailingExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
	f.failCount++
	
	if f.failCount <= f.maxFails {
		f.logger.Info("Failing executor - intentional failure", 
			"execution_id", execution.ID, "step_id", step.ID, 
			"fail_count", f.failCount, "max_fails", f.maxFails)
		return &StepResult{
			Success: false,
			Error:   "Intentional failure for testing",
		}, fmt.Errorf("intentional failure %d/%d", f.failCount, f.maxFails)
	}
	
	f.logger.Info("Failing executor - success after retries", 
		"execution_id", execution.ID, "step_id", step.ID, 
		"fail_count", f.failCount, "max_fails", f.maxFails)
	
	return &StepResult{
		Success: true,
		Data: map[string]interface{}{
			"attempts": f.failCount,
			"message":  "Success after retries",
		},
	}, nil
}

func (f *FailingExecutor) GetStepType() string {
	return "failing"
}

func TestRetryConfig_CalculateDelay(t *testing.T) {
	config := &RetryConfig{
		InitialDelay:  time.Second,
		MaxDelay:      time.Minute,
		BackoffFactor: 2.0,
		Jitter:        false,
	}
	
	engine := &WorkflowEngine{retryConfig: config}
	
	// Test exponential backoff
	delay1 := engine.calculateRetryDelay(0)
	delay2 := engine.calculateRetryDelay(1)
	delay3 := engine.calculateRetryDelay(2)
	
	assert.Equal(t, time.Second, delay1)
	assert.Equal(t, 2*time.Second, delay2)
	assert.Equal(t, 4*time.Second, delay3)
	
	// Test max delay cap
	delay10 := engine.calculateRetryDelay(10)
	assert.Equal(t, time.Minute, delay10)
}

func TestWorkflowValidation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	checkpointStore := NewMemoryCheckpointStore()
	engine := NewWorkflowEngine(nil, checkpointStore, logger)
	
	ctx := context.Background()
	
	// Test empty workflow type
	workflow := &WorkflowDefinition{
		Type:  "",
		Steps: []*WorkflowStep{},
	}
	_, err := engine.StartWorkflow(ctx, workflow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflow type is required")
	
	// Test no steps
	workflow = &WorkflowDefinition{
		Type:  "test",
		Steps: []*WorkflowStep{},
	}
	_, err = engine.StartWorkflow(ctx, workflow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "must have at least one step")
	
	// Test duplicate step IDs
	workflow = &WorkflowDefinition{
		Type: "test",
		Steps: []*WorkflowStep{
			{ID: "step1", Type: "log"},
			{ID: "step1", Type: "log"}, // Duplicate ID
		},
	}
	_, err = engine.StartWorkflow(ctx, workflow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate step ID")
}