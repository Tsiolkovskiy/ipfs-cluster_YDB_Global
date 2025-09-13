# Orchestrator

The Orchestrator component provides a workflow engine for managing long-running operations in the Global Data Controller. It implements the saga pattern with checkpoint and compensation support, along with robust retry logic with exponential backoff.

## Features

### Core Workflow Engine
- **Workflow Execution**: Manages complex multi-step workflows with dependencies
- **Step Dependencies**: Supports complex dependency graphs between workflow steps
- **Concurrent Execution**: Handles multiple workflows simultaneously with configurable concurrency limits
- **Lifecycle Management**: Start, stop, cancel, and monitor workflow executions

### Saga Pattern Support
- **Compensating Transactions**: Automatic rollback of completed steps when failures occur
- **Checkpoint Management**: Persistent state management for workflow recovery
- **Transaction Isolation**: Each step can define compensation logic for rollback scenarios

### Retry Logic
- **Exponential Backoff**: Configurable retry delays with exponential growth
- **Jitter Support**: Optional randomization to prevent thundering herd problems
- **Per-Step Configuration**: Individual retry settings for each workflow step
- **Context Cancellation**: Respects context cancellation and timeouts

### Event Integration
- **Progress Events**: Publishes workflow and step progress events
- **Status Updates**: Real-time status updates through the event bus
- **Tracing Support**: Distributed tracing integration for debugging

## Architecture

### Core Components

#### WorkflowEngine
The main orchestrator implementation that manages workflow execution:

```go
type WorkflowEngine struct {
    executions      map[string]*WorkflowExecution
    stepExecutors   map[string]StepExecutor
    compensators    map[string]CompensationHandler
    eventBus        eventbus.EventBus
    checkpointStore CheckpointStore
    retryConfig     *RetryConfig
}
```

#### StepExecutor Interface
Defines how individual workflow steps are executed:

```go
type StepExecutor interface {
    Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error)
    GetStepType() string
}
```

#### CompensationHandler Interface
Defines how to compensate (rollback) completed steps:

```go
type CompensationHandler interface {
    Compensate(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) error
    GetStepType() string
}
```

#### CheckpointStore Interface
Provides persistent storage for workflow state:

```go
type CheckpointStore interface {
    SaveCheckpoint(ctx context.Context, execution *WorkflowExecution) error
    LoadCheckpoint(ctx context.Context, executionID string) (*WorkflowExecution, error)
    DeleteCheckpoint(ctx context.Context, executionID string) error
    ListCheckpoints(ctx context.Context, filter *HistoryFilter) ([]*WorkflowExecution, error)
}
```

## Usage Examples

### Basic Workflow

```go
// Create workflow engine
logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
checkpointStore := NewMemoryCheckpointStore()
engine := NewWorkflowEngine(nil, checkpointStore, logger)

// Register step executors
engine.RegisterStepExecutor(NewLogStepExecutor(logger))
engine.RegisterStepExecutor(NewDelayStepExecutor(logger))

// Start the engine
ctx := context.Background()
engine.Start(ctx)
defer engine.Stop(ctx)

// Define a workflow
workflow := &WorkflowDefinition{
    Type: "pin_operation",
    Steps: []*WorkflowStep{
        {
            ID:   "validate",
            Type: "log",
            Parameters: map[string]interface{}{
                "message": "Validating pin request",
            },
        },
        {
            ID:   "execute",
            Type: "delay",
            Parameters: map[string]interface{}{
                "delay": "2s",
            },
            Dependencies: []string{"validate"},
        },
    },
    Timeout: 5 * time.Minute,
}

// Start workflow execution
execution, err := engine.StartWorkflow(ctx, workflow)
if err != nil {
    log.Fatal(err)
}

// Monitor progress
status, err := engine.GetWorkflowStatus(ctx, execution.ID)
```

### Saga Workflow with Compensation

```go
// Define a saga workflow
sagaWorkflow := &WorkflowDefinition{
    Type: "distributed_pin",
    Steps: []*WorkflowStep{
        {
            ID:   "pin_zone1",
            Type: "pin",
            Parameters: map[string]interface{}{
                "zone": "msk",
                "cid":  "QmExample123",
            },
        },
        {
            ID:   "pin_zone2", 
            Type: "pin",
            Parameters: map[string]interface{}{
                "zone": "nn",
                "cid":  "QmExample123",
            },
            Dependencies: []string{"pin_zone1"},
        },
    },
    Metadata: map[string]interface{}{
        "saga": true, // Enable saga compensation
    },
    Timeout: 10 * time.Minute,
}

// Register compensation handlers
engine.RegisterCompensationHandler(NewPinCompensationHandler(logger))

// If pin_zone2 fails, pin_zone1 will be automatically compensated (unpinned)
```

### Custom Step Executor

```go
// Implement a custom step executor
type PinStepExecutor struct {
    clusterAdapter cluster.Adapter
    logger         *slog.Logger
}

func (p *PinStepExecutor) Execute(ctx context.Context, step *WorkflowStep, execution *WorkflowExecution) (*StepResult, error) {
    cid := step.Parameters["cid"].(string)
    zone := step.Parameters["zone"].(string)
    
    // Execute pin operation
    err := p.clusterAdapter.Pin(ctx, zone, cid)
    if err != nil {
        return &StepResult{
            Success: false,
            Error:   err.Error(),
        }, err
    }
    
    return &StepResult{
        Success: true,
        Data: map[string]interface{}{
            "pinned_cid": cid,
            "zone":       zone,
        },
        Checkpoint: map[string]interface{}{
            "cid":  cid,
            "zone": zone,
        },
    }, nil
}

func (p *PinStepExecutor) GetStepType() string {
    return "pin"
}

// Register the executor
engine.RegisterStepExecutor(pinExecutor)
```

## Built-in Step Executors

### DelayStepExecutor
Executes a configurable delay (useful for testing and timing):

```go
{
    ID:   "wait",
    Type: "delay",
    Parameters: map[string]interface{}{
        "delay": "30s", // Duration string or number of seconds
    },
}
```

### LogStepExecutor
Logs a message at a specified level:

```go
{
    ID:   "log_progress",
    Type: "log", 
    Parameters: map[string]interface{}{
        "message": "Processing started",
        "level":   "info", // debug, info, warn, error
    },
}
```

### ConditionalStepExecutor
Evaluates simple conditions:

```go
{
    ID:   "check_condition",
    Type: "conditional",
    Parameters: map[string]interface{}{
        "condition": "always_true", // or context key name
    },
}
```

### FailStepExecutor
Always fails (useful for testing error handling):

```go
{
    ID:   "test_failure",
    Type: "fail",
    Parameters: map[string]interface{}{
        "message": "Intentional test failure",
    },
}
```

## Configuration

### Retry Configuration

```go
retryConfig := &RetryConfig{
    MaxAttempts:   5,                    // Maximum retry attempts
    InitialDelay:  time.Second,          // Initial delay between retries
    MaxDelay:      time.Minute * 5,      // Maximum delay cap
    BackoffFactor: 2.0,                  // Exponential backoff multiplier
    Jitter:        true,                 // Add randomization to delays
}

engine := NewWorkflowEngine(eventBus, checkpointStore, logger)
engine.retryConfig = retryConfig
```

### Workflow Definition

```go
workflow := &WorkflowDefinition{
    Type:    "operation_type",           // Workflow type identifier
    Steps:   []*WorkflowStep{...},       // Ordered list of steps
    Retries: 3,                          // Default retries for all steps
    Timeout: 30 * time.Minute,          // Overall workflow timeout
    Metadata: map[string]interface{}{    // Additional metadata
        "saga":     true,                // Enable saga compensation
        "priority": "high",              // Custom metadata
    },
}
```

### Step Definition

```go
step := &WorkflowStep{
    ID:           "unique_step_id",      // Unique identifier within workflow
    Type:         "executor_type",       // Matches registered executor type
    Parameters:   map[string]interface{}{...}, // Step-specific parameters
    Dependencies: []string{"step1", "step2"},  // Dependencies on other steps
    Retries:      5,                     // Override default retry count
    Timeout:      2 * time.Minute,      // Step-specific timeout
}
```

## Error Handling

The orchestrator provides comprehensive error handling:

### Retry Logic
- Automatic retries with exponential backoff
- Configurable per-step and global retry limits
- Jitter support to prevent thundering herd

### Saga Compensation
- Automatic rollback of completed steps on failure
- Custom compensation handlers for each step type
- Maintains transaction integrity across distributed operations

### Graceful Degradation
- Context cancellation support
- Timeout handling at workflow and step levels
- Clean shutdown with active workflow completion

## Testing

The orchestrator includes comprehensive tests covering:

- Basic workflow execution
- Failure scenarios and retry logic
- Saga compensation patterns
- Workflow cancellation
- Dependency resolution
- Checkpoint recovery
- Configuration validation

Run tests with:
```bash
go test ./internal/orchestrator -v
```

## Integration with GDC

The orchestrator integrates with other GDC components:

### Policy Engine
Workflows can evaluate policies before execution:
```go
{
    ID:   "evaluate_policies",
    Type: "policy_evaluation",
    Parameters: map[string]interface{}{
        "cid":      "QmExample123",
        "policies": []string{"replication", "placement"},
    },
}
```

### Scheduler
Workflows can create placement plans:
```go
{
    ID:   "create_plan",
    Type: "placement_planning",
    Parameters: map[string]interface{}{
        "cid":         "QmExample123",
        "rf":          4,
        "constraints": map[string]string{"zones": "msk,nn"},
    },
}
```

### Cluster Adapter
Workflows can execute operations on IPFS clusters:
```go
{
    ID:   "execute_pins",
    Type: "cluster_operation",
    Parameters: map[string]interface{}{
        "operation": "pin",
        "plan_id":   "plan123",
    },
}
```

This orchestrator provides the foundation for managing complex, long-running operations in the Global Data Controller with robust error handling, compensation, and recovery capabilities.