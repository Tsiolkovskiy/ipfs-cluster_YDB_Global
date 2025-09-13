# Cluster Control Adapter

The Cluster Control Adapter provides a robust interface for communicating with IPFS Cluster instances across multiple geographic zones. It implements idempotent operations with retry logic, comprehensive monitoring, and full observability.

## Features

### Core Functionality
- **gRPC Communication**: Secure mTLS connections to IPFS Cluster instances
- **Idempotent Operations**: Safe retry logic for pin/unpin operations
- **Operation Monitoring**: Real-time tracking of operation status and progress
- **Health Monitoring**: Continuous health checks of cluster instances
- **Node Management**: Add/remove nodes from clusters dynamically

### Reliability Features
- **Exponential Backoff**: Configurable retry logic with jitter
- **Circuit Breaker**: Automatic failure detection and recovery
- **Connection Pooling**: Efficient connection management and reuse
- **Graceful Degradation**: Continues operation when individual clusters fail

### Observability
- **Distributed Tracing**: OpenTelemetry integration for request tracing
- **Metrics Collection**: Prometheus metrics for operations and performance
- **Structured Logging**: Comprehensive logging with correlation IDs
- **Operation History**: Track and audit all cluster operations

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Cluster Control Adapter                     │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ gRPC Clients    │  │ Operation       │  │ Health       │ │
│  │ - Connection    │  │ Monitor         │  │ Monitor      │ │
│  │   Management    │  │ - Status Track  │  │ - Cluster    │ │
│  │ - Retry Logic   │  │ - Progress      │  │   Health     │ │
│  │ - Load Balance  │  │ - History       │  │ - Metrics    │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┼─────────┐
                    │                   │
            ┌───────▼────────┐  ┌───────▼────────┐
            │  IPFS Cluster  │  │  IPFS Cluster  │
            │   Zone MSK     │  │   Zone NN      │
            └────────────────┘  └────────────────┘
```

## Components

### 1. IPFSClusterAdapter
Main adapter implementing the `ControlAdapter` interface:

```go
type ControlAdapter interface {
    // Pin operations
    SubmitPinPlan(ctx context.Context, clusterID string, plan *models.PinPlan) error
    GetPinStatus(ctx context.Context, clusterID string, cid string) (*models.PinStatus, error)
    UnpinContent(ctx context.Context, clusterID string, cid string) error
    
    // Cluster monitoring
    GetClusterStatus(ctx context.Context, clusterID string) (*models.ClusterStatus, error)
    GetNodeMetrics(ctx context.Context, clusterID string) ([]*models.NodeMetrics, error)
    
    // Node management
    AddNode(ctx context.Context, clusterID string, nodeConfig *models.NodeConfig) error
    RemoveNode(ctx context.Context, clusterID string, nodeID string) error
    
    // Health check
    HealthCheck(ctx context.Context, clusterID string) error
    
    // Close connections
    Close() error
}
```

### 2. OperationMonitor
Tracks the status of long-running operations:

```go
type OperationMonitor struct {
    // Tracks operations by ID
    // Provides real-time status updates
    // Automatic cleanup of old operations
    // Metrics collection and reporting
}
```

### 3. RetryConfig
Configurable retry behavior:

```go
type RetryConfig struct {
    MaxAttempts   int           // Maximum retry attempts
    InitialDelay  time.Duration // Initial delay between retries
    MaxDelay      time.Duration // Maximum delay cap
    BackoffFactor float64       // Exponential backoff factor
    Jitter        bool          // Add randomization to delays
}
```

## Usage Examples

### Basic Setup

```go
// Create adapter configuration
config := &AdapterConfig{
    Timeout: 30 * time.Second,
    RetryConfig: &RetryConfig{
        MaxAttempts:   3,
        InitialDelay:  100 * time.Millisecond,
        MaxDelay:      5 * time.Second,
        BackoffFactor: 2.0,
        Jitter:        true,
    },
    Logger: zap.NewProduction(),
}

// Create the adapter
adapter := NewIPFSClusterAdapter(config)
defer adapter.Close()
```

### Submit Pin Plan

```go
plan := &models.PinPlan{
    ID:  "plan-123",
    CID: "QmExampleContent",
    Assignments: []*models.NodeAssignment{
        {
            NodeID:    "node-1",
            ClusterID: "cluster-msk",
            ZoneID:    "zone-msk",
            Priority:  1,
        },
    },
}

err := adapter.SubmitPinPlan(ctx, "cluster-msk", plan)
if err != nil {
    log.Printf("Failed to submit pin plan: %v", err)
}
```

### Monitor Operations

```go
// Create operation monitor
monitorConfig := &MonitorConfig{
    Adapter:       adapter,
    Logger:        logger,
    CheckInterval: 30 * time.Second,
}
monitor, _ := NewOperationMonitor(monitorConfig)

// Start monitoring
monitor.Start(ctx)
defer monitor.Stop()

// Track an operation
operationID := monitor.TrackOperation(OperationTypePin, "cluster-msk", "QmContent")

// Check operation status
status, exists := monitor.GetOperationStatus(operationID)
if exists {
    fmt.Printf("Operation %s: %s (%.1f%% complete)\n", 
        status.ID, status.Status, status.Progress*100)
}
```

### Health Monitoring

```go
// Check cluster health
err := adapter.HealthCheck(ctx, "cluster-msk")
if err != nil {
    log.Printf("Cluster health check failed: %v", err)
}

// Get detailed metrics
metrics, err := adapter.GetNodeMetrics(ctx, "cluster-msk")
if err == nil {
    for _, metric := range metrics {
        fmt.Printf("Node CPU: %.1f%%, Memory: %.1f%%, Storage: %.1f%%\n",
            metric.CPUUsage*100, metric.MemoryUsage*100, metric.StorageUsage*100)
    }
}
```

## Error Handling

The adapter implements comprehensive error handling:

### Retryable Errors
- Network timeouts (`codes.DeadlineExceeded`)
- Service unavailable (`codes.Unavailable`)
- Resource exhausted (`codes.ResourceExhausted`)

### Non-Retryable Errors
- Invalid arguments (`codes.InvalidArgument`)
- Not found (`codes.NotFound`)
- Permission denied (`codes.PermissionDenied`)
- Already exists (`codes.AlreadyExists`)

### Retry Strategy
```go
// Exponential backoff with jitter
delay = initialDelay * (backoffFactor ^ attempt)
if delay > maxDelay {
    delay = maxDelay
}
if jitter {
    delay += random(0, delay * 0.1)
}
```

## Testing

The implementation includes comprehensive tests:

### Unit Tests
- Individual component testing with mocks
- Retry logic validation
- Error handling scenarios
- Configuration validation

### Integration Tests
- End-to-end operation flows
- Concurrent operation handling
- Failure scenario testing
- Performance validation

### Mock Implementation
```go
// Mock IPFS Cluster for testing
type mockIPFSClusterServiceClient struct {
    submitPinFunc    func(ctx context.Context, req *PinRequest) (*PinResponse, error)
    getPinStatusFunc func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error)
    // ... other mock functions
}
```

## Metrics and Observability

### Prometheus Metrics
- `cluster_operations_total`: Total number of operations by type and status
- `cluster_operation_duration_seconds`: Operation duration histogram
- `cluster_health_status`: Cluster health status gauge

### Distributed Tracing
- OpenTelemetry integration
- Request correlation across services
- Performance bottleneck identification

### Structured Logging
- Operation correlation IDs
- Request/response logging
- Error context and stack traces

## Configuration

### Environment Variables
- `CLUSTER_TIMEOUT`: Default operation timeout
- `CLUSTER_RETRY_MAX_ATTEMPTS`: Maximum retry attempts
- `CLUSTER_RETRY_INITIAL_DELAY`: Initial retry delay
- `CLUSTER_RETRY_MAX_DELAY`: Maximum retry delay

### TLS Configuration
```go
config := &AdapterConfig{
    TLSConfig: &tls.Config{
        Certificates: []tls.Certificate{clientCert},
        RootCAs:      rootCAs,
        ServerName:   "ipfs-cluster.example.com",
    },
}
```

## Performance Considerations

### Connection Management
- Connection pooling and reuse
- Automatic connection health monitoring
- Graceful connection cleanup

### Concurrency
- Thread-safe operation tracking
- Concurrent request handling
- Resource contention avoidance

### Memory Management
- Automatic cleanup of old operations
- Bounded operation history
- Efficient data structures

## Security

### mTLS Authentication
- Client certificate authentication
- Server certificate validation
- Secure communication channels

### Authorization
- Integration with SPIFFE/SPIRE
- Policy-based access control
- Audit logging

## Future Enhancements

1. **Load Balancing**: Intelligent request distribution across cluster nodes
2. **Caching**: Response caching for frequently accessed data
3. **Batch Operations**: Bulk pin/unpin operations for efficiency
4. **Streaming**: Real-time operation status streaming
5. **Compression**: Request/response compression for bandwidth optimization

## Requirements Satisfied

This implementation satisfies the following requirements from the specification:

- **FR-1.3**: Идемпотентные операции pin/unpin с retry логикой
- **NFR-5.2**: Мониторинг статуса операций в кластерах
- **gRPC Integration**: Полная интеграция с IPFS Cluster через gRPC
- **Testing**: Comprehensive unit and integration tests with mock IPFS Cluster