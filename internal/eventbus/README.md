# Event Bus Implementation

This package implements a robust event bus system for the Global Data Controller using NATS JetStream as the underlying messaging infrastructure.

## Features

- **Persistent Event Streams**: Uses NATS JetStream for durable message storage
- **Typed Events**: Strongly typed event system with predefined event types
- **Publisher/Subscriber Pattern**: Supports both direct event type subscriptions and pattern-based subscriptions
- **Message Deduplication**: Automatic deduplication based on event IDs
- **Async Publishing**: Support for both synchronous and asynchronous event publishing
- **Trace Support**: Built-in support for distributed tracing with trace IDs
- **Graceful Shutdown**: Proper cleanup of subscriptions and connections
- **Error Handling**: Comprehensive error handling with retry mechanisms

## Architecture

### Core Components

1. **Event Types**: Predefined event types for different system operations
2. **Event Bus Interface**: Unified interface for publishing and subscribing to events
3. **NATS Implementation**: NATS JetStream-based implementation of the event bus
4. **Event Handlers**: Function-based event handling with context support

### Event Types

The system supports the following event types:

#### Pin Operations
- `pin.requested` - Pin operation requested
- `pin.completed` - Pin operation completed successfully
- `pin.failed` - Pin operation failed

#### Planning
- `plan.ready` - Placement plan is ready
- `plan.failed` - Planning failed

#### Execution Progress
- `exec.progress` - Execution progress update
- `exec.completed` - Execution completed
- `exec.failed` - Execution failed

#### Health Monitoring
- `health.changed` - Resource health status changed
- `node.down` - Node went down
- `node.up` - Node came back up
- `zone.down` - Zone went down
- `zone.up` - Zone came back up

#### Policy Management
- `policy.updated` - Policy was updated
- `policy.deleted` - Policy was deleted

#### Alerting
- `alert.triggered` - Alert was triggered
- `alert.resolved` - Alert was resolved

## Usage

### Basic Setup

```go
import (
    "github.com/global-data-controller/gdc/internal/eventbus"
    "go.uber.org/zap"
)

// Create NATS configuration
config := &eventbus.NATSConfig{
    URL:        "nats://localhost:4222",
    StreamName: "GDC_EVENTS",
    StreamSubjects: []string{"gdc.events.>"},
    MaxAge:     24 * time.Hour,
    MaxBytes:   1024 * 1024 * 1024, // 1GB
    MaxMsgs:    1000000,
    Replicas:   1,
}

// Create event bus
logger := zap.NewProduction()
bus, err := eventbus.NewNATSEventBus(config, logger)
if err != nil {
    log.Fatal(err)
}
defer bus.Close()
```

### Publishing Events

```go
// Create a typed event
pinEvent := eventbus.NewPinRequestedEvent("api-gateway", &eventbus.PinRequestedEvent{
    CID:         "QmTest123",
    Size:        1024,
    Policies:    []string{"rf-3", "zone-msk-nn"},
    Constraints: map[string]string{"priority": "high"},
    Priority:    1,
    RequestedBy: "user123",
}, "trace-123")

// Publish synchronously
err := bus.PublishEvent(ctx, pinEvent)
if err != nil {
    log.Printf("Failed to publish event: %v", err)
}

// Publish asynchronously
err = bus.PublishEventAsync(ctx, pinEvent)
if err != nil {
    log.Printf("Failed to publish event async: %v", err)
}
```

### Subscribing to Events

```go
// Subscribe to specific event type
handler := eventbus.EventHandlerFunc(func(ctx context.Context, event *eventbus.Event) error {
    log.Printf("Received event: %s", event.Type)
    
    // Parse typed event data
    var pinData eventbus.PinRequestedEvent
    if err := eventbus.ParseEventData(event, &pinData); err != nil {
        return err
    }
    
    log.Printf("Pin requested for CID: %s", pinData.CID)
    return nil
})

err := bus.SubscribeToEventType(ctx, eventbus.EventTypePinRequested, handler)
if err != nil {
    log.Printf("Failed to subscribe: %v", err)
}

// Subscribe to pattern (all pin events)
err = bus.SubscribeToPattern(ctx, "pin.*", handler)
if err != nil {
    log.Printf("Failed to subscribe to pattern: %v", err)
}
```

### Event Creation Helpers

The package provides helper functions for creating typed events:

```go
// Pin events
pinReqEvent := eventbus.NewPinRequestedEvent(source, data, traceID)
pinCompletedEvent := eventbus.NewPinCompletedEvent(source, cid, assignments, traceID)
pinFailedEvent := eventbus.NewPinFailedEvent(source, cid, reason, traceID)

// Plan events
planReadyEvent := eventbus.NewPlanReadyEvent(source, data, traceID)
planFailedEvent := eventbus.NewPlanFailedEvent(source, cid, reason, traceID)

// Execution events
execProgressEvent := eventbus.NewExecProgressEvent(source, data, traceID)
execCompletedEvent := eventbus.NewExecCompletedEvent(source, workflowID, result, traceID)
execFailedEvent := eventbus.NewExecFailedEvent(source, workflowID, reason, traceID)

// Health events
healthChangedEvent := eventbus.NewHealthChangedEvent(source, data, traceID)
nodeDownEvent := eventbus.NewNodeDownEvent(source, nodeID, reason, traceID)
nodeUpEvent := eventbus.NewNodeUpEvent(source, nodeID, traceID)

// Policy events
policyUpdatedEvent := eventbus.NewPolicyUpdatedEvent(source, data, traceID)
policyDeletedEvent := eventbus.NewPolicyDeletedEvent(source, policyID, deletedBy, traceID)

// Alert events
alertTriggeredEvent := eventbus.NewAlertTriggeredEvent(source, data, traceID)
alertResolvedEvent := eventbus.NewAlertResolvedEvent(source, alertID, resolvedBy, traceID)
```

## Configuration

### NATS Configuration

```go
type NATSConfig struct {
    URL                  string        // NATS server URL
    StreamName           string        // JetStream stream name
    StreamSubjects       []string      // Stream subjects pattern
    MaxAge               time.Duration // Message retention time
    MaxBytes             int64         // Maximum stream size in bytes
    MaxMsgs              int64         // Maximum number of messages
    Replicas             int           // Stream replicas
    ConnectTimeout       time.Duration // Connection timeout
    ReconnectWait        time.Duration // Reconnection wait time
    MaxReconnectAttempts int           // Maximum reconnection attempts
}
```

### Default Configuration

```go
config := eventbus.DefaultNATSConfig()
// Returns:
// {
//     URL:                  "nats://localhost:4222",
//     StreamName:           "GDC_EVENTS",
//     StreamSubjects:       []string{"gdc.events.>"},
//     MaxAge:               24 * time.Hour,
//     MaxBytes:             1024 * 1024 * 1024, // 1GB
//     MaxMsgs:              1000000,
//     Replicas:             1,
//     ConnectTimeout:       10 * time.Second,
//     ReconnectWait:        2 * time.Second,
//     MaxReconnectAttempts: 10,
// }
```

## Event Structure

All events follow a consistent structure:

```go
type Event struct {
    ID        string                 `json:"id"`         // Unique event ID
    Type      EventType              `json:"type"`       // Event type
    Source    string                 `json:"source"`     // Event source component
    Subject   string                 `json:"subject"`    // Event subject (e.g., CID, node ID)
    Data      map[string]interface{} `json:"data"`       // Event payload
    TraceID   string                 `json:"trace_id"`   // Distributed tracing ID
    Timestamp time.Time              `json:"timestamp"`  // Event timestamp
    Version   string                 `json:"version"`    // Event schema version
}
```

## Error Handling

The event bus implements several error handling strategies:

1. **Connection Resilience**: Automatic reconnection with exponential backoff
2. **Message Retry**: Failed message processing is retried with configurable limits
3. **Circuit Breaker**: Prevents cascading failures during outages
4. **Graceful Degradation**: System continues operating even if event bus is unavailable

## Testing

The package includes comprehensive tests:

- Unit tests for event creation and parsing
- Integration tests with embedded NATS server
- End-to-end workflow tests
- Performance and resilience tests

Run tests:

```bash
go test ./internal/eventbus/... -v
```

## Docker Integration

The event bus is configured to work with the NATS JetStream container defined in `docker-compose.yml`:

```yaml
nats:
  image: nats:2.10-alpine
  container_name: gdc-nats
  command: 
    - "--jetstream"
    - "--store_dir=/data"
    - "--max_file_store=1GB"
    - "--max_mem_store=256MB"
  ports:
    - "4222:4222"
    - "8222:8222"
  volumes:
    - nats_data:/data
```

## Monitoring

The event bus provides metrics and logging for monitoring:

- Connection status and health
- Message publish/subscribe rates
- Error rates and types
- Stream statistics (message count, size, age)
- Consumer lag and processing times

## Best Practices

1. **Use Trace IDs**: Always include trace IDs for distributed tracing
2. **Handle Errors**: Implement proper error handling in event handlers
3. **Idempotent Handlers**: Make event handlers idempotent to handle retries
4. **Resource Cleanup**: Always close the event bus on application shutdown
5. **Monitor Performance**: Monitor event processing latency and throughput
6. **Use Typed Events**: Prefer typed event creation helpers over generic events

## Integration with GDC Components

The event bus integrates with various GDC components:

- **API Gateway**: Publishes pin requests and API events
- **Policy Engine**: Publishes policy update events
- **Scheduler**: Publishes planning events and subscribes to pin requests
- **Orchestrator**: Publishes execution progress and subscribes to plans
- **Cluster Adapter**: Publishes completion/failure events
- **Health Monitor**: Publishes health change events
- **Alert Manager**: Publishes alert events