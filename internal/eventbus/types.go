package eventbus

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// EventType represents the type of event
type EventType string

const (
	// Pin operation events
	EventTypePinRequested EventType = "pin.requested"
	EventTypePinCompleted EventType = "pin.completed"
	EventTypePinFailed    EventType = "pin.failed"

	// Plan events
	EventTypePlanReady    EventType = "plan.ready"
	EventTypePlanFailed   EventType = "plan.failed"

	// Execution progress events
	EventTypeExecProgress EventType = "exec.progress"
	EventTypeExecCompleted EventType = "exec.completed"
	EventTypeExecFailed   EventType = "exec.failed"

	// Health events
	EventTypeHealthChanged EventType = "health.changed"
	EventTypeNodeDown      EventType = "node.down"
	EventTypeNodeUp        EventType = "node.up"
	EventTypeZoneDown      EventType = "zone.down"
	EventTypeZoneUp        EventType = "zone.up"

	// Policy events
	EventTypePolicyUpdated EventType = "policy.updated"
	EventTypePolicyDeleted EventType = "policy.deleted"

	// Alert events
	EventTypeAlertTriggered EventType = "alert.triggered"
	EventTypeAlertResolved  EventType = "alert.resolved"
)

// Event represents a generic event in the system
type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Source    string                 `json:"source"`
	Subject   string                 `json:"subject"`
	Data      map[string]interface{} `json:"data"`
	TraceID   string                 `json:"trace_id,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Version   string                 `json:"version"`
}

// NewEvent creates a new event with generated ID and timestamp
func NewEvent(eventType EventType, source, subject string, data map[string]interface{}) *Event {
	return &Event{
		ID:        uuid.New().String(),
		Type:      eventType,
		Source:    source,
		Subject:   subject,
		Data:      data,
		Timestamp: time.Now().UTC(),
		Version:   "1.0",
	}
}

// WithTraceID adds a trace ID to the event
func (e *Event) WithTraceID(traceID string) *Event {
	e.TraceID = traceID
	return e
}

// PinRequestedEvent represents a pin request event
type PinRequestedEvent struct {
	CID         string            `json:"cid"`
	Size        int64             `json:"size"`
	Policies    []string          `json:"policies"`
	Constraints map[string]string `json:"constraints"`
	Priority    int               `json:"priority"`
	RequestedBy string            `json:"requested_by"`
}

// PlanReadyEvent represents a placement plan ready event
type PlanReadyEvent struct {
	PlanID      string `json:"plan_id"`
	CID         string `json:"cid"`
	Assignments []struct {
		NodeID    string `json:"node_id"`
		ClusterID string `json:"cluster_id"`
		ZoneID    string `json:"zone_id"`
	} `json:"assignments"`
	EstimatedCost float64 `json:"estimated_cost"`
}

// ExecProgressEvent represents execution progress
type ExecProgressEvent struct {
	WorkflowID    string  `json:"workflow_id"`
	StepID        string  `json:"step_id"`
	Status        string  `json:"status"`
	Progress      float64 `json:"progress"` // 0.0 to 1.0
	Message       string  `json:"message,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

// HealthChangedEvent represents health status change
type HealthChangedEvent struct {
	ResourceType string `json:"resource_type"` // "node", "cluster", "zone"
	ResourceID   string `json:"resource_id"`
	OldStatus    string `json:"old_status"`
	NewStatus    string `json:"new_status"`
	Reason       string `json:"reason,omitempty"`
}

// PolicyUpdatedEvent represents policy update
type PolicyUpdatedEvent struct {
	PolicyID    string `json:"policy_id"`
	PolicyName  string `json:"policy_name"`
	Version     int    `json:"version"`
	UpdatedBy   string `json:"updated_by"`
	Changes     []string `json:"changes,omitempty"`
}

// AlertTriggeredEvent represents an alert
type AlertTriggeredEvent struct {
	AlertID     string            `json:"alert_id"`
	AlertName   string            `json:"alert_name"`
	Severity    string            `json:"severity"`
	Description string            `json:"description"`
	Labels      map[string]string `json:"labels"`
	Value       float64           `json:"value,omitempty"`
	Threshold   float64           `json:"threshold,omitempty"`
}

// EventHandler defines the interface for handling events
type EventHandler interface {
	Handle(ctx context.Context, event *Event) error
}

// EventHandlerFunc is a function adapter for EventHandler
type EventHandlerFunc func(ctx context.Context, event *Event) error

func (f EventHandlerFunc) Handle(ctx context.Context, event *Event) error {
	return f(ctx, event)
}

// Publisher defines the interface for publishing events
type Publisher interface {
	Publish(ctx context.Context, event *Event) error
	PublishAsync(ctx context.Context, event *Event) error
	Close() error
}

// Subscriber defines the interface for subscribing to events
type Subscriber interface {
	Subscribe(ctx context.Context, eventType EventType, handler EventHandler) error
	SubscribePattern(ctx context.Context, pattern string, handler EventHandler) error
	Unsubscribe(eventType EventType) error
	Close() error
}

// EventBus defines the interface for event publishing and subscription
// This interface is compatible with the existing GDC EventBus interface
type EventBus interface {
	// Legacy interface methods for backward compatibility
	Publish(ctx context.Context, topic string, event interface{}) error
	Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, event interface{}) error) error
	Unsubscribe(ctx context.Context, topic string, handler func(ctx context.Context, event interface{}) error) error
	Close() error
	
	// New typed event methods
	PublishEvent(ctx context.Context, event *Event) error
	PublishEventAsync(ctx context.Context, event *Event) error
	SubscribeToEventType(ctx context.Context, eventType EventType, handler EventHandler) error
	SubscribeToPattern(ctx context.Context, pattern string, handler EventHandler) error
	UnsubscribeFromEventType(eventType EventType) error
}