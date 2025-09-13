package eventbus

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event creation helpers for typed events

// NewPinRequestedEvent creates a new pin requested event
func NewPinRequestedEvent(source string, data *PinRequestedEvent, traceID string) *Event {
	eventData := make(map[string]interface{})
	if jsonData, err := json.Marshal(data); err == nil {
		json.Unmarshal(jsonData, &eventData)
	}
	
	event := NewEvent(EventTypePinRequested, source, data.CID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewPinCompletedEvent creates a new pin completed event
func NewPinCompletedEvent(source, cid string, assignments []map[string]string, traceID string) *Event {
	eventData := map[string]interface{}{
		"cid":         cid,
		"assignments": assignments,
		"completed_at": time.Now().UTC(),
	}
	
	event := NewEvent(EventTypePinCompleted, source, cid, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewPinFailedEvent creates a new pin failed event
func NewPinFailedEvent(source, cid, reason string, traceID string) *Event {
	eventData := map[string]interface{}{
		"cid":       cid,
		"reason":    reason,
		"failed_at": time.Now().UTC(),
	}
	
	event := NewEvent(EventTypePinFailed, source, cid, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewPlanReadyEvent creates a new plan ready event
func NewPlanReadyEvent(source string, data *PlanReadyEvent, traceID string) *Event {
	eventData := make(map[string]interface{})
	if jsonData, err := json.Marshal(data); err == nil {
		json.Unmarshal(jsonData, &eventData)
	}
	
	event := NewEvent(EventTypePlanReady, source, data.CID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewPlanFailedEvent creates a new plan failed event
func NewPlanFailedEvent(source, cid, reason string, traceID string) *Event {
	eventData := map[string]interface{}{
		"cid":       cid,
		"reason":    reason,
		"failed_at": time.Now().UTC(),
	}
	
	event := NewEvent(EventTypePlanFailed, source, cid, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewExecProgressEvent creates a new execution progress event
func NewExecProgressEvent(source string, data *ExecProgressEvent, traceID string) *Event {
	eventData := make(map[string]interface{})
	if jsonData, err := json.Marshal(data); err == nil {
		json.Unmarshal(jsonData, &eventData)
	}
	
	subject := fmt.Sprintf("workflow.%s", data.WorkflowID)
	event := NewEvent(EventTypeExecProgress, source, subject, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewExecCompletedEvent creates a new execution completed event
func NewExecCompletedEvent(source, workflowID string, result map[string]interface{}, traceID string) *Event {
	eventData := map[string]interface{}{
		"workflow_id":  workflowID,
		"result":       result,
		"completed_at": time.Now().UTC(),
	}
	
	subject := fmt.Sprintf("workflow.%s", workflowID)
	event := NewEvent(EventTypeExecCompleted, source, subject, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewExecFailedEvent creates a new execution failed event
func NewExecFailedEvent(source, workflowID, reason string, traceID string) *Event {
	eventData := map[string]interface{}{
		"workflow_id": workflowID,
		"reason":      reason,
		"failed_at":   time.Now().UTC(),
	}
	
	subject := fmt.Sprintf("workflow.%s", workflowID)
	event := NewEvent(EventTypeExecFailed, source, subject, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewHealthChangedEvent creates a new health changed event
func NewHealthChangedEvent(source string, data *HealthChangedEvent, traceID string) *Event {
	eventData := make(map[string]interface{})
	if jsonData, err := json.Marshal(data); err == nil {
		json.Unmarshal(jsonData, &eventData)
	}
	
	subject := fmt.Sprintf("%s.%s", data.ResourceType, data.ResourceID)
	event := NewEvent(EventTypeHealthChanged, source, subject, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewNodeDownEvent creates a new node down event
func NewNodeDownEvent(source, nodeID, reason string, traceID string) *Event {
	eventData := map[string]interface{}{
		"node_id":   nodeID,
		"reason":    reason,
		"down_at":   time.Now().UTC(),
	}
	
	event := NewEvent(EventTypeNodeDown, source, nodeID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewNodeUpEvent creates a new node up event
func NewNodeUpEvent(source, nodeID string, traceID string) *Event {
	eventData := map[string]interface{}{
		"node_id": nodeID,
		"up_at":   time.Now().UTC(),
	}
	
	event := NewEvent(EventTypeNodeUp, source, nodeID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewZoneDownEvent creates a new zone down event
func NewZoneDownEvent(source, zoneID, reason string, traceID string) *Event {
	eventData := map[string]interface{}{
		"zone_id":  zoneID,
		"reason":   reason,
		"down_at":  time.Now().UTC(),
	}
	
	event := NewEvent(EventTypeZoneDown, source, zoneID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewZoneUpEvent creates a new zone up event
func NewZoneUpEvent(source, zoneID string, traceID string) *Event {
	eventData := map[string]interface{}{
		"zone_id": zoneID,
		"up_at":   time.Now().UTC(),
	}
	
	event := NewEvent(EventTypeZoneUp, source, zoneID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewPolicyUpdatedEvent creates a new policy updated event
func NewPolicyUpdatedEvent(source string, data *PolicyUpdatedEvent, traceID string) *Event {
	eventData := make(map[string]interface{})
	if jsonData, err := json.Marshal(data); err == nil {
		json.Unmarshal(jsonData, &eventData)
	}
	
	event := NewEvent(EventTypePolicyUpdated, source, data.PolicyID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewPolicyDeletedEvent creates a new policy deleted event
func NewPolicyDeletedEvent(source, policyID, deletedBy string, traceID string) *Event {
	eventData := map[string]interface{}{
		"policy_id":  policyID,
		"deleted_by": deletedBy,
		"deleted_at": time.Now().UTC(),
	}
	
	event := NewEvent(EventTypePolicyDeleted, source, policyID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewAlertTriggeredEvent creates a new alert triggered event
func NewAlertTriggeredEvent(source string, data *AlertTriggeredEvent, traceID string) *Event {
	eventData := make(map[string]interface{})
	if jsonData, err := json.Marshal(data); err == nil {
		json.Unmarshal(jsonData, &eventData)
	}
	
	event := NewEvent(EventTypeAlertTriggered, source, data.AlertID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// NewAlertResolvedEvent creates a new alert resolved event
func NewAlertResolvedEvent(source, alertID, resolvedBy string, traceID string) *Event {
	eventData := map[string]interface{}{
		"alert_id":    alertID,
		"resolved_by": resolvedBy,
		"resolved_at": time.Now().UTC(),
	}
	
	event := NewEvent(EventTypeAlertResolved, source, alertID, eventData)
	if traceID != "" {
		event.WithTraceID(traceID)
	}
	return event
}

// ParseEventData parses event data into a specific type
func ParseEventData[T any](event *Event, target *T) error {
	jsonData, err := json.Marshal(event.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}
	
	if err := json.Unmarshal(jsonData, target); err != nil {
		return fmt.Errorf("failed to unmarshal event data: %w", err)
	}
	
	return nil
}