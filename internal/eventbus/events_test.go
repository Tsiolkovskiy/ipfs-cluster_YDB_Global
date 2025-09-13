package eventbus

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEvent(t *testing.T) {
	eventType := EventTypePinRequested
	source := "test-source"
	subject := "test-subject"
	data := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
	}

	event := NewEvent(eventType, source, subject, data)

	assert.NotEmpty(t, event.ID)
	assert.Equal(t, eventType, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, subject, event.Subject)
	assert.Equal(t, data, event.Data)
	assert.Equal(t, "1.0", event.Version)
	assert.WithinDuration(t, time.Now(), event.Timestamp, time.Second)
	assert.Empty(t, event.TraceID)
}

func TestEvent_WithTraceID(t *testing.T) {
	event := NewEvent(EventTypePinRequested, "source", "subject", nil)
	traceID := "trace-123"

	result := event.WithTraceID(traceID)

	assert.Equal(t, event, result) // Should return the same instance
	assert.Equal(t, traceID, event.TraceID)
}

func TestNewPinRequestedEvent(t *testing.T) {
	source := "scheduler"
	traceID := "trace-123"
	data := &PinRequestedEvent{
		CID:         "QmTest123",
		Size:        1024,
		Policies:    []string{"policy1", "policy2"},
		Constraints: map[string]string{"zone": "msk"},
		Priority:    1,
		RequestedBy: "user123",
	}

	event := NewPinRequestedEvent(source, data, traceID)

	assert.Equal(t, EventTypePinRequested, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, data.CID, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	// Verify data was marshaled correctly
	assert.Equal(t, data.CID, event.Data["cid"])
	assert.Equal(t, float64(data.Size), event.Data["size"]) // JSON numbers are float64
	assert.Equal(t, data.RequestedBy, event.Data["requested_by"])
}

func TestNewPinCompletedEvent(t *testing.T) {
	source := "orchestrator"
	cid := "QmTest123"
	traceID := "trace-123"
	assignments := []map[string]string{
		{"node_id": "node1", "cluster_id": "cluster1", "zone_id": "msk"},
		{"node_id": "node2", "cluster_id": "cluster2", "zone_id": "nn"},
	}

	event := NewPinCompletedEvent(source, cid, assignments, traceID)

	assert.Equal(t, EventTypePinCompleted, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, cid, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, cid, event.Data["cid"])
	assert.Equal(t, assignments, event.Data["assignments"])
	assert.NotNil(t, event.Data["completed_at"])
}

func TestNewPinFailedEvent(t *testing.T) {
	source := "orchestrator"
	cid := "QmTest123"
	reason := "insufficient nodes"
	traceID := "trace-123"

	event := NewPinFailedEvent(source, cid, reason, traceID)

	assert.Equal(t, EventTypePinFailed, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, cid, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, cid, event.Data["cid"])
	assert.Equal(t, reason, event.Data["reason"])
	assert.NotNil(t, event.Data["failed_at"])
}

func TestNewPlanReadyEvent(t *testing.T) {
	source := "scheduler"
	traceID := "trace-123"
	data := &PlanReadyEvent{
		PlanID: "plan-123",
		CID:    "QmTest123",
		Assignments: []struct {
			NodeID    string `json:"node_id"`
			ClusterID string `json:"cluster_id"`
			ZoneID    string `json:"zone_id"`
		}{
			{NodeID: "node1", ClusterID: "cluster1", ZoneID: "msk"},
			{NodeID: "node2", ClusterID: "cluster2", ZoneID: "nn"},
		},
		EstimatedCost: 10.5,
	}

	event := NewPlanReadyEvent(source, data, traceID)

	assert.Equal(t, EventTypePlanReady, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, data.CID, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, data.PlanID, event.Data["plan_id"])
	assert.Equal(t, data.CID, event.Data["cid"])
	assert.Equal(t, data.EstimatedCost, event.Data["estimated_cost"])
}

func TestNewExecProgressEvent(t *testing.T) {
	source := "orchestrator"
	traceID := "trace-123"
	completedAt := time.Now().UTC()
	data := &ExecProgressEvent{
		WorkflowID:  "workflow-123",
		StepID:      "step-1",
		Status:      "running",
		Progress:    0.75,
		Message:     "Processing nodes",
		CompletedAt: &completedAt,
	}

	event := NewExecProgressEvent(source, data, traceID)

	assert.Equal(t, EventTypeExecProgress, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, "workflow.workflow-123", event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, data.WorkflowID, event.Data["workflow_id"])
	assert.Equal(t, data.StepID, event.Data["step_id"])
	assert.Equal(t, data.Status, event.Data["status"])
	assert.Equal(t, data.Progress, event.Data["progress"])
	assert.Equal(t, data.Message, event.Data["message"])
}

func TestNewHealthChangedEvent(t *testing.T) {
	source := "monitor"
	traceID := "trace-123"
	data := &HealthChangedEvent{
		ResourceType: "node",
		ResourceID:   "node-123",
		OldStatus:    "healthy",
		NewStatus:    "degraded",
		Reason:       "high CPU usage",
	}

	event := NewHealthChangedEvent(source, data, traceID)

	assert.Equal(t, EventTypeHealthChanged, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, "node.node-123", event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, data.ResourceType, event.Data["resource_type"])
	assert.Equal(t, data.ResourceID, event.Data["resource_id"])
	assert.Equal(t, data.OldStatus, event.Data["old_status"])
	assert.Equal(t, data.NewStatus, event.Data["new_status"])
	assert.Equal(t, data.Reason, event.Data["reason"])
}

func TestNewNodeDownEvent(t *testing.T) {
	source := "monitor"
	nodeID := "node-123"
	reason := "connection timeout"
	traceID := "trace-123"

	event := NewNodeDownEvent(source, nodeID, reason, traceID)

	assert.Equal(t, EventTypeNodeDown, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, nodeID, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, nodeID, event.Data["node_id"])
	assert.Equal(t, reason, event.Data["reason"])
	assert.NotNil(t, event.Data["down_at"])
}

func TestNewZoneUpEvent(t *testing.T) {
	source := "monitor"
	zoneID := "msk"
	traceID := "trace-123"

	event := NewZoneUpEvent(source, zoneID, traceID)

	assert.Equal(t, EventTypeZoneUp, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, zoneID, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, zoneID, event.Data["zone_id"])
	assert.NotNil(t, event.Data["up_at"])
}

func TestNewPolicyUpdatedEvent(t *testing.T) {
	source := "policy-engine"
	traceID := "trace-123"
	data := &PolicyUpdatedEvent{
		PolicyID:   "policy-123",
		PolicyName: "replication-policy",
		Version:    2,
		UpdatedBy:  "admin",
		Changes:    []string{"updated RF from 2 to 3", "added zone constraint"},
	}

	event := NewPolicyUpdatedEvent(source, data, traceID)

	assert.Equal(t, EventTypePolicyUpdated, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, data.PolicyID, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, data.PolicyID, event.Data["policy_id"])
	assert.Equal(t, data.PolicyName, event.Data["policy_name"])
	assert.Equal(t, float64(data.Version), event.Data["version"]) // JSON numbers are float64
	assert.Equal(t, data.UpdatedBy, event.Data["updated_by"])
}

func TestNewAlertTriggeredEvent(t *testing.T) {
	source := "monitor"
	traceID := "trace-123"
	data := &AlertTriggeredEvent{
		AlertID:     "alert-123",
		AlertName:   "high-cpu-usage",
		Severity:    "critical",
		Description: "CPU usage above 90%",
		Labels: map[string]string{
			"node_id": "node-123",
			"zone":    "msk",
		},
		Value:     95.5,
		Threshold: 90.0,
	}

	event := NewAlertTriggeredEvent(source, data, traceID)

	assert.Equal(t, EventTypeAlertTriggered, event.Type)
	assert.Equal(t, source, event.Source)
	assert.Equal(t, data.AlertID, event.Subject)
	assert.Equal(t, traceID, event.TraceID)
	
	assert.Equal(t, data.AlertID, event.Data["alert_id"])
	assert.Equal(t, data.AlertName, event.Data["alert_name"])
	assert.Equal(t, data.Severity, event.Data["severity"])
	assert.Equal(t, data.Description, event.Data["description"])
	assert.Equal(t, data.Value, event.Data["value"])
	assert.Equal(t, data.Threshold, event.Data["threshold"])
}

func TestParseEventData(t *testing.T) {
	// Create a PinRequestedEvent
	originalData := &PinRequestedEvent{
		CID:         "QmTest123",
		Size:        1024,
		Policies:    []string{"policy1", "policy2"},
		Constraints: map[string]string{"zone": "msk"},
		Priority:    1,
		RequestedBy: "user123",
	}

	event := NewPinRequestedEvent("test", originalData, "")

	// Parse the event data back
	var parsedData PinRequestedEvent
	err := ParseEventData(event, &parsedData)
	require.NoError(t, err)

	assert.Equal(t, originalData.CID, parsedData.CID)
	assert.Equal(t, originalData.Size, parsedData.Size)
	assert.Equal(t, originalData.Policies, parsedData.Policies)
	assert.Equal(t, originalData.Constraints, parsedData.Constraints)
	assert.Equal(t, originalData.Priority, parsedData.Priority)
	assert.Equal(t, originalData.RequestedBy, parsedData.RequestedBy)
}

func TestParseEventData_InvalidType(t *testing.T) {
	event := NewEvent(EventTypePinRequested, "test", "test", map[string]interface{}{
		"invalid_field": "value",
	})

	var parsedData PinRequestedEvent
	err := ParseEventData(event, &parsedData)
	
	// Should not error, but fields will be zero values
	require.NoError(t, err)
	assert.Empty(t, parsedData.CID)
	assert.Zero(t, parsedData.Size)
}

func TestEventHandlerFunc(t *testing.T) {
	var receivedEvent *Event
	
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		receivedEvent = event
		return nil
	})

	testEvent := NewEvent(EventTypePinRequested, "test", "test", nil)
	err := handler.Handle(context.Background(), testEvent)
	
	require.NoError(t, err)
	assert.Equal(t, testEvent, receivedEvent)
}

func TestEventTypes(t *testing.T) {
	// Test that all event types are defined correctly
	eventTypes := []EventType{
		EventTypePinRequested,
		EventTypePinCompleted,
		EventTypePinFailed,
		EventTypePlanReady,
		EventTypePlanFailed,
		EventTypeExecProgress,
		EventTypeExecCompleted,
		EventTypeExecFailed,
		EventTypeHealthChanged,
		EventTypeNodeDown,
		EventTypeNodeUp,
		EventTypeZoneDown,
		EventTypeZoneUp,
		EventTypePolicyUpdated,
		EventTypePolicyDeleted,
		EventTypeAlertTriggered,
		EventTypeAlertResolved,
	}

	for _, eventType := range eventTypes {
		assert.NotEmpty(t, string(eventType))
		assert.Contains(t, string(eventType), ".")
	}
}

func TestEventWithoutTraceID(t *testing.T) {
	event := NewPinRequestedEvent("test", &PinRequestedEvent{
		CID:  "QmTest123",
		Size: 1024,
	}, "")

	assert.Empty(t, event.TraceID)
}

func TestEventWithTraceID(t *testing.T) {
	traceID := "trace-123"
	event := NewPinRequestedEvent("test", &PinRequestedEvent{
		CID:  "QmTest123",
		Size: 1024,
	}, traceID)

	assert.Equal(t, traceID, event.TraceID)
}