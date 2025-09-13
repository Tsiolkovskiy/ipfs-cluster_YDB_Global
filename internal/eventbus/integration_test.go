package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestEventFlows tests complete event flows for different scenarios
func TestEventFlows(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()

	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"test.events.>"},
		MaxAge:     time.Hour,
		MaxBytes:   1024 * 1024,
		MaxMsgs:    1000,
		Replicas:   1,
	}

	bus, err := NewNATSEventBus(config, logger)
	require.NoError(t, err)
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("PinWorkflow", func(t *testing.T) {
		testPinWorkflow(t, ctx, bus)
	})

	t.Run("HealthMonitoring", func(t *testing.T) {
		testHealthMonitoring(t, ctx, bus)
	})

	t.Run("PolicyManagement", func(t *testing.T) {
		testPolicyManagement(t, ctx, bus)
	})

	t.Run("AlertingFlow", func(t *testing.T) {
		testAlertingFlow(t, ctx, bus)
	})
}

func testPinWorkflow(t *testing.T, ctx context.Context, bus EventBus) {
	var events []*Event
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Subscribe to all pin-related events
	pinHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		wg.Done()
		return nil
	})

	err := bus.SubscribeToPattern(ctx, "pin.*", pinHandler)
	require.NoError(t, err)

	// Subscribe to plan events
	planHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		wg.Done()
		return nil
	})

	err = bus.SubscribeToEventType(ctx, EventTypePlanReady, planHandler)
	require.NoError(t, err)

	// Subscribe to execution events
	execHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		wg.Done()
		return nil
	})

	err = bus.SubscribeToEventType(ctx, EventTypeExecProgress, execHandler)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(100 * time.Millisecond)

	traceID := "pin-workflow-trace-123"
	cid := "QmTestPin123"

	// Simulate pin workflow
	wg.Add(5) // Expecting 5 events

	// 1. Pin requested
	pinReqEvent := NewPinRequestedEvent("api-gateway", &PinRequestedEvent{
		CID:         cid,
		Size:        1024,
		Policies:    []string{"rf-3", "zone-msk-nn"},
		Constraints: map[string]string{"priority": "high"},
		Priority:    1,
		RequestedBy: "user123",
	}, traceID)

	err = bus.PublishEvent(ctx, pinReqEvent)
	require.NoError(t, err)

	// 2. Plan ready
	planReadyEvent := NewPlanReadyEvent("scheduler", &PlanReadyEvent{
		PlanID: "plan-123",
		CID:    cid,
		Assignments: []struct {
			NodeID    string `json:"node_id"`
			ClusterID string `json:"cluster_id"`
			ZoneID    string `json:"zone_id"`
		}{
			{NodeID: "node1", ClusterID: "cluster1", ZoneID: "msk"},
			{NodeID: "node2", ClusterID: "cluster2", ZoneID: "nn"},
		},
		EstimatedCost: 10.5,
	}, traceID)

	err = bus.PublishEvent(ctx, planReadyEvent)
	require.NoError(t, err)

	// 3. Execution progress
	execProgressEvent := NewExecProgressEvent("orchestrator", &ExecProgressEvent{
		WorkflowID: "workflow-123",
		StepID:     "pin-step-1",
		Status:     "running",
		Progress:   0.5,
		Message:    "Pinning to cluster1",
	}, traceID)

	err = bus.PublishEvent(ctx, execProgressEvent)
	require.NoError(t, err)

	// 4. Another execution progress
	execProgressEvent2 := NewExecProgressEvent("orchestrator", &ExecProgressEvent{
		WorkflowID: "workflow-123",
		StepID:     "pin-step-2",
		Status:     "running",
		Progress:   1.0,
		Message:    "Pinning to cluster2",
	}, traceID)

	err = bus.PublishEvent(ctx, execProgressEvent2)
	require.NoError(t, err)

	// 5. Pin completed
	assignments := []map[string]string{
		{"node_id": "node1", "cluster_id": "cluster1", "zone_id": "msk"},
		{"node_id": "node2", "cluster_id": "cluster2", "zone_id": "nn"},
	}
	pinCompletedEvent := NewPinCompletedEvent("orchestrator", cid, assignments, traceID)

	err = bus.PublishEvent(ctx, pinCompletedEvent)
	require.NoError(t, err)

	// Wait for all events
	wg.Wait()

	// Verify events were received in order and with correct trace ID
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, events, 5)

	// All events should have the same trace ID
	for _, event := range events {
		assert.Equal(t, traceID, event.TraceID)
	}

	// Verify event types
	eventTypes := make([]EventType, len(events))
	for i, event := range events {
		eventTypes[i] = event.Type
	}

	expectedTypes := []EventType{
		EventTypePinRequested,
		EventTypePlanReady,
		EventTypeExecProgress,
		EventTypeExecProgress,
		EventTypePinCompleted,
	}

	assert.ElementsMatch(t, expectedTypes, eventTypes)
}

func testHealthMonitoring(t *testing.T, ctx context.Context, bus EventBus) {
	var healthEvents []*Event
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Subscribe to health events
	healthHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		healthEvents = append(healthEvents, event)
		mu.Unlock()
		wg.Done()
		return nil
	})

	err := bus.SubscribeToEventType(ctx, EventTypeHealthChanged, healthHandler)
	require.NoError(t, err)

	err = bus.SubscribeToEventType(ctx, EventTypeNodeDown, healthHandler)
	require.NoError(t, err)

	err = bus.SubscribeToEventType(ctx, EventTypeNodeUp, healthHandler)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(100 * time.Millisecond)

	traceID := "health-monitoring-trace-123"
	nodeID := "node-123"

	wg.Add(3) // Expecting 3 events

	// 1. Health changed
	healthChangedEvent := NewHealthChangedEvent("monitor", &HealthChangedEvent{
		ResourceType: "node",
		ResourceID:   nodeID,
		OldStatus:    "healthy",
		NewStatus:    "degraded",
		Reason:       "high CPU usage",
	}, traceID)

	err = bus.PublishEvent(ctx, healthChangedEvent)
	require.NoError(t, err)

	// 2. Node down
	nodeDownEvent := NewNodeDownEvent("monitor", nodeID, "connection timeout", traceID)
	err = bus.PublishEvent(ctx, nodeDownEvent)
	require.NoError(t, err)

	// 3. Node up (after recovery)
	nodeUpEvent := NewNodeUpEvent("monitor", nodeID, traceID)
	err = bus.PublishEvent(ctx, nodeUpEvent)
	require.NoError(t, err)

	// Wait for all events
	wg.Wait()

	// Verify events
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, healthEvents, 3)

	// All events should have the same trace ID
	for _, event := range healthEvents {
		assert.Equal(t, traceID, event.TraceID)
	}

	// Verify event sequence
	assert.Equal(t, EventTypeHealthChanged, healthEvents[0].Type)
	assert.Equal(t, EventTypeNodeDown, healthEvents[1].Type)
	assert.Equal(t, EventTypeNodeUp, healthEvents[2].Type)
}

func testPolicyManagement(t *testing.T, ctx context.Context, bus EventBus) {
	var policyEvents []*Event
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Subscribe to policy events
	policyHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		policyEvents = append(policyEvents, event)
		mu.Unlock()
		wg.Done()
		return nil
	})

	err := bus.SubscribeToEventType(ctx, EventTypePolicyUpdated, policyHandler)
	require.NoError(t, err)

	err = bus.SubscribeToEventType(ctx, EventTypePolicyDeleted, policyHandler)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(100 * time.Millisecond)

	traceID := "policy-management-trace-123"
	policyID := "policy-123"

	wg.Add(2) // Expecting 2 events

	// 1. Policy updated
	policyUpdatedEvent := NewPolicyUpdatedEvent("policy-engine", &PolicyUpdatedEvent{
		PolicyID:   policyID,
		PolicyName: "replication-policy",
		Version:    2,
		UpdatedBy:  "admin",
		Changes:    []string{"updated RF from 2 to 3", "added zone constraint"},
	}, traceID)

	err = bus.PublishEvent(ctx, policyUpdatedEvent)
	require.NoError(t, err)

	// 2. Policy deleted
	policyDeletedEvent := NewPolicyDeletedEvent("policy-engine", policyID, "admin", traceID)
	err = bus.PublishEvent(ctx, policyDeletedEvent)
	require.NoError(t, err)

	// Wait for all events
	wg.Wait()

	// Verify events
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, policyEvents, 2)

	// All events should have the same trace ID
	for _, event := range policyEvents {
		assert.Equal(t, traceID, event.TraceID)
	}

	// Verify event sequence
	assert.Equal(t, EventTypePolicyUpdated, policyEvents[0].Type)
	assert.Equal(t, EventTypePolicyDeleted, policyEvents[1].Type)
}

func testAlertingFlow(t *testing.T, ctx context.Context, bus EventBus) {
	var alertEvents []*Event
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Subscribe to alert events
	alertHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		alertEvents = append(alertEvents, event)
		mu.Unlock()
		wg.Done()
		return nil
	})

	err := bus.SubscribeToEventType(ctx, EventTypeAlertTriggered, alertHandler)
	require.NoError(t, err)

	err = bus.SubscribeToEventType(ctx, EventTypeAlertResolved, alertHandler)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(100 * time.Millisecond)

	traceID := "alerting-flow-trace-123"
	alertID := "alert-123"

	wg.Add(2) // Expecting 2 events

	// 1. Alert triggered
	alertTriggeredEvent := NewAlertTriggeredEvent("monitor", &AlertTriggeredEvent{
		AlertID:     alertID,
		AlertName:   "high-cpu-usage",
		Severity:    "critical",
		Description: "CPU usage above 90%",
		Labels: map[string]string{
			"node_id": "node-123",
			"zone":    "msk",
		},
		Value:     95.5,
		Threshold: 90.0,
	}, traceID)

	err = bus.PublishEvent(ctx, alertTriggeredEvent)
	require.NoError(t, err)

	// 2. Alert resolved
	alertResolvedEvent := NewAlertResolvedEvent("monitor", alertID, "auto-resolved", traceID)
	err = bus.PublishEvent(ctx, alertResolvedEvent)
	require.NoError(t, err)

	// Wait for all events
	wg.Wait()

	// Verify events
	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, alertEvents, 2)

	// All events should have the same trace ID
	for _, event := range alertEvents {
		assert.Equal(t, traceID, event.TraceID)
	}

	// Verify event sequence
	assert.Equal(t, EventTypeAlertTriggered, alertEvents[0].Type)
	assert.Equal(t, EventTypeAlertResolved, alertEvents[1].Type)
}

func TestEventBusResilience(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()

	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"test.events.>"},
		MaxAge:     time.Hour,
		MaxBytes:   1024 * 1024,
		MaxMsgs:    1000,
		Replicas:   1,
	}

	bus, err := NewNATSEventBus(config, logger)
	require.NoError(t, err)
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test publishing many events rapidly
	t.Run("HighThroughput", func(t *testing.T) {
		var receivedCount int
		var mu sync.Mutex
		var wg sync.WaitGroup

		handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
			mu.Lock()
			receivedCount++
			mu.Unlock()
			wg.Done()
			return nil
		})

		err = bus.SubscribeToEventType(ctx, EventTypePinRequested, handler)
		require.NoError(t, err)

		// Give subscriber time to set up
		time.Sleep(100 * time.Millisecond)

		numEvents := 100
		wg.Add(numEvents)

		// Publish events rapidly
		for i := 0; i < numEvents; i++ {
			event := NewPinRequestedEvent("test", &PinRequestedEvent{
				CID:  "QmTest" + string(rune(i)),
				Size: int64(i * 1024),
			}, "")

			err = bus.PublishEventAsync(ctx, event)
			require.NoError(t, err)
		}

		// Wait for all events to be processed
		wg.Wait()

		mu.Lock()
		assert.Equal(t, numEvents, receivedCount)
		mu.Unlock()
	})

	// Test error handling in subscribers
	t.Run("ErrorHandling", func(t *testing.T) {
		var attempts int
		var mu sync.Mutex

		// Handler that fails first few times
		handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
			mu.Lock()
			attempts++
			if attempts < 3 {
				mu.Unlock()
				return assert.AnError // Simulate error
			}
			mu.Unlock()
			return nil
		})

		err = bus.SubscribeToEventType(ctx, EventTypePlanReady, handler)
		require.NoError(t, err)

		// Give subscriber time to set up
		time.Sleep(100 * time.Millisecond)

		event := NewPlanReadyEvent("test", &PlanReadyEvent{
			PlanID: "test-plan",
			CID:    "QmTest123",
		}, "")

		err = bus.PublishEvent(ctx, event)
		require.NoError(t, err)

		// Wait for retries
		time.Sleep(2 * time.Second)

		mu.Lock()
		assert.GreaterOrEqual(t, attempts, 3) // Should have retried
		mu.Unlock()
	})
}