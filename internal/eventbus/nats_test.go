package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestNATSServer starts an embedded NATS server for testing
func startTestNATSServer(t *testing.T) *server.Server {
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Random port
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	
	s, err := server.NewServer(opts)
	require.NoError(t, err)
	
	go s.Start()
	
	// Wait for server to be ready
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server not ready")
	}
	
	return s
}

func TestNATSEventBus_NewNATSEventBus(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
		MaxAge:     time.Hour,
		MaxBytes:   1024 * 1024,
		MaxMsgs:    1000,
		Replicas:   1,
	}
	
	bus, err := NewNATSEventBus(config, logger)
	require.NoError(t, err)
	require.NotNil(t, bus)
	
	defer bus.Close()
	
	// Verify stream was created
	info, err := bus.GetStreamInfo()
	require.NoError(t, err)
	assert.Equal(t, "TEST_EVENTS", info.Config.Name)
	assert.Equal(t, []string{"gdc.events.>"}, info.Config.Subjects)
}

func TestNATSEventBus_PublishAndSubscribe(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
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
	
	// Create test event
	testEvent := NewEvent(EventTypePinRequested, "test-source", "test-cid", map[string]interface{}{
		"cid":  "test-cid",
		"size": 1024,
	})
	
	// Set up subscriber
	var receivedEvent *Event
	var wg sync.WaitGroup
	wg.Add(1)
	
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		receivedEvent = event
		wg.Done()
		return nil
	})
	
	err = bus.SubscribeToEventType(ctx, EventTypePinRequested, handler)
	require.NoError(t, err)
	
	// Give subscriber time to set up
	time.Sleep(100 * time.Millisecond)
	
	// Publish event
	err = bus.PublishEvent(ctx, testEvent)
	require.NoError(t, err)
	
	// Wait for event to be received
	wg.Wait()
	
	// Verify received event
	require.NotNil(t, receivedEvent)
	assert.Equal(t, testEvent.ID, receivedEvent.ID)
	assert.Equal(t, testEvent.Type, receivedEvent.Type)
	assert.Equal(t, testEvent.Source, receivedEvent.Source)
	assert.Equal(t, testEvent.Subject, receivedEvent.Subject)
	assert.Equal(t, testEvent.Data, receivedEvent.Data)
}

func TestNATSEventBus_PublishAsync(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
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
	
	// Create test event
	testEvent := NewEvent(EventTypePlanReady, "test-source", "test-plan", map[string]interface{}{
		"plan_id": "test-plan",
		"cid":     "test-cid",
	})
	
	// Set up subscriber
	var receivedEvent *Event
	var wg sync.WaitGroup
	wg.Add(1)
	
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		receivedEvent = event
		wg.Done()
		return nil
	})
	
	err = bus.SubscribeToEventType(ctx, EventTypePlanReady, handler)
	require.NoError(t, err)
	
	// Give subscriber time to set up
	time.Sleep(100 * time.Millisecond)
	
	// Publish event asynchronously
	err = bus.PublishEventAsync(ctx, testEvent)
	require.NoError(t, err)
	
	// Wait for event to be received
	wg.Wait()
	
	// Verify received event
	require.NotNil(t, receivedEvent)
	assert.Equal(t, testEvent.ID, receivedEvent.ID)
	assert.Equal(t, testEvent.Type, receivedEvent.Type)
}

func TestNATSEventBus_SubscribePattern(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
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
	
	// Set up pattern subscriber for all pin events
	var receivedEvents []*Event
	var mu sync.Mutex
	var wg sync.WaitGroup
	
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		receivedEvents = append(receivedEvents, event)
		mu.Unlock()
		wg.Done()
		return nil
	})
	
	err = bus.SubscribeToPattern(ctx, "pin.*", handler)
	require.NoError(t, err)
	
	// Give subscriber time to set up
	time.Sleep(100 * time.Millisecond)
	
	// Publish multiple pin events
	events := []*Event{
		NewEvent(EventTypePinRequested, "test", "cid1", map[string]interface{}{"cid": "cid1"}),
		NewEvent(EventTypePinCompleted, "test", "cid2", map[string]interface{}{"cid": "cid2"}),
		NewEvent(EventTypePinFailed, "test", "cid3", map[string]interface{}{"cid": "cid3"}),
	}
	
	wg.Add(len(events))
	
	for _, event := range events {
		err = bus.PublishEvent(ctx, event)
		require.NoError(t, err)
	}
	
	// Wait for all events to be received
	wg.Wait()
	
	// Verify all events were received
	mu.Lock()
	assert.Len(t, receivedEvents, len(events))
	mu.Unlock()
}

func TestNATSEventBus_MultipleSubscribers(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
		MaxAge:     time.Hour,
		MaxBytes:   1024 * 1024,
		MaxMsgs:    1000,
		Replicas:   1,
	}
	
	// Create two separate event bus instances
	bus1, err := NewNATSEventBus(config, logger)
	require.NoError(t, err)
	defer bus1.Close()
	
	bus2, err := NewNATSEventBus(config, logger)
	require.NoError(t, err)
	defer bus2.Close()
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Set up subscribers on both buses
	var received1, received2 *Event
	var wg sync.WaitGroup
	wg.Add(2)
	
	handler1 := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		received1 = event
		wg.Done()
		return nil
	})
	
	handler2 := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		received2 = event
		wg.Done()
		return nil
	})
	
	err = bus1.SubscribeToEventType(ctx, EventTypeExecProgress, handler1)
	require.NoError(t, err)
	
	err = bus2.SubscribeToEventType(ctx, EventTypeExecProgress, handler2)
	require.NoError(t, err)
	
	// Give subscribers time to set up
	time.Sleep(100 * time.Millisecond)
	
	// Publish event from first bus
	testEvent := NewEvent(EventTypeExecProgress, "test", "workflow1", map[string]interface{}{
		"workflow_id": "workflow1",
		"progress":    0.5,
	})
	
	err = bus1.PublishEvent(ctx, testEvent)
	require.NoError(t, err)
	
	// Wait for both subscribers to receive the event
	wg.Wait()
	
	// Both subscribers should receive the same event
	require.NotNil(t, received1)
	require.NotNil(t, received2)
	assert.Equal(t, testEvent.ID, received1.ID)
	assert.Equal(t, testEvent.ID, received2.ID)
}

func TestNATSEventBus_Unsubscribe(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
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
	
	// Subscribe to event
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		return nil
	})
	
	err = bus.SubscribeToEventType(ctx, EventTypeHealthChanged, handler)
	require.NoError(t, err)
	
	// Unsubscribe
	err = bus.UnsubscribeFromEventType(EventTypeHealthChanged)
	require.NoError(t, err)
	
	// Try to unsubscribe again (should fail)
	err = bus.UnsubscribeFromEventType(EventTypeHealthChanged)
	assert.Error(t, err)
}

func TestNATSEventBus_MessageDeduplication(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
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
	
	// Create event with specific ID
	testEvent := NewEvent(EventTypeAlertTriggered, "test", "alert1", map[string]interface{}{
		"alert_id": "alert1",
		"severity": "high",
	})
	
	// Set up subscriber
	var receivedCount int
	var mu sync.Mutex
	
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		return nil
	})
	
	err = bus.SubscribeToEventType(ctx, EventTypeAlertTriggered, handler)
	require.NoError(t, err)
	
	// Give subscriber time to set up
	time.Sleep(100 * time.Millisecond)
	
	// Publish the same event multiple times
	for i := 0; i < 3; i++ {
		err = bus.PublishEvent(ctx, testEvent)
		require.NoError(t, err)
	}
	
	// Wait a bit for processing
	time.Sleep(500 * time.Millisecond)
	
	// Should only receive the event once due to deduplication
	mu.Lock()
	assert.Equal(t, 1, receivedCount)
	mu.Unlock()
}

func TestNATSEventBus_ErrorHandling(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
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
	
	// Test subscribing to the same event type twice
	handler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		return nil
	})
	
	err = bus.SubscribeToEventType(ctx, EventTypePolicyUpdated, handler)
	require.NoError(t, err)
	
	err = bus.SubscribeToEventType(ctx, EventTypePolicyUpdated, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already subscribed")
}

func TestNATSEventBus_ConnectionFailure(t *testing.T) {
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:            "nats://localhost:9999", // Non-existent server
		StreamName:     "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
		ConnectTimeout: 1 * time.Second,
	}
	
	_, err := NewNATSEventBus(config, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to NATS")
}

func TestNATSEventBus_StreamPurge(t *testing.T) {
	s := startTestNATSServer(t)
	defer s.Shutdown()
	
	logger := zaptest.NewLogger(t)
	config := &NATSConfig{
		URL:        s.ClientURL(),
		StreamName: "TEST_EVENTS",
		StreamSubjects: []string{"gdc.events.>"},
		MaxAge:     time.Hour,
		MaxBytes:   1024 * 1024,
		MaxMsgs:    1000,
		Replicas:   1,
	}
	
	bus, err := NewNATSEventBus(config, logger)
	require.NoError(t, err)
	defer bus.Close()
	
	ctx := context.Background()
	
	// Publish some events
	for i := 0; i < 5; i++ {
		event := NewEvent(EventTypePinRequested, "test", "cid", map[string]interface{}{
			"cid": "test-cid",
		})
		err = bus.PublishEvent(ctx, event)
		require.NoError(t, err)
	}
	
	// Check stream has messages
	info, err := bus.GetStreamInfo()
	require.NoError(t, err)
	assert.Greater(t, info.State.Msgs, uint64(0))
	
	// Purge stream
	err = bus.PurgeStream()
	require.NoError(t, err)
	
	// Check stream is empty
	info, err = bus.GetStreamInfo()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), info.State.Msgs)
}