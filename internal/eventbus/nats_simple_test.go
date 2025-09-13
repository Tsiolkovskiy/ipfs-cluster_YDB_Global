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

func TestNATSEventBus_BasicFunctionality(t *testing.T) {
	// Start embedded NATS server
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
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Test 1: Verify stream was created
	info, err := bus.GetStreamInfo()
	require.NoError(t, err)
	assert.Equal(t, "TEST_EVENTS", info.Config.Name)
	assert.Equal(t, []string{"gdc.events.>"}, info.Config.Subjects)
	
	// Test 2: Publish and Subscribe
	testEvent := NewEvent(EventTypePinRequested, "test-source", "test-cid", map[string]interface{}{
		"cid":  "test-cid",
		"size": 1024,
	})
	
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
	time.Sleep(200 * time.Millisecond)
	
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
	// JSON unmarshaling converts numbers to float64
	assert.Equal(t, "test-cid", receivedEvent.Data["cid"])
	assert.Equal(t, float64(1024), receivedEvent.Data["size"])
	
	// Test 3: Async publish
	testEvent2 := NewEvent(EventTypePlanReady, "test-source", "test-plan", map[string]interface{}{
		"plan_id": "test-plan",
		"cid":     "test-cid",
	})
	
	var receivedEvent2 *Event
	var wg2 sync.WaitGroup
	wg2.Add(1)
	
	handler2 := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		receivedEvent2 = event
		wg2.Done()
		return nil
	})
	
	err = bus.SubscribeToEventType(ctx, EventTypePlanReady, handler2)
	require.NoError(t, err)
	
	// Give subscriber time to set up
	time.Sleep(200 * time.Millisecond)
	
	// Publish event asynchronously
	err = bus.PublishEventAsync(ctx, testEvent2)
	require.NoError(t, err)
	
	// Wait for event to be received
	wg2.Wait()
	
	// Verify received event
	require.NotNil(t, receivedEvent2)
	assert.Equal(t, testEvent2.ID, receivedEvent2.ID)
	assert.Equal(t, testEvent2.Type, receivedEvent2.Type)
}

func TestNATSEventBus_SimplePatternSubscription(t *testing.T) {
	// Start embedded NATS server
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
	time.Sleep(200 * time.Millisecond)
	
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

func TestNATSEventBus_SimpleMessageDeduplication(t *testing.T) {
	// Start embedded NATS server
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
	time.Sleep(200 * time.Millisecond)
	
	// Publish the same event multiple times
	for i := 0; i < 3; i++ {
		err = bus.PublishEvent(ctx, testEvent)
		require.NoError(t, err)
	}
	
	// Wait a bit for processing
	time.Sleep(1 * time.Second)
	
	// Should only receive the event once due to deduplication
	mu.Lock()
	assert.Equal(t, 1, receivedCount)
	mu.Unlock()
}