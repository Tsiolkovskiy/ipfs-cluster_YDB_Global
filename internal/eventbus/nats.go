package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// NATSEventBus implements EventBus using NATS JetStream
type NATSEventBus struct {
	conn        *nats.Conn
	js          nats.JetStreamContext
	logger      *zap.Logger
	config      *NATSConfig
	
	// Subscription management
	subscriptions map[string]*nats.Subscription
	subMutex      sync.RWMutex
	
	// Graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NATSConfig holds NATS JetStream configuration
type NATSConfig struct {
	URL                string        `json:"url" yaml:"url"`
	StreamName         string        `json:"stream_name" yaml:"stream_name"`
	StreamSubjects     []string      `json:"stream_subjects" yaml:"stream_subjects"`
	MaxAge             time.Duration `json:"max_age" yaml:"max_age"`
	MaxBytes           int64         `json:"max_bytes" yaml:"max_bytes"`
	MaxMsgs            int64         `json:"max_msgs" yaml:"max_msgs"`
	Replicas           int           `json:"replicas" yaml:"replicas"`
	ConnectTimeout     time.Duration `json:"connect_timeout" yaml:"connect_timeout"`
	ReconnectWait      time.Duration `json:"reconnect_wait" yaml:"reconnect_wait"`
	MaxReconnectAttempts int         `json:"max_reconnect_attempts" yaml:"max_reconnect_attempts"`
}

// DefaultNATSConfig returns default NATS configuration
func DefaultNATSConfig() *NATSConfig {
	return &NATSConfig{
		URL:                  "nats://localhost:4222",
		StreamName:           "GDC_EVENTS",
		StreamSubjects:       []string{"gdc.events.>"},
		MaxAge:               24 * time.Hour,
		MaxBytes:             1024 * 1024 * 1024, // 1GB
		MaxMsgs:              1000000,
		Replicas:             1,
		ConnectTimeout:       10 * time.Second,
		ReconnectWait:        2 * time.Second,
		MaxReconnectAttempts: 10,
	}
}

// NewNATSEventBus creates a new NATS JetStream event bus
func NewNATSEventBus(config *NATSConfig, logger *zap.Logger) (*NATSEventBus, error) {
	if config == nil {
		config = DefaultNATSConfig()
	}
	
	if logger == nil {
		logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	bus := &NATSEventBus{
		logger:        logger,
		config:        config,
		subscriptions: make(map[string]*nats.Subscription),
		ctx:           ctx,
		cancel:        cancel,
	}

	if err := bus.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	if err := bus.setupStream(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to setup JetStream: %w", err)
	}

	return bus, nil
}

// connect establishes connection to NATS server
func (n *NATSEventBus) connect() error {
	opts := []nats.Option{
		nats.Name("gdc-eventbus"),
		nats.Timeout(n.config.ConnectTimeout),
		nats.ReconnectWait(n.config.ReconnectWait),
		nats.MaxReconnects(n.config.MaxReconnectAttempts),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			n.logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			n.logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			n.logger.Info("NATS connection closed")
		}),
	}

	conn, err := nats.Connect(n.config.URL, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}

	n.conn = conn
	n.js = js
	
	n.logger.Info("Connected to NATS JetStream", 
		zap.String("url", n.config.URL),
		zap.String("stream", n.config.StreamName))
	
	return nil
}

// setupStream creates or updates the JetStream stream
func (n *NATSEventBus) setupStream() error {
	streamConfig := &nats.StreamConfig{
		Name:        n.config.StreamName,
		Subjects:    n.config.StreamSubjects,
		Retention:   nats.LimitsPolicy,
		MaxAge:      n.config.MaxAge,
		MaxBytes:    n.config.MaxBytes,
		MaxMsgs:     n.config.MaxMsgs,
		Replicas:    n.config.Replicas,
		Storage:     nats.FileStorage,
		Duplicates:  5 * time.Minute, // Duplicate detection window
	}

	// Try to get existing stream info
	_, err := n.js.StreamInfo(n.config.StreamName)
	if err != nil {
		// Stream doesn't exist, create it
		_, err = n.js.AddStream(streamConfig)
		if err != nil {
			return fmt.Errorf("failed to create stream: %w", err)
		}
		n.logger.Info("Created JetStream stream", zap.String("stream", n.config.StreamName))
	} else {
		// Stream exists, update it
		_, err = n.js.UpdateStream(streamConfig)
		if err != nil {
			return fmt.Errorf("failed to update stream: %w", err)
		}
		n.logger.Info("Updated JetStream stream", zap.String("stream", n.config.StreamName))
	}

	return nil
}

// Legacy interface implementation for backward compatibility
func (n *NATSEventBus) Publish(ctx context.Context, topic string, event interface{}) error {
	// Convert legacy call to new typed event
	var typedEvent *Event
	
	if e, ok := event.(*Event); ok {
		typedEvent = e
	} else {
		// Create a generic event from the interface
		typedEvent = NewEvent(EventType(topic), "legacy", topic, map[string]interface{}{
			"data": event,
		})
	}
	
	return n.PublishEvent(ctx, typedEvent)
}

func (n *NATSEventBus) Subscribe(ctx context.Context, topic string, handler func(ctx context.Context, event interface{}) error) error {
	// Convert legacy handler to new typed handler
	typedHandler := EventHandlerFunc(func(ctx context.Context, event *Event) error {
		return handler(ctx, event)
	})
	
	return n.SubscribeToEventType(ctx, EventType(topic), typedHandler)
}

func (n *NATSEventBus) Unsubscribe(ctx context.Context, topic string, handler func(ctx context.Context, event interface{}) error) error {
	return n.UnsubscribeFromEventType(EventType(topic))
}

// New typed event methods
func (n *NATSEventBus) PublishEvent(ctx context.Context, event *Event) error {
	subject := n.eventTypeToSubject(event.Type)
	
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Set message ID for deduplication
	msgID := event.ID
	
	_, err = n.js.Publish(subject, data, nats.MsgId(msgID))
	if err != nil {
		n.logger.Error("Failed to publish event",
			zap.String("event_id", event.ID),
			zap.String("event_type", string(event.Type)),
			zap.String("subject", subject),
			zap.Error(err))
		return fmt.Errorf("failed to publish event: %w", err)
	}

	n.logger.Debug("Published event",
		zap.String("event_id", event.ID),
		zap.String("event_type", string(event.Type)),
		zap.String("subject", subject))

	return nil
}

// PublishEventAsync publishes an event asynchronously
func (n *NATSEventBus) PublishEventAsync(ctx context.Context, event *Event) error {
	subject := n.eventTypeToSubject(event.Type)
	
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Set message ID for deduplication
	msgID := event.ID
	
	_, err = n.js.PublishAsync(subject, data, nats.MsgId(msgID))
	if err != nil {
		n.logger.Error("Failed to publish event async",
			zap.String("event_id", event.ID),
			zap.String("event_type", string(event.Type)),
			zap.String("subject", subject),
			zap.Error(err))
		return fmt.Errorf("failed to publish event async: %w", err)
	}

	n.logger.Debug("Published event async",
		zap.String("event_id", event.ID),
		zap.String("event_type", string(event.Type)),
		zap.String("subject", subject))

	return nil
}

// SubscribeToEventType subscribes to events of a specific type
func (n *NATSEventBus) SubscribeToEventType(ctx context.Context, eventType EventType, handler EventHandler) error {
	subject := n.eventTypeToSubject(eventType)
	consumerName := n.eventTypeToConsumer(eventType)
	
	n.subMutex.Lock()
	defer n.subMutex.Unlock()
	
	// Check if already subscribed
	if _, exists := n.subscriptions[string(eventType)]; exists {
		return fmt.Errorf("already subscribed to event type: %s", eventType)
	}

	// Create durable consumer
	sub, err := n.js.PullSubscribe(subject, consumerName, 
		nats.AckExplicit(),
		nats.DeliverNew(),
		nats.MaxDeliver(3),
		nats.AckWait(30*time.Second))
	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}

	n.subscriptions[string(eventType)] = sub

	// Start message processing goroutine
	n.wg.Add(1)
	go n.processMessages(ctx, sub, handler, eventType)

	n.logger.Info("Subscribed to event type",
		zap.String("event_type", string(eventType)),
		zap.String("subject", subject),
		zap.String("consumer", consumerName))

	return nil
}

// SubscribeToPattern subscribes to events matching a pattern
func (n *NATSEventBus) SubscribeToPattern(ctx context.Context, pattern string, handler EventHandler) error {
	subject := fmt.Sprintf("gdc.events.%s", pattern)
	// Replace all invalid characters for NATS consumer names
	safeName := strings.ReplaceAll(pattern, ".", "-")
	safeName = strings.ReplaceAll(safeName, "*", "star")
	safeName = strings.ReplaceAll(safeName, ">", "gt")
	consumerName := fmt.Sprintf("gdc-consumer-%s", safeName)
	
	n.subMutex.Lock()
	defer n.subMutex.Unlock()
	
	// Check if already subscribed
	if _, exists := n.subscriptions[pattern]; exists {
		return fmt.Errorf("already subscribed to pattern: %s", pattern)
	}

	// Create durable consumer
	sub, err := n.js.PullSubscribe(subject, consumerName, 
		nats.AckExplicit(),
		nats.DeliverNew(),
		nats.MaxDeliver(3),
		nats.AckWait(30*time.Second))
	if err != nil {
		return fmt.Errorf("failed to create pattern subscription: %w", err)
	}

	n.subscriptions[pattern] = sub

	// Start message processing goroutine
	n.wg.Add(1)
	go n.processMessages(ctx, sub, handler, EventType(pattern))

	n.logger.Info("Subscribed to event pattern",
		zap.String("pattern", pattern),
		zap.String("subject", subject),
		zap.String("consumer", consumerName))

	return nil
}

// processMessages processes messages from a subscription
func (n *NATSEventBus) processMessages(ctx context.Context, sub *nats.Subscription, handler EventHandler, eventType EventType) {
	defer n.wg.Done()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.ctx.Done():
			return
		default:
			// Fetch messages in batches
			msgs, err := sub.Fetch(10, nats.MaxWait(1*time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					continue // Normal timeout, continue polling
				}
				n.logger.Error("Failed to fetch messages",
					zap.String("event_type", string(eventType)),
					zap.Error(err))
				continue
			}

			for _, msg := range msgs {
				if err := n.handleMessage(ctx, msg, handler); err != nil {
					n.logger.Error("Failed to handle message",
						zap.String("event_type", string(eventType)),
						zap.Error(err))
					// Negative acknowledgment to retry later
					msg.Nak()
				} else {
					// Acknowledge successful processing
					msg.Ack()
				}
			}
		}
	}
}

// handleMessage processes a single message
func (n *NATSEventBus) handleMessage(ctx context.Context, msg *nats.Msg, handler EventHandler) error {
	var event Event
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Add message metadata to context
	msgCtx := context.WithValue(ctx, "nats_msg", msg)
	
	return handler.Handle(msgCtx, &event)
}

// UnsubscribeFromEventType unsubscribes from an event type
func (n *NATSEventBus) UnsubscribeFromEventType(eventType EventType) error {
	n.subMutex.Lock()
	defer n.subMutex.Unlock()
	
	sub, exists := n.subscriptions[string(eventType)]
	if !exists {
		return fmt.Errorf("not subscribed to event type: %s", eventType)
	}

	if err := sub.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe: %w", err)
	}

	delete(n.subscriptions, string(eventType))
	
	n.logger.Info("Unsubscribed from event type", zap.String("event_type", string(eventType)))
	return nil
}

// Close closes the event bus and all connections
func (n *NATSEventBus) Close() error {
	n.logger.Info("Closing NATS EventBus")
	
	// Cancel context to stop all goroutines
	n.cancel()
	
	// Close all subscriptions
	n.subMutex.Lock()
	for eventType, sub := range n.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			n.logger.Error("Failed to unsubscribe",
				zap.String("event_type", eventType),
				zap.Error(err))
		}
	}
	n.subscriptions = make(map[string]*nats.Subscription)
	n.subMutex.Unlock()
	
	// Wait for all goroutines to finish
	n.wg.Wait()
	
	// Close NATS connection
	if n.conn != nil {
		n.conn.Close()
	}
	
	n.logger.Info("NATS EventBus closed")
	return nil
}

// eventTypeToSubject converts event type to NATS subject
func (n *NATSEventBus) eventTypeToSubject(eventType EventType) string {
	// Convert "pin.requested" to "gdc.events.pin.requested"
	return fmt.Sprintf("gdc.events.%s", string(eventType))
}

// eventTypeToConsumer converts event type to consumer name
func (n *NATSEventBus) eventTypeToConsumer(eventType EventType) string {
	// Convert "pin.requested" to "gdc-consumer-pin-requested"
	// Replace all invalid characters for NATS consumer names
	name := strings.ReplaceAll(string(eventType), ".", "-")
	name = strings.ReplaceAll(name, "*", "star")
	name = strings.ReplaceAll(name, ">", "gt")
	return fmt.Sprintf("gdc-consumer-%s", name)
}

// GetStreamInfo returns information about the JetStream stream
func (n *NATSEventBus) GetStreamInfo() (*nats.StreamInfo, error) {
	return n.js.StreamInfo(n.config.StreamName)
}

// PurgeStream purges all messages from the stream (useful for testing)
func (n *NATSEventBus) PurgeStream() error {
	return n.js.PurgeStream(n.config.StreamName)
}