package eventbus

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Config represents the event bus configuration
type Config struct {
	Type string      `json:"type" yaml:"type" mapstructure:"type"`
	NATS *NATSConfig `json:"nats,omitempty" yaml:"nats,omitempty" mapstructure:"nats"`
}

// DefaultConfig returns default event bus configuration
func DefaultConfig() *Config {
	return &Config{
		Type: "nats",
		NATS: DefaultNATSConfig(),
	}
}

// Validate validates the event bus configuration
func (c *Config) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("event bus type is required")
	}

	switch c.Type {
	case "nats":
		if c.NATS == nil {
			return fmt.Errorf("NATS configuration is required when type is 'nats'")
		}
		return c.NATS.Validate()
	default:
		return fmt.Errorf("unsupported event bus type: %s", c.Type)
	}
}

// Validate validates the NATS configuration
func (c *NATSConfig) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("NATS URL is required")
	}

	if c.StreamName == "" {
		return fmt.Errorf("NATS stream name is required")
	}

	if len(c.StreamSubjects) == 0 {
		return fmt.Errorf("NATS stream subjects are required")
	}

	if c.MaxAge <= 0 {
		return fmt.Errorf("NATS max age must be positive")
	}

	if c.MaxBytes <= 0 {
		return fmt.Errorf("NATS max bytes must be positive")
	}

	if c.MaxMsgs <= 0 {
		return fmt.Errorf("NATS max messages must be positive")
	}

	if c.Replicas < 1 {
		return fmt.Errorf("NATS replicas must be at least 1")
	}

	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 10 * time.Second
	}

	if c.ReconnectWait <= 0 {
		c.ReconnectWait = 2 * time.Second
	}

	if c.MaxReconnectAttempts < 0 {
		c.MaxReconnectAttempts = 10
	}

	return nil
}

// NewEventBusFromConfig creates an event bus based on configuration
func NewEventBusFromConfig(config *Config, logger *zap.Logger) (EventBus, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid event bus configuration: %w", err)
	}

	switch config.Type {
	case "nats":
		return NewNATSEventBus(config.NATS, logger)
	default:
		return nil, fmt.Errorf("unsupported event bus type: %s", config.Type)
	}
}