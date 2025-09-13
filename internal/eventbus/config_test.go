package eventbus

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	
	assert.Equal(t, "nats", config.Type)
	assert.NotNil(t, config.NATS)
	assert.Equal(t, "nats://localhost:4222", config.NATS.URL)
	assert.Equal(t, "GDC_EVENTS", config.NATS.StreamName)
	assert.Equal(t, []string{"gdc.events.>"}, config.NATS.StreamSubjects)
	assert.Equal(t, 24*time.Hour, config.NATS.MaxAge)
	assert.Equal(t, int64(1024*1024*1024), config.NATS.MaxBytes)
	assert.Equal(t, int64(1000000), config.NATS.MaxMsgs)
	assert.Equal(t, 1, config.NATS.Replicas)
}

func TestDefaultNATSConfig(t *testing.T) {
	config := DefaultNATSConfig()
	
	assert.Equal(t, "nats://localhost:4222", config.URL)
	assert.Equal(t, "GDC_EVENTS", config.StreamName)
	assert.Equal(t, []string{"gdc.events.>"}, config.StreamSubjects)
	assert.Equal(t, 24*time.Hour, config.MaxAge)
	assert.Equal(t, int64(1024*1024*1024), config.MaxBytes)
	assert.Equal(t, int64(1000000), config.MaxMsgs)
	assert.Equal(t, 1, config.Replicas)
	assert.Equal(t, 10*time.Second, config.ConnectTimeout)
	assert.Equal(t, 2*time.Second, config.ReconnectWait)
	assert.Equal(t, 10, config.MaxReconnectAttempts)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:   "valid config",
			config: DefaultConfig(),
			wantErr: false,
		},
		{
			name: "empty type",
			config: &Config{
				Type: "",
				NATS: DefaultNATSConfig(),
			},
			wantErr: true,
		},
		{
			name: "unsupported type",
			config: &Config{
				Type: "kafka",
				NATS: DefaultNATSConfig(),
			},
			wantErr: true,
		},
		{
			name: "nats type without config",
			config: &Config{
				Type: "nats",
				NATS: nil,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNATSConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *NATSConfig
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultNATSConfig(),
			wantErr: false,
		},
		{
			name: "empty URL",
			config: &NATSConfig{
				URL:            "",
				StreamName:     "TEST",
				StreamSubjects: []string{"test.>"},
				MaxAge:         time.Hour,
				MaxBytes:       1024,
				MaxMsgs:        100,
				Replicas:       1,
			},
			wantErr: true,
		},
		{
			name: "empty stream name",
			config: &NATSConfig{
				URL:            "nats://localhost:4222",
				StreamName:     "",
				StreamSubjects: []string{"test.>"},
				MaxAge:         time.Hour,
				MaxBytes:       1024,
				MaxMsgs:        100,
				Replicas:       1,
			},
			wantErr: true,
		},
		{
			name: "empty stream subjects",
			config: &NATSConfig{
				URL:            "nats://localhost:4222",
				StreamName:     "TEST",
				StreamSubjects: []string{},
				MaxAge:         time.Hour,
				MaxBytes:       1024,
				MaxMsgs:        100,
				Replicas:       1,
			},
			wantErr: true,
		},
		{
			name: "zero max age",
			config: &NATSConfig{
				URL:            "nats://localhost:4222",
				StreamName:     "TEST",
				StreamSubjects: []string{"test.>"},
				MaxAge:         0,
				MaxBytes:       1024,
				MaxMsgs:        100,
				Replicas:       1,
			},
			wantErr: true,
		},
		{
			name: "zero max bytes",
			config: &NATSConfig{
				URL:            "nats://localhost:4222",
				StreamName:     "TEST",
				StreamSubjects: []string{"test.>"},
				MaxAge:         time.Hour,
				MaxBytes:       0,
				MaxMsgs:        100,
				Replicas:       1,
			},
			wantErr: true,
		},
		{
			name: "zero max messages",
			config: &NATSConfig{
				URL:            "nats://localhost:4222",
				StreamName:     "TEST",
				StreamSubjects: []string{"test.>"},
				MaxAge:         time.Hour,
				MaxBytes:       1024,
				MaxMsgs:        0,
				Replicas:       1,
			},
			wantErr: true,
		},
		{
			name: "zero replicas",
			config: &NATSConfig{
				URL:            "nats://localhost:4222",
				StreamName:     "TEST",
				StreamSubjects: []string{"test.>"},
				MaxAge:         time.Hour,
				MaxBytes:       1024,
				MaxMsgs:        100,
				Replicas:       0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewEventBusFromConfig(t *testing.T) {
	logger := zaptest.NewLogger(t)
	
	t.Run("valid nats config", func(t *testing.T) {
		config := &Config{
			Type: "nats",
			NATS: &NATSConfig{
				URL:            "nats://localhost:9999", // Non-existent server for test
				StreamName:     "TEST_EVENTS",
				StreamSubjects: []string{"test.events.>"},
				MaxAge:         time.Hour,
				MaxBytes:       1024 * 1024,
				MaxMsgs:        1000,
				Replicas:       1,
				ConnectTimeout: 1 * time.Second,
			},
		}
		
		// This should fail because server doesn't exist, but validates config parsing
		_, err := NewEventBusFromConfig(config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to connect to NATS")
	})
	
	t.Run("invalid config", func(t *testing.T) {
		config := &Config{
			Type: "invalid",
		}
		
		_, err := NewEventBusFromConfig(config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid event bus configuration")
	})
	
	t.Run("unsupported type", func(t *testing.T) {
		config := &Config{
			Type: "kafka",
			NATS: DefaultNATSConfig(),
		}
		
		_, err := NewEventBusFromConfig(config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported event bus type")
	})
}