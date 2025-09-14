package resource

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// DefaultConfig returns a default configuration for the resource manager
func DefaultConfig() *Config {
	return &Config{
		// Pool management
		AutoExpandPools:     true,
		CapacityThreshold:   0.8,  // 80%
		MinFreeSpace:        100 * 1024 * 1024 * 1024, // 100GB
		
		// Disk management
		AutoAddDisks:        true,
		DiskScanInterval:    5 * time.Minute,
		HealthCheckInterval: 1 * time.Minute,
		
		// Performance optimization
		AutoOptimizeLayout:  true,
		OptimizationInterval: 1 * time.Hour,
		
		// Replacement settings
		AutoReplaceFailed:   true,
		ReplacementTimeout:  30 * time.Minute,
		
		// Monitoring
		MetricsInterval:     30 * time.Second,
		AlertThresholds: AlertThresholds{
			CapacityWarning:      0.8,  // 80%
			CapacityCritical:     0.9,  // 90%
			FragmentationWarning: 0.3,  // 30%
			TemperatureWarning:   50,   // 50°C
			TemperatureCritical:  60,   // 60°C
		},
	}
}

// LoadConfigFromFile loads configuration from a JSON file
func LoadConfigFromFile(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// SaveConfigToFile saves configuration to a JSON file
func (c *Config) SaveConfigToFile(filename string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("writing config file: %w", err)
	}

	return nil
}

// Merge merges another configuration into this one, with the other config taking precedence
func (c *Config) Merge(other *Config) {
	if other.AutoExpandPools != c.AutoExpandPools {
		c.AutoExpandPools = other.AutoExpandPools
	}
	if other.CapacityThreshold != c.CapacityThreshold {
		c.CapacityThreshold = other.CapacityThreshold
	}
	if other.MinFreeSpace != c.MinFreeSpace {
		c.MinFreeSpace = other.MinFreeSpace
	}
	if other.AutoAddDisks != c.AutoAddDisks {
		c.AutoAddDisks = other.AutoAddDisks
	}
	if other.DiskScanInterval != c.DiskScanInterval {
		c.DiskScanInterval = other.DiskScanInterval
	}
	if other.HealthCheckInterval != c.HealthCheckInterval {
		c.HealthCheckInterval = other.HealthCheckInterval
	}
	if other.AutoOptimizeLayout != c.AutoOptimizeLayout {
		c.AutoOptimizeLayout = other.AutoOptimizeLayout
	}
	if other.OptimizationInterval != c.OptimizationInterval {
		c.OptimizationInterval = other.OptimizationInterval
	}
	if other.AutoReplaceFailed != c.AutoReplaceFailed {
		c.AutoReplaceFailed = other.AutoReplaceFailed
	}
	if other.ReplacementTimeout != c.ReplacementTimeout {
		c.ReplacementTimeout = other.ReplacementTimeout
	}
	if other.MetricsInterval != c.MetricsInterval {
		c.MetricsInterval = other.MetricsInterval
	}
	
	// Merge alert thresholds
	if other.AlertThresholds.CapacityWarning != c.AlertThresholds.CapacityWarning {
		c.AlertThresholds.CapacityWarning = other.AlertThresholds.CapacityWarning
	}
	if other.AlertThresholds.CapacityCritical != c.AlertThresholds.CapacityCritical {
		c.AlertThresholds.CapacityCritical = other.AlertThresholds.CapacityCritical
	}
	if other.AlertThresholds.FragmentationWarning != c.AlertThresholds.FragmentationWarning {
		c.AlertThresholds.FragmentationWarning = other.AlertThresholds.FragmentationWarning
	}
	if other.AlertThresholds.TemperatureWarning != c.AlertThresholds.TemperatureWarning {
		c.AlertThresholds.TemperatureWarning = other.AlertThresholds.TemperatureWarning
	}
	if other.AlertThresholds.TemperatureCritical != c.AlertThresholds.TemperatureCritical {
		c.AlertThresholds.TemperatureCritical = other.AlertThresholds.TemperatureCritical
	}
}

// Clone creates a deep copy of the configuration
func (c *Config) Clone() *Config {
	clone := *c
	clone.AlertThresholds = c.AlertThresholds
	return &clone
}