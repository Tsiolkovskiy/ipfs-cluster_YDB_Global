package zfsmetrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultConfig returns a default configuration for ZFS metrics collector
func DefaultConfig() *Config {
	return &Config{
		PoolName:                 "tank",
		Datasets:                 []string{},
		CollectionInterval:       30 * time.Second,
		MetricTTL:               5 * time.Minute,
		EnableIOPSMonitoring:     true,
		EnableARCStats:          true,
		EnableCompressionStats:   true,
		EnableDeduplicationStats: true,
		EnableFragmentationStats: true,
	}
}

// Validate validates the configuration
func (cfg *Config) Validate() error {
	if cfg.PoolName == "" {
		return errors.New("pool_name is required")
	}
	
	if cfg.CollectionInterval <= 0 {
		return errors.New("collection_interval must be positive")
	}
	
	if cfg.MetricTTL <= 0 {
		return errors.New("metric_ttl must be positive")
	}
	
	// Ensure collection interval is not too frequent
	if cfg.CollectionInterval < 5*time.Second {
		return errors.New("collection_interval must be at least 5 seconds")
	}
	
	// Ensure TTL is longer than collection interval
	if cfg.MetricTTL <= cfg.CollectionInterval {
		return errors.New("metric_ttl must be longer than collection_interval")
	}
	
	return nil
}

// LoadJSON loads configuration from JSON bytes
func (cfg *Config) LoadJSON(raw []byte) error {
	return json.Unmarshal(raw, cfg)
}

// ToJSON converts configuration to JSON bytes
func (cfg *Config) ToJSON() ([]byte, error) {
	return json.MarshalIndent(cfg, "", "  ")
}

// String returns a string representation of the configuration
func (cfg *Config) String() string {
	data, err := cfg.ToJSON()
	if err != nil {
		return fmt.Sprintf("Config{PoolName: %s, Error: %s}", cfg.PoolName, err)
	}
	return string(data)
}

// Clone creates a deep copy of the configuration
func (cfg *Config) Clone() *Config {
	clone := *cfg
	
	// Deep copy datasets slice
	if cfg.Datasets != nil {
		clone.Datasets = make([]string, len(cfg.Datasets))
		copy(clone.Datasets, cfg.Datasets)
	}
	
	return &clone
}

// GetEnabledMetrics returns a list of enabled metric types
func (cfg *Config) GetEnabledMetrics() []string {
	var enabled []string
	
	if cfg.EnableARCStats {
		enabled = append(enabled, "arc")
	}
	if cfg.EnableCompressionStats {
		enabled = append(enabled, "compression")
	}
	if cfg.EnableDeduplicationStats {
		enabled = append(enabled, "deduplication")
	}
	if cfg.EnableFragmentationStats {
		enabled = append(enabled, "fragmentation")
	}
	if cfg.EnableIOPSMonitoring {
		enabled = append(enabled, "iops")
	}
	
	return enabled
}

// SetDefaults sets default values for unspecified configuration fields
func (cfg *Config) SetDefaults() {
	if cfg.CollectionInterval == 0 {
		cfg.CollectionInterval = 30 * time.Second
	}
	
	if cfg.MetricTTL == 0 {
		cfg.MetricTTL = 5 * time.Minute
	}
	
	if cfg.PoolName == "" {
		cfg.PoolName = "tank"
	}
}

// IsMetricEnabled checks if a specific metric type is enabled
func (cfg *Config) IsMetricEnabled(metricType string) bool {
	switch metricType {
	case "arc":
		return cfg.EnableARCStats
	case "compression":
		return cfg.EnableCompressionStats
	case "deduplication":
		return cfg.EnableDeduplicationStats
	case "fragmentation":
		return cfg.EnableFragmentationStats
	case "iops":
		return cfg.EnableIOPSMonitoring
	default:
		return false
	}
}