package zfshttp

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/ipfs-cluster/ipfs-cluster/config"
	"github.com/ipfs-cluster/ipfs-cluster/ipfsconn/ipfshttp"
)

const (
	// DefaultZFSPoolName is the default ZFS pool name
	DefaultZFSPoolName = "ipfs-cluster"
	
	// DefaultCompressionType is the default compression algorithm
	DefaultCompressionType = "lz4"
	
	// DefaultRecordSize is the default ZFS record size
	DefaultRecordSize = "128K"
	
	// DefaultMetricsInterval is the default metrics collection interval
	DefaultMetricsInterval = 30 * time.Second
	
	// DefaultMaxPinsPerDataset is the default maximum pins per dataset
	DefaultMaxPinsPerDataset = 1000000
)

// Config holds configuration for the ZFS HTTP connector
type Config struct {
	// HTTPConfig embeds the standard HTTP connector configuration
	HTTPConfig *ipfshttp.Config `json:"http_config"`
	
	// ZFSConfig contains ZFS-specific configuration
	ZFSConfig *ZFSConfig `json:"zfs_config"`
}

// ZFSConfig holds ZFS-specific configuration options
type ZFSConfig struct {
	// PoolName is the ZFS pool name to use
	PoolName string `json:"pool_name"`
	
	// BasePath is the base path for IPFS datasets
	BasePath string `json:"base_path"`
	
	// MetadataPath is the path for storing pin metadata
	MetadataPath string `json:"metadata_path"`
	
	// CompressionType specifies the compression algorithm (lz4, gzip, zstd)
	CompressionType string `json:"compression_type"`
	
	// RecordSize specifies the ZFS record size
	RecordSize string `json:"record_size"`
	
	// EnableDeduplication enables ZFS deduplication
	EnableDeduplication bool `json:"enable_deduplication"`
	
	// EnableEncryption enables ZFS encryption
	EnableEncryption bool `json:"enable_encryption"`
	
	// EncryptionKey is the encryption key for ZFS datasets
	EncryptionKey string `json:"encryption_key,omitempty"`
	
	// MaxPinsPerDataset is the maximum number of pins per dataset before sharding
	MaxPinsPerDataset int64 `json:"max_pins_per_dataset"`
	
	// MetricsInterval is the interval for collecting ZFS metrics
	MetricsInterval time.Duration `json:"metrics_interval"`
	
	// ShardingStrategy defines how to shard data across datasets
	ShardingStrategy string `json:"sharding_strategy"`
	
	// AutoOptimization enables automatic ZFS parameter optimization
	AutoOptimization bool `json:"auto_optimization"`
	
	// SnapshotSchedule defines automatic snapshot creation schedule
	SnapshotSchedule string `json:"snapshot_schedule"`
	
	// RetentionPolicy defines how long to keep snapshots
	RetentionPolicy string `json:"retention_policy"`
}

// ConfigKey returns a human-friendly identifier for this type of Connector.
func (cfg *Config) ConfigKey() string {
	return "zfshttp"
}

// Default initializes this Config with working values.
func (cfg *Config) Default() error {
	// Initialize HTTP config with defaults
	cfg.HTTPConfig = &ipfshttp.Config{}
	if err := cfg.HTTPConfig.Default(); err != nil {
		return err
	}
	
	// Initialize ZFS config with defaults
	cfg.ZFSConfig = &ZFSConfig{
		PoolName:            DefaultZFSPoolName,
		BasePath:            "/ipfs-cluster",
		MetadataPath:        "/ipfs-cluster/metadata",
		CompressionType:     DefaultCompressionType,
		RecordSize:          DefaultRecordSize,
		EnableDeduplication: true,
		EnableEncryption:    false,
		MaxPinsPerDataset:   DefaultMaxPinsPerDataset,
		MetricsInterval:     DefaultMetricsInterval,
		ShardingStrategy:    "consistent_hash",
		AutoOptimization:    true,
		SnapshotSchedule:    "0 2 * * *", // Daily at 2 AM
		RetentionPolicy:     "30d",       // Keep snapshots for 30 days
	}
	
	return nil
}

// ApplyEnvVars fills in any Config fields found as environment variables.
func (cfg *Config) ApplyEnvVars() error {
	// Apply environment variables to HTTP config
	if err := cfg.HTTPConfig.ApplyEnvVars(); err != nil {
		return err
	}
	
	// Apply ZFS-specific environment variables using envconfig
	err := envconfig.Process("CLUSTER_ZFSHTTP", cfg.ZFSConfig)
	if err != nil {
		return err
	}
	
	return nil
}

// Validate checks that the configuration has working values,
// at least in appearance.
func (cfg *Config) Validate() error {
	// Validate HTTP config
	if err := cfg.HTTPConfig.Validate(); err != nil {
		return err
	}
	
	// Validate ZFS config
	if cfg.ZFSConfig == nil {
		return errors.New("zfs_config cannot be nil")
	}
	
	if cfg.ZFSConfig.PoolName == "" {
		return errors.New("zfs pool_name cannot be empty")
	}
	
	if cfg.ZFSConfig.BasePath == "" {
		return errors.New("zfs base_path cannot be empty")
	}
	
	if cfg.ZFSConfig.MetadataPath == "" {
		return errors.New("zfs metadata_path cannot be empty")
	}
	
	if cfg.ZFSConfig.MaxPinsPerDataset <= 0 {
		return errors.New("max_pins_per_dataset must be positive")
	}
	
	// Validate compression type
	validCompressionTypes := map[string]bool{
		"off":  true,
		"lz4":  true,
		"gzip": true,
		"zstd": true,
	}
	if !validCompressionTypes[cfg.ZFSConfig.CompressionType] {
		return errors.New("invalid compression_type, must be one of: off, lz4, gzip, zstd")
	}
	
	// Validate sharding strategy
	validShardingStrategies := map[string]bool{
		"consistent_hash": true,
		"round_robin":     true,
		"size_based":      true,
	}
	if !validShardingStrategies[cfg.ZFSConfig.ShardingStrategy] {
		return errors.New("invalid sharding_strategy, must be one of: consistent_hash, round_robin, size_based")
	}
	
	return nil
}

// LoadJSON loads the fields from a JSON byte slice created by ToJSON.
func (cfg *Config) LoadJSON(raw []byte) error {
	jcfg := &jsonConfig{}
	err := json.Unmarshal(raw, jcfg)
	if err != nil {
		return err
	}
	
	cfg.HTTPConfig = jcfg.HTTPConfig
	cfg.ZFSConfig = jcfg.ZFSConfig
	
	return cfg.Validate()
}

// ToJSON generates a JSON representation of this Config.
func (cfg *Config) ToJSON() (raw []byte, err error) {
	jcfg := &jsonConfig{
		HTTPConfig: cfg.HTTPConfig,
		ZFSConfig:  cfg.ZFSConfig,
	}
	
	return config.DefaultJSONMarshal(jcfg)
}

// GetHTTPConfig returns the embedded HTTP connector configuration
func (cfg *Config) GetHTTPConfig() *ipfshttp.Config {
	return cfg.HTTPConfig
}

// GetZFSConfig returns the ZFS-specific configuration
func (cfg *Config) GetZFSConfig() *ZFSConfig {
	return cfg.ZFSConfig
}

// jsonConfig is used for JSON serialization
type jsonConfig struct {
	HTTPConfig *ipfshttp.Config `json:"http_config"`
	ZFSConfig  *ZFSConfig       `json:"zfs_config"`
}

// ToDisplayJSON returns JSON config as a string.
func (cfg *Config) ToDisplayJSON() ([]byte, error) {
	return config.DisplayJSON(cfg.toDisplayJSON())
}

func (cfg *Config) toDisplayJSON() *jsonConfig {
	return &jsonConfig{
		HTTPConfig: cfg.HTTPConfig,
		ZFSConfig:  cfg.ZFSConfig,
	}
}