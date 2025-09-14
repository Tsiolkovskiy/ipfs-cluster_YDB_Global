package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// LoadTestConfigPresets contains predefined configurations for different test scenarios
var LoadTestConfigPresets = map[string]*LoadTestConfig{
	"trillion_scale": {
		TotalPins:         1000000000000, // 1 trillion
		ConcurrentWorkers: 10000,
		BatchSize:         10000,
		TestDuration:      time.Hour * 24,
		AccessPattern:     AccessPatternHotCold,
		HotDataPercentage: 0.2,
		BurstIntensity:    100000,
		BurstInterval:     time.Minute * 5,
		MaxLatencyP99:     50 * time.Millisecond,
		MinThroughput:     1000000, // 1M ops/sec
		MaxErrorRate:      0.001,   // 0.1%
		ZFSPoolName:       "ipfs-cluster-pool",
		DatasetPrefix:     "ipfs-pins",
		CompressionType:   "zstd",
		RecordSize:        "1M",
	},
	"development": {
		TotalPins:         1000000, // 1 million
		ConcurrentWorkers: 100,
		BatchSize:         1000,
		TestDuration:      time.Hour,
		AccessPattern:     AccessPatternRandom,
		HotDataPercentage: 0.2,
		BurstIntensity:    10000,
		BurstInterval:     time.Second * 30,
		MaxLatencyP99:     100 * time.Millisecond,
		MinThroughput:     10000, // 10K ops/sec
		MaxErrorRate:      0.01,  // 1%
		ZFSPoolName:       "dev-pool",
		DatasetPrefix:     "dev-pins",
		CompressionType:   "lz4",
		RecordSize:        "128K",
	},
	"stress_test": {
		TotalPins:         100000000, // 100 million
		ConcurrentWorkers: 1000,
		BatchSize:         5000,
		TestDuration:      time.Hour * 6,
		AccessPattern:     AccessPatternBurst,
		HotDataPercentage: 0.1,
		BurstIntensity:    50000,
		BurstInterval:     time.Minute,
		MaxLatencyP99:     25 * time.Millisecond,
		MinThroughput:     100000, // 100K ops/sec
		MaxErrorRate:      0.005,  // 0.5%
		ZFSPoolName:       "stress-pool",
		DatasetPrefix:     "stress-pins",
		CompressionType:   "gzip-6",
		RecordSize:        "512K",
	},
	"quick_test": {
		TotalPins:         10000,
		ConcurrentWorkers: 10,
		BatchSize:         100,
		TestDuration:      time.Minute * 5,
		AccessPattern:     AccessPatternSequential,
		HotDataPercentage: 0.3,
		BurstIntensity:    1000,
		BurstInterval:     time.Second * 10,
		MaxLatencyP99:     200 * time.Millisecond,
		MinThroughput:     1000, // 1K ops/sec
		MaxErrorRate:      0.05, // 5%
		ZFSPoolName:       "test-pool",
		DatasetPrefix:     "test-pins",
		CompressionType:   "lz4",
		RecordSize:        "64K",
	},
	"endurance_test": {
		TotalPins:         10000000000, // 10 billion
		ConcurrentWorkers: 5000,
		BatchSize:         2000,
		TestDuration:      time.Hour * 72, // 3 days
		AccessPattern:     AccessPatternZipfian,
		HotDataPercentage: 0.15,
		BurstIntensity:    25000,
		BurstInterval:     time.Minute * 10,
		MaxLatencyP99:     75 * time.Millisecond,
		MinThroughput:     50000, // 50K ops/sec
		MaxErrorRate:      0.002, // 0.2%
		ZFSPoolName:       "endurance-pool",
		DatasetPrefix:     "endurance-pins",
		CompressionType:   "zstd-3",
		RecordSize:        "256K",
	},
}

// LoadConfigFromFile loads configuration from a JSON file
func LoadConfigFromFile(filename string) (*LoadTestConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	
	var config LoadTestConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	
	return &config, nil
}

// SaveConfigToFile saves configuration to a JSON file
func SaveConfigToFile(config *LoadTestConfig, filename string) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	
	return nil
}

// GetPresetConfig returns a predefined configuration by name
func GetPresetConfig(presetName string) (*LoadTestConfig, error) {
	config, exists := LoadTestConfigPresets[presetName]
	if !exists {
		return nil, fmt.Errorf("preset configuration '%s' not found", presetName)
	}
	
	// Return a copy to avoid modifications to the preset
	configCopy := *config
	return &configCopy, nil
}

// ListPresets returns a list of available preset names
func ListPresets() []string {
	presets := make([]string, 0, len(LoadTestConfigPresets))
	for name := range LoadTestConfigPresets {
		presets = append(presets, name)
	}
	return presets
}

// ValidateConfig validates the configuration parameters
func ValidateConfig(config *LoadTestConfig) error {
	if config.TotalPins <= 0 {
		return fmt.Errorf("total_pins must be positive")
	}
	
	if config.ConcurrentWorkers <= 0 {
		return fmt.Errorf("concurrent_workers must be positive")
	}
	
	if config.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be positive")
	}
	
	if config.TestDuration <= 0 {
		return fmt.Errorf("test_duration must be positive")
	}
	
	if config.HotDataPercentage < 0 || config.HotDataPercentage > 1 {
		return fmt.Errorf("hot_data_percentage must be between 0 and 1")
	}
	
	if config.MaxErrorRate < 0 || config.MaxErrorRate > 1 {
		return fmt.Errorf("max_error_rate must be between 0 and 1")
	}
	
	if config.MinThroughput < 0 {
		return fmt.Errorf("min_throughput must be non-negative")
	}
	
	if config.MaxLatencyP99 <= 0 {
		return fmt.Errorf("max_latency_p99 must be positive")
	}
	
	// Validate ZFS configuration
	if config.ZFSPoolName == "" {
		return fmt.Errorf("zfs_pool_name cannot be empty")
	}
	
	if config.DatasetPrefix == "" {
		return fmt.Errorf("dataset_prefix cannot be empty")
	}
	
	validCompressionTypes := map[string]bool{
		"off": true, "lz4": true, "gzip": true, "gzip-1": true, "gzip-2": true,
		"gzip-3": true, "gzip-4": true, "gzip-5": true, "gzip-6": true,
		"gzip-7": true, "gzip-8": true, "gzip-9": true, "zstd": true,
		"zstd-1": true, "zstd-2": true, "zstd-3": true, "zstd-4": true,
		"zstd-5": true, "zstd-6": true, "zstd-7": true, "zstd-8": true,
		"zstd-9": true, "zstd-10": true, "zstd-11": true, "zstd-12": true,
		"zstd-13": true, "zstd-14": true, "zstd-15": true, "zstd-16": true,
		"zstd-17": true, "zstd-18": true, "zstd-19": true,
	}
	
	if !validCompressionTypes[config.CompressionType] {
		return fmt.Errorf("invalid compression_type: %s", config.CompressionType)
	}
	
	validRecordSizes := map[string]bool{
		"512": true, "1K": true, "2K": true, "4K": true, "8K": true,
		"16K": true, "32K": true, "64K": true, "128K": true, "256K": true,
		"512K": true, "1M": true, "2M": true, "4M": true, "8M": true, "16M": true,
	}
	
	if !validRecordSizes[config.RecordSize] {
		return fmt.Errorf("invalid record_size: %s", config.RecordSize)
	}
	
	return nil
}

// OptimizeConfigForHardware adjusts configuration based on available hardware
func OptimizeConfigForHardware(config *LoadTestConfig, cpuCores int, memoryGB int, diskCount int) {
	// Adjust concurrent workers based on CPU cores
	optimalWorkers := cpuCores * 100 // 100 workers per core as starting point
	if config.ConcurrentWorkers > optimalWorkers*2 {
		config.ConcurrentWorkers = optimalWorkers
	}
	
	// Adjust batch size based on memory
	optimalBatchSize := memoryGB * 100 // Scale with available memory
	if config.BatchSize > optimalBatchSize {
		config.BatchSize = optimalBatchSize
	}
	
	// Adjust throughput expectations based on disk count
	baselineThroughput := int64(diskCount * 10000) // 10K ops per disk
	if config.MinThroughput > baselineThroughput*2 {
		config.MinThroughput = baselineThroughput
	}
	
	// Optimize ZFS record size based on workload
	if config.AccessPattern == AccessPatternSequential {
		config.RecordSize = "1M" // Larger record size for sequential access
	} else if config.AccessPattern == AccessPatternRandom {
		config.RecordSize = "128K" // Smaller record size for random access
	}
	
	// Optimize compression based on CPU availability
	if cpuCores >= 32 {
		config.CompressionType = "zstd-3" // Higher compression with more CPUs
	} else if cpuCores >= 16 {
		config.CompressionType = "lz4" // Balanced compression
	} else {
		config.CompressionType = "off" // No compression for limited CPUs
	}
}

// GenerateConfigVariations creates multiple configuration variations for testing
func GenerateConfigVariations(baseConfig *LoadTestConfig) []*LoadTestConfig {
	variations := make([]*LoadTestConfig, 0)
	
	// Variation 1: Different access patterns
	for _, pattern := range []AccessPattern{
		AccessPatternSequential,
		AccessPatternRandom,
		AccessPatternBurst,
		AccessPatternHotCold,
		AccessPatternZipfian,
	} {
		config := *baseConfig
		config.AccessPattern = pattern
		variations = append(variations, &config)
	}
	
	// Variation 2: Different worker counts
	for _, workers := range []int{
		baseConfig.ConcurrentWorkers / 4,
		baseConfig.ConcurrentWorkers / 2,
		baseConfig.ConcurrentWorkers,
		baseConfig.ConcurrentWorkers * 2,
	} {
		if workers > 0 {
			config := *baseConfig
			config.ConcurrentWorkers = workers
			variations = append(variations, &config)
		}
	}
	
	// Variation 3: Different batch sizes
	for _, batchSize := range []int{
		baseConfig.BatchSize / 4,
		baseConfig.BatchSize / 2,
		baseConfig.BatchSize,
		baseConfig.BatchSize * 2,
	} {
		if batchSize > 0 {
			config := *baseConfig
			config.BatchSize = batchSize
			variations = append(variations, &config)
		}
	}
	
	// Variation 4: Different ZFS configurations
	zfsConfigs := []struct {
		compression string
		recordSize  string
	}{
		{"lz4", "128K"},
		{"gzip-6", "256K"},
		{"zstd-3", "512K"},
		{"off", "1M"},
	}
	
	for _, zfsConfig := range zfsConfigs {
		config := *baseConfig
		config.CompressionType = zfsConfig.compression
		config.RecordSize = zfsConfig.recordSize
		variations = append(variations, &config)
	}
	
	return variations
}

// ConfigSummary provides a human-readable summary of the configuration
func ConfigSummary(config *LoadTestConfig) string {
	return fmt.Sprintf(`Load Test Configuration Summary:
- Scale: %d total pins
- Concurrency: %d workers, batch size %d
- Duration: %v
- Access Pattern: %v
- Performance Targets: %v latency, %.0f ops/sec throughput, %.2f%% error rate
- ZFS Config: %s pool, %s compression, %s record size
- Hot Data: %.1f%% of dataset`,
		config.TotalPins,
		config.ConcurrentWorkers,
		config.BatchSize,
		config.TestDuration,
		config.AccessPattern,
		config.MaxLatencyP99,
		config.MinThroughput,
		config.MaxErrorRate*100,
		config.ZFSPoolName,
		config.CompressionType,
		config.RecordSize,
		config.HotDataPercentage*100)
}