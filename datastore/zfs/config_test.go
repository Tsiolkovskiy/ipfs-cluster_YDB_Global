package zfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDefault(t *testing.T) {
	cfg := &Config{}
	err := cfg.Default()
	require.NoError(t, err)

	assert.Equal(t, DefaultSubFolder, cfg.Folder)
	assert.Equal(t, DefaultPoolName, cfg.PoolName)
	assert.Equal(t, "ipfs-cluster/datastore", cfg.DatasetName)
	assert.Equal(t, DefaultCompression, cfg.Compression)
	assert.Equal(t, DefaultRecordSize, cfg.RecordSize)
	assert.Equal(t, DefaultDeduplication, cfg.Deduplication)
	assert.Equal(t, DefaultSync, cfg.Sync)
	assert.Equal(t, DefaultATime, cfg.ATime)
	assert.Equal(t, false, cfg.Encryption)
	assert.Equal(t, int64(1000000000), cfg.MaxPinsPerShard)
	assert.Equal(t, true, cfg.AutoOptimize)
	assert.Equal(t, 3600, cfg.SnapshotInterval)
	assert.Equal(t, 24, cfg.MaxSnapshots)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name        string
		setupConfig func(*Config)
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid config",
			setupConfig: func(cfg *Config) {
				cfg.Default()
			},
			expectError: false,
		},
		{
			name: "empty folder",
			setupConfig: func(cfg *Config) {
				cfg.Default()
				cfg.Folder = ""
			},
			expectError: true,
			errorMsg:    "folder is unset",
		},
		{
			name: "empty pool name",
			setupConfig: func(cfg *Config) {
				cfg.Default()
				cfg.PoolName = ""
			},
			expectError: true,
			errorMsg:    "pool_name is unset",
		},
		{
			name: "empty dataset name",
			setupConfig: func(cfg *Config) {
				cfg.Default()
				cfg.DatasetName = ""
			},
			expectError: true,
			errorMsg:    "dataset_name is unset",
		},
		{
			name: "invalid compression",
			setupConfig: func(cfg *Config) {
				cfg.Default()
				cfg.Compression = "invalid"
			},
			expectError: true,
			errorMsg:    "invalid compression algorithm",
		},
		{
			name: "invalid sync mode",
			setupConfig: func(cfg *Config) {
				cfg.Default()
				cfg.Sync = "invalid"
			},
			expectError: true,
			errorMsg:    "invalid sync mode",
		},
		{
			name: "invalid max pins per shard",
			setupConfig: func(cfg *Config) {
				cfg.Default()
				cfg.MaxPinsPerShard = 0
			},
			expectError: true,
			errorMsg:    "max_pins_per_shard must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			tt.setupConfig(cfg)

			err := cfg.Validate()
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfigJSON(t *testing.T) {
	cfg := &Config{}
	err := cfg.Default()
	require.NoError(t, err)

	// Modify some values
	cfg.Compression = "zstd"
	cfg.RecordSize = "1M"
	cfg.Deduplication = false
	cfg.MaxPinsPerShard = 500000000

	// Convert to JSON
	jsonData, err := cfg.ToJSON()
	require.NoError(t, err)

	// Parse back from JSON
	cfg2 := &Config{}
	err = cfg2.LoadJSON(jsonData)
	require.NoError(t, err)

	// Compare values
	assert.Equal(t, cfg.Compression, cfg2.Compression)
	assert.Equal(t, cfg.RecordSize, cfg2.RecordSize)
	assert.Equal(t, cfg.Deduplication, cfg2.Deduplication)
	assert.Equal(t, cfg.MaxPinsPerShard, cfg2.MaxPinsPerShard)
}

func TestConfigGetFolder(t *testing.T) {
	cfg := &Config{}
	cfg.Default()

	// Test relative path
	cfg.BaseDir = "/tmp/test"
	cfg.Folder = "zfs"
	expected := filepath.Join("/tmp/test", "zfs")
	assert.Equal(t, expected, cfg.GetFolder())

	// Test absolute path
	cfg.Folder = "/absolute/path"
	assert.Equal(t, "/absolute/path", cfg.GetFolder())
}

func TestConfigGetDatasetPath(t *testing.T) {
	cfg := &Config{}
	cfg.Default()
	cfg.PoolName = "testpool"
	cfg.DatasetName = "test/dataset"

	expected := "testpool/test/dataset"
	assert.Equal(t, expected, cfg.GetDatasetPath())
}

func TestConfigApplyEnvVars(t *testing.T) {
	// Set environment variables
	os.Setenv("CLUSTER_ZFS_POOL_NAME", "envpool")
	os.Setenv("CLUSTER_ZFS_COMPRESSION", "gzip")
	os.Setenv("CLUSTER_ZFS_MAX_PINS_PER_SHARD", "2000000000")
	defer func() {
		os.Unsetenv("CLUSTER_ZFS_POOL_NAME")
		os.Unsetenv("CLUSTER_ZFS_COMPRESSION")
		os.Unsetenv("CLUSTER_ZFS_MAX_PINS_PER_SHARD")
	}()

	cfg := &Config{}
	cfg.Default()

	err := cfg.ApplyEnvVars()
	require.NoError(t, err)

	assert.Equal(t, "envpool", cfg.PoolName)
	assert.Equal(t, "gzip", cfg.Compression)
	assert.Equal(t, int64(2000000000), cfg.MaxPinsPerShard)
}

func TestConfigDisplayJSON(t *testing.T) {
	cfg := &Config{}
	cfg.Default()

	displayJSON, err := cfg.ToDisplayJSON()
	require.NoError(t, err)

	// Verify it's valid JSON
	var result map[string]interface{}
	err = json.Unmarshal(displayJSON, &result)
	require.NoError(t, err)

	// Check some expected fields
	assert.Equal(t, cfg.PoolName, result["pool_name"])
	assert.Equal(t, cfg.Compression, result["compression"])
	assert.Equal(t, cfg.Deduplication, result["deduplication"])
}

func TestConfigKey(t *testing.T) {
	cfg := &Config{}
	assert.Equal(t, "zfs", cfg.ConfigKey())
}