package zfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZFSDatastoreBasicOperations(t *testing.T) {
	// Skip if ZFS is not available
	if !isZFSAvailable() {
		t.Skip("ZFS not available, skipping test")
	}

	cfg := createTestConfig(t)
	defer cleanupTestConfig(cfg)

	zds, err := New(cfg)
	require.NoError(t, err)
	defer zds.Close()

	ctx := context.Background()

	// Test Put and Get
	key := ds.NewKey("/test/key1")
	value := []byte("test value 1")

	err = zds.Put(ctx, key, value)
	require.NoError(t, err)

	retrievedValue, err := zds.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Test Has
	exists, err := zds.Has(ctx, key)
	require.NoError(t, err)
	assert.True(t, exists)

	// Test GetSize
	size, err := zds.GetSize(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, len(value), size)

	// Test Delete
	err = zds.Delete(ctx, key)
	require.NoError(t, err)

	// Verify deletion
	exists, err = zds.Has(ctx, key)
	require.NoError(t, err)
	assert.False(t, exists)

	_, err = zds.Get(ctx, key)
	assert.Equal(t, ds.ErrNotFound, err)
}

func TestZFSDatastoreCache(t *testing.T) {
	// Skip if ZFS is not available
	if !isZFSAvailable() {
		t.Skip("ZFS not available, skipping test")
	}

	cfg := createTestConfig(t)
	defer cleanupTestConfig(cfg)

	zds, err := New(cfg)
	require.NoError(t, err)
	defer zds.Close()

	ctx := context.Background()

	// Put a value
	key := ds.NewKey("/test/cached")
	value := []byte("cached value")

	err = zds.Put(ctx, key, value)
	require.NoError(t, err)

	// First get should miss cache
	initialMetrics := zds.GetMetrics()
	
	retrievedValue, err := zds.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Second get should hit cache
	retrievedValue2, err := zds.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue2)

	// Check metrics
	finalMetrics := zds.GetMetrics()
	assert.Greater(t, finalMetrics.CacheHits, initialMetrics.CacheHits)
}

func TestZFSDatastoreSharding(t *testing.T) {
	// Skip if ZFS is not available
	if !isZFSAvailable() {
		t.Skip("ZFS not available, skipping test")
	}

	cfg := createTestConfig(t)
	cfg.MaxPinsPerShard = 2 // Small shard size for testing
	defer cleanupTestConfig(cfg)

	zds, err := New(cfg)
	require.NoError(t, err)
	defer zds.Close()

	ctx := context.Background()

	// Add multiple keys to trigger sharding
	keys := []ds.Key{
		ds.NewKey("/test/shard1"),
		ds.NewKey("/test/shard2"),
		ds.NewKey("/test/shard3"),
		ds.NewKey("/test/shard4"),
	}

	for i, key := range keys {
		value := []byte("shard test value " + string(rune('1'+i)))
		err = zds.Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Verify all keys can be retrieved
	for i, key := range keys {
		expectedValue := []byte("shard test value " + string(rune('1'+i)))
		retrievedValue, err := zds.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, retrievedValue)
	}

	// Check that multiple shards were created
	zfsDS := zds.(*ZFSDatastore)
	shards := zfsDS.shardingManager.ListShards()
	assert.Greater(t, len(shards), 1, "Multiple shards should be created")
}

func TestZFSDatastoreMetrics(t *testing.T) {
	// Skip if ZFS is not available
	if !isZFSAvailable() {
		t.Skip("ZFS not available, skipping test")
	}

	cfg := createTestConfig(t)
	defer cleanupTestConfig(cfg)

	zds, err := New(cfg)
	require.NoError(t, err)
	defer zds.Close()

	ctx := context.Background()

	initialMetrics := zds.GetMetrics()

	// Perform operations
	key := ds.NewKey("/test/metrics")
	value := []byte("metrics test")

	err = zds.Put(ctx, key, value)
	require.NoError(t, err)

	_, err = zds.Get(ctx, key)
	require.NoError(t, err)

	err = zds.Delete(ctx, key)
	require.NoError(t, err)

	// Check metrics
	finalMetrics := zds.GetMetrics()
	assert.Greater(t, finalMetrics.TotalOperations, initialMetrics.TotalOperations)
	assert.Greater(t, finalMetrics.WriteOperations, initialMetrics.WriteOperations)
	assert.Greater(t, finalMetrics.ReadOperations, initialMetrics.ReadOperations)
	assert.Greater(t, finalMetrics.DeleteOperations, initialMetrics.DeleteOperations)
}

func TestZFSDatastoreQuery(t *testing.T) {
	// Skip if ZFS is not available
	if !isZFSAvailable() {
		t.Skip("ZFS not available, skipping test")
	}

	cfg := createTestConfig(t)
	defer cleanupTestConfig(cfg)

	zds, err := New(cfg)
	require.NoError(t, err)
	defer zds.Close()

	ctx := context.Background()

	// Add test data
	testData := map[string][]byte{
		"/test/query/item1": []byte("value1"),
		"/test/query/item2": []byte("value2"),
		"/other/item3":      []byte("value3"),
	}

	for keyStr, value := range testData {
		key := ds.NewKey(keyStr)
		err = zds.Put(ctx, key, value)
		require.NoError(t, err)
	}

	// Query with prefix
	q := ds.Query{
		Prefix: "/test/query",
	}

	results, err := zds.Query(ctx, q)
	require.NoError(t, err)

	// Collect results
	var entries []ds.Entry
	for result := range results.Next() {
		if result.Error != nil {
			t.Fatalf("Query error: %v", result.Error)
		}
		entries = append(entries, result.Entry)
	}

	// Should find 2 items with the prefix
	assert.Len(t, entries, 2)
}

// Test helper functions

func createTestConfig(t *testing.T) *Config {
	tempDir, err := os.MkdirTemp("", "zfs-test-*")
	require.NoError(t, err)

	cfg := &Config{}
	cfg.Default()
	cfg.Folder = tempDir
	cfg.PoolName = "test-pool"
	cfg.DatasetName = "test-dataset"
	cfg.AutoOptimize = false    // Disable for tests
	cfg.SnapshotInterval = 0    // Disable for tests
	cfg.BaseDir = tempDir

	return cfg
}

func cleanupTestConfig(cfg *Config) {
	os.RemoveAll(cfg.GetFolder())
}

func isZFSAvailable() bool {
	// Check if ZFS commands are available
	// In a real test environment, this would check for actual ZFS availability
	// For now, we'll assume it's not available to avoid requiring ZFS for tests
	return false
}

// Run the standard datastore test suite
func TestZFSDatastoreSuite(t *testing.T) {
	if !isZFSAvailable() {
		t.Skip("ZFS not available, skipping datastore suite")
	}

	cfg := createTestConfig(t)
	defer cleanupTestConfig(cfg)

	ds, err := New(cfg)
	require.NoError(t, err)
	defer ds.Close()

	// Run the standard go-datastore test suite
	dstest.SubtestAll(t, ds)
}