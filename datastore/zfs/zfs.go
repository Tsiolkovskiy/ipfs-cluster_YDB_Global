// Package zfs provides a ZFS-optimized datastore for IPFS Cluster.
// It leverages ZFS features like compression, deduplication, and snapshots
// to efficiently store and manage trillion-scale pin metadata.
package zfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/pkg/errors"
)

// ZFSDatastore implements the go-datastore interface with ZFS backend
type ZFSDatastore struct {
	config          *Config
	zfsManager      *ZFSManager
	shardingManager *ShardingManager
	
	// Cache for frequently accessed data
	cache     map[string][]byte
	cacheMux  sync.RWMutex
	cacheSize int
	maxCache  int
	
	// Metrics
	metrics *DatastoreMetrics
	
	// Background optimization
	optimizationTicker *time.Ticker
	stopOptimization   chan struct{}
	
	// Snapshot management
	snapshotTicker *time.Ticker
	stopSnapshots  chan struct{}
}

// DatastoreMetrics tracks performance metrics
type DatastoreMetrics struct {
	TotalOperations    int64
	ReadOperations     int64
	WriteOperations    int64
	DeleteOperations   int64
	CacheHits          int64
	CacheMisses        int64
	CompressionSavings int64
	DeduplicationSavings int64
	mutex              sync.RWMutex
}

// New creates a new ZFS datastore instance
func New(cfg *Config) (ds.Datastore, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid configuration")
	}

	// Create base directory if it doesn't exist
	baseDir := cfg.GetFolder()
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, errors.Wrap(err, "creating base directory")
	}

	// Initialize ZFS manager
	zfsManager := NewZFSManager(cfg)
	
	// Create main dataset
	mainDatasetPath := cfg.GetDatasetPath()
	if err := zfsManager.CreateDataset(mainDatasetPath); err != nil {
		return nil, errors.Wrap(err, "creating main dataset")
	}

	// Initialize sharding manager
	shardingManager := NewShardingManager(cfg, zfsManager)

	// Create datastore instance
	zds := &ZFSDatastore{
		config:          cfg,
		zfsManager:      zfsManager,
		shardingManager: shardingManager,
		cache:           make(map[string][]byte),
		maxCache:        10000, // Cache up to 10k entries
		metrics:         &DatastoreMetrics{},
		stopOptimization: make(chan struct{}),
		stopSnapshots:   make(chan struct{}),
	}

	// Start background optimization if enabled
	if cfg.AutoOptimize {
		zds.startOptimization()
	}

	// Start snapshot management if enabled
	if cfg.SnapshotInterval > 0 {
		zds.startSnapshotManagement()
	}

	return zds, nil
}

// Put stores a key-value pair in the ZFS datastore
func (zds *ZFSDatastore) Put(ctx context.Context, key ds.Key, value []byte) error {
	// Update metrics
	zds.metrics.mutex.Lock()
	zds.metrics.TotalOperations++
	zds.metrics.WriteOperations++
	zds.metrics.mutex.Unlock()

	// Get appropriate shard for this key
	shard, err := zds.shardingManager.GetShardForKey(key)
	if err != nil {
		return errors.Wrapf(err, "getting shard for key %s", key)
	}

	// Construct file path within the shard
	shardPath := zds.shardingManager.GetShardPath(shard)
	filePath := filepath.Join(shardPath, key.String())
	
	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrapf(err, "creating directory %s", dir)
	}

	// Write data to file
	// ZFS compression and deduplication will be applied automatically
	if err := os.WriteFile(filePath, value, 0644); err != nil {
		return errors.Wrapf(err, "writing file %s", filePath)
	}

	// Update cache
	zds.updateCache(key.String(), value)

	// Increment pin count for the shard
	zds.shardingManager.IncrementPinCount(shard.ID)

	return nil
}

// Get retrieves a value by key from the ZFS datastore
func (zds *ZFSDatastore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	// Update metrics
	zds.metrics.mutex.Lock()
	zds.metrics.TotalOperations++
	zds.metrics.ReadOperations++
	zds.metrics.mutex.Unlock()

	keyStr := key.String()

	// Check cache first
	if value, found := zds.getFromCache(keyStr); found {
		zds.metrics.mutex.Lock()
		zds.metrics.CacheHits++
		zds.metrics.mutex.Unlock()
		return value, nil
	}

	zds.metrics.mutex.Lock()
	zds.metrics.CacheMisses++
	zds.metrics.mutex.Unlock()

	// Get shard for this key
	shard, err := zds.shardingManager.GetShardForKey(key)
	if err != nil {
		return nil, errors.Wrapf(err, "getting shard for key %s", key)
	}

	// Construct file path
	shardPath := zds.shardingManager.GetShardPath(shard)
	filePath := filepath.Join(shardPath, keyStr)

	// Read data from file
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ds.ErrNotFound
		}
		return nil, errors.Wrapf(err, "reading file %s", filePath)
	}

	// Update cache
	zds.updateCache(keyStr, data)

	return data, nil
}

// Has checks if a key exists in the ZFS datastore
func (zds *ZFSDatastore) Has(ctx context.Context, key ds.Key) (bool, error) {
	// Check cache first
	if _, found := zds.getFromCache(key.String()); found {
		return true, nil
	}

	// Get shard for this key
	shard, err := zds.shardingManager.GetShardForKey(key)
	if err != nil {
		return false, errors.Wrapf(err, "getting shard for key %s", key)
	}

	// Check if file exists
	shardPath := zds.shardingManager.GetShardPath(shard)
	filePath := filepath.Join(shardPath, key.String())
	
	_, err = os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "checking file %s", filePath)
	}

	return true, nil
}

// GetSize returns the size of a value by key
func (zds *ZFSDatastore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	// Check cache first
	if value, found := zds.getFromCache(key.String()); found {
		return len(value), nil
	}

	// Get shard for this key
	shard, err := zds.shardingManager.GetShardForKey(key)
	if err != nil {
		return 0, errors.Wrapf(err, "getting shard for key %s", key)
	}

	// Get file info
	shardPath := zds.shardingManager.GetShardPath(shard)
	filePath := filepath.Join(shardPath, key.String())
	
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ds.ErrNotFound
		}
		return 0, errors.Wrapf(err, "getting file info %s", filePath)
	}

	return int(info.Size()), nil
}

// Delete removes a key-value pair from the ZFS datastore
func (zds *ZFSDatastore) Delete(ctx context.Context, key ds.Key) error {
	// Update metrics
	zds.metrics.mutex.Lock()
	zds.metrics.TotalOperations++
	zds.metrics.DeleteOperations++
	zds.metrics.mutex.Unlock()

	keyStr := key.String()

	// Remove from cache
	zds.removeFromCache(keyStr)

	// Get shard for this key
	shard, err := zds.shardingManager.GetShardForKey(key)
	if err != nil {
		return errors.Wrapf(err, "getting shard for key %s", key)
	}

	// Remove file
	shardPath := zds.shardingManager.GetShardPath(shard)
	filePath := filepath.Join(shardPath, keyStr)
	
	err = os.Remove(filePath)
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "removing file %s", filePath)
	}

	// Decrement pin count for the shard
	zds.shardingManager.DecrementPinCount(shard.ID)

	return nil
}

// Query executes a query against the ZFS datastore
func (zds *ZFSDatastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	// This is a simplified implementation
	// In a production system, this would need to efficiently query across shards
	
	var entries []dsq.Entry
	
	// Get all shards
	shards := zds.shardingManager.ListShards()
	
	for _, shard := range shards {
		shardPath := zds.shardingManager.GetShardPath(shard)
		
		// Walk through shard directory
		err := filepath.Walk(shardPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			
			if info.IsDir() {
				return nil
			}
			
			// Get relative path as key
			relPath, err := filepath.Rel(shardPath, path)
			if err != nil {
				return err
			}
			
			key := ds.NewKey(relPath)
			
			// Apply query filters
			if q.Prefix != "" && !key.HasPrefix(ds.NewKey(q.Prefix)) {
				return nil
			}
			
			entry := dsq.Entry{
				Key: key.String(),
			}
			
			// Include value if requested
			if !q.KeysOnly {
				value, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				entry.Value = value
			}
			
			entries = append(entries, entry)
			return nil
		})
		
		if err != nil {
			return nil, errors.Wrapf(err, "walking shard %s", shard.ID)
		}
	}
	
	// Apply query ordering and limits
	results := dsq.ResultsWithEntries(q, entries)
	
	return results, nil
}

// Sync ensures all data is written to persistent storage
func (zds *ZFSDatastore) Sync(ctx context.Context, prefix ds.Key) error {
	// ZFS handles sync automatically based on the sync property
	// For immediate sync, we could call 'zfs sync' command, but it's usually not necessary
	return nil
}

// Close closes the ZFS datastore and cleans up resources
func (zds *ZFSDatastore) Close() error {
	// Stop background processes
	if zds.optimizationTicker != nil {
		zds.optimizationTicker.Stop()
		close(zds.stopOptimization)
	}
	
	if zds.snapshotTicker != nil {
		zds.snapshotTicker.Stop()
		close(zds.stopSnapshots)
	}
	
	// Clear cache
	zds.cacheMux.Lock()
	zds.cache = nil
	zds.cacheMux.Unlock()
	
	return nil
}

// Cache management methods

func (zds *ZFSDatastore) updateCache(key string, value []byte) {
	zds.cacheMux.Lock()
	defer zds.cacheMux.Unlock()
	
	// Implement simple LRU eviction if cache is full
	if len(zds.cache) >= zds.maxCache {
		// Remove oldest entry (simplified - in production use proper LRU)
		for k := range zds.cache {
			delete(zds.cache, k)
			break
		}
	}
	
	zds.cache[key] = value
}

func (zds *ZFSDatastore) getFromCache(key string) ([]byte, bool) {
	zds.cacheMux.RLock()
	defer zds.cacheMux.RUnlock()
	
	value, found := zds.cache[key]
	return value, found
}

func (zds *ZFSDatastore) removeFromCache(key string) {
	zds.cacheMux.Lock()
	defer zds.cacheMux.Unlock()
	
	delete(zds.cache, key)
}

// Background optimization

func (zds *ZFSDatastore) startOptimization() {
	zds.optimizationTicker = time.NewTicker(time.Hour) // Optimize every hour
	
	go func() {
		for {
			select {
			case <-zds.optimizationTicker.C:
				zds.performOptimization()
			case <-zds.stopOptimization:
				return
			}
		}
	}()
}

func (zds *ZFSDatastore) performOptimization() {
	ctx := context.Background()
	
	// Optimize all shards
	if err := zds.shardingManager.OptimizeShards(); err != nil {
		// Log error but continue
		return
	}
	
	// Optimize main dataset
	mainDatasetPath := zds.config.GetDatasetPath()
	if err := zds.zfsManager.OptimizeDataset(ctx, mainDatasetPath); err != nil {
		// Log error but continue
		return
	}
}

// Snapshot management

func (zds *ZFSDatastore) startSnapshotManagement() {
	interval := time.Duration(zds.config.SnapshotInterval) * time.Second
	zds.snapshotTicker = time.NewTicker(interval)
	
	go func() {
		for {
			select {
			case <-zds.snapshotTicker.C:
				zds.createScheduledSnapshot()
			case <-zds.stopSnapshots:
				return
			}
		}
	}()
}

func (zds *ZFSDatastore) createScheduledSnapshot() {
	// Create snapshot of main dataset
	mainDatasetPath := zds.config.GetDatasetPath()
	snapshotName := fmt.Sprintf("auto-%d", time.Now().Unix())
	
	if err := zds.zfsManager.CreateSnapshot(mainDatasetPath, snapshotName); err != nil {
		// Log error but continue
		return
	}
	
	// Create snapshots for all shards
	shards := zds.shardingManager.ListShards()
	for _, shard := range shards {
		if err := zds.zfsManager.CreateSnapshot(shard.DatasetPath, snapshotName); err != nil {
			// Log error but continue with other shards
			continue
		}
	}
}

// GetMetrics returns current datastore metrics
func (zds *ZFSDatastore) GetMetrics() *DatastoreMetrics {
	zds.metrics.mutex.RLock()
	defer zds.metrics.mutex.RUnlock()
	
	// Return a copy to avoid race conditions
	return &DatastoreMetrics{
		TotalOperations:      zds.metrics.TotalOperations,
		ReadOperations:       zds.metrics.ReadOperations,
		WriteOperations:      zds.metrics.WriteOperations,
		DeleteOperations:     zds.metrics.DeleteOperations,
		CacheHits:            zds.metrics.CacheHits,
		CacheMisses:          zds.metrics.CacheMisses,
		CompressionSavings:   zds.metrics.CompressionSavings,
		DeduplicationSavings: zds.metrics.DeduplicationSavings,
	}
}

// Cleanup removes the ZFS datastore and all associated datasets
func Cleanup(cfg *Config) error {
	zfsManager := NewZFSManager(cfg)
	
	// This is a destructive operation - in production, add safety checks
	mainDatasetPath := cfg.GetDatasetPath()
	
	// Check if dataset exists before trying to destroy it
	exists, err := zfsManager.DatasetExists(mainDatasetPath)
	if err != nil {
		return errors.Wrap(err, "checking dataset existence")
	}
	
	if exists {
		// Destroy dataset and all children (snapshots, child datasets)
		cmd := fmt.Sprintf("zfs destroy -r %s", mainDatasetPath)
		if err := executeCommand(cmd); err != nil {
			return errors.Wrapf(err, "destroying dataset %s", mainDatasetPath)
		}
	}
	
	// Remove base directory
	baseDir := cfg.GetFolder()
	if err := os.RemoveAll(baseDir); err != nil && !os.IsNotExist(err) {
		return errors.Wrapf(err, "removing directory %s", baseDir)
	}
	
	return nil
}

// executeCommand is a helper function to execute shell commands
func executeCommand(cmd string) error {
	// This is a simplified implementation
	// In production, use proper command execution with context and error handling
	return nil
}