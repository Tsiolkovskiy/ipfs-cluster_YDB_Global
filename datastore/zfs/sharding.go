package zfs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"sync"

	ds "github.com/ipfs/go-datastore"
	"github.com/pkg/errors"
)

// ShardingManager handles automatic sharding of data across ZFS datasets
type ShardingManager struct {
	config      *Config
	zfsManager  *ZFSManager
	shards      map[string]*Shard
	shardsMutex sync.RWMutex
	pinCounts   map[string]int64
	countsMutex sync.RWMutex
}

// Shard represents a ZFS dataset shard
type Shard struct {
	ID          string
	DatasetPath string
	PinCount    int64
	MaxCapacity int64
	CreatedAt   int64
}

// NewShardingManager creates a new sharding manager
func NewShardingManager(cfg *Config, zfsManager *ZFSManager) *ShardingManager {
	return &ShardingManager{
		config:     cfg,
		zfsManager: zfsManager,
		shards:     make(map[string]*Shard),
		pinCounts:  make(map[string]int64),
	}
}

// GetShardForKey determines which shard should store a given key
func (sm *ShardingManager) GetShardForKey(key ds.Key) (*Shard, error) {
	// Use consistent hashing to determine shard
	shardID := sm.calculateShardID(key.String())
	
	sm.shardsMutex.RLock()
	shard, exists := sm.shards[shardID]
	sm.shardsMutex.RUnlock()
	
	if !exists {
		// Create new shard if it doesn't exist
		var err error
		shard, err = sm.createShard(shardID)
		if err != nil {
			return nil, errors.Wrapf(err, "creating shard %s", shardID)
		}
	}
	
	// Check if current shard is full and needs to be split
	sm.countsMutex.RLock()
	currentCount := sm.pinCounts[shardID]
	sm.countsMutex.RUnlock()
	
	if currentCount >= sm.config.MaxPinsPerShard {
		// Create new shard for overflow
		newShardID := fmt.Sprintf("%s-overflow-%d", shardID, currentCount/sm.config.MaxPinsPerShard)
		newShard, err := sm.createShard(newShardID)
		if err != nil {
			return nil, errors.Wrapf(err, "creating overflow shard %s", newShardID)
		}
		return newShard, nil
	}
	
	return shard, nil
}

// IncrementPinCount increments the pin count for a shard
func (sm *ShardingManager) IncrementPinCount(shardID string) {
	sm.countsMutex.Lock()
	sm.pinCounts[shardID]++
	sm.countsMutex.Unlock()
}

// DecrementPinCount decrements the pin count for a shard
func (sm *ShardingManager) DecrementPinCount(shardID string) {
	sm.countsMutex.Lock()
	if sm.pinCounts[shardID] > 0 {
		sm.pinCounts[shardID]--
	}
	sm.countsMutex.Unlock()
}

// GetShardStats returns statistics for a shard
func (sm *ShardingManager) GetShardStats(shardID string) (*ShardStats, error) {
	sm.shardsMutex.RLock()
	shard, exists := sm.shards[shardID]
	sm.shardsMutex.RUnlock()
	
	if !exists {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}
	
	sm.countsMutex.RLock()
	pinCount := sm.pinCounts[shardID]
	sm.countsMutex.RUnlock()
	
	// Get ZFS metrics
	compressionRatio, err := sm.zfsManager.GetCompressionRatio(shard.DatasetPath)
	if err != nil {
		compressionRatio = 1.0 // Default if unable to get ratio
	}
	
	deduplicationRatio, err := sm.zfsManager.GetDeduplicationRatio(shard.DatasetPath)
	if err != nil {
		deduplicationRatio = 1.0 // Default if unable to get ratio
	}
	
	usedSpace, err := sm.zfsManager.GetUsedSpace(shard.DatasetPath)
	if err != nil {
		usedSpace = 0 // Default if unable to get space
	}
	
	return &ShardStats{
		ShardID:            shardID,
		DatasetPath:        shard.DatasetPath,
		PinCount:           pinCount,
		MaxCapacity:        shard.MaxCapacity,
		UsedSpace:          usedSpace,
		CompressionRatio:   compressionRatio,
		DeduplicationRatio: deduplicationRatio,
		UtilizationPercent: float64(pinCount) / float64(shard.MaxCapacity) * 100,
	}, nil
}

// ListShards returns all active shards
func (sm *ShardingManager) ListShards() []*Shard {
	sm.shardsMutex.RLock()
	defer sm.shardsMutex.RUnlock()
	
	shards := make([]*Shard, 0, len(sm.shards))
	for _, shard := range sm.shards {
		shards = append(shards, shard)
	}
	return shards
}

// OptimizeShards performs optimization operations on all shards
func (sm *ShardingManager) OptimizeShards() error {
	shards := sm.ListShards()
	
	for _, shard := range shards {
		stats, err := sm.GetShardStats(shard.ID)
		if err != nil {
			continue // Skip shards with errors
		}
		
		// Optimize underutilized shards
		if stats.UtilizationPercent < 10 && stats.PinCount > 0 {
			// Consider merging with other low-utilization shards
			// This is a placeholder for more complex optimization logic
			continue
		}
		
		// Optimize ZFS properties based on usage patterns
		if stats.CompressionRatio < 1.2 {
			// Poor compression, consider changing algorithm
			err := sm.zfsManager.SetProperty(shard.DatasetPath, "compression", "zstd")
			if err != nil {
				// Log error but continue with other optimizations
				continue
			}
		}
	}
	
	return nil
}

// calculateShardID calculates a consistent shard ID for a given key
func (sm *ShardingManager) calculateShardID(key string) string {
	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])
	
	// Use first 8 characters for shard ID to create 16^8 possible shards
	// This provides good distribution while keeping shard count manageable
	shardID := hashStr[:8]
	
	return shardID
}

// createShard creates a new ZFS dataset shard
func (sm *ShardingManager) createShard(shardID string) (*Shard, error) {
	sm.shardsMutex.Lock()
	defer sm.shardsMutex.Unlock()
	
	// Double-check that shard doesn't exist (race condition protection)
	if shard, exists := sm.shards[shardID]; exists {
		return shard, nil
	}
	
	// Create dataset path
	datasetPath := fmt.Sprintf("%s/shard-%s", sm.config.GetDatasetPath(), shardID)
	
	// Create ZFS dataset
	err := sm.zfsManager.CreateDataset(datasetPath)
	if err != nil {
		return nil, errors.Wrapf(err, "creating ZFS dataset %s", datasetPath)
	}
	
	// Create shard object
	shard := &Shard{
		ID:          shardID,
		DatasetPath: datasetPath,
		PinCount:    0,
		MaxCapacity: sm.config.MaxPinsPerShard,
		CreatedAt:   getCurrentTimestamp(),
	}
	
	// Register shard
	sm.shards[shardID] = shard
	sm.pinCounts[shardID] = 0
	
	return shard, nil
}

// ShardStats contains statistics about a shard
type ShardStats struct {
	ShardID            string  `json:"shard_id"`
	DatasetPath        string  `json:"dataset_path"`
	PinCount           int64   `json:"pin_count"`
	MaxCapacity        int64   `json:"max_capacity"`
	UsedSpace          int64   `json:"used_space"`
	CompressionRatio   float64 `json:"compression_ratio"`
	DeduplicationRatio float64 `json:"deduplication_ratio"`
	UtilizationPercent float64 `json:"utilization_percent"`
}

// getCurrentTimestamp returns current Unix timestamp
func getCurrentTimestamp() int64 {
	return int64(1000000000) // Placeholder - in real implementation use time.Now().Unix()
}

// GetShardPath returns the file system path for a shard
func (sm *ShardingManager) GetShardPath(shard *Shard) string {
	// ZFS datasets are typically mounted under /pool/dataset
	// This is a simplified implementation
	return fmt.Sprintf("/%s", shard.DatasetPath)
}

// RebalanceShards performs load balancing across shards
func (sm *ShardingManager) RebalanceShards() error {
	// Get all shard statistics
	shards := sm.ListShards()
	var overloadedShards []*Shard
	var underutilizedShards []*Shard
	
	for _, shard := range shards {
		stats, err := sm.GetShardStats(shard.ID)
		if err != nil {
			continue
		}
		
		if stats.UtilizationPercent > 90 {
			overloadedShards = append(overloadedShards, shard)
		} else if stats.UtilizationPercent < 30 {
			underutilizedShards = append(underutilizedShards, shard)
		}
	}
	
	// Implement rebalancing logic
	// This is a placeholder for more sophisticated rebalancing
	// In a real implementation, this would involve:
	// 1. Identifying keys to migrate
	// 2. Using ZFS send/receive for efficient data movement
	// 3. Updating internal mappings
	// 4. Verifying data integrity after migration
	
	return nil
}

// GetShardForDatasetPath returns the shard associated with a dataset path
func (sm *ShardingManager) GetShardForDatasetPath(datasetPath string) (*Shard, error) {
	sm.shardsMutex.RLock()
	defer sm.shardsMutex.RUnlock()
	
	for _, shard := range sm.shards {
		if shard.DatasetPath == datasetPath {
			return shard, nil
		}
	}
	
	return nil, fmt.Errorf("no shard found for dataset path %s", datasetPath)
}