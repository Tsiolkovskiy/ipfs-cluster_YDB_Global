// Package sharding provides advanced sharding management for IPFS Cluster ZFS integration
// It implements consistent hashing, dynamic shard management, and load monitoring
// to handle trillion-scale pin distribution across ZFS datasets.
package sharding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"
)

var logger = logging.Logger("sharding-manager")

// ShardingManager manages automatic sharding of pins across ZFS datasets
// using consistent hashing for optimal distribution and load balancing
type ShardingManager struct {
	config          *Config
	consistentHash  *ConsistentHashRing
	shards          map[string]*Shard
	shardsMutex     sync.RWMutex
	loadMonitor     *LoadMonitor
	metricsCollector *MetricsCollector
	
	// Background processes
	monitorTicker   *time.Ticker
	stopMonitoring  chan struct{}
	rebalanceTicker *time.Ticker
	stopRebalancing chan struct{}
}

// Config holds configuration for the sharding manager
type Config struct {
	MaxPinsPerShard     int64         `json:"max_pins_per_shard"`
	VirtualNodes        int           `json:"virtual_nodes"`
	ReplicationFactor   int           `json:"replication_factor"`
	LoadThreshold       float64       `json:"load_threshold"`
	MonitorInterval     time.Duration `json:"monitor_interval"`
	RebalanceInterval   time.Duration `json:"rebalance_interval"`
	AutoRebalance       bool          `json:"auto_rebalance"`
	CompressionEnabled  bool          `json:"compression_enabled"`
	DeduplicationEnabled bool         `json:"deduplication_enabled"`
}

// Shard represents a ZFS dataset shard with metadata and statistics
type Shard struct {
	ID              string            `json:"id"`
	DatasetPath     string            `json:"dataset_path"`
	MountPath       string            `json:"mount_path"`
	PinCount        int64             `json:"pin_count"`
	MaxCapacity     int64             `json:"max_capacity"`
	CreatedAt       time.Time         `json:"created_at"`
	LastAccessed    time.Time         `json:"last_accessed"`
	Properties      map[string]string `json:"properties"`
	
	// Performance metrics
	ReadOps         int64   `json:"read_ops"`
	WriteOps        int64   `json:"write_ops"`
	BytesRead       int64   `json:"bytes_read"`
	BytesWritten    int64   `json:"bytes_written"`
	CompressionRatio float64 `json:"compression_ratio"`
	DeduplicationRatio float64 `json:"deduplication_ratio"`
	
	// Load balancing
	LoadScore       float64   `json:"load_score"`
	LastRebalanced  time.Time `json:"last_rebalanced"`
	
	mutex sync.RWMutex
}



// NewShardingManager creates a new sharding manager instance
func NewShardingManager(config *Config) (*ShardingManager, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid configuration")
	}

	sm := &ShardingManager{
		config:           config,
		consistentHash:   NewConsistentHashRing(config.VirtualNodes),
		shards:           make(map[string]*Shard),
		loadMonitor:      NewLoadMonitor(),
		metricsCollector: NewMetricsCollector(),
		stopMonitoring:   make(chan struct{}),
		stopRebalancing:  make(chan struct{}),
	}

	// Start background monitoring
	if config.MonitorInterval > 0 {
		sm.startMonitoring()
	}

	// Start background rebalancing
	if config.AutoRebalance && config.RebalanceInterval > 0 {
		sm.startRebalancing()
	}

	logger.Info("Sharding manager initialized successfully")
	return sm, nil
}

// GetShardForCID returns the optimal shard for storing a CID using consistent hashing
func (sm *ShardingManager) GetShardForCID(cid string) (*Shard, error) {
	sm.shardsMutex.RLock()
	defer sm.shardsMutex.RUnlock()

	if len(sm.shards) == 0 {
		return nil, fmt.Errorf("no shards available")
	}

	// Get shard ID from consistent hash ring
	shardID := sm.consistentHash.GetShardForKey(cid)
	if shardID == "" {
		return nil, fmt.Errorf("no shard found for CID %s", cid)
	}

	shard, exists := sm.shards[shardID]
	if !exists {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	// Check if shard has capacity
	shard.mutex.RLock()
	hasCapacity := shard.PinCount < shard.MaxCapacity
	shard.mutex.RUnlock()

	if !hasCapacity {
		// Find alternative shard with capacity
		return sm.findAlternativeShard(cid)
	}

	// Update last accessed time
	shard.mutex.Lock()
	shard.LastAccessed = time.Now()
	shard.mutex.Unlock()

	return shard, nil
}

// CreateShard creates a new ZFS dataset shard with optimal configuration
func (sm *ShardingManager) CreateShard(ctx context.Context) (*Shard, error) {
	sm.shardsMutex.Lock()
	defer sm.shardsMutex.Unlock()

	// Generate unique shard ID
	shardID := sm.generateShardID()
	
	// Create shard configuration
	shard := &Shard{
		ID:          shardID,
		DatasetPath: fmt.Sprintf("cluster/shards/%s", shardID),
		MountPath:   fmt.Sprintf("/cluster/shards/%s", shardID),
		PinCount:    0,
		MaxCapacity: sm.config.MaxPinsPerShard,
		CreatedAt:   time.Now(),
		Properties:  make(map[string]string),
		CompressionRatio: 1.0,
		DeduplicationRatio: 1.0,
	}

	// Set ZFS properties based on configuration
	if sm.config.CompressionEnabled {
		shard.Properties["compression"] = "lz4"
	}
	if sm.config.DeduplicationEnabled {
		shard.Properties["dedup"] = "on"
	}

	// Set optimal record size for pin metadata
	shard.Properties["recordsize"] = "128K"
	shard.Properties["atime"] = "off"
	shard.Properties["sync"] = "standard"

	// Register shard
	sm.shards[shardID] = shard
	
	// Add to consistent hash ring
	sm.consistentHash.AddShard(shardID)

	// Initialize monitoring for the shard
	sm.loadMonitor.InitializeShard(shardID)

	logger.Infof("Created new shard: %s", shardID)
	return shard, nil
}

// RemoveShard removes a shard and migrates its data to other shards
func (sm *ShardingManager) RemoveShard(ctx context.Context, shardID string) error {
	sm.shardsMutex.Lock()
	defer sm.shardsMutex.Unlock()

	shard, exists := sm.shards[shardID]
	if !exists {
		return fmt.Errorf("shard %s not found", shardID)
	}

	// Check if shard has data that needs migration
	shard.mutex.RLock()
	pinCount := shard.PinCount
	shard.mutex.RUnlock()

	if pinCount > 0 {
		// Migrate data to other shards
		if err := sm.migrateShard(ctx, shardID); err != nil {
			return errors.Wrapf(err, "migrating shard %s", shardID)
		}
	}

	// Remove from consistent hash ring
	sm.consistentHash.RemoveShard(shardID)

	// Remove from monitoring
	sm.loadMonitor.RemoveShard(shardID)

	// Remove shard
	delete(sm.shards, shardID)

	logger.Infof("Removed shard: %s", shardID)
	return nil
}

// ListShards returns all active shards with their current status
func (sm *ShardingManager) ListShards() []*Shard {
	sm.shardsMutex.RLock()
	defer sm.shardsMutex.RUnlock()

	shards := make([]*Shard, 0, len(sm.shards))
	for _, shard := range sm.shards {
		// Create a copy to avoid race conditions
		shardCopy := *shard
		shards = append(shards, &shardCopy)
	}

	return shards
}

// GetShardMetrics returns detailed metrics for a specific shard
func (sm *ShardingManager) GetShardMetrics(shardID string) (*ShardMetrics, error) {
	return sm.metricsCollector.GetMetrics(shardID)
}

// GetAllMetrics returns metrics for all shards
func (sm *ShardingManager) GetAllMetrics() map[string]*ShardMetrics {
	return sm.metricsCollector.GetAllMetrics()
}

// UpdateShardLoad updates the load information for a shard
func (sm *ShardingManager) UpdateShardLoad(shardID string, pinCount int64, operations int64) error {
	sm.shardsMutex.RLock()
	shard, exists := sm.shards[shardID]
	sm.shardsMutex.RUnlock()

	if !exists {
		return fmt.Errorf("shard %s not found", shardID)
	}

	shard.mutex.Lock()
	shard.PinCount = pinCount
	shard.LastAccessed = time.Now()
	shard.mutex.Unlock()

	// Update load monitor
	sm.loadMonitor.UpdateLoad(shardID, pinCount, operations)

	// Create and update metrics in collector
	metrics := &ShardMetrics{
		ShardID:            shardID,
		Timestamp:          time.Now(),
		PinCount:           pinCount,
		UtilizationPercent: float64(pinCount) / float64(shard.MaxCapacity) * 100,
	}
	sm.metricsCollector.UpdateMetrics(shardID, metrics)

	return nil
}

// CheckRebalanceNeeded determines if rebalancing is needed
func (sm *ShardingManager) CheckRebalanceNeeded() (bool, []*RebalanceOperation) {
	shards := sm.ListShards()
	if len(shards) < 2 {
		return false, nil
	}

	var operations []*RebalanceOperation
	
	// Calculate average load
	var totalPins int64
	for _, shard := range shards {
		shard.mutex.RLock()
		totalPins += shard.PinCount
		shard.mutex.RUnlock()
	}

	avgLoad := float64(totalPins) / float64(len(shards))
	threshold := avgLoad * 0.2 // 20% deviation threshold

	// Find imbalanced shards
	var overloaded, underloaded []*Shard
	for _, shard := range shards {
		shard.mutex.RLock()
		pinCount := shard.PinCount
		shard.mutex.RUnlock()

		deviation := float64(pinCount) - avgLoad
		if deviation > threshold {
			overloaded = append(overloaded, shard)
		} else if deviation < -threshold {
			underloaded = append(underloaded, shard)
		}
	}

	// Create rebalance operations
	for i := 0; i < len(overloaded) && i < len(underloaded); i++ {
		sourceShard := overloaded[i]
		targetShard := underloaded[i]

		sourceShard.mutex.RLock()
		sourcePins := sourceShard.PinCount
		sourceShard.mutex.RUnlock()

		targetShard.mutex.RLock()
		targetPins := targetShard.PinCount
		targetShard.mutex.RUnlock()

		pinsToMove := (sourcePins - targetPins) / 2
		if pinsToMove > 0 {
			operation := &RebalanceOperation{
				SourceShardID: sourceShard.ID,
				TargetShardID: targetShard.ID,
				PinsToMove:    pinsToMove,
				EstimatedTime: sm.estimateRebalanceTime(pinsToMove),
				Priority:      sm.calculateRebalancePriority(sourceShard, targetShard),
			}
			operations = append(operations, operation)
		}
	}

	return len(operations) > 0, operations
}

// RebalanceOperation represents a data rebalancing operation
type RebalanceOperation struct {
	SourceShardID string        `json:"source_shard_id"`
	TargetShardID string        `json:"target_shard_id"`
	PinsToMove    int64         `json:"pins_to_move"`
	EstimatedTime time.Duration `json:"estimated_time"`
	Priority      int           `json:"priority"`
	Status        string        `json:"status"`
	Progress      float64       `json:"progress"`
	StartedAt     time.Time     `json:"started_at"`
	CompletedAt   time.Time     `json:"completed_at"`
}

// Close shuts down the sharding manager and cleans up resources
func (sm *ShardingManager) Close() error {
	// Stop background processes
	if sm.monitorTicker != nil {
		sm.monitorTicker.Stop()
		close(sm.stopMonitoring)
	}

	if sm.rebalanceTicker != nil {
		sm.rebalanceTicker.Stop()
		close(sm.stopRebalancing)
	}

	logger.Info("Sharding manager closed")
	return nil
}

// Helper methods

func (sm *ShardingManager) generateShardID() string {
	timestamp := time.Now().UnixNano()
	hash := sha256.Sum256([]byte(fmt.Sprintf("shard-%d", timestamp)))
	return hex.EncodeToString(hash[:8]) // Use first 8 bytes for shorter ID
}

func (sm *ShardingManager) findAlternativeShard(cid string) (*Shard, error) {
	// Find shard with lowest load that has capacity
	var bestShard *Shard
	var lowestLoad float64 = -1

	for _, shard := range sm.shards {
		shard.mutex.RLock()
		hasCapacity := shard.PinCount < shard.MaxCapacity
		loadScore := shard.LoadScore
		shard.mutex.RUnlock()

		if hasCapacity && (lowestLoad == -1 || loadScore < lowestLoad) {
			lowestLoad = loadScore
			bestShard = shard
		}
	}

	if bestShard == nil {
		return nil, fmt.Errorf("no shards with available capacity")
	}

	return bestShard, nil
}

func (sm *ShardingManager) migrateShard(ctx context.Context, shardID string) error {
	// This would implement ZFS send/receive for efficient data migration
	// For now, we'll simulate the migration process
	logger.Infof("Migrating data from shard %s", shardID)
	
	// In a real implementation, this would:
	// 1. Create snapshots of the source shard
	// 2. Use ZFS send/receive to transfer data
	// 3. Update metadata mappings
	// 4. Verify data integrity
	// 5. Clean up source data
	
	return nil
}

func (sm *ShardingManager) estimateRebalanceTime(pinsToMove int64) time.Duration {
	// Estimate based on average pin size and transfer rate
	avgPinSize := int64(1024) // 1KB average metadata size
	totalBytes := pinsToMove * avgPinSize
	transferRate := int64(100 * 1024 * 1024) // 100MB/s
	
	seconds := totalBytes / transferRate
	if seconds < 1 {
		seconds = 1
	}
	
	return time.Duration(seconds) * time.Second
}

func (sm *ShardingManager) calculateRebalancePriority(source, target *Shard) int {
	source.mutex.RLock()
	sourceUtil := float64(source.PinCount) / float64(source.MaxCapacity)
	source.mutex.RUnlock()

	target.mutex.RLock()
	targetUtil := float64(target.PinCount) / float64(target.MaxCapacity)
	target.mutex.RUnlock()

	// Higher priority for larger imbalances
	imbalance := sourceUtil - targetUtil
	if imbalance > 0.5 {
		return 1 // High priority
	} else if imbalance > 0.3 {
		return 2 // Medium priority
	}
	return 3 // Low priority
}

func (sm *ShardingManager) startMonitoring() {
	sm.monitorTicker = time.NewTicker(sm.config.MonitorInterval)
	
	go func() {
		for {
			select {
			case <-sm.monitorTicker.C:
				sm.collectMetrics()
			case <-sm.stopMonitoring:
				return
			}
		}
	}()
}

func (sm *ShardingManager) startRebalancing() {
	sm.rebalanceTicker = time.NewTicker(sm.config.RebalanceInterval)
	
	go func() {
		for {
			select {
			case <-sm.rebalanceTicker.C:
				sm.checkAndRebalance()
			case <-sm.stopRebalancing:
				return
			}
		}
	}()
}

func (sm *ShardingManager) collectMetrics() {
	shards := sm.ListShards()
	for _, shard := range shards {
		metrics := sm.gatherShardMetrics(shard)
		sm.metricsCollector.UpdateMetrics(shard.ID, metrics)
		sm.loadMonitor.UpdateMetrics(shard.ID, metrics)
	}
}

func (sm *ShardingManager) gatherShardMetrics(shard *Shard) *ShardMetrics {
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	return &ShardMetrics{
		ShardID:            shard.ID,
		Timestamp:          time.Now(),
		PinCount:           shard.PinCount,
		UtilizationPercent: float64(shard.PinCount) / float64(shard.MaxCapacity) * 100,
		CompressionRatio:   shard.CompressionRatio,
		DeduplicationRatio: shard.DeduplicationRatio,
		// Additional metrics would be gathered from ZFS in real implementation
	}
}

func (sm *ShardingManager) checkAndRebalance() {
	needed, operations := sm.CheckRebalanceNeeded()
	if !needed {
		return
	}

	logger.Infof("Rebalancing needed: %d operations", len(operations))
	
	// Execute rebalancing operations
	for _, op := range operations {
		if err := sm.executeRebalanceOperation(op); err != nil {
			logger.Errorf("Rebalance operation failed: %v", err)
		}
	}
}

func (sm *ShardingManager) executeRebalanceOperation(op *RebalanceOperation) error {
	// This would implement the actual rebalancing using ZFS send/receive
	logger.Infof("Executing rebalance: %s -> %s (%d pins)", 
		op.SourceShardID, op.TargetShardID, op.PinsToMove)
	
	// In a real implementation, this would:
	// 1. Create incremental snapshots
	// 2. Use ZFS send/receive for efficient transfer
	// 3. Update consistent hash ring if needed
	// 4. Verify data integrity
	// 5. Update shard metadata
	
	return nil
}

// Validate validates the sharding manager configuration
func (c *Config) Validate() error {
	if c.MaxPinsPerShard <= 0 {
		return fmt.Errorf("max_pins_per_shard must be positive")
	}
	if c.VirtualNodes <= 0 {
		return fmt.Errorf("virtual_nodes must be positive")
	}
	if c.ReplicationFactor <= 0 {
		return fmt.Errorf("replication_factor must be positive")
	}
	if c.LoadThreshold <= 0 || c.LoadThreshold >= 1 {
		return fmt.Errorf("load_threshold must be between 0 and 1")
	}
	return nil
}