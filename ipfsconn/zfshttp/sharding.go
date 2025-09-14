package zfshttp

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var shardLogger = logging.Logger("zfs-sharding")

// ShardingStrategy defines the interface for different sharding strategies
type ShardingStrategy interface {
	GetShardForCID(cid string, shards []*Shard) (*Shard, error)
	ShouldCreateNewShard(shards []*Shard, config *ZFSConfig) bool
	RebalanceShards(shards []*Shard) ([]*ShardRebalanceOperation, error)
}

// ShardRebalanceOperation represents a data movement operation during rebalancing
type ShardRebalanceOperation struct {
	SourceShard string   `json:"source_shard"`
	TargetShard string   `json:"target_shard"`
	CIDs        []string `json:"cids"`
	EstimatedSize int64  `json:"estimated_size"`
}

// ConsistentHashStrategy implements consistent hashing for shard selection
type ConsistentHashStrategy struct {
	virtualNodes int
	hashRing     []uint32
	shardMap     map[uint32]string
	mu           sync.RWMutex
}

// NewConsistentHashStrategy creates a new consistent hash strategy
func NewConsistentHashStrategy(virtualNodes int) *ConsistentHashStrategy {
	return &ConsistentHashStrategy{
		virtualNodes: virtualNodes,
		shardMap:     make(map[uint32]string),
	}
}

// GetShardForCID returns the shard for a given CID using consistent hashing
func (chs *ConsistentHashStrategy) GetShardForCID(cid string, shards []*Shard) (*Shard, error) {
	chs.mu.RLock()
	defer chs.mu.RUnlock()
	
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards available")
	}
	
	// Update hash ring if needed
	if len(chs.hashRing) != len(shards)*chs.virtualNodes {
		chs.updateHashRing(shards)
	}
	
	// Hash the CID
	hash := crc32.ChecksumIEEE([]byte(cid))
	
	// Find the first shard in the ring >= hash
	idx := sort.Search(len(chs.hashRing), func(i int) bool {
		return chs.hashRing[i] >= hash
	})
	
	// Wrap around if necessary
	if idx == len(chs.hashRing) {
		idx = 0
	}
	
	shardID := chs.shardMap[chs.hashRing[idx]]
	
	// Find the actual shard object
	for _, shard := range shards {
		if shard.ID == shardID {
			return shard, nil
		}
	}
	
	return nil, fmt.Errorf("shard %s not found", shardID)
}

// ShouldCreateNewShard determines if a new shard should be created
func (chs *ConsistentHashStrategy) ShouldCreateNewShard(shards []*Shard, config *ZFSConfig) bool {
	if len(shards) == 0 {
		return true
	}
	
	// Check if any shard is approaching capacity
	for _, shard := range shards {
		shard.mu.RLock()
		utilization := float64(shard.PinCount) / float64(shard.MaxCapacity)
		shard.mu.RUnlock()
		
		if utilization > 0.8 { // 80% threshold
			return true
		}
	}
	
	return false
}

// RebalanceShards creates rebalancing operations for consistent hashing
func (chs *ConsistentHashStrategy) RebalanceShards(shards []*Shard) ([]*ShardRebalanceOperation, error) {
	if len(shards) < 2 {
		return nil, nil // No rebalancing needed with less than 2 shards
	}
	
	var operations []*ShardRebalanceOperation
	
	// Calculate average load
	var totalPins int64
	for _, shard := range shards {
		shard.mu.RLock()
		totalPins += shard.PinCount
		shard.mu.RUnlock()
	}
	
	avgLoad := totalPins / int64(len(shards))
	threshold := float64(avgLoad) * 0.1 // 10% threshold
	
	// Find overloaded and underloaded shards
	var overloaded, underloaded []*Shard
	
	for _, shard := range shards {
		shard.mu.RLock()
		pinCount := shard.PinCount
		shard.mu.RUnlock()
		
		if float64(pinCount-avgLoad) > threshold {
			overloaded = append(overloaded, shard)
		} else if float64(avgLoad-pinCount) > threshold {
			underloaded = append(underloaded, shard)
		}
	}
	
	// Create rebalancing operations
	for i := 0; i < len(overloaded) && i < len(underloaded); i++ {
		sourceShard := overloaded[i]
		targetShard := underloaded[i]
		
		sourceShard.mu.RLock()
		sourcePins := sourceShard.PinCount
		sourceShard.mu.RUnlock()
		
		targetShard.mu.RLock()
		targetPins := targetShard.PinCount
		targetShard.mu.RUnlock()
		
		// Calculate how many pins to move
		pinsToMove := (sourcePins - targetPins) / 2
		if pinsToMove > 0 {
			operation := &ShardRebalanceOperation{
				SourceShard:   sourceShard.ID,
				TargetShard:   targetShard.ID,
				EstimatedSize: pinsToMove * 1024, // Estimate 1KB per pin metadata
			}
			operations = append(operations, operation)
		}
	}
	
	return operations, nil
}

// updateHashRing updates the consistent hash ring
func (chs *ConsistentHashStrategy) updateHashRing(shards []*Shard) {
	chs.hashRing = nil
	chs.shardMap = make(map[uint32]string)
	
	for _, shard := range shards {
		for i := 0; i < chs.virtualNodes; i++ {
			virtualKey := fmt.Sprintf("%s:%d", shard.ID, i)
			hash := crc32.ChecksumIEEE([]byte(virtualKey))
			chs.hashRing = append(chs.hashRing, hash)
			chs.shardMap[hash] = shard.ID
		}
	}
	
	sort.Slice(chs.hashRing, func(i, j int) bool {
		return chs.hashRing[i] < chs.hashRing[j]
	})
}

// RoundRobinStrategy implements round-robin shard selection
type RoundRobinStrategy struct {
	counter uint64
	mu      sync.Mutex
}

// NewRoundRobinStrategy creates a new round-robin strategy
func NewRoundRobinStrategy() *RoundRobinStrategy {
	return &RoundRobinStrategy{}
}

// GetShardForCID returns the next shard in round-robin order
func (rrs *RoundRobinStrategy) GetShardForCID(cid string, shards []*Shard) (*Shard, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards available")
	}
	
	rrs.mu.Lock()
	defer rrs.mu.Unlock()
	
	idx := rrs.counter % uint64(len(shards))
	rrs.counter++
	
	return shards[idx], nil
}

// ShouldCreateNewShard determines if a new shard should be created
func (rrs *RoundRobinStrategy) ShouldCreateNewShard(shards []*Shard, config *ZFSConfig) bool {
	if len(shards) == 0 {
		return true
	}
	
	// Check average utilization
	var totalPins, totalCapacity int64
	for _, shard := range shards {
		shard.mu.RLock()
		totalPins += shard.PinCount
		totalCapacity += shard.MaxCapacity
		shard.mu.RUnlock()
	}
	
	if totalCapacity == 0 {
		return true
	}
	
	utilization := float64(totalPins) / float64(totalCapacity)
	return utilization > 0.75 // 75% threshold
}

// RebalanceShards creates rebalancing operations for round-robin
func (rrs *RoundRobinStrategy) RebalanceShards(shards []*Shard) ([]*ShardRebalanceOperation, error) {
	// Round-robin doesn't typically need rebalancing since it distributes evenly
	// But we can still balance if there are significant differences
	
	if len(shards) < 2 {
		return nil, nil
	}
	
	var operations []*ShardRebalanceOperation
	
	// Find min and max loaded shards
	var minShard, maxShard *Shard
	var minPins, maxPins int64 = -1, -1
	
	for _, shard := range shards {
		shard.mu.RLock()
		pinCount := shard.PinCount
		shard.mu.RUnlock()
		
		if minPins == -1 || pinCount < minPins {
			minPins = pinCount
			minShard = shard
		}
		if maxPins == -1 || pinCount > maxPins {
			maxPins = pinCount
			maxShard = shard
		}
	}
	
	// If difference is significant, create rebalancing operation
	if maxPins-minPins > 1000 { // Threshold of 1000 pins difference
		pinsToMove := (maxPins - minPins) / 2
		operation := &ShardRebalanceOperation{
			SourceShard:   maxShard.ID,
			TargetShard:   minShard.ID,
			EstimatedSize: pinsToMove * 1024,
		}
		operations = append(operations, operation)
	}
	
	return operations, nil
}

// SizeBasedStrategy implements size-based shard selection
type SizeBasedStrategy struct {
	compressionWeight float64
	capacityWeight    float64
}

// NewSizeBasedStrategy creates a new size-based strategy
func NewSizeBasedStrategy(compressionWeight, capacityWeight float64) *SizeBasedStrategy {
	return &SizeBasedStrategy{
		compressionWeight: compressionWeight,
		capacityWeight:    capacityWeight,
	}
}

// GetShardForCID returns the optimal shard based on size and compression
func (sbs *SizeBasedStrategy) GetShardForCID(cid string, shards []*Shard) (*Shard, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards available")
	}
	
	var bestShard *Shard
	var bestScore float64 = -1
	
	for _, shard := range shards {
		shard.mu.RLock()
		pinCount := shard.PinCount
		maxCapacity := shard.MaxCapacity
		shard.mu.RUnlock()
		
		// Calculate available capacity
		availableCapacity := float64(maxCapacity - pinCount)
		if availableCapacity <= 0 {
			continue // Skip full shards
		}
		
		// Calculate compression efficiency (mock - would need real metrics)
		compressionRatio := 1.5 // Default compression ratio
		
		// Calculate score based on available capacity and compression
		score := (availableCapacity * sbs.capacityWeight) + 
				(compressionRatio * sbs.compressionWeight)
		
		if bestScore == -1 || score > bestScore {
			bestScore = score
			bestShard = shard
		}
	}
	
	if bestShard == nil {
		return nil, fmt.Errorf("no available shards with capacity")
	}
	
	return bestShard, nil
}

// ShouldCreateNewShard determines if a new shard should be created
func (sbs *SizeBasedStrategy) ShouldCreateNewShard(shards []*Shard, config *ZFSConfig) bool {
	if len(shards) == 0 {
		return true
	}
	
	// Check if all shards are approaching capacity
	availableShards := 0
	for _, shard := range shards {
		shard.mu.RLock()
		utilization := float64(shard.PinCount) / float64(shard.MaxCapacity)
		shard.mu.RUnlock()
		
		if utilization < 0.9 { // 90% threshold
			availableShards++
		}
	}
	
	return availableShards == 0
}

// RebalanceShards creates rebalancing operations based on size optimization
func (sbs *SizeBasedStrategy) RebalanceShards(shards []*Shard) ([]*ShardRebalanceOperation, error) {
	if len(shards) < 2 {
		return nil, nil
	}
	
	var operations []*ShardRebalanceOperation
	
	// Sort shards by utilization
	type shardUtilization struct {
		shard       *Shard
		utilization float64
	}
	
	var shardUtils []shardUtilization
	for _, shard := range shards {
		shard.mu.RLock()
		util := float64(shard.PinCount) / float64(shard.MaxCapacity)
		shard.mu.RUnlock()
		
		shardUtils = append(shardUtils, shardUtilization{
			shard:       shard,
			utilization: util,
		})
	}
	
	sort.Slice(shardUtils, func(i, j int) bool {
		return shardUtils[i].utilization > shardUtils[j].utilization
	})
	
	// Move data from most utilized to least utilized
	for i := 0; i < len(shardUtils)/2; i++ {
		highUtil := shardUtils[i]
		lowUtil := shardUtils[len(shardUtils)-1-i]
		
		if highUtil.utilization-lowUtil.utilization > 0.2 { // 20% difference threshold
			highUtil.shard.mu.RLock()
			highPins := highUtil.shard.PinCount
			highUtil.shard.mu.RUnlock()
			
			lowUtil.shard.mu.RLock()
			lowPins := lowUtil.shard.PinCount
			lowUtil.shard.mu.RUnlock()
			
			pinsToMove := (highPins - lowPins) / 4 // Move 25% of difference
			if pinsToMove > 0 {
				operation := &ShardRebalanceOperation{
					SourceShard:   highUtil.shard.ID,
					TargetShard:   lowUtil.shard.ID,
					EstimatedSize: pinsToMove * 1024,
				}
				operations = append(operations, operation)
			}
		}
	}
	
	return operations, nil
}

// ShardManager manages sharding operations and strategies
type ShardManager struct {
	strategy ShardingStrategy
	config   *ZFSConfig
	mu       sync.RWMutex
}

// NewShardManager creates a new shard manager
func NewShardManager(config *ZFSConfig) *ShardManager {
	var strategy ShardingStrategy
	
	switch config.ShardingStrategy {
	case "consistent_hash":
		strategy = NewConsistentHashStrategy(150) // 150 virtual nodes
	case "round_robin":
		strategy = NewRoundRobinStrategy()
	case "size_based":
		strategy = NewSizeBasedStrategy(0.3, 0.7) // 30% compression, 70% capacity weight
	default:
		strategy = NewConsistentHashStrategy(150)
	}
	
	return &ShardManager{
		strategy: strategy,
		config:   config,
	}
}

// GetShardForCID returns the optimal shard for a CID
func (sm *ShardManager) GetShardForCID(cid string, shards []*Shard) (*Shard, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	return sm.strategy.GetShardForCID(cid, shards)
}

// ShouldCreateNewShard determines if a new shard should be created
func (sm *ShardManager) ShouldCreateNewShard(shards []*Shard) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	return sm.strategy.ShouldCreateNewShard(shards, sm.config)
}

// RebalanceShards creates rebalancing operations
func (sm *ShardManager) RebalanceShards(shards []*Shard) ([]*ShardRebalanceOperation, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	return sm.strategy.RebalanceShards(shards)
}

// ExecuteRebalanceOperation executes a single rebalancing operation
func (sm *ShardManager) ExecuteRebalanceOperation(op *ShardRebalanceOperation, dm *DatasetManager) error {
	shardLogger.Infof("Executing rebalance operation: %s -> %s (%d CIDs)", 
		op.SourceShard, op.TargetShard, len(op.CIDs))
	
	start := time.Now()
	defer func() {
		shardLogger.Infof("Rebalance operation completed in %v", time.Since(start))
	}()
	
	// This would involve:
	// 1. Creating a snapshot of the source dataset
	// 2. Copying specified CIDs to target dataset
	// 3. Updating metadata
	// 4. Removing CIDs from source dataset
	// 5. Verifying integrity
	
	// For now, we'll simulate the operation
	// In a real implementation, this would use ZFS send/receive
	
	return nil
}

// AutoScaler handles automatic scaling of shards
type AutoScaler struct {
	shardManager *ShardManager
	config       *ZFSConfig
	lastScale    time.Time
	mu           sync.Mutex
}

// NewAutoScaler creates a new auto-scaler
func NewAutoScaler(shardManager *ShardManager, config *ZFSConfig) *AutoScaler {
	return &AutoScaler{
		shardManager: shardManager,
		config:       config,
		lastScale:    time.Now(),
	}
}

// CheckScaling checks if scaling is needed and returns recommendations
func (as *AutoScaler) CheckScaling(shards []*Shard) (*ScalingRecommendation, error) {
	as.mu.Lock()
	defer as.mu.Unlock()
	
	// Don't scale too frequently
	if time.Since(as.lastScale) < 5*time.Minute {
		return nil, nil
	}
	
	recommendation := &ScalingRecommendation{
		Timestamp: time.Now(),
	}
	
	// Check if we need to scale up
	if as.shardManager.ShouldCreateNewShard(shards) {
		recommendation.Action = ScaleUp
		recommendation.Reason = "Existing shards approaching capacity"
		recommendation.NewShardCount = 1
	}
	
	// Check if we can scale down (if we have too many underutilized shards)
	underutilizedCount := 0
	for _, shard := range shards {
		shard.mu.RLock()
		utilization := float64(shard.PinCount) / float64(shard.MaxCapacity)
		shard.mu.RUnlock()
		
		if utilization < 0.1 { // Less than 10% utilized
			underutilizedCount++
		}
	}
	
	if underutilizedCount > len(shards)/2 && len(shards) > 2 {
		recommendation.Action = ScaleDown
		recommendation.Reason = "Too many underutilized shards"
		recommendation.ShardsToRemove = underutilizedCount / 2
	}
	
	// Check if rebalancing is needed
	if ops, err := as.shardManager.RebalanceShards(shards); err == nil && len(ops) > 0 {
		recommendation.RebalanceOperations = ops
		if recommendation.Action == NoAction {
			recommendation.Action = Rebalance
			recommendation.Reason = "Load imbalance detected"
		}
	}
	
	if recommendation.Action != NoAction {
		as.lastScale = time.Now()
		return recommendation, nil
	}
	
	return nil, nil
}

// ScalingAction represents the type of scaling action
type ScalingAction int

const (
	NoAction ScalingAction = iota
	ScaleUp
	ScaleDown
	Rebalance
)

// ScalingRecommendation represents a scaling recommendation
type ScalingRecommendation struct {
	Action               ScalingAction              `json:"action"`
	Reason               string                     `json:"reason"`
	Timestamp            time.Time                  `json:"timestamp"`
	NewShardCount        int                        `json:"new_shard_count,omitempty"`
	ShardsToRemove       int                        `json:"shards_to_remove,omitempty"`
	RebalanceOperations  []*ShardRebalanceOperation `json:"rebalance_operations,omitempty"`
}