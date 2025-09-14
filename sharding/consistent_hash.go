package sharding

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// ConsistentHashRing implements consistent hashing for optimal shard distribution
// It provides O(log N) lookup time and minimal data movement when shards are added/removed
type ConsistentHashRing struct {
	virtualNodes int
	ring         []uint32
	shardMap     map[uint32]string
	shards       map[string]bool
	mutex        sync.RWMutex
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		ring:         make([]uint32, 0),
		shardMap:     make(map[uint32]string),
		shards:       make(map[string]bool),
	}
}

// AddShard adds a new shard to the hash ring
func (chr *ConsistentHashRing) AddShard(shardID string) {
	chr.mutex.Lock()
	defer chr.mutex.Unlock()

	if chr.shards[shardID] {
		return // Shard already exists
	}

	// Add virtual nodes for this shard
	for i := 0; i < chr.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s:%d", shardID, i)
		hash := chr.hashKey(virtualKey)
		
		chr.ring = append(chr.ring, hash)
		chr.shardMap[hash] = shardID
	}

	chr.shards[shardID] = true
	
	// Sort the ring to maintain order
	sort.Slice(chr.ring, func(i, j int) bool {
		return chr.ring[i] < chr.ring[j]
	})

	logger.Infof("Added shard %s to consistent hash ring with %d virtual nodes", 
		shardID, chr.virtualNodes)
}

// RemoveShard removes a shard from the hash ring
func (chr *ConsistentHashRing) RemoveShard(shardID string) {
	chr.mutex.Lock()
	defer chr.mutex.Unlock()

	if !chr.shards[shardID] {
		return // Shard doesn't exist
	}

	// Remove all virtual nodes for this shard
	newRing := make([]uint32, 0, len(chr.ring))
	for _, hash := range chr.ring {
		if chr.shardMap[hash] != shardID {
			newRing = append(newRing, hash)
		} else {
			delete(chr.shardMap, hash)
		}
	}

	chr.ring = newRing
	delete(chr.shards, shardID)

	logger.Infof("Removed shard %s from consistent hash ring", shardID)
}

// GetShardForKey returns the shard responsible for a given key
func (chr *ConsistentHashRing) GetShardForKey(key string) string {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	if len(chr.ring) == 0 {
		return ""
	}

	hash := chr.hashKey(key)
	
	// Find the first shard in the ring >= hash using binary search
	idx := sort.Search(len(chr.ring), func(i int) bool {
		return chr.ring[i] >= hash
	})

	// Wrap around if necessary
	if idx == len(chr.ring) {
		idx = 0
	}

	return chr.shardMap[chr.ring[idx]]
}

// GetShardsForKey returns multiple shards for replication
func (chr *ConsistentHashRing) GetShardsForKey(key string, replicationFactor int) []string {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	if len(chr.ring) == 0 || replicationFactor <= 0 {
		return nil
	}

	hash := chr.hashKey(key)
	
	// Find starting position
	startIdx := sort.Search(len(chr.ring), func(i int) bool {
		return chr.ring[i] >= hash
	})
	if startIdx == len(chr.ring) {
		startIdx = 0
	}

	// Collect unique shards
	shardSet := make(map[string]bool)
	shards := make([]string, 0, replicationFactor)
	
	for i := 0; i < len(chr.ring) && len(shards) < replicationFactor; i++ {
		idx := (startIdx + i) % len(chr.ring)
		shardID := chr.shardMap[chr.ring[idx]]
		
		if !shardSet[shardID] {
			shardSet[shardID] = true
			shards = append(shards, shardID)
		}
	}

	return shards
}

// GetShardDistribution returns the distribution of keys across shards
func (chr *ConsistentHashRing) GetShardDistribution() map[string]int {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	distribution := make(map[string]int)
	
	for _, shardID := range chr.shardMap {
		distribution[shardID]++
	}

	return distribution
}

// GetLoadBalance calculates the load balance factor (lower is better)
func (chr *ConsistentHashRing) GetLoadBalance() float64 {
	distribution := chr.GetShardDistribution()
	
	if len(distribution) <= 1 {
		return 0.0 // Perfect balance with 0 or 1 shard
	}

	var min, max int = -1, -1
	for _, count := range distribution {
		if min == -1 || count < min {
			min = count
		}
		if max == -1 || count > max {
			max = count
		}
	}

	if min == 0 {
		return float64(max) // Avoid division by zero
	}

	return float64(max) / float64(min)
}

// ListShards returns all shards in the ring
func (chr *ConsistentHashRing) ListShards() []string {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	shards := make([]string, 0, len(chr.shards))
	for shardID := range chr.shards {
		shards = append(shards, shardID)
	}

	return shards
}

// GetRingSize returns the number of positions in the ring
func (chr *ConsistentHashRing) GetRingSize() int {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	return len(chr.ring)
}

// GetShardCount returns the number of unique shards
func (chr *ConsistentHashRing) GetShardCount() int {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	return len(chr.shards)
}

// hashKey generates a hash for a given key using SHA-256
func (chr *ConsistentHashRing) hashKey(key string) uint32 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}

// GetKeyRange returns the range of keys handled by a specific shard
func (chr *ConsistentHashRing) GetKeyRange(shardID string) (uint32, uint32) {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	var minHash, maxHash uint32 = ^uint32(0), 0
	found := false

	for hash, shard := range chr.shardMap {
		if shard == shardID {
			found = true
			if hash < minHash {
				minHash = hash
			}
			if hash > maxHash {
				maxHash = hash
			}
		}
	}

	if !found {
		return 0, 0
	}

	return minHash, maxHash
}

// EstimateKeyMigration estimates how many keys would need to be migrated
// when adding or removing a shard
func (chr *ConsistentHashRing) EstimateKeyMigration(operation string, shardID string) float64 {
	chr.mutex.RLock()
	currentShardCount := len(chr.shards)
	chr.mutex.RUnlock()

	switch operation {
	case "add":
		// When adding a shard, approximately 1/N of keys will migrate to it
		if currentShardCount == 0 {
			return 1.0 // All keys go to the first shard
		}
		return 1.0 / float64(currentShardCount+1)
		
	case "remove":
		// When removing a shard, its keys will be redistributed
		if currentShardCount <= 1 {
			return 1.0 // All keys need to move
		}
		return 1.0 / float64(currentShardCount)
		
	default:
		return 0.0
	}
}

// Rebalance optimizes the hash ring by adjusting virtual node distribution
func (chr *ConsistentHashRing) Rebalance() {
	chr.mutex.Lock()
	defer chr.mutex.Unlock()

	if len(chr.shards) <= 1 {
		return // No rebalancing needed
	}

	// Calculate current load balance
	currentBalance := chr.getLoadBalanceUnlocked()
	
	// If balance is already good (< 1.2), no need to rebalance
	if currentBalance < 1.2 {
		return
	}

	logger.Infof("Rebalancing consistent hash ring (current balance: %.2f)", currentBalance)

	// Clear current ring
	chr.ring = make([]uint32, 0)
	chr.shardMap = make(map[uint32]string)

	// Redistribute virtual nodes more evenly
	shardList := make([]string, 0, len(chr.shards))
	for shardID := range chr.shards {
		shardList = append(shardList, shardID)
	}

	// Use a different hash seed for each shard to improve distribution
	for i, shardID := range shardList {
		for j := 0; j < chr.virtualNodes; j++ {
			virtualKey := fmt.Sprintf("%s:%d:%d", shardID, i, j)
			hash := chr.hashKey(virtualKey)
			
			chr.ring = append(chr.ring, hash)
			chr.shardMap[hash] = shardID
		}
	}

	// Sort the ring
	sort.Slice(chr.ring, func(i, j int) bool {
		return chr.ring[i] < chr.ring[j]
	})

	newBalance := chr.getLoadBalanceUnlocked()
	logger.Infof("Hash ring rebalanced (new balance: %.2f)", newBalance)
}

// getLoadBalanceUnlocked calculates load balance without locking (internal use)
func (chr *ConsistentHashRing) getLoadBalanceUnlocked() float64 {
	distribution := make(map[string]int)
	
	for _, shardID := range chr.shardMap {
		distribution[shardID]++
	}

	if len(distribution) <= 1 {
		return 0.0
	}

	var min, max int = -1, -1
	for _, count := range distribution {
		if min == -1 || count < min {
			min = count
		}
		if max == -1 || count > max {
			max = count
		}
	}

	if min == 0 {
		return float64(max)
	}

	return float64(max) / float64(min)
}

// GetRingInfo returns detailed information about the hash ring
func (chr *ConsistentHashRing) GetRingInfo() *RingInfo {
	chr.mutex.RLock()
	defer chr.mutex.RUnlock()

	distribution := chr.GetShardDistribution()
	
	return &RingInfo{
		TotalPositions:   len(chr.ring),
		UniqueShards:     len(chr.shards),
		VirtualNodes:     chr.virtualNodes,
		LoadBalance:      chr.getLoadBalanceUnlocked(),
		ShardDistribution: distribution,
	}
}

// RingInfo contains information about the hash ring state
type RingInfo struct {
	TotalPositions    int            `json:"total_positions"`
	UniqueShards      int            `json:"unique_shards"`
	VirtualNodes      int            `json:"virtual_nodes"`
	LoadBalance       float64        `json:"load_balance"`
	ShardDistribution map[string]int `json:"shard_distribution"`
}