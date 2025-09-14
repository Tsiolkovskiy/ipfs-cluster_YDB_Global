package sharding

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// RequestRouter handles intelligent request routing to shards
type RequestRouter struct {
	strategy       RoutingStrategy
	shardWeights   map[string]float64
	shardLatencies map[string]time.Duration
	roundRobinIdx  uint64
	mutex          sync.RWMutex
}

// NewRequestRouter creates a new request router
func NewRequestRouter(strategy RoutingStrategy) *RequestRouter {
	return &RequestRouter{
		strategy:       strategy,
		shardWeights:   make(map[string]float64),
		shardLatencies: make(map[string]time.Duration),
	}
}

// RouteRequest routes a request to the optimal shard based on the configured strategy
func (rr *RequestRouter) RouteRequest(operation string, cid string, availableShards []*Shard) (*Shard, error) {
	if len(availableShards) == 0 {
		return nil, fmt.Errorf("no available shards")
	}

	switch rr.strategy {
	case RoutingRoundRobin:
		return rr.routeRoundRobin(availableShards)
	case RoutingLeastLoaded:
		return rr.routeLeastLoaded(availableShards)
	case RoutingWeightedRoundRobin:
		return rr.routeWeightedRoundRobin(availableShards)
	case RoutingLatencyBased:
		return rr.routeLatencyBased(availableShards)
	case RoutingConsistentHash:
		return rr.routeConsistentHash(cid, availableShards)
	default:
		return rr.routeRoundRobin(availableShards)
	}
}

// UpdateShardWeight updates the weight for a shard (used in weighted routing)
func (rr *RequestRouter) UpdateShardWeight(shardID string, weight float64) {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()
	rr.shardWeights[shardID] = weight
}

// UpdateShardLatency updates the latency metric for a shard
func (rr *RequestRouter) UpdateShardLatency(shardID string, latency time.Duration) {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()
	rr.shardLatencies[shardID] = latency
}

// GetRoutingStats returns current routing statistics
func (rr *RequestRouter) GetRoutingStats() *RoutingStats {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	stats := &RoutingStats{
		Strategy:       rr.strategy,
		ShardWeights:   make(map[string]float64),
		ShardLatencies: make(map[string]time.Duration),
	}

	for shardID, weight := range rr.shardWeights {
		stats.ShardWeights[shardID] = weight
	}

	for shardID, latency := range rr.shardLatencies {
		stats.ShardLatencies[shardID] = latency
	}

	return stats
}

// RoutingStats contains routing statistics
type RoutingStats struct {
	Strategy       RoutingStrategy           `json:"strategy"`
	ShardWeights   map[string]float64        `json:"shard_weights"`
	ShardLatencies map[string]time.Duration  `json:"shard_latencies"`
}

// Routing strategy implementations

func (rr *RequestRouter) routeRoundRobin(shards []*Shard) (*Shard, error) {
	rr.mutex.Lock()
	defer rr.mutex.Unlock()

	idx := rr.roundRobinIdx % uint64(len(shards))
	rr.roundRobinIdx++

	return shards[idx], nil
}

func (rr *RequestRouter) routeLeastLoaded(shards []*Shard) (*Shard, error) {
	var bestShard *Shard
	var lowestLoad float64 = -1

	for _, shard := range shards {
		shard.mutex.RLock()
		utilization := float64(shard.PinCount) / float64(shard.MaxCapacity)
		shard.mutex.RUnlock()

		if lowestLoad == -1 || utilization < lowestLoad {
			lowestLoad = utilization
			bestShard = shard
		}
	}

	if bestShard == nil {
		return nil, fmt.Errorf("no suitable shard found")
	}

	return bestShard, nil
}

func (rr *RequestRouter) routeWeightedRoundRobin(shards []*Shard) (*Shard, error) {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	// Calculate total weight
	var totalWeight float64
	shardWeights := make([]float64, len(shards))

	for i, shard := range shards {
		weight := rr.shardWeights[shard.ID]
		if weight <= 0 {
			weight = 1.0 // Default weight
		}
		shardWeights[i] = weight
		totalWeight += weight
	}

	if totalWeight == 0 {
		return rr.routeRoundRobin(shards)
	}

	// Generate random number and select shard based on weight
	random := rand.Float64() * totalWeight
	var currentWeight float64

	for i, weight := range shardWeights {
		currentWeight += weight
		if random <= currentWeight {
			return shards[i], nil
		}
	}

	// Fallback to last shard
	return shards[len(shards)-1], nil
}

func (rr *RequestRouter) routeLatencyBased(shards []*Shard) (*Shard, error) {
	rr.mutex.RLock()
	defer rr.mutex.RUnlock()

	var bestShard *Shard
	var lowestLatency time.Duration = -1

	for _, shard := range shards {
		latency := rr.shardLatencies[shard.ID]
		if latency == 0 {
			latency = 10 * time.Millisecond // Default latency
		}

		if lowestLatency == -1 || latency < lowestLatency {
			lowestLatency = latency
			bestShard = shard
		}
	}

	if bestShard == nil {
		return rr.routeRoundRobin(shards)
	}

	return bestShard, nil
}

func (rr *RequestRouter) routeConsistentHash(cid string, shards []*Shard) (*Shard, error) {
	// Use consistent hashing based on CID
	hash := rr.hashCID(cid)
	
	// Sort shards by ID for consistent ordering
	sortedShards := make([]*Shard, len(shards))
	copy(sortedShards, shards)
	sort.Slice(sortedShards, func(i, j int) bool {
		return sortedShards[i].ID < sortedShards[j].ID
	})

	// Select shard based on hash
	idx := hash % uint32(len(sortedShards))
	return sortedShards[idx], nil
}

func (rr *RequestRouter) hashCID(cid string) uint32 {
	// Simple hash function for CID
	var hash uint32 = 2166136261 // FNV offset basis
	for _, b := range []byte(cid) {
		hash ^= uint32(b)
		hash *= 16777619 // FNV prime
	}
	return hash
}