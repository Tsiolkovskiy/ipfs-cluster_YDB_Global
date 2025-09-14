package sharding

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var lbLogger = logging.Logger("load-balancer")

// LoadBalancer manages request distribution and data tiering across ZFS datasets
type LoadBalancer struct {
	config          *LoadBalancerConfig
	shardingManager *ShardingManager
	tierManager     *TierManager
	requestRouter   *RequestRouter
	
	// Request tracking
	requestMetrics  map[string]*RequestMetrics
	metricsMutex    sync.RWMutex
	
	// Background processes
	balanceTicker   *time.Ticker
	stopBalancing   chan struct{}
	tieringTicker   *time.Ticker
	stopTiering     chan struct{}
}

// LoadBalancerConfig holds configuration for the load balancer
type LoadBalancerConfig struct {
	// Request routing
	RoutingStrategy      RoutingStrategy `json:"routing_strategy"`
	MaxRequestsPerShard  int64          `json:"max_requests_per_shard"`
	RequestTimeout       time.Duration  `json:"request_timeout"`
	
	// Load balancing
	BalanceInterval      time.Duration  `json:"balance_interval"`
	LoadThreshold        float64        `json:"load_threshold"`
	LatencyThreshold     time.Duration  `json:"latency_threshold"`
	
	// Data tiering
	TieringEnabled       bool           `json:"tiering_enabled"`
	TieringInterval      time.Duration  `json:"tiering_interval"`
	HotDataThreshold     time.Duration  `json:"hot_data_threshold"`
	ColdDataThreshold    time.Duration  `json:"cold_data_threshold"`
	
	// Storage tiers
	HotTierConfig        *TierConfig    `json:"hot_tier_config"`
	WarmTierConfig       *TierConfig    `json:"warm_tier_config"`
	ColdTierConfig       *TierConfig    `json:"cold_tier_config"`
}

// RoutingStrategy defines how requests are routed to shards
type RoutingStrategy int

const (
	RoutingRoundRobin RoutingStrategy = iota
	RoutingLeastLoaded
	RoutingWeightedRoundRobin
	RoutingLatencyBased
	RoutingConsistentHash
)

// TierConfig defines configuration for a storage tier
type TierConfig struct {
	Name             string            `json:"name"`
	MediaType        string            `json:"media_type"`        // nvme_ssd, sata_ssd, hdd
	ZFSProperties    map[string]string `json:"zfs_properties"`
	MaxCapacity      int64             `json:"max_capacity"`
	CostPerGB        float64           `json:"cost_per_gb"`
	PerformanceScore float64           `json:"performance_score"`
}

// RequestMetrics tracks metrics for request routing
type RequestMetrics struct {
	ShardID          string        `json:"shard_id"`
	RequestCount     int64         `json:"request_count"`
	SuccessCount     int64         `json:"success_count"`
	ErrorCount       int64         `json:"error_count"`
	TotalLatency     time.Duration `json:"total_latency"`
	AvgLatency       time.Duration `json:"avg_latency"`
	LastRequest      time.Time     `json:"last_request"`
	RequestsPerSec   float64       `json:"requests_per_sec"`
}

// StorageTier represents a storage tier with its characteristics
type StorageTier struct {
	Config           *TierConfig       `json:"config"`
	Shards           []*Shard          `json:"shards"`
	CurrentCapacity  int64             `json:"current_capacity"`
	UsedCapacity     int64             `json:"used_capacity"`
	PerformanceScore float64           `json:"performance_score"`
	CostScore        float64           `json:"cost_score"`
}

// AccessLog tracks data access patterns for tiering decisions
type AccessLog struct {
	entries map[string]*AccessEntry
	mutex   sync.RWMutex
}

// AccessEntry tracks access information for a CID
type AccessEntry struct {
	CID           string    `json:"cid"`
	AccessCount   int64     `json:"access_count"`
	LastAccess    time.Time `json:"last_access"`
	FirstAccess   time.Time `json:"first_access"`
	AccessPattern string    `json:"access_pattern"` // hot, warm, cold
	CurrentTier   string    `json:"current_tier"`
}



// NewLoadBalancer creates a new load balancer instance
func NewLoadBalancer(config *LoadBalancerConfig, shardingManager *ShardingManager) (*LoadBalancer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid load balancer config: %w", err)
	}

	lb := &LoadBalancer{
		config:          config,
		shardingManager: shardingManager,
		tierManager:     NewTierManager(config),
		requestRouter:   NewRequestRouter(config.RoutingStrategy),
		requestMetrics:  make(map[string]*RequestMetrics),
		stopBalancing:   make(chan struct{}),
		stopTiering:     make(chan struct{}),
	}

	// Start background processes
	if config.BalanceInterval > 0 {
		lb.startLoadBalancing()
	}

	if config.TieringEnabled && config.TieringInterval > 0 {
		lb.startDataTiering()
	}

	lbLogger.Info("Load balancer initialized successfully")
	return lb, nil
}

// RouteRequest routes a request to the optimal shard
func (lb *LoadBalancer) RouteRequest(ctx context.Context, operation string, cid string) (*Shard, error) {
	start := time.Now()
	defer func() {
		lb.recordRequestMetrics(operation, time.Since(start))
	}()

	// Get available shards
	shards := lb.shardingManager.ListShards()
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards available")
	}

	// Filter shards based on load and health
	availableShards := lb.filterAvailableShards(shards)
	if len(availableShards) == 0 {
		return nil, fmt.Errorf("no healthy shards available")
	}

	// Route based on strategy
	selectedShard, err := lb.requestRouter.RouteRequest(operation, cid, availableShards)
	if err != nil {
		return nil, fmt.Errorf("failed to route request: %w", err)
	}

	// Update access log for tiering
	if lb.config.TieringEnabled {
		lb.tierManager.RecordAccess(cid, selectedShard.ID)
	}

	return selectedShard, nil
}

// BalanceLoad performs load balancing across shards
func (lb *LoadBalancer) BalanceLoad(ctx context.Context) error {
	lbLogger.Info("Starting load balancing operation")

	// Get current shard loads
	shards := lb.shardingManager.ListShards()
	shardLoads := make(map[string]*ShardLoadInfo)

	for _, shard := range shards {
		if loadInfo, exists := lb.shardingManager.loadMonitor.GetShardLoad(shard.ID); exists {
			shardLoads[shard.ID] = loadInfo
		}
	}

	// Identify imbalanced shards
	rebalanceOps := lb.identifyRebalanceOperations(shardLoads)
	if len(rebalanceOps) == 0 {
		lbLogger.Info("No rebalancing needed")
		return nil
	}

	// Execute rebalancing operations
	for _, op := range rebalanceOps {
		if err := lb.executeLoadBalanceOperation(ctx, op); err != nil {
			lbLogger.Errorf("Load balance operation failed: %v", err)
			continue
		}
	}

	lbLogger.Infof("Completed load balancing: %d operations", len(rebalanceOps))
	return nil
}

// LoadBalanceOperation represents a load balancing operation
type LoadBalanceOperation struct {
	Type          string    `json:"type"`           // rebalance, migrate, scale
	SourceShardID string    `json:"source_shard_id"`
	TargetShardID string    `json:"target_shard_id"`
	CIDs          []string  `json:"cids"`
	Priority      int       `json:"priority"`
	EstimatedTime time.Duration `json:"estimated_time"`
}

// MoveTier moves data between storage tiers based on access patterns
func (lb *LoadBalancer) MoveTier(ctx context.Context, cid string, targetTier string) error {
	return lb.tierManager.MoveToTier(ctx, cid, targetTier)
}

// GetLoadMetrics returns current load balancing metrics
func (lb *LoadBalancer) GetLoadMetrics() map[string]*RequestMetrics {
	lb.metricsMutex.RLock()
	defer lb.metricsMutex.RUnlock()

	result := make(map[string]*RequestMetrics)
	for shardID, metrics := range lb.requestMetrics {
		metricsCopy := *metrics
		result[shardID] = &metricsCopy
	}

	return result
}

// GetTieringStatus returns current data tiering status
func (lb *LoadBalancer) GetTieringStatus() *TieringStatus {
	return lb.tierManager.GetStatus()
}

// TieringStatus contains information about data tiering
type TieringStatus struct {
	Timestamp     time.Time                    `json:"timestamp"`
	TotalData     int64                        `json:"total_data"`
	TierBreakdown map[string]*TierBreakdown    `json:"tier_breakdown"`
	PendingMoves  []*TierMoveOperation         `json:"pending_moves"`
}

// TierBreakdown shows data distribution in a tier
type TierBreakdown struct {
	TierName      string  `json:"tier_name"`
	DataSize      int64   `json:"data_size"`
	PinCount      int64   `json:"pin_count"`
	Utilization   float64 `json:"utilization"`
	CostPerMonth  float64 `json:"cost_per_month"`
}

// TierMoveOperation represents a pending tier move
type TierMoveOperation struct {
	CID           string    `json:"cid"`
	SourceTier    string    `json:"source_tier"`
	TargetTier    string    `json:"target_tier"`
	Reason        string    `json:"reason"`
	ScheduledTime time.Time `json:"scheduled_time"`
	Priority      int       `json:"priority"`
}

// Close shuts down the load balancer
func (lb *LoadBalancer) Close() error {
	// Stop background processes
	if lb.balanceTicker != nil {
		lb.balanceTicker.Stop()
		close(lb.stopBalancing)
	}

	if lb.tieringTicker != nil {
		lb.tieringTicker.Stop()
		close(lb.stopTiering)
	}

	lbLogger.Info("Load balancer closed")
	return nil
}

// Helper methods

func (lb *LoadBalancer) filterAvailableShards(shards []*Shard) []*Shard {
	var available []*Shard

	for _, shard := range shards {
		// Check shard health and capacity
		if loadInfo, exists := lb.shardingManager.loadMonitor.GetShardLoad(shard.ID); exists {
			// Skip overloaded shards
			if loadInfo.UtilizationPercent > lb.config.LoadThreshold*100 {
				continue
			}

			// Skip shards with high latency
			if time.Duration(loadInfo.AvgLatency)*time.Millisecond > lb.config.LatencyThreshold {
				continue
			}

			// Skip shards with high error rate
			if loadInfo.ErrorRate > 0.05 { // 5% error threshold
				continue
			}
		}

		available = append(available, shard)
	}

	return available
}

func (lb *LoadBalancer) identifyRebalanceOperations(shardLoads map[string]*ShardLoadInfo) []*LoadBalanceOperation {
	if len(shardLoads) < 2 {
		return nil
	}

	var operations []*LoadBalanceOperation

	// Calculate average load
	var totalLoad float64
	for _, load := range shardLoads {
		totalLoad += load.LoadScore
	}
	avgLoad := totalLoad / float64(len(shardLoads))

	// Find imbalanced shards
	type shardLoad struct {
		shardID string
		load    float64
	}

	var overloaded, underloaded []shardLoad
	threshold := avgLoad * 0.2 // 20% threshold

	for shardID, load := range shardLoads {
		deviation := load.LoadScore - avgLoad
		if deviation > threshold {
			overloaded = append(overloaded, shardLoad{shardID, load.LoadScore})
		} else if deviation < -threshold {
			underloaded = append(underloaded, shardLoad{shardID, load.LoadScore})
		}
	}

	// Sort by load deviation
	sort.Slice(overloaded, func(i, j int) bool {
		return overloaded[i].load > overloaded[j].load
	})
	sort.Slice(underloaded, func(i, j int) bool {
		return underloaded[i].load < underloaded[j].load
	})

	// Create rebalance operations
	for i := 0; i < len(overloaded) && i < len(underloaded); i++ {
		source := overloaded[i]
		target := underloaded[i]

		operation := &LoadBalanceOperation{
			Type:          "rebalance",
			SourceShardID: source.shardID,
			TargetShardID: target.shardID,
			Priority:      lb.calculateOperationPriority(source.load, target.load),
			EstimatedTime: lb.estimateOperationTime(source.load - target.load),
		}

		operations = append(operations, operation)
	}

	return operations
}

func (lb *LoadBalancer) executeLoadBalanceOperation(ctx context.Context, op *LoadBalanceOperation) error {
	lbLogger.Infof("Executing load balance operation: %s -> %s", op.SourceShardID, op.TargetShardID)

	// This would implement the actual load balancing logic
	// In a real implementation, this would:
	// 1. Identify specific CIDs to move
	// 2. Use ZFS send/receive for efficient data movement
	// 3. Update routing tables
	// 4. Verify data integrity
	// 5. Update shard metadata

	return nil
}

func (lb *LoadBalancer) calculateOperationPriority(sourceLoad, targetLoad float64) int {
	imbalance := sourceLoad - targetLoad
	if imbalance > 0.5 {
		return 1 // High priority
	} else if imbalance > 0.3 {
		return 2 // Medium priority
	}
	return 3 // Low priority
}

func (lb *LoadBalancer) estimateOperationTime(loadDifference float64) time.Duration {
	// Estimate based on load difference and transfer capabilities
	baseTime := 5 * time.Minute
	scaleFactor := math.Max(1.0, loadDifference*10)
	return time.Duration(float64(baseTime) * scaleFactor)
}

func (lb *LoadBalancer) recordRequestMetrics(operation string, latency time.Duration) {
	lb.metricsMutex.Lock()
	defer lb.metricsMutex.Unlock()

	// This is a simplified implementation
	// In practice, you'd track metrics per shard
	if lb.requestMetrics["global"] == nil {
		lb.requestMetrics["global"] = &RequestMetrics{
			ShardID: "global",
		}
	}

	metrics := lb.requestMetrics["global"]
	metrics.RequestCount++
	metrics.TotalLatency += latency
	metrics.AvgLatency = metrics.TotalLatency / time.Duration(metrics.RequestCount)
	metrics.LastRequest = time.Now()
}

func (lb *LoadBalancer) startLoadBalancing() {
	lb.balanceTicker = time.NewTicker(lb.config.BalanceInterval)

	go func() {
		for {
			select {
			case <-lb.balanceTicker.C:
				ctx := context.Background()
				if err := lb.BalanceLoad(ctx); err != nil {
					lbLogger.Errorf("Load balancing failed: %v", err)
				}
			case <-lb.stopBalancing:
				return
			}
		}
	}()
}

func (lb *LoadBalancer) startDataTiering() {
	lb.tieringTicker = time.NewTicker(lb.config.TieringInterval)

	go func() {
		for {
			select {
			case <-lb.tieringTicker.C:
				ctx := context.Background()
				if err := lb.tierManager.PerformTiering(ctx); err != nil {
					lbLogger.Errorf("Data tiering failed: %v", err)
				}
			case <-lb.stopTiering:
				return
			}
		}
	}()
}

// Validate validates the load balancer configuration
func (c *LoadBalancerConfig) Validate() error {
	if c.MaxRequestsPerShard <= 0 {
		return fmt.Errorf("max_requests_per_shard must be positive")
	}
	if c.LoadThreshold <= 0 || c.LoadThreshold >= 1 {
		return fmt.Errorf("load_threshold must be between 0 and 1")
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request_timeout must be positive")
	}
	return nil
}