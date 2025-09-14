package sharding

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestLoadBalancer_Creation(t *testing.T) {
	config := &LoadBalancerConfig{
		RoutingStrategy:     RoutingLeastLoaded,
		MaxRequestsPerShard: 1000,
		RequestTimeout:      5 * time.Second,
		BalanceInterval:     time.Hour,
		LoadThreshold:       0.8,
		LatencyThreshold:    100 * time.Millisecond,
		TieringEnabled:      true,
		TieringInterval:     time.Hour,
		HotDataThreshold:    time.Hour,
		ColdDataThreshold:   24 * time.Hour,
		HotTierConfig: &TierConfig{
			Name:             "hot",
			MediaType:        "nvme_ssd",
			MaxCapacity:      1000000,
			CostPerGB:        0.10,
			PerformanceScore: 10.0,
		},
		ColdTierConfig: &TierConfig{
			Name:             "cold",
			MediaType:        "hdd",
			MaxCapacity:      10000000,
			CostPerGB:        0.01,
			PerformanceScore: 1.0,
		},
	}

	// Create sharding manager first
	smConfig := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(smConfig)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	// Create load balancer
	lb, err := NewLoadBalancer(config, sm)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}
	defer lb.Close()

	if lb.config.RoutingStrategy != RoutingLeastLoaded {
		t.Errorf("Expected routing strategy %v, got %v", RoutingLeastLoaded, lb.config.RoutingStrategy)
	}

	if !lb.config.TieringEnabled {
		t.Error("Expected tiering to be enabled")
	}
}

func TestLoadBalancer_RouteRequest(t *testing.T) {
	config := &LoadBalancerConfig{
		RoutingStrategy:     RoutingRoundRobin,
		MaxRequestsPerShard: 1000,
		RequestTimeout:      5 * time.Second,
		LoadThreshold:       0.8,
		LatencyThreshold:    100 * time.Millisecond,
	}

	// Create sharding manager and shards
	smConfig := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(smConfig)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	// Create some shards
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := sm.CreateShard(ctx)
		if err != nil {
			t.Fatalf("Failed to create shard %d: %v", i, err)
		}
	}

	// Create load balancer
	lb, err := NewLoadBalancer(config, sm)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}
	defer lb.Close()

	// Test request routing
	testCID := "QmTest123456789"
	shard, err := lb.RouteRequest(ctx, "pin", testCID)
	if err != nil {
		t.Fatalf("Failed to route request: %v", err)
	}

	if shard == nil {
		t.Error("Expected shard to be returned")
	}

	if shard.ID == "" {
		t.Error("Expected shard to have valid ID")
	}
}

func TestRequestRouter_RoundRobin(t *testing.T) {
	router := NewRequestRouter(RoutingRoundRobin)

	// Create test shards
	shards := []*Shard{
		{ID: "shard1", MaxCapacity: 1000000},
		{ID: "shard2", MaxCapacity: 1000000},
		{ID: "shard3", MaxCapacity: 1000000},
	}

	// Test round-robin distribution
	selectedShards := make(map[string]int)
	for i := 0; i < 9; i++ {
		shard, err := router.RouteRequest("pin", fmt.Sprintf("cid%d", i), shards)
		if err != nil {
			t.Fatalf("Failed to route request %d: %v", i, err)
		}
		selectedShards[shard.ID]++
	}

	// Each shard should be selected 3 times
	for shardID, count := range selectedShards {
		if count != 3 {
			t.Errorf("Shard %s selected %d times, expected 3", shardID, count)
		}
	}
}

func TestRequestRouter_LeastLoaded(t *testing.T) {
	router := NewRequestRouter(RoutingLeastLoaded)

	// Create test shards with different loads
	shards := []*Shard{
		{ID: "shard1", PinCount: 800000, MaxCapacity: 1000000}, // 80% loaded
		{ID: "shard2", PinCount: 200000, MaxCapacity: 1000000}, // 20% loaded
		{ID: "shard3", PinCount: 500000, MaxCapacity: 1000000}, // 50% loaded
	}

	// Test least loaded routing
	shard, err := router.RouteRequest("pin", "testcid", shards)
	if err != nil {
		t.Fatalf("Failed to route request: %v", err)
	}

	// Should select shard2 (least loaded)
	if shard.ID != "shard2" {
		t.Errorf("Expected shard2 (least loaded), got %s", shard.ID)
	}
}

func TestRequestRouter_ConsistentHash(t *testing.T) {
	router := NewRequestRouter(RoutingConsistentHash)

	// Create test shards
	shards := []*Shard{
		{ID: "shard1", MaxCapacity: 1000000},
		{ID: "shard2", MaxCapacity: 1000000},
		{ID: "shard3", MaxCapacity: 1000000},
	}

	testCID := "QmTest123456789"

	// Test consistency - same CID should always route to same shard
	shard1, err := router.RouteRequest("pin", testCID, shards)
	if err != nil {
		t.Fatalf("Failed to route request (first): %v", err)
	}

	shard2, err := router.RouteRequest("pin", testCID, shards)
	if err != nil {
		t.Fatalf("Failed to route request (second): %v", err)
	}

	if shard1.ID != shard2.ID {
		t.Errorf("Consistent hashing failed: got %s then %s", shard1.ID, shard2.ID)
	}
}

func TestTierManager_AccessTracking(t *testing.T) {
	config := &LoadBalancerConfig{
		TieringEnabled:    true,
		HotDataThreshold:  time.Hour,
		ColdDataThreshold: 24 * time.Hour,
		HotTierConfig: &TierConfig{
			Name:             "hot",
			MediaType:        "nvme_ssd",
			PerformanceScore: 10.0,
		},
		ColdTierConfig: &TierConfig{
			Name:             "cold",
			MediaType:        "hdd",
			PerformanceScore: 1.0,
		},
	}

	tm := NewTierManager(config)

	// Test access recording
	testCID := "QmTest123456789"
	tm.RecordAccess(testCID, "shard1")

	// Verify access was recorded
	entry := tm.accessLog.GetEntry(testCID)
	if entry == nil {
		t.Error("Expected access entry to be created")
	}

	if entry.CID != testCID {
		t.Errorf("Expected CID %s, got %s", testCID, entry.CID)
	}

	if entry.AccessCount != 1 {
		t.Errorf("Expected access count 1, got %d", entry.AccessCount)
	}
}

func TestTierManager_TierDetermination(t *testing.T) {
	config := &LoadBalancerConfig{
		HotDataThreshold:  time.Hour,
		ColdDataThreshold: 24 * time.Hour,
	}

	tm := NewTierManager(config)

	now := time.Now()

	// Test hot data (recent and frequent access)
	hotEntry := &AccessEntry{
		CID:         "hot_cid",
		AccessCount: 20,
		LastAccess:  now.Add(-30 * time.Minute), // 30 minutes ago
		CurrentTier: "warm",
	}

	hotTier := tm.determineOptimalTier(hotEntry, now)
	if hotTier != "hot" {
		t.Errorf("Expected hot tier, got %s", hotTier)
	}

	// Test cold data (old access)
	coldEntry := &AccessEntry{
		CID:         "cold_cid",
		AccessCount: 5,
		LastAccess:  now.Add(-48 * time.Hour), // 48 hours ago
		CurrentTier: "warm",
	}

	coldTier := tm.determineOptimalTier(coldEntry, now)
	if coldTier != "cold" {
		t.Errorf("Expected cold tier, got %s", coldTier)
	}

	// Test warm data (moderate access)
	warmEntry := &AccessEntry{
		CID:         "warm_cid",
		AccessCount: 5,
		LastAccess:  now.Add(-2 * time.Hour), // 2 hours ago
		CurrentTier: "hot",
	}

	warmTier := tm.determineOptimalTier(warmEntry, now)
	if warmTier != "warm" {
		t.Errorf("Expected warm tier, got %s", warmTier)
	}
}

func TestLoadBalancer_LoadBalancing(t *testing.T) {
	config := &LoadBalancerConfig{
		RoutingStrategy:     RoutingLeastLoaded,
		MaxRequestsPerShard: 1000,
		RequestTimeout:      5 * time.Second,
		LoadThreshold:       0.8,
		LatencyThreshold:    100 * time.Millisecond,
	}

	// Create sharding manager
	smConfig := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(smConfig)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	// Create shards with different loads
	ctx := context.Background()
	shard1, _ := sm.CreateShard(ctx)
	shard2, _ := sm.CreateShard(ctx)

	// Set different loads
	sm.UpdateShardLoad(shard1.ID, 800000, 1000) // High load
	sm.UpdateShardLoad(shard2.ID, 200000, 200)  // Low load

	// Create load balancer
	lb, err := NewLoadBalancer(config, sm)
	if err != nil {
		t.Fatalf("Failed to create load balancer: %v", err)
	}
	defer lb.Close()

	// Test load balancing
	err = lb.BalanceLoad(ctx)
	if err != nil {
		t.Fatalf("Load balancing failed: %v", err)
	}

	// Verify load balancing was attempted
	// In a real implementation, we would check that rebalancing operations were created
}

