package sharding

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestShardingManager_CreateShard(t *testing.T) {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
		MonitorInterval:   time.Minute,
		RebalanceInterval: time.Hour,
		AutoRebalance:     true,
	}

	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()
	shard, err := sm.CreateShard(ctx)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	if shard.ID == "" {
		t.Error("Shard ID should not be empty")
	}

	if shard.MaxCapacity != config.MaxPinsPerShard {
		t.Errorf("Expected max capacity %d, got %d", config.MaxPinsPerShard, shard.MaxCapacity)
	}

	if shard.PinCount != 0 {
		t.Errorf("Expected initial pin count 0, got %d", shard.PinCount)
	}
}

func TestShardingManager_GetShardForCID(t *testing.T) {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	// Create a shard first
	ctx := context.Background()
	shard1, err := sm.CreateShard(ctx)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	// Test getting shard for CID
	testCID := "QmTest123456789"
	selectedShard, err := sm.GetShardForCID(testCID)
	if err != nil {
		t.Fatalf("Failed to get shard for CID: %v", err)
	}

	if selectedShard.ID != shard1.ID {
		t.Errorf("Expected shard %s, got %s", shard1.ID, selectedShard.ID)
	}

	// Test consistent hashing - same CID should always return same shard
	selectedShard2, err := sm.GetShardForCID(testCID)
	if err != nil {
		t.Fatalf("Failed to get shard for CID (second call): %v", err)
	}

	if selectedShard2.ID != selectedShard.ID {
		t.Error("Consistent hashing failed - same CID returned different shards")
	}
}

func TestShardingManager_MultipleShards(t *testing.T) {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()

	// Create multiple shards
	var shards []*Shard
	for i := 0; i < 5; i++ {
		shard, err := sm.CreateShard(ctx)
		if err != nil {
			t.Fatalf("Failed to create shard %d: %v", i, err)
		}
		shards = append(shards, shard)
	}

	// Test that different CIDs get distributed across shards
	cidToShard := make(map[string]string)
	shardCounts := make(map[string]int)

	for i := 0; i < 1000; i++ {
		cid := generateTestCID(i)
		shard, err := sm.GetShardForCID(cid)
		if err != nil {
			t.Fatalf("Failed to get shard for CID %s: %v", cid, err)
		}

		cidToShard[cid] = shard.ID
		shardCounts[shard.ID]++
	}

	// Check that CIDs are distributed across multiple shards
	if len(shardCounts) < 2 {
		t.Error("CIDs should be distributed across multiple shards")
	}

	// Check distribution is reasonably balanced (no shard should have > 50% of CIDs)
	for shardID, count := range shardCounts {
		if float64(count)/1000.0 > 0.5 {
			t.Errorf("Shard %s has too many CIDs: %d (%.1f%%)", shardID, count, float64(count)/10.0)
		}
	}
}

func TestShardingManager_LoadMonitoring(t *testing.T) {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
		MonitorInterval:   100 * time.Millisecond,
	}

	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()
	shard, err := sm.CreateShard(ctx)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	// Update shard load
	err = sm.UpdateShardLoad(shard.ID, 500000, 1000)
	if err != nil {
		t.Fatalf("Failed to update shard load: %v", err)
	}

	// Get shard metrics
	metrics, err := sm.GetShardMetrics(shard.ID)
	if err != nil {
		t.Fatalf("Failed to get shard metrics: %v", err)
	}

	if metrics.PinCount != 500000 {
		t.Errorf("Expected pin count 500000, got %d", metrics.PinCount)
	}
}

func TestShardingManager_RebalanceCheck(t *testing.T) {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()

	// Create two shards with different loads
	shard1, err := sm.CreateShard(ctx)
	if err != nil {
		t.Fatalf("Failed to create shard 1: %v", err)
	}

	shard2, err := sm.CreateShard(ctx)
	if err != nil {
		t.Fatalf("Failed to create shard 2: %v", err)
	}

	// Set different loads
	sm.UpdateShardLoad(shard1.ID, 800000, 1000) // High load
	sm.UpdateShardLoad(shard2.ID, 200000, 200)  // Low load

	// Check if rebalancing is needed
	needed, operations := sm.CheckRebalanceNeeded()
	if !needed {
		t.Error("Rebalancing should be needed with imbalanced loads")
	}

	if len(operations) == 0 {
		t.Error("Should have rebalance operations")
	}

	// Verify operation details
	op := operations[0]
	if op.SourceShardID != shard1.ID {
		t.Errorf("Expected source shard %s, got %s", shard1.ID, op.SourceShardID)
	}

	if op.TargetShardID != shard2.ID {
		t.Errorf("Expected target shard %s, got %s", shard2.ID, op.TargetShardID)
	}

	if op.PinsToMove <= 0 {
		t.Error("Should have pins to move")
	}
}

func TestShardingManager_RemoveShard(t *testing.T) {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	ctx := context.Background()
	shard, err := sm.CreateShard(ctx)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	// Verify shard exists
	shards := sm.ListShards()
	if len(shards) != 1 {
		t.Errorf("Expected 1 shard, got %d", len(shards))
	}

	// Remove shard (with no data, so no migration needed)
	err = sm.RemoveShard(ctx, shard.ID)
	if err != nil {
		t.Fatalf("Failed to remove shard: %v", err)
	}

	// Verify shard is removed
	shards = sm.ListShards()
	if len(shards) != 0 {
		t.Errorf("Expected 0 shards after removal, got %d", len(shards))
	}
}

func TestConsistentHashRing_Distribution(t *testing.T) {
	ring := NewConsistentHashRing(150)

	// Add multiple shards
	shardIDs := []string{"shard1", "shard2", "shard3", "shard4", "shard5"}
	for _, shardID := range shardIDs {
		ring.AddShard(shardID)
	}

	// Test distribution
	distribution := ring.GetShardDistribution()
	if len(distribution) != len(shardIDs) {
		t.Errorf("Expected %d shards in distribution, got %d", len(shardIDs), len(distribution))
	}

	// Each shard should have the same number of virtual nodes
	expectedVirtualNodes := 150
	for shardID, count := range distribution {
		if count != expectedVirtualNodes {
			t.Errorf("Shard %s has %d virtual nodes, expected %d", shardID, count, expectedVirtualNodes)
		}
	}

	// Test load balance
	balance := ring.GetLoadBalance()
	if balance != 1.0 {
		t.Errorf("Expected perfect balance (1.0), got %.2f", balance)
	}
}

func TestConsistentHashRing_KeyDistribution(t *testing.T) {
	ring := NewConsistentHashRing(150)

	// Add shards
	shardIDs := []string{"shard1", "shard2", "shard3"}
	for _, shardID := range shardIDs {
		ring.AddShard(shardID)
	}

	// Test key distribution
	shardCounts := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := generateTestCID(i)
		shardID := ring.GetShardForKey(key)
		shardCounts[shardID]++
	}

	// Check that all shards got some keys
	if len(shardCounts) != len(shardIDs) {
		t.Errorf("Expected keys distributed to %d shards, got %d", len(shardIDs), len(shardCounts))
	}

	// Check reasonably balanced distribution (each shard should get 20-40% of keys)
	for shardID, count := range shardCounts {
		percentage := float64(count) / 100.0 // Out of 10000 keys
		if percentage < 20.0 || percentage > 40.0 {
			t.Errorf("Shard %s got %.1f%% of keys, expected 20-40%%", shardID, percentage)
		}
	}
}

// Helper functions

func generateTestCID(i int) string {
	return fmt.Sprintf("QmTest%010d", i)
}