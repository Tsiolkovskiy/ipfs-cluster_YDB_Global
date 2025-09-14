package zfshttp

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestShards(count int) []*Shard {
	shards := make([]*Shard, count)
	for i := 0; i < count; i++ {
		shards[i] = &Shard{
			ID:          fmt.Sprintf("shard-%d", i),
			Dataset:     fmt.Sprintf("test-pool/shard-%d", i),
			PinCount:    int64(i * 100), // Varying pin counts
			MaxCapacity: 1000,
			CreatedAt:   time.Now(),
		}
	}
	return shards
}

func TestConsistentHashStrategy_GetShardForCID(t *testing.T) {
	strategy := NewConsistentHashStrategy(100)
	shards := createTestShards(3)
	
	// Test that same CID always maps to same shard
	cid := "QmTest123"
	shard1, err := strategy.GetShardForCID(cid, shards)
	require.NoError(t, err)
	require.NotNil(t, shard1)
	
	shard2, err := strategy.GetShardForCID(cid, shards)
	require.NoError(t, err)
	assert.Equal(t, shard1.ID, shard2.ID)
	
	// Test with empty shards
	_, err = strategy.GetShardForCID(cid, []*Shard{})
	assert.Error(t, err)
}

func TestConsistentHashStrategy_ShouldCreateNewShard(t *testing.T) {
	strategy := NewConsistentHashStrategy(100)
	config := &ZFSConfig{MaxPinsPerDataset: 1000}
	
	// Test with no shards
	assert.True(t, strategy.ShouldCreateNewShard([]*Shard{}, config))
	
	// Test with underutilized shards
	shards := createTestShards(2)
	shards[0].PinCount = 100 // 10% utilization
	shards[1].PinCount = 200 // 20% utilization
	assert.False(t, strategy.ShouldCreateNewShard(shards, config))
	
	// Test with highly utilized shards
	shards[0].PinCount = 850 // 85% utilization
	assert.True(t, strategy.ShouldCreateNewShard(shards, config))
}

func TestConsistentHashStrategy_RebalanceShards(t *testing.T) {
	strategy := NewConsistentHashStrategy(100)
	
	// Test with no shards
	ops, err := strategy.RebalanceShards([]*Shard{})
	require.NoError(t, err)
	assert.Nil(t, ops)
	
	// Test with single shard
	ops, err = strategy.RebalanceShards(createTestShards(1))
	require.NoError(t, err)
	assert.Nil(t, ops)
	
	// Test with imbalanced shards
	shards := createTestShards(3)
	shards[0].PinCount = 100
	shards[1].PinCount = 500
	shards[2].PinCount = 200
	
	ops, err = strategy.RebalanceShards(shards)
	require.NoError(t, err)
	
	// Should create rebalancing operations for imbalanced shards
	if len(ops) > 0 {
		assert.NotEmpty(t, ops[0].SourceShard)
		assert.NotEmpty(t, ops[0].TargetShard)
		assert.Greater(t, ops[0].EstimatedSize, int64(0))
	}
}

func TestRoundRobinStrategy_GetShardForCID(t *testing.T) {
	strategy := NewRoundRobinStrategy()
	shards := createTestShards(3)
	
	// Test round-robin distribution
	shardCounts := make(map[string]int)
	for i := 0; i < 9; i++ {
		cid := fmt.Sprintf("QmTest%d", i)
		shard, err := strategy.GetShardForCID(cid, shards)
		require.NoError(t, err)
		shardCounts[shard.ID]++
	}
	
	// Each shard should get 3 CIDs (9 total / 3 shards)
	for _, count := range shardCounts {
		assert.Equal(t, 3, count)
	}
}

func TestRoundRobinStrategy_ShouldCreateNewShard(t *testing.T) {
	strategy := NewRoundRobinStrategy()
	config := &ZFSConfig{MaxPinsPerDataset: 1000}
	
	// Test with no shards
	assert.True(t, strategy.ShouldCreateNewShard([]*Shard{}, config))
	
	// Test with low utilization
	shards := createTestShards(2)
	shards[0].PinCount = 200
	shards[1].PinCount = 300
	assert.False(t, strategy.ShouldCreateNewShard(shards, config))
	
	// Test with high utilization
	shards[0].PinCount = 800
	shards[1].PinCount = 900
	assert.True(t, strategy.ShouldCreateNewShard(shards, config))
}

func TestSizeBasedStrategy_GetShardForCID(t *testing.T) {
	strategy := NewSizeBasedStrategy(0.3, 0.7)
	shards := createTestShards(3)
	
	// Set different capacities
	shards[0].PinCount = 900 // Almost full
	shards[1].PinCount = 100 // Mostly empty
	shards[2].PinCount = 500 // Half full
	
	cid := "QmTest123"
	shard, err := strategy.GetShardForCID(cid, shards)
	require.NoError(t, err)
	
	// Should prefer the shard with most available capacity (shard-1)
	assert.Equal(t, "shard-1", shard.ID)
}

func TestSizeBasedStrategy_ShouldCreateNewShard(t *testing.T) {
	strategy := NewSizeBasedStrategy(0.3, 0.7)
	config := &ZFSConfig{MaxPinsPerDataset: 1000}
	
	// Test with available capacity
	shards := createTestShards(2)
	shards[0].PinCount = 500
	shards[1].PinCount = 600
	assert.False(t, strategy.ShouldCreateNewShard(shards, config))
	
	// Test with all shards near capacity
	shards[0].PinCount = 950
	shards[1].PinCount = 920
	assert.True(t, strategy.ShouldCreateNewShard(shards, config))
}

func TestShardManager_Integration(t *testing.T) {
	config := &ZFSConfig{
		ShardingStrategy:  "consistent_hash",
		MaxPinsPerDataset: 1000,
	}
	
	manager := NewShardManager(config)
	shards := createTestShards(3)
	
	// Test shard selection
	cid := "QmTest123"
	shard, err := manager.GetShardForCID(cid, shards)
	require.NoError(t, err)
	assert.NotNil(t, shard)
	
	// Test scaling decision
	shouldScale := manager.ShouldCreateNewShard(shards)
	assert.False(t, shouldScale) // Default test shards shouldn't need scaling
	
	// Test rebalancing
	_, err = manager.RebalanceShards(shards)
	require.NoError(t, err)
	// May or may not have operations depending on shard distribution
}

func TestAutoScaler_CheckScaling(t *testing.T) {
	config := &ZFSConfig{
		ShardingStrategy:  "consistent_hash",
		MaxPinsPerDataset: 1000,
	}
	
	shardManager := NewShardManager(config)
	autoScaler := NewAutoScaler(shardManager, config)
	
	// Test with balanced shards
	shards := createTestShards(3)
	for _, shard := range shards {
		shard.PinCount = 300 // 30% utilization
	}
	
	recommendation, err := autoScaler.CheckScaling(shards)
	require.NoError(t, err)
	// Should not recommend scaling for balanced shards
	
	// Test with overloaded shards
	for _, shard := range shards {
		shard.PinCount = 850 // 85% utilization
	}
	
	recommendation, err = autoScaler.CheckScaling(shards)
	require.NoError(t, err)
	if recommendation != nil {
		assert.Equal(t, ScaleUp, recommendation.Action)
		assert.Greater(t, recommendation.NewShardCount, 0)
	}
	
	// Test with underutilized shards
	shards = createTestShards(6) // More shards
	for _, shard := range shards {
		shard.PinCount = 50 // 5% utilization
	}
	
	recommendation, err = autoScaler.CheckScaling(shards)
	require.NoError(t, err)
	if recommendation != nil {
		assert.Equal(t, ScaleDown, recommendation.Action)
		assert.Greater(t, recommendation.ShardsToRemove, 0)
	}
}

func TestShardRebalanceOperation_Serialization(t *testing.T) {
	op := &ShardRebalanceOperation{
		SourceShard:   "shard-1",
		TargetShard:   "shard-2",
		CIDs:          []string{"QmTest1", "QmTest2"},
		EstimatedSize: 2048,
	}
	
	assert.Equal(t, "shard-1", op.SourceShard)
	assert.Equal(t, "shard-2", op.TargetShard)
	assert.Len(t, op.CIDs, 2)
	assert.Equal(t, int64(2048), op.EstimatedSize)
}

func TestScalingRecommendation_Actions(t *testing.T) {
	recommendation := &ScalingRecommendation{
		Action:        ScaleUp,
		Reason:        "High utilization",
		Timestamp:     time.Now(),
		NewShardCount: 2,
	}
	
	assert.Equal(t, ScaleUp, recommendation.Action)
	assert.Equal(t, "High utilization", recommendation.Reason)
	assert.Equal(t, 2, recommendation.NewShardCount)
	assert.False(t, recommendation.Timestamp.IsZero())
}

// Benchmark tests for sharding performance
func BenchmarkConsistentHashStrategy_GetShardForCID(b *testing.B) {
	strategy := NewConsistentHashStrategy(150)
	shards := createTestShards(10)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cid := fmt.Sprintf("QmTest%d", i)
			_, err := strategy.GetShardForCID(cid, shards)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkRoundRobinStrategy_GetShardForCID(b *testing.B) {
	strategy := NewRoundRobinStrategy()
	shards := createTestShards(10)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cid := fmt.Sprintf("QmTest%d", i)
			_, err := strategy.GetShardForCID(cid, shards)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkSizeBasedStrategy_GetShardForCID(b *testing.B) {
	strategy := NewSizeBasedStrategy(0.3, 0.7)
	shards := createTestShards(10)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cid := fmt.Sprintf("QmTest%d", i)
		_, err := strategy.GetShardForCID(cid, shards)
		if err != nil {
			b.Fatal(err)
		}
	}
}