package resource

import (
	"context"
	"testing"
	"time"
)

func TestNewTieredStorageManager(t *testing.T) {
	config := &TieredConfig{
		Tiers: []TierDefinition{
			{
				Name:            "hot",
				Type:            "hot",
				MediaType:       "nvme",
				ZFSPool:         "hot-pool",
				TotalCapacity:   1024 * 1024 * 1024 * 1024, // 1TB
				MaxUtilization:  0.8,
				StorageCost:     0.10, // $0.10/GB/month
				AccessCost:      0.01, // $0.01/GB accessed
				ReadLatency:     1 * time.Millisecond,
				WriteLatency:    2 * time.Millisecond,
				MinAccessFreq:   10,
				MaxAccessFreq:   1000,
				RetentionPeriod: 30 * 24 * time.Hour,
			},
			{
				Name:            "cold",
				Type:            "cold",
				MediaType:       "hdd",
				ZFSPool:         "cold-pool",
				TotalCapacity:   10 * 1024 * 1024 * 1024 * 1024, // 10TB
				MaxUtilization:  0.9,
				StorageCost:     0.02, // $0.02/GB/month
				AccessCost:      0.05, // $0.05/GB accessed
				ReadLatency:     10 * time.Millisecond,
				WriteLatency:    20 * time.Millisecond,
				MinAccessFreq:   0,
				MaxAccessFreq:   1,
				RetentionPeriod: 365 * 24 * time.Hour,
			},
		},
		MigrationInterval:       1 * time.Hour,
		MaxConcurrentMigrations: 2,
		MigrationBandwidth:      100 * 1024 * 1024, // 100MB/s
		AnalysisInterval:        30 * time.Minute,
		AccessWindowSize:        24 * time.Hour,
		PredictionHorizon:       7 * 24 * time.Hour,
		CostOptimizationEnabled: true,
		CostThresholds: CostThresholds{
			MaxMonthlyCost:     1000.0,
			CostPerGBThreshold: 0.05,
			ROIThreshold:       2.0,
		},
		MLModelEnabled:      true,
		ModelUpdateInterval: 1 * time.Hour,
		TrainingDataSize:    1000,
	}

	tsm, err := NewTieredStorageManager(config)
	if err != nil {
		t.Fatalf("Failed to create tiered storage manager: %v", err)
	}

	if tsm == nil {
		t.Fatal("Tiered storage manager is nil")
	}

	if len(tsm.tiers) != 2 {
		t.Errorf("Expected 2 tiers, got %d", len(tsm.tiers))
	}

	// Check that tiers were created correctly
	hotTier, exists := tsm.tiers["hot"]
	if !exists {
		t.Error("Hot tier not found")
	} else if hotTier.Definition.Name != "hot" {
		t.Errorf("Expected hot tier name 'hot', got '%s'", hotTier.Definition.Name)
	}

	coldTier, exists := tsm.tiers["cold"]
	if !exists {
		t.Error("Cold tier not found")
	} else if coldTier.Definition.Name != "cold" {
		t.Errorf("Expected cold tier name 'cold', got '%s'", coldTier.Definition.Name)
	}
}

func TestTieredConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *TieredConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: &TieredConfig{
				Tiers: []TierDefinition{
					{
						Name:           "test",
						TotalCapacity:  1024,
						MaxUtilization: 0.8,
					},
				},
				MigrationInterval:       time.Hour,
				MaxConcurrentMigrations: 1,
				AnalysisInterval:        time.Hour,
				AccessWindowSize:        time.Hour,
			},
			expectError: false,
		},
		{
			name: "no tiers",
			config: &TieredConfig{
				Tiers:                   []TierDefinition{},
				MigrationInterval:       time.Hour,
				MaxConcurrentMigrations: 1,
				AnalysisInterval:        time.Hour,
				AccessWindowSize:        time.Hour,
			},
			expectError: true,
		},
		{
			name: "zero migration interval",
			config: &TieredConfig{
				Tiers: []TierDefinition{
					{
						Name:           "test",
						TotalCapacity:  1024,
						MaxUtilization: 0.8,
					},
				},
				MigrationInterval:       0,
				MaxConcurrentMigrations: 1,
				AnalysisInterval:        time.Hour,
				AccessWindowSize:        time.Hour,
			},
			expectError: true,
		},
		{
			name: "invalid tier capacity",
			config: &TieredConfig{
				Tiers: []TierDefinition{
					{
						Name:           "test",
						TotalCapacity:  0, // Invalid
						MaxUtilization: 0.8,
					},
				},
				MigrationInterval:       time.Hour,
				MaxConcurrentMigrations: 1,
				AnalysisInterval:        time.Hour,
				AccessWindowSize:        time.Hour,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestAddDataItem(t *testing.T) {
	config := &TieredConfig{
		Tiers: []TierDefinition{
			{
				Name:           "hot",
				TotalCapacity:  1024 * 1024 * 1024, // 1GB
				UsedCapacity:   0,
				MaxUtilization: 0.8,
				StorageCost:    0.10,
				AccessCost:     0.01,
			},
		},
		MigrationInterval:       time.Hour,
		MaxConcurrentMigrations: 1,
		AnalysisInterval:        time.Hour,
		AccessWindowSize:        time.Hour,
	}

	tsm, err := NewTieredStorageManager(config)
	if err != nil {
		t.Fatalf("Failed to create tiered storage manager: %v", err)
	}

	ctx := context.Background()
	cid := "QmTest123"
	size := int64(1024 * 1024) // 1MB

	err = tsm.AddDataItem(ctx, cid, size)
	if err != nil {
		t.Errorf("Failed to add data item: %v", err)
	}

	// Check that data item was added
	dataItem, err := tsm.GetDataItemInfo(cid)
	if err != nil {
		t.Errorf("Failed to get data item info: %v", err)
	}

	if dataItem.CID != cid {
		t.Errorf("Expected CID %s, got %s", cid, dataItem.CID)
	}

	if dataItem.Size != size {
		t.Errorf("Expected size %d, got %d", size, dataItem.Size)
	}

	if dataItem.CurrentTier != "hot" {
		t.Errorf("Expected tier 'hot', got '%s'", dataItem.CurrentTier)
	}

	// Check that data item is in the tier
	hotTier := tsm.tiers["hot"]
	hotTier.mu.RLock()
	_, exists := hotTier.DataItems[cid]
	hotTier.mu.RUnlock()

	if !exists {
		t.Error("Data item not found in hot tier")
	}
}

func TestRecordAccess(t *testing.T) {
	config := &TieredConfig{
		Tiers: []TierDefinition{
			{
				Name:           "hot",
				TotalCapacity:  1024 * 1024 * 1024,
				MaxUtilization: 0.8,
			},
		},
		MigrationInterval:       time.Hour,
		MaxConcurrentMigrations: 1,
		AnalysisInterval:        time.Hour,
		AccessWindowSize:        24 * time.Hour,
	}

	tsm, err := NewTieredStorageManager(config)
	if err != nil {
		t.Fatalf("Failed to create tiered storage manager: %v", err)
	}

	ctx := context.Background()
	cid := "QmTest123"
	size := int64(1024)

	// Add data item first
	err = tsm.AddDataItem(ctx, cid, size)
	if err != nil {
		t.Fatalf("Failed to add data item: %v", err)
	}

	// Record access
	err = tsm.RecordAccess(cid, 512)
	if err != nil {
		t.Errorf("Failed to record access: %v", err)
	}

	// Check that access was recorded
	dataItem, err := tsm.GetDataItemInfo(cid)
	if err != nil {
		t.Errorf("Failed to get data item info: %v", err)
	}

	if dataItem.AccessCount != 1 {
		t.Errorf("Expected access count 1, got %d", dataItem.AccessCount)
	}

	// Check access history
	tsm.mu.RLock()
	history, exists := tsm.accessHistory[cid]
	tsm.mu.RUnlock()

	if !exists {
		t.Error("Access history not found")
	} else if len(history.AccessTimes) != 2 { // Initial + recorded access
		t.Errorf("Expected 2 access times, got %d", len(history.AccessTimes))
	}
}

func TestSelectOptimalTierForNewData(t *testing.T) {
	config := &TieredConfig{
		Tiers: []TierDefinition{
			{
				Name:           "hot",
				TotalCapacity:  1000,
				UsedCapacity:   900, // 90% used
				MaxUtilization: 0.8, // 80% max, so no capacity
			},
			{
				Name:           "warm",
				TotalCapacity:  2000,
				UsedCapacity:   1000, // 50% used
				MaxUtilization: 0.8,  // Has capacity
			},
		},
		MigrationInterval:       time.Hour,
		MaxConcurrentMigrations: 1,
		AnalysisInterval:        time.Hour,
		AccessWindowSize:        time.Hour,
	}

	tsm, err := NewTieredStorageManager(config)
	if err != nil {
		t.Fatalf("Failed to create tiered storage manager: %v", err)
	}

	// Should select warm tier since hot is over capacity
	tier := tsm.selectOptimalTierForNewData(100)
	if tier != "warm" {
		t.Errorf("Expected 'warm' tier, got '%s'", tier)
	}

	// Test when no tier has capacity
	tsm.tiers["warm"].Definition.UsedCapacity = 1900 // Now over capacity too
	tier = tsm.selectOptimalTierForNewData(100)
	if tier != "" {
		t.Errorf("Expected empty tier (no capacity), got '%s'", tier)
	}
}

func TestAccessPatternUpdate(t *testing.T) {
	config := &TieredConfig{
		Tiers: []TierDefinition{
			{
				Name:           "hot",
				TotalCapacity:  1024 * 1024 * 1024,
				MaxUtilization: 0.8,
			},
		},
		MigrationInterval:       time.Hour,
		MaxConcurrentMigrations: 1,
		AnalysisInterval:        time.Hour,
		AccessWindowSize:        24 * time.Hour,
	}

	tsm, err := NewTieredStorageManager(config)
	if err != nil {
		t.Fatalf("Failed to create tiered storage manager: %v", err)
	}

	ctx := context.Background()
	cid := "QmTest123"

	// Add data item
	err = tsm.AddDataItem(ctx, cid, 1024)
	if err != nil {
		t.Fatalf("Failed to add data item: %v", err)
	}

	// Record multiple accesses
	for i := 0; i < 5; i++ {
		err = tsm.RecordAccess(cid, 100)
		if err != nil {
			t.Errorf("Failed to record access %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Small delay between accesses
	}

	// Check that access pattern was updated
	dataItem, err := tsm.GetDataItemInfo(cid)
	if err != nil {
		t.Errorf("Failed to get data item info: %v", err)
	}

	if dataItem.AccessPattern.Frequency <= 0 {
		t.Error("Access frequency should be positive")
	}

	if len(dataItem.AccessPattern.Seasonality) != 24 {
		t.Errorf("Expected 24 seasonality values, got %d", len(dataItem.AccessPattern.Seasonality))
	}
}

func TestGetTierMetrics(t *testing.T) {
	config := &TieredConfig{
		Tiers: []TierDefinition{
			{
				Name:           "hot",
				TotalCapacity:  1024 * 1024 * 1024,
				MaxUtilization: 0.8,
			},
		},
		MigrationInterval:       time.Hour,
		MaxConcurrentMigrations: 1,
		AnalysisInterval:        time.Hour,
		AccessWindowSize:        time.Hour,
	}

	tsm, err := NewTieredStorageManager(config)
	if err != nil {
		t.Fatalf("Failed to create tiered storage manager: %v", err)
	}

	ctx := context.Background()

	// Add some data items
	for i := 0; i < 3; i++ {
		cid := fmt.Sprintf("QmTest%d", i)
		err = tsm.AddDataItem(ctx, cid, 1024)
		if err != nil {
			t.Errorf("Failed to add data item %d: %v", i, err)
		}
	}

	// Update metrics
	tsm.updateTierMetrics()

	// Get metrics
	metrics := tsm.GetTierMetrics()
	hotMetrics, exists := metrics["hot"]
	if !exists {
		t.Error("Hot tier metrics not found")
	}

	if hotMetrics.TotalItems != 3 {
		t.Errorf("Expected 3 items, got %d", hotMetrics.TotalItems)
	}

	if hotMetrics.TotalSize != 3*1024 {
		t.Errorf("Expected total size 3072, got %d", hotMetrics.TotalSize)
	}

	expectedUtilization := float64(3*1024) / float64(1024*1024*1024)
	if hotMetrics.Utilization != expectedUtilization {
		t.Errorf("Expected utilization %f, got %f", expectedUtilization, hotMetrics.Utilization)
	}
}