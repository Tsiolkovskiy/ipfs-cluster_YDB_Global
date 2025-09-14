package zfsmetrics

import (
	"context"
	"testing"
	"time"
)

func TestNewZFSMetricsCollector(t *testing.T) {
	config := DefaultConfig()
	config.PoolName = "test-pool"
	
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		t.Fatalf("Failed to create ZFS metrics collector: %v", err)
	}
	
	if collector.poolName != "test-pool" {
		t.Errorf("Expected pool name 'test-pool', got '%s'", collector.poolName)
	}
	
	if collector.config.CollectionInterval != 30*time.Second {
		t.Errorf("Expected collection interval 30s, got %v", collector.config.CollectionInterval)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name:        "valid config",
			config:      DefaultConfig(),
			expectError: false,
		},
		{
			name: "empty pool name",
			config: &Config{
				PoolName:           "",
				CollectionInterval: 30 * time.Second,
				MetricTTL:         5 * time.Minute,
			},
			expectError: true,
		},
		{
			name: "zero collection interval",
			config: &Config{
				PoolName:           "test-pool",
				CollectionInterval: 0,
				MetricTTL:         5 * time.Minute,
			},
			expectError: true,
		},
		{
			name: "TTL shorter than interval",
			config: &Config{
				PoolName:           "test-pool",
				CollectionInterval: 60 * time.Second,
				MetricTTL:         30 * time.Second,
			},
			expectError: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no validation error, got: %v", err)
			}
		})
	}
}

func TestMetricHistory(t *testing.T) {
	history := &MetricHistory{
		MaxSize: 3,
	}
	
	// Add values
	history.Values = []float64{1.0, 2.0, 3.0}
	history.Timestamps = []time.Time{
		time.Now().Add(-2 * time.Minute),
		time.Now().Add(-1 * time.Minute),
		time.Now(),
	}
	
	if len(history.Values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(history.Values))
	}
	
	// Test that adding more values trims the history
	history.Values = append(history.Values, 4.0)
	history.Timestamps = append(history.Timestamps, time.Now())
	
	// Simulate trimming (this would be done in updateMetricHistory)
	if len(history.Values) > history.MaxSize {
		history.Values = history.Values[1:]
		history.Timestamps = history.Timestamps[1:]
	}
	
	if len(history.Values) != 3 {
		t.Errorf("Expected history to be trimmed to 3 values, got %d", len(history.Values))
	}
	
	if history.Values[0] != 2.0 {
		t.Errorf("Expected first value to be 2.0 after trimming, got %f", history.Values[0])
	}
}

func TestZFSMetricCreation(t *testing.T) {
	metric := ZFSMetric{
		PoolName:   "test-pool",
		Dataset:    "test-pool/dataset1",
		MetricType: "compression",
		RawValue:   "2.5x",
		Unit:       "ratio",
	}
	
	metric.Name = "zfs_compression_ratio"
	metric.Value = "2.5"
	metric.Valid = true
	metric.Weight = 2500
	
	if metric.PoolName != "test-pool" {
		t.Errorf("Expected pool name 'test-pool', got '%s'", metric.PoolName)
	}
	
	if metric.MetricType != "compression" {
		t.Errorf("Expected metric type 'compression', got '%s'", metric.MetricType)
	}
	
	if metric.Unit != "ratio" {
		t.Errorf("Expected unit 'ratio', got '%s'", metric.Unit)
	}
}

func TestConfigEnabledMetrics(t *testing.T) {
	config := &Config{
		EnableARCStats:          true,
		EnableCompressionStats:  true,
		EnableDeduplicationStats: false,
		EnableFragmentationStats: false,
		EnableIOPSMonitoring:    true,
	}
	
	enabled := config.GetEnabledMetrics()
	expected := []string{"arc", "compression", "iops"}
	
	if len(enabled) != len(expected) {
		t.Errorf("Expected %d enabled metrics, got %d", len(expected), len(enabled))
	}
	
	for _, expectedMetric := range expected {
		found := false
		for _, enabledMetric := range enabled {
			if enabledMetric == expectedMetric {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected metric '%s' to be enabled", expectedMetric)
		}
	}
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.Datasets = []string{"dataset1", "dataset2"}
	
	clone := original.Clone()
	
	// Modify clone
	clone.PoolName = "different-pool"
	clone.Datasets[0] = "modified-dataset"
	
	// Original should be unchanged
	if original.PoolName == "different-pool" {
		t.Error("Original config was modified when clone was changed")
	}
	
	if original.Datasets[0] == "modified-dataset" {
		t.Error("Original datasets slice was modified when clone was changed")
	}
}

func TestInformerIntegration(t *testing.T) {
	config := DefaultConfig()
	config.PoolName = "test-pool"
	
	informer, err := NewZFSInformer(config)
	if err != nil {
		t.Fatalf("Failed to create ZFS informer: %v", err)
	}
	
	if informer.Name() != "zfs-metrics" {
		t.Errorf("Expected informer name 'zfs-metrics', got '%s'", informer.Name())
	}
	
	// Test getting metrics (will return empty/invalid metrics without real ZFS)
	ctx := context.Background()
	metrics := informer.GetMetrics(ctx)
	
	// Should return at least one metric (even if invalid)
	if len(metrics) == 0 {
		t.Error("Expected at least one metric from informer")
	}
	
	// Test shutdown
	if err := informer.Shutdown(ctx); err != nil {
		t.Errorf("Failed to shutdown informer: %v", err)
	}
}

func TestDashboardCreation(t *testing.T) {
	config := DefaultConfig()
	config.PoolName = "test-pool"
	
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	
	informer, err := NewZFSInformer(config)
	if err != nil {
		t.Fatalf("Failed to create informer: %v", err)
	}
	
	dashboardConfig := &DashboardConfig{
		Port:         8081,
		Title:        "Test Dashboard",
		RefreshRate:  60,
		EnableAlerts: true,
	}
	
	dashboard := NewDashboard(collector, informer, dashboardConfig)
	
	if dashboard.config.Port != 8081 {
		t.Errorf("Expected dashboard port 8081, got %d", dashboard.config.Port)
	}
	
	if dashboard.config.Title != "Test Dashboard" {
		t.Errorf("Expected dashboard title 'Test Dashboard', got '%s'", dashboard.config.Title)
	}
}

// Benchmark tests
func BenchmarkMetricCollection(b *testing.B) {
	config := DefaultConfig()
	config.PoolName = "test-pool"
	
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		b.Fatalf("Failed to create collector: %v", err)
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.GetMetrics(ctx)
	}
}

func BenchmarkMetricHistoryUpdate(b *testing.B) {
	history := &MetricHistory{
		MaxSize: 100,
	}
	
	metric := ZFSMetric{
		MetricType: "test",
		RawValue:   "1.0",
		Unit:       "test",
	}
	metric.Name = "test_metric"
	metric.Value = "1.0"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		history.Values = append(history.Values, float64(i))
		history.Timestamps = append(history.Timestamps, time.Now())
		
		if len(history.Values) > history.MaxSize {
			history.Values = history.Values[1:]
			history.Timestamps = history.Timestamps[1:]
		}
	}
}