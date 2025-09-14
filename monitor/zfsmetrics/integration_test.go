package zfsmetrics

import (
	"context"
	"testing"
	"time"
)

// TestZFSMetricsIntegration tests the complete ZFS metrics system integration
func TestZFSMetricsIntegration(t *testing.T) {
	// Create test configuration
	config := &Config{
		PoolName:                 "test-pool",
		Datasets:                 []string{"test-pool/dataset1", "test-pool/dataset2"},
		CollectionInterval:       10 * time.Second,
		MetricTTL:               1 * time.Minute,
		EnableIOPSMonitoring:     true,
		EnableARCStats:          true,
		EnableCompressionStats:   true,
		EnableDeduplicationStats: true,
		EnableFragmentationStats: true,
	}
	
	// Test 1: Create and start metrics collector
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		t.Fatalf("Failed to create metrics collector: %v", err)
	}
	
	collector.Start()
	defer collector.Stop()
	
	// Test 2: Create and start informer
	informer, err := NewZFSInformer(config)
	if err != nil {
		t.Fatalf("Failed to create informer: %v", err)
	}
	
	defer informer.Shutdown(context.Background())
	
	// Test 3: Create dashboard
	dashboardConfig := &DashboardConfig{
		Port:         8082,
		Title:        "Test ZFS Dashboard",
		RefreshRate:  30,
		EnableAlerts: true,
	}
	
	dashboard := NewDashboard(collector, informer, dashboardConfig)
	if dashboard == nil {
		t.Fatal("Failed to create dashboard")
	}
	
	// Test 4: Create performance optimizer
	optimizerConfig := &OptimizerConfig{
		OptimizationInterval: 1 * time.Minute,
		ABTestDuration:      10 * time.Minute,
		ABTestConfidence:    0.95,
		MaxConcurrentTests:  2,
		MaxParameterChange:  0.3,
		RollbackThreshold:   -0.05,
	}
	
	optimizer, err := NewPerformanceOptimizer(optimizerConfig, collector)
	if err != nil {
		t.Fatalf("Failed to create performance optimizer: %v", err)
	}
	
	err = optimizer.Start()
	if err != nil {
		t.Fatalf("Failed to start optimizer: %v", err)
	}
	defer optimizer.Stop()
	
	// Test 5: Create capacity planner
	capacityConfig := &CapacityPlannerConfig{
		PlanningInterval:    1 * time.Minute,
		DataPointInterval:   30 * time.Second,
		WarningThreshold:    80.0,
		CriticalThreshold:   90.0,
		MinHistoryPoints:    5,
		MonitoredPools:      []string{"test-pool"},
	}
	
	planner, err := NewCapacityPlanner(capacityConfig, collector)
	if err != nil {
		t.Fatalf("Failed to create capacity planner: %v", err)
	}
	
	err = planner.Start()
	if err != nil {
		t.Fatalf("Failed to start capacity planner: %v", err)
	}
	defer planner.Stop()
	
	// Test 6: Wait for some data collection
	time.Sleep(2 * time.Second)
	
	// Test 7: Verify metrics collection
	ctx := context.Background()
	metrics := informer.GetMetrics(ctx)
	
	if len(metrics) == 0 {
		t.Error("Expected at least one metric from informer")
	}
	
	// Test 8: Verify ZFS metrics
	zfsMetrics := collector.GetZFSMetrics()
	t.Logf("Collected %d ZFS metrics", len(zfsMetrics))
	
	// Test 9: Verify collection stats
	stats := collector.GetCollectionStats()
	if stats == nil {
		t.Error("Expected collection stats")
	}
	
	// Test 10: Verify optimizer stats
	optimizerStats := optimizer.GetOptimizationStats()
	if optimizerStats == nil {
		t.Error("Expected optimizer stats")
	}
	
	// Test 11: Verify capacity planner stats
	plannerStats := planner.GetPlanningStats()
	if plannerStats == nil {
		t.Error("Expected planner stats")
	}
	
	// Test 12: Test ML model functionality
	mlStats := optimizer.mlModel.GetModelStats()
	if mlStats == nil {
		t.Error("Expected ML model stats")
	}
	
	// Test 13: Test A/B testing functionality
	abStats := optimizer.GetABTestStats()
	if abStats == nil {
		t.Error("Expected A/B test stats")
	}
	
	// Test 14: Test capacity predictions
	predictions := planner.GetCapacityPredictions()
	t.Logf("Generated %d capacity predictions", len(predictions))
	
	// Test 15: Test resource recommendations
	recommendations := planner.GetResourceRecommendations()
	t.Logf("Generated %d resource recommendations", len(recommendations))
	
	// Test 16: Test capacity alerts
	alerts := planner.GetCapacityAlerts()
	t.Logf("Generated %d capacity alerts", len(alerts))
	
	t.Log("ZFS metrics integration test completed successfully")
}

// TestMetricsCollectorPerformance tests the performance of metrics collection
func TestMetricsCollectorPerformance(t *testing.T) {
	config := DefaultConfig()
	config.PoolName = "test-pool"
	
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	
	// Measure collection time
	start := time.Now()
	ctx := context.Background()
	
	for i := 0; i < 100; i++ {
		collector.GetMetrics(ctx)
	}
	
	duration := time.Since(start)
	avgTime := duration / 100
	
	t.Logf("Average metrics collection time: %v", avgTime)
	
	if avgTime > 100*time.Millisecond {
		t.Errorf("Metrics collection too slow: %v > 100ms", avgTime)
	}
}

// TestOptimizerMLModel tests the ML model functionality
func TestOptimizerMLModel(t *testing.T) {
	config := &OptimizerConfig{
		OptimizationInterval: 1 * time.Hour,
		ABTestDuration:      1 * time.Hour,
		ABTestConfidence:    0.95,
		MaxConcurrentTests:  1,
	}
	
	mlModel, err := NewMLModel(config)
	if err != nil {
		t.Fatalf("Failed to create ML model: %v", err)
	}
	
	// Add some training data
	for i := 0; i < 20; i++ {
		example := TrainingExample{
			Features: map[string]float64{
				"arc_hit_ratio":      80.0 + float64(i),
				"compression_ratio":  2.0 + float64(i)*0.1,
				"fragmentation_level": 10.0 - float64(i)*0.2,
			},
			Targets: map[string]float64{
				"performance_score": 100.0 + float64(i)*2,
			},
			Timestamp:    time.Now().Add(-time.Duration(i) * time.Hour),
			WorkloadType: "balanced",
		}
		mlModel.AddTrainingExample(example)
	}
	
	// Train the model
	err = mlModel.Train()
	if err != nil {
		t.Fatalf("Failed to train ML model: %v", err)
	}
	
	// Test prediction
	features := map[string]float64{
		"arc_hit_ratio":      85.0,
		"compression_ratio":  2.5,
		"fragmentation_level": 8.0,
	}
	
	prediction, err := mlModel.Predict(features)
	if err != nil {
		t.Fatalf("Failed to generate prediction: %v", err)
	}
	
	if prediction.Confidence <= 0 {
		t.Error("Expected positive confidence in prediction")
	}
	
	if len(prediction.Parameters) == 0 {
		t.Error("Expected parameter recommendations in prediction")
	}
	
	t.Logf("ML prediction: confidence=%.2f, expected_gain=%.2f, parameters=%d", 
		prediction.Confidence, prediction.ExpectedGain, len(prediction.Parameters))
}

// TestCapacityPlannerPredictions tests capacity planning predictions
func TestCapacityPlannerPredictions(t *testing.T) {
	config := &CapacityPlannerConfig{
		PlanningInterval:  1 * time.Hour,
		DataPointInterval: 15 * time.Minute,
		WarningThreshold:  80.0,
		CriticalThreshold: 90.0,
		MinHistoryPoints:  5,
	}
	
	collector, err := NewZFSMetricsCollector(DefaultConfig())
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	
	planner, err := NewCapacityPlanner(config, collector)
	if err != nil {
		t.Fatalf("Failed to create capacity planner: %v", err)
	}
	
	// Add some historical data points
	baseTime := time.Now().Add(-30 * 24 * time.Hour) // 30 days ago
	baseUsage := int64(1000000000000) // 1TB
	
	for i := 0; i < 30; i++ {
		dataPoint := StorageDataPoint{
			Timestamp:      baseTime.Add(time.Duration(i) * 24 * time.Hour),
			PoolName:       "test-pool",
			UsedSpace:      baseUsage + int64(i)*10000000000, // Growing by 10GB per day
			AvailableSpace: 4000000000000 - (baseUsage + int64(i)*10000000000), // 4TB total
			TotalSpace:     4000000000000,
		}
		
		planner.mu.Lock()
		planner.storageHistory = append(planner.storageHistory, dataPoint)
		planner.mu.Unlock()
	}
	
	// Analyze growth patterns
	planner.analyzeGrowthPatterns()
	
	// Generate predictions
	planner.generateCapacityPredictions()
	
	// Check results
	patterns := planner.GetGrowthPatterns()
	if len(patterns) == 0 {
		t.Error("Expected at least one growth pattern")
	}
	
	predictions := planner.GetCapacityPredictions()
	if len(predictions) == 0 {
		t.Error("Expected at least one capacity prediction")
	}
	
	for _, prediction := range predictions {
		t.Logf("Prediction: %s on %s, usage=%.1f%%, confidence=%.2f", 
			prediction.ResourceName, 
			prediction.PredictionDate.Format("2006-01-02"),
			prediction.UtilizationRate,
			prediction.Confidence)
		
		if prediction.Confidence <= 0 {
			t.Error("Expected positive confidence in prediction")
		}
	}
}

// TestDashboardData tests dashboard data generation
func TestDashboardData(t *testing.T) {
	config := DefaultConfig()
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	
	informer, err := NewZFSInformer(config)
	if err != nil {
		t.Fatalf("Failed to create informer: %v", err)
	}
	
	dashboardConfig := &DashboardConfig{
		Port:         8083,
		Title:        "Test Dashboard",
		RefreshRate:  60,
		EnableAlerts: true,
	}
	
	dashboard := NewDashboard(collector, informer, dashboardConfig)
	
	// Generate dashboard data
	data := dashboard.getDashboardData()
	
	if data.Title != "Test Dashboard" {
		t.Errorf("Expected title 'Test Dashboard', got '%s'", data.Title)
	}
	
	if data.PoolName == "" {
		t.Error("Expected pool name in dashboard data")
	}
	
	if data.CollectionStats == nil {
		t.Error("Expected collection stats in dashboard data")
	}
	
	t.Logf("Dashboard data: title=%s, pool=%s, metrics=%d, alerts=%d", 
		data.Title, data.PoolName, len(data.MetricSummaries), len(data.Alerts))
}

// BenchmarkFullSystem benchmarks the complete system
func BenchmarkFullSystem(b *testing.B) {
	config := DefaultConfig()
	config.CollectionInterval = 1 * time.Second
	
	collector, err := NewZFSMetricsCollector(config)
	if err != nil {
		b.Fatalf("Failed to create collector: %v", err)
	}
	
	optimizerConfig := &OptimizerConfig{
		OptimizationInterval: 1 * time.Hour,
		ABTestDuration:      1 * time.Hour,
		MaxConcurrentTests:  1,
	}
	
	optimizer, err := NewPerformanceOptimizer(optimizerConfig, collector)
	if err != nil {
		b.Fatalf("Failed to create optimizer: %v", err)
	}
	
	capacityConfig := &CapacityPlannerConfig{
		PlanningInterval:  1 * time.Hour,
		DataPointInterval: 1 * time.Minute,
		MinHistoryPoints:  5,
	}
	
	planner, err := NewCapacityPlanner(capacityConfig, collector)
	if err != nil {
		b.Fatalf("Failed to create planner: %v", err)
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate full system operation
		collector.GetMetrics(ctx)
		optimizer.GetOptimizationStats()
		planner.GetPlanningStats()
	}
}