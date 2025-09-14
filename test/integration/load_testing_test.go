package integration

import (
	"context"
	"testing"
	"time"
)

func TestLoadTestingFramework(t *testing.T) {
	// Test configuration for trillion-scale pin simulation
	config := &LoadTestConfig{
		TotalPins:         1000000, // 1M for testing (scaled down from 1T)
		ConcurrentWorkers: 100,
		BatchSize:         1000,
		TestDuration:      time.Minute * 5,
		AccessPattern:     AccessPatternRandom,
		HotDataPercentage: 0.2,
		BurstIntensity:    10000,
		BurstInterval:     time.Second * 10,
		MaxLatencyP99:     50 * time.Millisecond,
		MinThroughput:     10000, // 10K ops/sec for test
		MaxErrorRate:      0.01,
		ZFSPoolName:       "test-pool",
		DatasetPrefix:     "ipfs-test",
		CompressionType:   "lz4",
		RecordSize:        "128K",
	}
	
	framework := NewLoadTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	
	results, err := framework.RunLoadTest(ctx)
	if err != nil {
		t.Fatalf("Load test failed: %v", err)
	}
	
	// Validate results
	if results.Metrics.TotalOperations == 0 {
		t.Error("No operations were executed")
	}
	
	if results.Metrics.ThroughputOpsPerSec < config.MinThroughput {
		t.Errorf("Throughput below threshold: %.0f < %.0f", 
			results.Metrics.ThroughputOpsPerSec, config.MinThroughput)
	}
	
	if len(results.PassedTests) == 0 {
		t.Error("No tests passed")
	}
	
	t.Logf("Load test completed successfully:")
	t.Logf("- Total operations: %d", results.Metrics.TotalOperations)
	t.Logf("- Successful operations: %d", results.Metrics.SuccessfulOps)
	t.Logf("- Failed operations: %d", results.Metrics.FailedOps)
	t.Logf("- Throughput: %.0f ops/sec", results.Metrics.ThroughputOpsPerSec)
	t.Logf("- P99 Latency: %v", results.Metrics.LatencyP99)
	t.Logf("- Passed tests: %d", len(results.PassedTests))
	t.Logf("- Failed tests: %d", len(results.FailedTests))
}

func TestSequentialAccessPattern(t *testing.T) {
	config := &LoadTestConfig{
		TotalPins:         10000,
		ConcurrentWorkers: 10,
		BatchSize:         100,
		TestDuration:      time.Minute,
		AccessPattern:     AccessPatternSequential,
		MaxLatencyP99:     100 * time.Millisecond,
		MinThroughput:     1000,
		MaxErrorRate:      0.05,
	}
	
	framework := NewLoadTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	
	results, err := framework.RunLoadTest(ctx)
	if err != nil {
		t.Fatalf("Sequential load test failed: %v", err)
	}
	
	if results.Metrics.TotalOperations < config.TotalPins {
		t.Errorf("Not all operations completed: %d < %d", 
			results.Metrics.TotalOperations, config.TotalPins)
	}
}

func TestBurstAccessPattern(t *testing.T) {
	config := &LoadTestConfig{
		TotalPins:         5000,
		ConcurrentWorkers: 20,
		BatchSize:         50,
		TestDuration:      time.Minute,
		AccessPattern:     AccessPatternBurst,
		BurstIntensity:    1000,
		BurstInterval:     time.Second * 5,
		MaxLatencyP99:     200 * time.Millisecond,
		MinThroughput:     500,
		MaxErrorRate:      0.02,
	}
	
	framework := NewLoadTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	
	results, err := framework.RunLoadTest(ctx)
	if err != nil {
		t.Fatalf("Burst load test failed: %v", err)
	}
	
	// Burst pattern should show higher peak throughput
	if results.PerformanceData == nil {
		t.Error("Performance data not collected")
	}
}

func TestHotColdAccessPattern(t *testing.T) {
	config := &LoadTestConfig{
		TotalPins:         8000,
		ConcurrentWorkers: 15,
		BatchSize:         80,
		TestDuration:      time.Minute,
		AccessPattern:     AccessPatternHotCold,
		HotDataPercentage: 0.2,
		MaxLatencyP99:     75 * time.Millisecond,
		MinThroughput:     800,
		MaxErrorRate:      0.01,
	}
	
	framework := NewLoadTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	
	results, err := framework.RunLoadTest(ctx)
	if err != nil {
		t.Fatalf("Hot/Cold load test failed: %v", err)
	}
	
	// Hot/Cold pattern should show good cache performance
	if results.PerformanceData != nil && results.PerformanceData.ZFSMetrics.ARCHitRatio < 80.0 {
		t.Logf("Warning: ARC hit ratio lower than expected: %.1f%%", 
			results.PerformanceData.ZFSMetrics.ARCHitRatio)
	}
}

func TestZipfianAccessPattern(t *testing.T) {
	config := &LoadTestConfig{
		TotalPins:         6000,
		ConcurrentWorkers: 12,
		BatchSize:         60,
		TestDuration:      time.Minute,
		AccessPattern:     AccessPatternZipfian,
		MaxLatencyP99:     100 * time.Millisecond,
		MinThroughput:     600,
		MaxErrorRate:      0.015,
	}
	
	framework := NewLoadTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()
	
	results, err := framework.RunLoadTest(ctx)
	if err != nil {
		t.Fatalf("Zipfian load test failed: %v", err)
	}
	
	// Zipfian pattern should show realistic access distribution
	if results.Metrics.TotalOperations == 0 {
		t.Error("No operations completed in Zipfian test")
	}
}

func TestPerformanceProfiler(t *testing.T) {
	profiler := NewPerformanceProfiler()
	
	profiler.Start()
	
	// Simulate some work
	time.Sleep(time.Second * 2)
	
	profiler.Stop()
	
	profile := profiler.GetProfile()
	
	if profile.SampleCount == 0 {
		t.Error("No performance samples collected")
	}
	
	if len(profile.TimeSeries) == 0 {
		t.Error("No time series data collected")
	}
	
	if profile.CPUMetrics == nil {
		t.Error("CPU metrics not collected")
	}
	
	if profile.MemoryMetrics == nil {
		t.Error("Memory metrics not collected")
	}
	
	if profile.ZFSMetrics == nil {
		t.Error("ZFS metrics not collected")
	}
	
	t.Logf("Performance profiler collected %d samples over %v", 
		profile.SampleCount, profile.EndTime.Sub(profile.StartTime))
}

func TestResultAnalyzer(t *testing.T) {
	analyzer := NewResultAnalyzer()
	
	// Create mock metrics
	metrics := &LoadTestMetrics{
		TotalOperations:    100000,
		SuccessfulOps:      99500,
		FailedOps:          500,
		ThroughputOpsPerSec: 50000,
		LatencyP50:         5 * time.Millisecond,
		LatencyP95:         25 * time.Millisecond,
		LatencyP99:         45 * time.Millisecond,
		LatencyMax:         100 * time.Millisecond,
		ErrorsByType:       map[string]int64{"timeout": 300, "network": 200},
	}
	
	// Create mock performance data
	perfData := &PerformanceData{
		CPUMetrics: &CPUMetrics{
			UserPercent:   45.0,
			SystemPercent: 25.0,
			IdlePercent:   30.0,
		},
		MemoryMetrics: &MemoryMetrics{
			TotalMB:      32768,
			UsedMB:       16384,
			UsagePercent: 50.0,
		},
		ZFSMetrics: &ZFSMetrics{
			ARCHitRatio:          96.5,
			CompressionRatio:     2.3,
			DeduplicationRatio:   1.8,
			FragmentationPercent: 15.0,
		},
		TimeSeries: []*TimeSeriesPoint{
			{
				Timestamp:        time.Now(),
				CPUPercent:       70.0,
				MemoryPercent:    50.0,
				OperationsPerSec: 50000,
			},
		},
	}
	
	results := analyzer.AnalyzeResults(metrics, perfData)
	
	if len(results.PassedTests) == 0 {
		t.Error("No tests passed in analysis")
	}
	
	if len(results.Recommendations) == 0 {
		t.Error("No recommendations generated")
	}
	
	t.Logf("Analysis results:")
	t.Logf("- Passed tests: %d", len(results.PassedTests))
	t.Logf("- Failed tests: %d", len(results.FailedTests))
	t.Logf("- Recommendations: %d", len(results.Recommendations))
}

func TestWorkloadGenerator(t *testing.T) {
	config := &LoadTestConfig{
		TotalPins:         1000,
		ConcurrentWorkers: 5,
		BatchSize:         10,
		AccessPattern:     AccessPatternRandom,
		HotDataPercentage: 0.2,
	}
	
	generator := &WorkloadGenerator{
		config: config,
	}
	
	// Test CID pre-generation
	err := generator.PreGenerateCIDs(100)
	if err != nil {
		t.Fatalf("Failed to pre-generate CIDs: %v", err)
	}
	
	if len(generator.cidCache) != 100 {
		t.Errorf("Expected 100 CIDs, got %d", len(generator.cidCache))
	}
	
	// Test hot data set initialization
	generator.InitializeHotDataSet()
	
	expectedHotData := int(float64(len(generator.cidCache)) * config.HotDataPercentage)
	if len(generator.hotDataSet) != expectedHotData {
		t.Errorf("Expected %d hot data entries, got %d", expectedHotData, len(generator.hotDataSet))
	}
	
	// Test workload generation
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	
	workloadChan := generator.GenerateWorkload(ctx)
	
	operationCount := 0
	for op := range workloadChan {
		if op == nil {
			break
		}
		operationCount++
		if operationCount >= 10 { // Test first 10 operations
			break
		}
	}
	
	if operationCount == 0 {
		t.Error("No workload operations generated")
	}
	
	t.Logf("Generated %d workload operations", operationCount)
}

func TestZipfianGenerator(t *testing.T) {
	generator := NewZipfianGenerator(1000, 0.99)
	
	// Generate some numbers and check distribution
	counts := make(map[int]int)
	for i := 0; i < 10000; i++ {
		num := generator.Next()
		if num < 0 || num >= 1000 {
			t.Errorf("Generated number out of range: %d", num)
		}
		counts[num]++
	}
	
	// Check that lower numbers are more frequent (Zipfian property)
	if counts[0] <= counts[999] {
		t.Error("Zipfian distribution not working correctly - lower numbers should be more frequent")
	}
	
	t.Logf("Zipfian generator test completed. First element frequency: %d, Last element frequency: %d", 
		counts[0], counts[999])
}

// Benchmark tests for performance validation

func BenchmarkLoadTestingFramework(b *testing.B) {
	config := &LoadTestConfig{
		TotalPins:         1000,
		ConcurrentWorkers: 10,
		BatchSize:         100,
		TestDuration:      time.Second * 30,
		AccessPattern:     AccessPatternRandom,
		MinThroughput:     1000,
		MaxErrorRate:      0.01,
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		framework := NewLoadTestingFramework(config)
		
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*35)
		
		_, err := framework.RunLoadTest(ctx)
		if err != nil {
			b.Fatalf("Benchmark load test failed: %v", err)
		}
		
		cancel()
	}
}

func BenchmarkWorkloadGeneration(b *testing.B) {
	config := &LoadTestConfig{
		TotalPins:         10000,
		AccessPattern:     AccessPatternRandom,
		HotDataPercentage: 0.2,
		BatchSize:         100,
	}
	
	generator := &WorkloadGenerator{
		config: config,
	}
	
	generator.PreGenerateCIDs(1000)
	generator.InitializeHotDataSet()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		
		workloadChan := generator.GenerateWorkload(ctx)
		
		count := 0
		for range workloadChan {
			count++
			if count >= 1000 {
				break
			}
		}
		
		cancel()
	}
}

func BenchmarkPerformanceProfiler(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		profiler := NewPerformanceProfiler()
		profiler.Start()
		
		time.Sleep(time.Millisecond * 100)
		
		profiler.Stop()
		
		profile := profiler.GetProfile()
		if profile.SampleCount == 0 {
			b.Error("No samples collected")
		}
	}
}