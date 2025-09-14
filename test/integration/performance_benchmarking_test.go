package integration

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPerformanceBenchmarkingFramework(t *testing.T) {
	// Create temporary directory for reports
	tempDir, err := os.MkdirTemp("", "benchmark_reports")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &BenchmarkConfig{
		BenchmarkName:    "IPFS Cluster ZFS Integration Test",
		TestDuration:     time.Minute * 2,
		WarmupDuration:   time.Second * 10,
		CooldownDuration: time.Second * 5,
		Iterations:       3,
		
		WorkloadTypes:     []WorkloadType{WorkloadTypePin, WorkloadTypeQuery, WorkloadTypeMixed},
		ScaleFactors:      []float64{0.5, 1.0, 2.0},
		ConcurrencyLevels: []int{10, 50, 100},
		DataSizes:         []int64{1024, 1024 * 1024, 10 * 1024 * 1024},
		
		PerformanceTargets: map[string]float64{
			"pin_operations_min_throughput":     1000,
			"pin_operations_max_p99_latency_ms": 100,
			"query_operations_min_throughput":   5000,
			"query_operations_max_p99_latency_ms": 50,
		},
		
		RegressionThresholds: map[string]float64{
			"pin_operations_throughput":   10.0, // 10% regression threshold
			"query_operations_throughput": 15.0, // 15% regression threshold
		},
		
		OutputFormats:   []string{"json", "html", "csv"},
		ReportDirectory: tempDir,
		ContinuousMonitoring: false,
	}
	
	framework := NewPerformanceBenchmarkingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	
	results, err := framework.RunBenchmarkSuite(ctx)
	if err != nil {
		t.Fatalf("Benchmark suite failed: %v", err)
	}
	
	// Validate results
	if results.SuiteName != config.BenchmarkName {
		t.Errorf("Expected suite name '%s', got '%s'", config.BenchmarkName, results.SuiteName)
	}
	
	if len(results.Results) == 0 {
		t.Error("No benchmark results generated")
	}
	
	if results.Summary == nil {
		t.Error("No summary generated")
	}
	
	// Check that all expected benchmarks ran
	expectedBenchmarks := []string{
		"pin_operations",
		"query_operations", 
		"bulk_operations",
		"mixed_workload",
		"zfs_performance",
		"scalability",
	}
	
	for _, expected := range expectedBenchmarks {
		if _, exists := results.Results[expected]; !exists {
			t.Errorf("Expected benchmark '%s' not found in results", expected)
		}
	}
	
	// Validate performance metrics
	for name, result := range results.Results {
		if result.Throughput <= 0 {
			t.Errorf("Benchmark '%s' has invalid throughput: %f", name, result.Throughput)
		}
		
		if result.Duration <= 0 {
			t.Errorf("Benchmark '%s' has invalid duration: %v", name, result.Duration)
		}
		
		if result.SampleCount <= 0 {
			t.Errorf("Benchmark '%s' has invalid sample count: %d", name, result.SampleCount)
		}
		
		if result.ErrorRate < 0 || result.ErrorRate > 1 {
			t.Errorf("Benchmark '%s' has invalid error rate: %f", name, result.ErrorRate)
		}
	}
	
	t.Logf("Benchmark suite completed successfully:")
	t.Logf("- Total benchmarks: %d", results.Summary.TotalBenchmarks)
	t.Logf("- Passed benchmarks: %d", results.Summary.PassedBenchmarks)
	t.Logf("- Failed benchmarks: %d", results.Summary.FailedBenchmarks)
	t.Logf("- Overall score: %.1f", results.Summary.OverallScore)
	t.Logf("- Performance grade: %s", results.Summary.PerformanceGrade)
}

func TestPinBenchmarkRunner(t *testing.T) {
	runner := &PinBenchmarkRunner{}
	
	config := map[string]interface{}{
		"operation_count": 1000,
		"concurrency":     10,
		"batch_size":      100,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	err := runner.Setup(ctx, config)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	
	result, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	
	err = runner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	
	// Validate result
	if result.BenchmarkName != "pin_operations" {
		t.Errorf("Expected benchmark name 'pin_operations', got '%s'", result.BenchmarkName)
	}
	
	if result.WorkloadType != WorkloadTypePin {
		t.Errorf("Expected workload type Pin, got %v", result.WorkloadType)
	}
	
	if result.Throughput <= 0 {
		t.Errorf("Invalid throughput: %f", result.Throughput)
	}
	
	if result.SampleCount != 1000 {
		t.Errorf("Expected 1000 samples, got %d", result.SampleCount)
	}
	
	t.Logf("Pin benchmark completed:")
	t.Logf("- Throughput: %.0f ops/sec", result.Throughput)
	t.Logf("- P99 Latency: %v", result.Latency.P99)
	t.Logf("- Error Rate: %.4f%%", result.ErrorRate*100)
}

func TestQueryBenchmarkRunner(t *testing.T) {
	runner := &QueryBenchmarkRunner{}
	
	config := map[string]interface{}{
		"query_count":  5000,
		"concurrency":  20,
		"cache_ratio":  0.8,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	err := runner.Setup(ctx, config)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	
	result, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	
	err = runner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	
	// Validate result
	if result.BenchmarkName != "query_operations" {
		t.Errorf("Expected benchmark name 'query_operations', got '%s'", result.BenchmarkName)
	}
	
	if result.WorkloadType != WorkloadTypeQuery {
		t.Errorf("Expected workload type Query, got %v", result.WorkloadType)
	}
	
	if result.Throughput <= 0 {
		t.Errorf("Invalid throughput: %f", result.Throughput)
	}
	
	// Query operations should generally be faster than pin operations
	if result.Latency.Mean > 50*time.Millisecond {
		t.Errorf("Query latency too high: %v", result.Latency.Mean)
	}
	
	t.Logf("Query benchmark completed:")
	t.Logf("- Throughput: %.0f ops/sec", result.Throughput)
	t.Logf("- Mean Latency: %v", result.Latency.Mean)
	t.Logf("- P99 Latency: %v", result.Latency.P99)
}

func TestBulkBenchmarkRunner(t *testing.T) {
	runner := &BulkBenchmarkRunner{}
	
	config := map[string]interface{}{
		"batch_count":  100,
		"batch_size":   1000,
		"concurrency":  5,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	err := runner.Setup(ctx, config)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	
	result, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	
	err = runner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	
	// Validate result
	if result.BenchmarkName != "bulk_operations" {
		t.Errorf("Expected benchmark name 'bulk_operations', got '%s'", result.BenchmarkName)
	}
	
	if result.WorkloadType != WorkloadTypeBulkPin {
		t.Errorf("Expected workload type BulkPin, got %v", result.WorkloadType)
	}
	
	expectedSamples := int64(100 * 1000) // batch_count * batch_size
	if result.SampleCount != expectedSamples {
		t.Errorf("Expected %d samples, got %d", expectedSamples, result.SampleCount)
	}
	
	t.Logf("Bulk benchmark completed:")
	t.Logf("- Throughput: %.0f ops/sec", result.Throughput)
	t.Logf("- Total operations: %d", result.SampleCount)
}

func TestMixedWorkloadBenchmarkRunner(t *testing.T) {
	runner := &MixedWorkloadBenchmarkRunner{}
	
	config := map[string]interface{}{
		"pin_ratio":    0.6,
		"query_ratio":  0.3,
		"unpin_ratio":  0.1,
		"duration":     time.Second * 30,
		"concurrency":  50,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	err := runner.Setup(ctx, config)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	
	result, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	
	err = runner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	
	// Validate result
	if result.BenchmarkName != "mixed_workload" {
		t.Errorf("Expected benchmark name 'mixed_workload', got '%s'", result.BenchmarkName)
	}
	
	if result.WorkloadType != WorkloadTypeMixed {
		t.Errorf("Expected workload type Mixed, got %v", result.WorkloadType)
	}
	
	// Mixed workload should have generated some operations
	if result.SampleCount == 0 {
		t.Error("No operations generated in mixed workload")
	}
	
	t.Logf("Mixed workload benchmark completed:")
	t.Logf("- Throughput: %.0f ops/sec", result.Throughput)
	t.Logf("- Total operations: %d", result.SampleCount)
	t.Logf("- Duration: %v", result.Duration)
}

func TestZFSBenchmarkRunner(t *testing.T) {
	runner := &ZFSBenchmarkRunner{}
	
	config := map[string]interface{}{
		"io_size":       "1M",
		"io_depth":      32,
		"test_duration": time.Second * 15,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	err := runner.Setup(ctx, config)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	
	result, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	
	err = runner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	
	// Validate ZFS-specific metrics
	if result.ZFSMetrics.ARCHitRatio <= 0 || result.ZFSMetrics.ARCHitRatio > 100 {
		t.Errorf("Invalid ARC hit ratio: %f", result.ZFSMetrics.ARCHitRatio)
	}
	
	if result.ZFSMetrics.CompressionRatio <= 0 {
		t.Errorf("Invalid compression ratio: %f", result.ZFSMetrics.CompressionRatio)
	}
	
	if result.ZFSMetrics.ReadIOPS <= 0 {
		t.Errorf("Invalid read IOPS: %d", result.ZFSMetrics.ReadIOPS)
	}
	
	t.Logf("ZFS benchmark completed:")
	t.Logf("- ARC Hit Ratio: %.1f%%", result.ZFSMetrics.ARCHitRatio)
	t.Logf("- Compression Ratio: %.1fx", result.ZFSMetrics.CompressionRatio)
	t.Logf("- Read IOPS: %d", result.ZFSMetrics.ReadIOPS)
	t.Logf("- Write IOPS: %d", result.ZFSMetrics.WriteIOPS)
}

func TestScalabilityBenchmarkRunner(t *testing.T) {
	runner := &ScalabilityBenchmarkRunner{}
	
	config := map[string]interface{}{
		"scale_factors": []float64{0.1, 0.5, 1.0, 2.0},
		"base_load":     1000,
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	err := runner.Setup(ctx, config)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	
	result, err := runner.Run(ctx)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	
	err = runner.Cleanup(ctx)
	if err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}
	
	// Validate scalability results
	expectedOperations := int64(100 + 500 + 1000 + 2000) // Sum of scaled loads
	if result.SampleCount != expectedOperations {
		t.Errorf("Expected %d operations, got %d", expectedOperations, result.SampleCount)
	}
	
	t.Logf("Scalability benchmark completed:")
	t.Logf("- Total operations: %d", result.SampleCount)
	t.Logf("- Duration: %v", result.Duration)
}

func TestReportGeneration(t *testing.T) {
	// Create temporary directory for reports
	tempDir, err := os.MkdirTemp("", "test_reports")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	config := &BenchmarkConfig{
		ReportDirectory: tempDir,
		OutputFormats:   []string{"json", "html", "csv"},
	}
	
	generator := &ReportGenerator{config: config}
	
	// Create mock results
	results := &BenchmarkResults{
		SuiteName: "Test Suite",
		Timestamp: time.Now(),
		Configuration: config,
		Results: map[string]*BenchmarkResult{
			"test_benchmark": {
				BenchmarkName: "test_benchmark",
				WorkloadType:  WorkloadTypePin,
				StartTime:     time.Now().Add(-time.Minute),
				EndTime:       time.Now(),
				Duration:      time.Minute,
				Throughput:    5000,
				Latency: LatencyMetrics{
					Mean:   10 * time.Millisecond,
					P99:    50 * time.Millisecond,
					Max:    100 * time.Millisecond,
				},
				ResourceUsage: ResourceMetrics{
					CPUUsagePercent:    65.0,
					MemoryUsagePercent: 70.0,
					DiskIOPS:           5000,
				},
				ZFSMetrics: ZFSPerformanceMetrics{
					ARCHitRatio:      95.0,
					CompressionRatio: 2.5,
				},
				SampleCount: 300000,
				ErrorCount:  150,
				ErrorRate:   0.0005,
			},
		},
		Summary: &BenchmarkSummary{
			TotalBenchmarks:  1,
			PassedBenchmarks: 1,
			FailedBenchmarks: 0,
			OverallScore:     85.0,
			PerformanceGrade: "B+",
			KeyMetrics: map[string]float64{
				"test_benchmark_throughput": 5000,
			},
			Recommendations: []string{
				"Consider optimizing for higher throughput",
			},
		},
	}
	
	// Test JSON report generation
	err = generator.GenerateJSONReport(results)
	if err != nil {
		t.Errorf("JSON report generation failed: %v", err)
	}
	
	// Test HTML report generation
	err = generator.GenerateHTMLReport(results)
	if err != nil {
		t.Errorf("HTML report generation failed: %v", err)
	}
	
	// Test CSV report generation
	err = generator.GenerateCSVReport(results)
	if err != nil {
		t.Errorf("CSV report generation failed: %v", err)
	}
	
	// Test Markdown report generation
	err = generator.GenerateMarkdownReport(results)
	if err != nil {
		t.Errorf("Markdown report generation failed: %v", err)
	}
	
	// Verify files were created
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("Failed to read report directory: %v", err)
	}
	
	expectedFiles := 4 // JSON, HTML, CSV, Markdown
	if len(files) != expectedFiles {
		t.Errorf("Expected %d report files, got %d", expectedFiles, len(files))
	}
	
	t.Logf("Report generation test completed successfully")
	t.Logf("Generated %d report files in %s", len(files), tempDir)
}

func TestWorkloadTypeString(t *testing.T) {
	testCases := []struct {
		workloadType WorkloadType
		expected     string
	}{
		{WorkloadTypePin, "pin"},
		{WorkloadTypeUnpin, "unpin"},
		{WorkloadTypeQuery, "query"},
		{WorkloadTypeBulkPin, "bulk_pin"},
		{WorkloadTypeMixed, "mixed"},
		{WorkloadTypeSequential, "sequential"},
		{WorkloadTypeRandom, "random"},
		{WorkloadTypeBurst, "burst"},
	}
	
	for _, tc := range testCases {
		result := tc.workloadType.String()
		if result != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, result)
		}
	}
}

// Benchmark tests for performance validation

func BenchmarkPinBenchmarkRunner(b *testing.B) {
	runner := &PinBenchmarkRunner{}
	
	config := map[string]interface{}{
		"operation_count": 1000,
		"concurrency":     10,
		"batch_size":      100,
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		runner.Setup(ctx, config)
		_, err := runner.Run(ctx)
		if err != nil {
			b.Fatalf("Benchmark run failed: %v", err)
		}
		runner.Cleanup(ctx)
	}
}

func BenchmarkQueryBenchmarkRunner(b *testing.B) {
	runner := &QueryBenchmarkRunner{}
	
	config := map[string]interface{}{
		"query_count":  1000,
		"concurrency":  20,
		"cache_ratio":  0.8,
	}
	
	ctx := context.Background()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		runner.Setup(ctx, config)
		_, err := runner.Run(ctx)
		if err != nil {
			b.Fatalf("Benchmark run failed: %v", err)
		}
		runner.Cleanup(ctx)
	}
}

func BenchmarkReportGeneration(b *testing.B) {
	tempDir, _ := os.MkdirTemp("", "benchmark_reports")
	defer os.RemoveAll(tempDir)
	
	config := &BenchmarkConfig{
		ReportDirectory: tempDir,
	}
	
	generator := &ReportGenerator{config: config}
	
	results := &BenchmarkResults{
		SuiteName: "Benchmark Suite",
		Timestamp: time.Now(),
		Results: map[string]*BenchmarkResult{
			"test": {
				BenchmarkName: "test",
				Throughput:    1000,
				SampleCount:   10000,
			},
		},
		Summary: &BenchmarkSummary{
			TotalBenchmarks: 1,
			OverallScore:    85.0,
		},
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		err := generator.GenerateJSONReport(results)
		if err != nil {
			b.Fatalf("Report generation failed: %v", err)
		}
	}
}