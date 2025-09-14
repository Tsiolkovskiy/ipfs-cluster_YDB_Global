package integration

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// PerformanceBenchmarkingFramework provides comprehensive performance benchmarking
// capabilities for IPFS Cluster ZFS integration
type PerformanceBenchmarkingFramework struct {
	config           *BenchmarkConfig
	baselineResults  *BenchmarkResults
	benchmarkSuite   *BenchmarkSuite
	regressionTester *RegressionTester
	cicdIntegration  *CICDIntegration
	reportGenerator  *ReportGenerator
	mu               sync.RWMutex
}

// BenchmarkConfig defines configuration for performance benchmarking
type BenchmarkConfig struct {
	// Benchmark Configuration
	BenchmarkName     string        `json:"benchmark_name"`
	TestDuration      time.Duration `json:"test_duration"`
	WarmupDuration    time.Duration `json:"warmup_duration"`
	CooldownDuration  time.Duration `json:"cooldown_duration"`
	Iterations        int           `json:"iterations"`
	
	// Workload Configuration
	WorkloadTypes     []WorkloadType `json:"workload_types"`
	ScaleFactors      []float64      `json:"scale_factors"`
	ConcurrencyLevels []int          `json:"concurrency_levels"`
	DataSizes         []int64        `json:"data_sizes"`
	
	// Performance Targets
	BaselineFile      string                    `json:"baseline_file"`
	PerformanceTargets map[string]float64       `json:"performance_targets"`
	RegressionThresholds map[string]float64     `json:"regression_thresholds"`
	
	// System Configuration
	ZFSConfiguration  *ZFSBenchmarkConfig      `json:"zfs_configuration"`
	ClusterConfiguration *ClusterBenchmarkConfig `json:"cluster_configuration"`
	
	// Reporting Configuration
	OutputFormats     []string      `json:"output_formats"`
	ReportDirectory   string        `json:"report_directory"`
	ContinuousMonitoring bool       `json:"continuous_monitoring"`
}

// WorkloadType defines different types of benchmark workloads
type WorkloadType int

const (
	WorkloadTypePin WorkloadType = iota
	WorkloadTypeUnpin
	WorkloadTypeQuery
	WorkloadTypeBulkPin
	WorkloadTypeMixed
	WorkloadTypeSequential
	WorkloadTypeRandom
	WorkloadTypeBurst
)

// ZFSBenchmarkConfig defines ZFS-specific benchmark configuration
type ZFSBenchmarkConfig struct {
	PoolConfigurations []ZFSPoolConfig `json:"pool_configurations"`
	CompressionTypes   []string        `json:"compression_types"`
	RecordSizes        []string        `json:"record_sizes"`
	DeduplicationModes []bool          `json:"deduplication_modes"`
}

// ZFSPoolConfig defines a ZFS pool configuration for benchmarking
type ZFSPoolConfig struct {
	Name        string   `json:"name"`
	VDevType    string   `json:"vdev_type"`    // mirror, raidz1, raidz2, raidz3
	DeviceCount int      `json:"device_count"`
	DeviceSize  string   `json:"device_size"`
	L2ARCSize   string   `json:"l2arc_size"`
	ZILDevice   string   `json:"zil_device"`
}

// ClusterBenchmarkConfig defines cluster-specific benchmark configuration
type ClusterBenchmarkConfig struct {
	NodeCount         int      `json:"node_count"`
	ReplicationFactor int      `json:"replication_factor"`
	ConsensusType     string   `json:"consensus_type"` // raft, crdt
	NetworkTopology   string   `json:"network_topology"`
	NodeSpecs         []NodeSpec `json:"node_specs"`
}

// NodeSpec defines specifications for a cluster node
type NodeSpec struct {
	CPUCores   int    `json:"cpu_cores"`
	MemoryGB   int    `json:"memory_gb"`
	StorageGB  int    `json:"storage_gb"`
	NetworkGbps int   `json:"network_gbps"`
}

// BenchmarkSuite manages a collection of performance benchmarks
type BenchmarkSuite struct {
	benchmarks map[string]*Benchmark
	config     *BenchmarkConfig
	mu         sync.RWMutex
}

// Benchmark represents a single performance benchmark
type Benchmark struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	WorkloadType WorkloadType          `json:"workload_type"`
	Config      map[string]interface{} `json:"config"`
	Runner      BenchmarkRunner        `json:"-"`
}

// BenchmarkRunner defines the interface for running benchmarks
type BenchmarkRunner interface {
	Setup(ctx context.Context, config map[string]interface{}) error
	Run(ctx context.Context) (*BenchmarkResult, error)
	Cleanup(ctx context.Context) error
	GetMetrics() map[string]float64
}

// BenchmarkResult contains the results of a single benchmark run
type BenchmarkResult struct {
	BenchmarkName     string            `json:"benchmark_name"`
	WorkloadType      WorkloadType      `json:"workload_type"`
	StartTime         time.Time         `json:"start_time"`
	EndTime           time.Time         `json:"end_time"`
	Duration          time.Duration     `json:"duration"`
	
	// Performance Metrics
	Throughput        float64           `json:"throughput"`        // ops/sec
	Latency           LatencyMetrics    `json:"latency"`
	ResourceUsage     ResourceMetrics   `json:"resource_usage"`
	ZFSMetrics        ZFSPerformanceMetrics `json:"zfs_metrics"`
	
	// Statistical Data
	SampleCount       int64             `json:"sample_count"`
	ErrorCount        int64             `json:"error_count"`
	ErrorRate         float64           `json:"error_rate"`
	
	// Configuration
	Configuration     map[string]interface{} `json:"configuration"`
	
	// Raw Data
	RawMetrics        []MetricSample    `json:"raw_metrics,omitempty"`
}

// LatencyMetrics contains detailed latency statistics
type LatencyMetrics struct {
	Mean      time.Duration `json:"mean"`
	Median    time.Duration `json:"median"`
	P90       time.Duration `json:"p90"`
	P95       time.Duration `json:"p95"`
	P99       time.Duration `json:"p99"`
	P999      time.Duration `json:"p999"`
	Min       time.Duration `json:"min"`
	Max       time.Duration `json:"max"`
	StdDev    time.Duration `json:"std_dev"`
}

// ResourceMetrics contains system resource utilization metrics
type ResourceMetrics struct {
	CPUUsagePercent    float64 `json:"cpu_usage_percent"`
	MemoryUsagePercent float64 `json:"memory_usage_percent"`
	DiskIOPS           int64   `json:"disk_iops"`
	DiskBandwidthMBps  float64 `json:"disk_bandwidth_mbps"`
	NetworkBandwidthMBps float64 `json:"network_bandwidth_mbps"`
	GoroutineCount     int     `json:"goroutine_count"`
	GCPauseTime        time.Duration `json:"gc_pause_time"`
}

// ZFSPerformanceMetrics contains ZFS-specific performance metrics
type ZFSPerformanceMetrics struct {
	ARCHitRatio        float64 `json:"arc_hit_ratio"`
	L2ARCHitRatio      float64 `json:"l2arc_hit_ratio"`
	CompressionRatio   float64 `json:"compression_ratio"`
	DeduplicationRatio float64 `json:"deduplication_ratio"`
	FragmentationPercent float64 `json:"fragmentation_percent"`
	PoolCapacityUsed   float64 `json:"pool_capacity_used"`
	ReadIOPS           int64   `json:"read_iops"`
	WriteIOPS          int64   `json:"write_iops"`
	ReadBandwidth      int64   `json:"read_bandwidth"`
	WriteBandwidth     int64   `json:"write_bandwidth"`
}

// MetricSample represents a single metric measurement
type MetricSample struct {
	Timestamp time.Time              `json:"timestamp"`
	Metrics   map[string]interface{} `json:"metrics"`
}

// BenchmarkResults contains results from multiple benchmark runs
type BenchmarkResults struct {
	SuiteName     string                        `json:"suite_name"`
	Timestamp     time.Time                     `json:"timestamp"`
	Configuration *BenchmarkConfig              `json:"configuration"`
	Results       map[string]*BenchmarkResult   `json:"results"`
	Summary       *BenchmarkSummary             `json:"summary"`
	Comparisons   []*BenchmarkComparison        `json:"comparisons,omitempty"`
}

// BenchmarkSummary provides an overall summary of benchmark results
type BenchmarkSummary struct {
	TotalBenchmarks   int                    `json:"total_benchmarks"`
	PassedBenchmarks  int                    `json:"passed_benchmarks"`
	FailedBenchmarks  int                    `json:"failed_benchmarks"`
	OverallScore      float64                `json:"overall_score"`
	PerformanceGrade  string                 `json:"performance_grade"`
	KeyMetrics        map[string]float64     `json:"key_metrics"`
	Recommendations   []string               `json:"recommendations"`
}

// BenchmarkComparison compares current results with baseline or previous runs
type BenchmarkComparison struct {
	BenchmarkName     string  `json:"benchmark_name"`
	BaselineValue     float64 `json:"baseline_value"`
	CurrentValue      float64 `json:"current_value"`
	PercentChange     float64 `json:"percent_change"`
	IsRegression      bool    `json:"is_regression"`
	Significance      string  `json:"significance"` // "improvement", "regression", "neutral"
}

// RegressionTester detects performance regressions
type RegressionTester struct {
	config     *BenchmarkConfig
	baseline   *BenchmarkResults
	thresholds map[string]float64
}

// CICDIntegration provides integration with CI/CD pipelines
type CICDIntegration struct {
	config         *BenchmarkConfig
	exitOnFailure  bool
	reportFormats  []string
	webhookURL     string
}

// ReportGenerator generates various types of benchmark reports
type ReportGenerator struct {
	config *BenchmarkConfig
}

// NewPerformanceBenchmarkingFramework creates a new benchmarking framework
func NewPerformanceBenchmarkingFramework(config *BenchmarkConfig) *PerformanceBenchmarkingFramework {
	return &PerformanceBenchmarkingFramework{
		config: config,
		benchmarkSuite: &BenchmarkSuite{
			benchmarks: make(map[string]*Benchmark),
			config:     config,
		},
		regressionTester: &RegressionTester{
			config:     config,
			thresholds: config.RegressionThresholds,
		},
		cicdIntegration: &CICDIntegration{
			config:        config,
			exitOnFailure: true,
			reportFormats: config.OutputFormats,
		},
		reportGenerator: &ReportGenerator{
			config: config,
		},
	}
}

// RunBenchmarkSuite executes the complete benchmark suite
func (pbf *PerformanceBenchmarkingFramework) RunBenchmarkSuite(ctx context.Context) (*BenchmarkResults, error) {
	pbf.mu.Lock()
	defer pbf.mu.Unlock()
	
	// Load baseline results if available
	if err := pbf.loadBaseline(); err != nil {
		fmt.Printf("Warning: Could not load baseline results: %v\n", err)
	}
	
	// Initialize benchmark suite
	if err := pbf.initializeBenchmarkSuite(); err != nil {
		return nil, fmt.Errorf("failed to initialize benchmark suite: %w", err)
	}
	
	// Run all benchmarks
	results, err := pbf.executeBenchmarkSuite(ctx)
	if err != nil {
		return nil, fmt.Errorf("benchmark suite execution failed: %w", err)
	}
	
	// Perform regression analysis
	if pbf.baselineResults != nil {
		pbf.performRegressionAnalysis(results)
	}
	
	// Generate reports
	if err := pbf.generateReports(results); err != nil {
		fmt.Printf("Warning: Report generation failed: %v\n", err)
	}
	
	return results, nil
}

// initializeBenchmarkSuite sets up all benchmarks in the suite
func (pbf *PerformanceBenchmarkingFramework) initializeBenchmarkSuite() error {
	// Pin Operation Benchmark
	pbf.benchmarkSuite.benchmarks["pin_operations"] = &Benchmark{
		Name:         "pin_operations",
		Description:  "Benchmark pin operations performance",
		WorkloadType: WorkloadTypePin,
		Config: map[string]interface{}{
			"operation_count": 100000,
			"concurrency":     100,
			"batch_size":      1000,
		},
		Runner: &PinBenchmarkRunner{},
	}
	
	// Query Operation Benchmark
	pbf.benchmarkSuite.benchmarks["query_operations"] = &Benchmark{
		Name:         "query_operations",
		Description:  "Benchmark query operations performance",
		WorkloadType: WorkloadTypeQuery,
		Config: map[string]interface{}{
			"query_count":  50000,
			"concurrency":  50,
			"cache_ratio":  0.8,
		},
		Runner: &QueryBenchmarkRunner{},
	}
	
	// Bulk Operations Benchmark
	pbf.benchmarkSuite.benchmarks["bulk_operations"] = &Benchmark{
		Name:         "bulk_operations",
		Description:  "Benchmark bulk operations performance",
		WorkloadType: WorkloadTypeBulkPin,
		Config: map[string]interface{}{
			"batch_count":  1000,
			"batch_size":   10000,
			"concurrency":  20,
		},
		Runner: &BulkBenchmarkRunner{},
	}
	
	// Mixed Workload Benchmark
	pbf.benchmarkSuite.benchmarks["mixed_workload"] = &Benchmark{
		Name:         "mixed_workload",
		Description:  "Benchmark mixed workload performance",
		WorkloadType: WorkloadTypeMixed,
		Config: map[string]interface{}{
			"pin_ratio":    0.6,
			"query_ratio":  0.3,
			"unpin_ratio":  0.1,
			"duration":     time.Minute * 5,
			"concurrency":  200,
		},
		Runner: &MixedWorkloadBenchmarkRunner{},
	}
	
	// ZFS Performance Benchmark
	pbf.benchmarkSuite.benchmarks["zfs_performance"] = &Benchmark{
		Name:         "zfs_performance",
		Description:  "Benchmark ZFS-specific performance characteristics",
		WorkloadType: WorkloadTypeSequential,
		Config: map[string]interface{}{
			"io_size":      "1M",
			"io_depth":     32,
			"test_duration": time.Minute * 3,
		},
		Runner: &ZFSBenchmarkRunner{},
	}
	
	// Scalability Benchmark
	pbf.benchmarkSuite.benchmarks["scalability"] = &Benchmark{
		Name:         "scalability",
		Description:  "Benchmark system scalability characteristics",
		WorkloadType: WorkloadTypeRandom,
		Config: map[string]interface{}{
			"scale_factors": []float64{0.1, 0.5, 1.0, 2.0, 5.0},
			"base_load":     10000,
		},
		Runner: &ScalabilityBenchmarkRunner{},
	}
	
	return nil
}

// executeBenchmarkSuite runs all benchmarks in the suite
func (pbf *PerformanceBenchmarkingFramework) executeBenchmarkSuite(ctx context.Context) (*BenchmarkResults, error) {
	results := &BenchmarkResults{
		SuiteName:     pbf.config.BenchmarkName,
		Timestamp:     time.Now(),
		Configuration: pbf.config,
		Results:       make(map[string]*BenchmarkResult),
	}
	
	for name, benchmark := range pbf.benchmarkSuite.benchmarks {
		fmt.Printf("Running benchmark: %s\n", name)
		
		// Warmup phase
		if pbf.config.WarmupDuration > 0 {
			fmt.Printf("  Warming up for %v...\n", pbf.config.WarmupDuration)
			time.Sleep(pbf.config.WarmupDuration)
		}
		
		// Setup benchmark
		if err := benchmark.Runner.Setup(ctx, benchmark.Config); err != nil {
			return nil, fmt.Errorf("benchmark setup failed for %s: %w", name, err)
		}
		
		// Run benchmark multiple iterations
		var bestResult *BenchmarkResult
		var allResults []*BenchmarkResult
		
		for i := 0; i < pbf.config.Iterations; i++ {
			fmt.Printf("  Iteration %d/%d...\n", i+1, pbf.config.Iterations)
			
			result, err := benchmark.Runner.Run(ctx)
			if err != nil {
				fmt.Printf("  Warning: Iteration %d failed: %v\n", i+1, err)
				continue
			}
			
			allResults = append(allResults, result)
			
			// Keep the best result (highest throughput)
			if bestResult == nil || result.Throughput > bestResult.Throughput {
				bestResult = result
			}
		}
		
		// Cleanup benchmark
		if err := benchmark.Runner.Cleanup(ctx); err != nil {
			fmt.Printf("  Warning: Cleanup failed for %s: %v\n", name, err)
		}
		
		if bestResult != nil {
			// Aggregate results from all iterations
			bestResult = pbf.aggregateIterationResults(allResults)
			results.Results[name] = bestResult
		}
		
		// Cooldown phase
		if pbf.config.CooldownDuration > 0 {
			fmt.Printf("  Cooling down for %v...\n", pbf.config.CooldownDuration)
			time.Sleep(pbf.config.CooldownDuration)
		}
		
		fmt.Printf("  Completed: %.0f ops/sec, P99: %v\n", 
			bestResult.Throughput, bestResult.Latency.P99)
	}
	
	// Generate summary
	results.Summary = pbf.generateSummary(results)
	
	return results, nil
}

// aggregateIterationResults combines results from multiple iterations
func (pbf *PerformanceBenchmarkingFramework) aggregateIterationResults(results []*BenchmarkResult) *BenchmarkResult {
	if len(results) == 0 {
		return nil
	}
	
	if len(results) == 1 {
		return results[0]
	}
	
	// Use the result with median throughput
	sort.Slice(results, func(i, j int) bool {
		return results[i].Throughput < results[j].Throughput
	})
	
	medianIndex := len(results) / 2
	return results[medianIndex]
}

// generateSummary creates a summary of all benchmark results
func (pbf *PerformanceBenchmarkingFramework) generateSummary(results *BenchmarkResults) *BenchmarkSummary {
	summary := &BenchmarkSummary{
		TotalBenchmarks:  len(results.Results),
		PassedBenchmarks: 0,
		FailedBenchmarks: 0,
		KeyMetrics:       make(map[string]float64),
		Recommendations:  make([]string, 0),
	}
	
	totalScore := 0.0
	
	for name, result := range results.Results {
		// Check if benchmark passed performance targets
		if pbf.checkPerformanceTargets(name, result) {
			summary.PassedBenchmarks++
		} else {
			summary.FailedBenchmarks++
		}
		
		// Calculate benchmark score (simplified)
		score := pbf.calculateBenchmarkScore(result)
		totalScore += score
		
		// Collect key metrics
		summary.KeyMetrics[name+"_throughput"] = result.Throughput
		summary.KeyMetrics[name+"_p99_latency_ms"] = float64(result.Latency.P99.Nanoseconds()) / 1e6
		summary.KeyMetrics[name+"_error_rate"] = result.ErrorRate
	}
	
	summary.OverallScore = totalScore / float64(len(results.Results))
	summary.PerformanceGrade = pbf.getPerformanceGrade(summary.OverallScore)
	
	// Generate recommendations
	summary.Recommendations = pbf.generateRecommendations(results)
	
	return summary
}

// checkPerformanceTargets verifies if benchmark meets performance targets
func (pbf *PerformanceBenchmarkingFramework) checkPerformanceTargets(name string, result *BenchmarkResult) bool {
	targets := pbf.config.PerformanceTargets
	
	// Check throughput target
	if minThroughput, exists := targets[name+"_min_throughput"]; exists {
		if result.Throughput < minThroughput {
			return false
		}
	}
	
	// Check latency target
	if maxLatency, exists := targets[name+"_max_p99_latency_ms"]; exists {
		latencyMs := float64(result.Latency.P99.Nanoseconds()) / 1e6
		if latencyMs > maxLatency {
			return false
		}
	}
	
	// Check error rate target
	if maxErrorRate, exists := targets[name+"_max_error_rate"]; exists {
		if result.ErrorRate > maxErrorRate {
			return false
		}
	}
	
	return true
}

// calculateBenchmarkScore calculates a score for a benchmark result
func (pbf *PerformanceBenchmarkingFramework) calculateBenchmarkScore(result *BenchmarkResult) float64 {
	score := 100.0
	
	// Penalize high error rates
	score -= result.ErrorRate * 1000 // 10% error rate = 100 point penalty
	
	// Penalize high latency (simplified)
	latencyMs := float64(result.Latency.P99.Nanoseconds()) / 1e6
	if latencyMs > 100 {
		score -= (latencyMs - 100) / 10 // 1 point per 10ms over 100ms
	}
	
	// Penalize high resource usage
	if result.ResourceUsage.CPUUsagePercent > 80 {
		score -= (result.ResourceUsage.CPUUsagePercent - 80) * 2
	}
	
	if result.ResourceUsage.MemoryUsagePercent > 85 {
		score -= (result.ResourceUsage.MemoryUsagePercent - 85) * 3
	}
	
	return math.Max(0, score)
}

// getPerformanceGrade assigns a letter grade based on overall score
func (pbf *PerformanceBenchmarkingFramework) getPerformanceGrade(score float64) string {
	switch {
	case score >= 95:
		return "A+"
	case score >= 90:
		return "A"
	case score >= 85:
		return "A-"
	case score >= 80:
		return "B+"
	case score >= 75:
		return "B"
	case score >= 70:
		return "B-"
	case score >= 65:
		return "C+"
	case score >= 60:
		return "C"
	case score >= 55:
		return "C-"
	default:
		return "F"
	}
}

// generateRecommendations creates performance improvement recommendations
func (pbf *PerformanceBenchmarkingFramework) generateRecommendations(results *BenchmarkResults) []string {
	recommendations := make([]string, 0)
	
	for name, result := range results.Results {
		// High latency recommendations
		if result.Latency.P99 > 100*time.Millisecond {
			recommendations = append(recommendations,
				fmt.Sprintf("Consider optimizing %s for lower latency (current P99: %v)", name, result.Latency.P99))
		}
		
		// Low throughput recommendations
		if result.Throughput < 1000 {
			recommendations = append(recommendations,
				fmt.Sprintf("Consider scaling %s for higher throughput (current: %.0f ops/sec)", name, result.Throughput))
		}
		
		// High error rate recommendations
		if result.ErrorRate > 0.01 {
			recommendations = append(recommendations,
				fmt.Sprintf("Investigate errors in %s (current error rate: %.2f%%)", name, result.ErrorRate*100))
		}
		
		// ZFS-specific recommendations
		if result.ZFSMetrics.ARCHitRatio < 90 {
			recommendations = append(recommendations,
				"Consider increasing ZFS ARC size for better cache performance")
		}
		
		if result.ZFSMetrics.FragmentationPercent > 25 {
			recommendations = append(recommendations,
				"Consider ZFS defragmentation or recordsize optimization")
		}
	}
	
	return recommendations
}

// performRegressionAnalysis compares current results with baseline
func (pbf *PerformanceBenchmarkingFramework) performRegressionAnalysis(results *BenchmarkResults) {
	if pbf.baselineResults == nil {
		return
	}
	
	comparisons := make([]*BenchmarkComparison, 0)
	
	for name, currentResult := range results.Results {
		if baselineResult, exists := pbf.baselineResults.Results[name]; exists {
			comparison := &BenchmarkComparison{
				BenchmarkName: name,
				BaselineValue: baselineResult.Throughput,
				CurrentValue:  currentResult.Throughput,
			}
			
			comparison.PercentChange = (comparison.CurrentValue - comparison.BaselineValue) / comparison.BaselineValue * 100
			
			// Check for regression
			threshold := pbf.regressionTester.thresholds[name+"_throughput"]
			if threshold == 0 {
				threshold = 5.0 // Default 5% threshold
			}
			
			comparison.IsRegression = comparison.PercentChange < -threshold
			
			if comparison.PercentChange > threshold {
				comparison.Significance = "improvement"
			} else if comparison.PercentChange < -threshold {
				comparison.Significance = "regression"
			} else {
				comparison.Significance = "neutral"
			}
			
			comparisons = append(comparisons, comparison)
		}
	}
	
	results.Comparisons = comparisons
}

// loadBaseline loads baseline benchmark results
func (pbf *PerformanceBenchmarkingFramework) loadBaseline() error {
	// In a real implementation, this would load from file
	// For now, we'll simulate baseline results
	pbf.baselineResults = &BenchmarkResults{
		SuiteName: "baseline",
		Timestamp: time.Now().Add(-time.Hour * 24),
		Results: map[string]*BenchmarkResult{
			"pin_operations": {
				BenchmarkName: "pin_operations",
				Throughput:    45000,
				Latency: LatencyMetrics{
					P99: 50 * time.Millisecond,
				},
			},
			"query_operations": {
				BenchmarkName: "query_operations",
				Throughput:    80000,
				Latency: LatencyMetrics{
					P99: 10 * time.Millisecond,
				},
			},
		},
	}
	
	return nil
}

// generateReports creates various benchmark reports
func (pbf *PerformanceBenchmarkingFramework) generateReports(results *BenchmarkResults) error {
	for _, format := range pbf.config.OutputFormats {
		switch format {
		case "json":
			if err := pbf.reportGenerator.GenerateJSONReport(results); err != nil {
				return fmt.Errorf("failed to generate JSON report: %w", err)
			}
		case "html":
			if err := pbf.reportGenerator.GenerateHTMLReport(results); err != nil {
				return fmt.Errorf("failed to generate HTML report: %w", err)
			}
		case "csv":
			if err := pbf.reportGenerator.GenerateCSVReport(results); err != nil {
				return fmt.Errorf("failed to generate CSV report: %w", err)
			}
		}
	}
	
	return nil
}

// String methods for enums

func (wt WorkloadType) String() string {
	switch wt {
	case WorkloadTypePin:
		return "pin"
	case WorkloadTypeUnpin:
		return "unpin"
	case WorkloadTypeQuery:
		return "query"
	case WorkloadTypeBulkPin:
		return "bulk_pin"
	case WorkloadTypeMixed:
		return "mixed"
	case WorkloadTypeSequential:
		return "sequential"
	case WorkloadTypeRandom:
		return "random"
	case WorkloadTypeBurst:
		return "burst"
	default:
		return "unknown"
	}
}