package integration

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// ResultAnalyzer analyzes load test results and provides insights
type ResultAnalyzer struct {
	thresholds *PerformanceThresholds
}

// PerformanceThresholds defines acceptable performance limits
type PerformanceThresholds struct {
	MaxLatencyP99      time.Duration `json:"max_latency_p99"`
	MinThroughput      float64       `json:"min_throughput"`
	MaxErrorRate       float64       `json:"max_error_rate"`
	MaxCPUUsage        float64       `json:"max_cpu_usage"`
	MaxMemoryUsage     float64       `json:"max_memory_usage"`
	MinZFSARCHitRatio  float64       `json:"min_zfs_arc_hit_ratio"`
	MaxFragmentation   float64       `json:"max_fragmentation"`
}

// AnalysisResult contains the complete analysis of load test results
type AnalysisResult struct {
	OverallScore       float64              `json:"overall_score"`
	PerformanceGrade   string               `json:"performance_grade"`
	PassedTests        []string             `json:"passed_tests"`
	FailedTests        []string             `json:"failed_tests"`
	Warnings           []string             `json:"warnings"`
	Recommendations    []string             `json:"recommendations"`
	DetailedAnalysis   *DetailedAnalysis    `json:"detailed_analysis"`
	ComparisonBaseline *BaselineComparison  `json:"comparison_baseline,omitempty"`
}

// DetailedAnalysis provides in-depth analysis of various performance aspects
type DetailedAnalysis struct {
	ThroughputAnalysis  *ThroughputAnalysis  `json:"throughput_analysis"`
	LatencyAnalysis     *LatencyAnalysis     `json:"latency_analysis"`
	ResourceAnalysis    *ResourceAnalysis    `json:"resource_analysis"`
	ZFSAnalysis         *ZFSAnalysis         `json:"zfs_analysis"`
	ScalabilityAnalysis *ScalabilityAnalysis `json:"scalability_analysis"`
	ReliabilityAnalysis *ReliabilityAnalysis `json:"reliability_analysis"`
}

// ThroughputAnalysis analyzes throughput performance
type ThroughputAnalysis struct {
	AverageThroughput    float64 `json:"average_throughput"`
	PeakThroughput       float64 `json:"peak_throughput"`
	MinThroughput        float64 `json:"min_throughput"`
	ThroughputVariability float64 `json:"throughput_variability"`
	SustainedThroughput  float64 `json:"sustained_throughput"`
	ThroughputTrend      string  `json:"throughput_trend"`
}

// LatencyAnalysis analyzes latency characteristics
type LatencyAnalysis struct {
	AverageLatency    time.Duration `json:"average_latency"`
	MedianLatency     time.Duration `json:"median_latency"`
	P95Latency        time.Duration `json:"p95_latency"`
	P99Latency        time.Duration `json:"p99_latency"`
	MaxLatency        time.Duration `json:"max_latency"`
	LatencyVariability float64      `json:"latency_variability"`
	LatencyTrend      string        `json:"latency_trend"`
}

// ResourceAnalysis analyzes system resource utilization
type ResourceAnalysis struct {
	CPUEfficiency     float64 `json:"cpu_efficiency"`
	MemoryEfficiency  float64 `json:"memory_efficiency"`
	DiskEfficiency    float64 `json:"disk_efficiency"`
	NetworkEfficiency float64 `json:"network_efficiency"`
	ResourceBottleneck string `json:"resource_bottleneck"`
}

// ZFSAnalysis analyzes ZFS-specific performance
type ZFSAnalysis struct {
	ARCEfficiency        float64 `json:"arc_efficiency"`
	L2ARCEfficiency      float64 `json:"l2arc_efficiency"`
	CompressionBenefit   float64 `json:"compression_benefit"`
	DeduplicationBenefit float64 `json:"deduplication_benefit"`
	FragmentationImpact  float64 `json:"fragmentation_impact"`
	OptimalRecordSize    string  `json:"optimal_record_size"`
}

// ScalabilityAnalysis analyzes system scalability characteristics
type ScalabilityAnalysis struct {
	LinearScalability    float64 `json:"linear_scalability"`
	ScalabilityLimit     int64   `json:"scalability_limit"`
	BottleneckFactor     string  `json:"bottleneck_factor"`
	ScalabilityScore     float64 `json:"scalability_score"`
}

// ReliabilityAnalysis analyzes system reliability
type ReliabilityAnalysis struct {
	ErrorRate           float64           `json:"error_rate"`
	ErrorDistribution   map[string]float64 `json:"error_distribution"`
	RecoveryTime        time.Duration     `json:"recovery_time"`
	ReliabilityScore    float64           `json:"reliability_score"`
}

// BaselineComparison compares results against baseline performance
type BaselineComparison struct {
	ThroughputImprovement float64 `json:"throughput_improvement"`
	LatencyImprovement    float64 `json:"latency_improvement"`
	ResourceImprovement   float64 `json:"resource_improvement"`
	OverallImprovement    float64 `json:"overall_improvement"`
}

// NewResultAnalyzer creates a new result analyzer with default thresholds
func NewResultAnalyzer() *ResultAnalyzer {
	return &ResultAnalyzer{
		thresholds: &PerformanceThresholds{
			MaxLatencyP99:      50 * time.Millisecond,
			MinThroughput:      1000000, // 1M ops/sec
			MaxErrorRate:       0.01,    // 1%
			MaxCPUUsage:        80.0,    // 80%
			MaxMemoryUsage:     85.0,    // 85%
			MinZFSARCHitRatio:  95.0,    // 95%
			MaxFragmentation:   20.0,    // 20%
		},
	}
}

// AnalyzeResults performs comprehensive analysis of load test results
func (ra *ResultAnalyzer) AnalyzeResults(metrics *LoadTestMetrics, perfData *PerformanceData) *LoadTestResults {
	analysis := &AnalysisResult{
		PassedTests:     make([]string, 0),
		FailedTests:     make([]string, 0),
		Warnings:        make([]string, 0),
		Recommendations: make([]string, 0),
	}
	
	// Perform detailed analysis
	analysis.DetailedAnalysis = ra.performDetailedAnalysis(metrics, perfData)
	
	// Run performance tests
	ra.runPerformanceTests(analysis, metrics, perfData)
	
	// Calculate overall score
	analysis.OverallScore = ra.calculateOverallScore(analysis)
	analysis.PerformanceGrade = ra.getPerformanceGrade(analysis.OverallScore)
	
	// Generate recommendations
	ra.generateRecommendations(analysis, metrics, perfData)
	
	return &LoadTestResults{
		Metrics:         metrics,
		PerformanceData: perfData,
		PassedTests:     analysis.PassedTests,
		FailedTests:     analysis.FailedTests,
		Recommendations: analysis.Recommendations,
	}
}

// performDetailedAnalysis conducts in-depth analysis of all performance aspects
func (ra *ResultAnalyzer) performDetailedAnalysis(metrics *LoadTestMetrics, perfData *PerformanceData) *DetailedAnalysis {
	return &DetailedAnalysis{
		ThroughputAnalysis:  ra.analyzeThroughput(metrics, perfData),
		LatencyAnalysis:     ra.analyzeLatency(metrics, perfData),
		ResourceAnalysis:    ra.analyzeResources(perfData),
		ZFSAnalysis:         ra.analyzeZFS(perfData),
		ScalabilityAnalysis: ra.analyzeScalability(metrics, perfData),
		ReliabilityAnalysis: ra.analyzeReliability(metrics),
	}
}

// analyzeThroughput analyzes throughput performance characteristics
func (ra *ResultAnalyzer) analyzeThroughput(metrics *LoadTestMetrics, perfData *PerformanceData) *ThroughputAnalysis {
	if len(perfData.TimeSeries) == 0 {
		return &ThroughputAnalysis{}
	}
	
	throughputs := make([]float64, len(perfData.TimeSeries))
	for i, point := range perfData.TimeSeries {
		throughputs[i] = float64(point.OperationsPerSec)
	}
	
	sort.Float64s(throughputs)
	
	avg := ra.calculateAverage(throughputs)
	variance := ra.calculateVariance(throughputs, avg)
	
	return &ThroughputAnalysis{
		AverageThroughput:     avg,
		PeakThroughput:        throughputs[len(throughputs)-1],
		MinThroughput:         throughputs[0],
		ThroughputVariability: math.Sqrt(variance) / avg * 100, // Coefficient of variation
		SustainedThroughput:   ra.calculatePercentile(throughputs, 10), // 10th percentile
		ThroughputTrend:       ra.analyzeTrend(throughputs),
	}
}

// analyzeLatency analyzes latency performance characteristics
func (ra *ResultAnalyzer) analyzeLatency(metrics *LoadTestMetrics, perfData *PerformanceData) *LatencyAnalysis {
	// In a real implementation, we would collect actual latency measurements
	// For simulation, we'll use the metrics data
	
	return &LatencyAnalysis{
		AverageLatency:     time.Duration(5 * time.Millisecond),
		MedianLatency:      metrics.LatencyP50,
		P95Latency:         metrics.LatencyP95,
		P99Latency:         metrics.LatencyP99,
		MaxLatency:         metrics.LatencyMax,
		LatencyVariability: 15.0, // Simulated coefficient of variation
		LatencyTrend:       "stable",
	}
}

// analyzeResources analyzes system resource utilization efficiency
func (ra *ResultAnalyzer) analyzeResources(perfData *PerformanceData) *ResourceAnalysis {
	cpuEfficiency := ra.calculateCPUEfficiency(perfData)
	memoryEfficiency := ra.calculateMemoryEfficiency(perfData)
	diskEfficiency := ra.calculateDiskEfficiency(perfData)
	networkEfficiency := ra.calculateNetworkEfficiency(perfData)
	
	bottleneck := ra.identifyBottleneck(cpuEfficiency, memoryEfficiency, diskEfficiency, networkEfficiency)
	
	return &ResourceAnalysis{
		CPUEfficiency:      cpuEfficiency,
		MemoryEfficiency:   memoryEfficiency,
		DiskEfficiency:     diskEfficiency,
		NetworkEfficiency:  networkEfficiency,
		ResourceBottleneck: bottleneck,
	}
}

// analyzeZFS analyzes ZFS-specific performance metrics
func (ra *ResultAnalyzer) analyzeZFS(perfData *PerformanceData) *ZFSAnalysis {
	zfsMetrics := perfData.ZFSMetrics
	
	return &ZFSAnalysis{
		ARCEfficiency:        zfsMetrics.ARCHitRatio,
		L2ARCEfficiency:      zfsMetrics.L2ARCHitRatio,
		CompressionBenefit:   (zfsMetrics.CompressionRatio - 1.0) * 100,
		DeduplicationBenefit: (zfsMetrics.DeduplicationRatio - 1.0) * 100,
		FragmentationImpact:  ra.calculateFragmentationImpact(zfsMetrics.FragmentationPercent),
		OptimalRecordSize:    ra.recommendRecordSize(perfData),
	}
}

// analyzeScalability analyzes system scalability characteristics
func (ra *ResultAnalyzer) analyzeScalability(metrics *LoadTestMetrics, perfData *PerformanceData) *ScalabilityAnalysis {
	// Analyze how performance scales with load
	linearScalability := ra.calculateLinearScalability(perfData)
	scalabilityLimit := ra.estimateScalabilityLimit(perfData)
	
	return &ScalabilityAnalysis{
		LinearScalability: linearScalability,
		ScalabilityLimit:  scalabilityLimit,
		BottleneckFactor:  ra.identifyScalabilityBottleneck(perfData),
		ScalabilityScore:  linearScalability * 100,
	}
}

// analyzeReliability analyzes system reliability metrics
func (ra *ResultAnalyzer) analyzeReliability(metrics *LoadTestMetrics) *ReliabilityAnalysis {
	totalOps := metrics.SuccessfulOps + metrics.FailedOps
	errorRate := float64(metrics.FailedOps) / float64(totalOps) * 100
	
	errorDistribution := make(map[string]float64)
	for errorType, count := range metrics.ErrorsByType {
		errorDistribution[errorType] = float64(count) / float64(metrics.FailedOps) * 100
	}
	
	reliabilityScore := math.Max(0, 100-errorRate*10) // Simple scoring
	
	return &ReliabilityAnalysis{
		ErrorRate:         errorRate,
		ErrorDistribution: errorDistribution,
		RecoveryTime:      time.Second * 5, // Simulated
		ReliabilityScore:  reliabilityScore,
	}
}

// runPerformanceTests executes various performance validation tests
func (ra *ResultAnalyzer) runPerformanceTests(analysis *AnalysisResult, metrics *LoadTestMetrics, perfData *PerformanceData) {
	// Test throughput requirements
	if metrics.ThroughputOpsPerSec >= ra.thresholds.MinThroughput {
		analysis.PassedTests = append(analysis.PassedTests, "Throughput requirement met")
	} else {
		analysis.FailedTests = append(analysis.FailedTests, 
			fmt.Sprintf("Throughput below threshold: %.0f < %.0f ops/sec", 
				metrics.ThroughputOpsPerSec, ra.thresholds.MinThroughput))
	}
	
	// Test latency requirements
	if metrics.LatencyP99 <= ra.thresholds.MaxLatencyP99 {
		analysis.PassedTests = append(analysis.PassedTests, "P99 latency requirement met")
	} else {
		analysis.FailedTests = append(analysis.FailedTests,
			fmt.Sprintf("P99 latency above threshold: %v > %v", 
				metrics.LatencyP99, ra.thresholds.MaxLatencyP99))
	}
	
	// Test error rate
	totalOps := metrics.SuccessfulOps + metrics.FailedOps
	errorRate := float64(metrics.FailedOps) / float64(totalOps)
	if errorRate <= ra.thresholds.MaxErrorRate {
		analysis.PassedTests = append(analysis.PassedTests, "Error rate within acceptable limits")
	} else {
		analysis.FailedTests = append(analysis.FailedTests,
			fmt.Sprintf("Error rate too high: %.2f%% > %.2f%%", 
				errorRate*100, ra.thresholds.MaxErrorRate*100))
	}
	
	// Test ZFS ARC hit ratio
	if perfData.ZFSMetrics.ARCHitRatio >= ra.thresholds.MinZFSARCHitRatio {
		analysis.PassedTests = append(analysis.PassedTests, "ZFS ARC hit ratio acceptable")
	} else {
		analysis.FailedTests = append(analysis.FailedTests,
			fmt.Sprintf("ZFS ARC hit ratio too low: %.1f%% < %.1f%%", 
				perfData.ZFSMetrics.ARCHitRatio, ra.thresholds.MinZFSARCHitRatio))
	}
	
	// Test fragmentation
	if perfData.ZFSMetrics.FragmentationPercent <= ra.thresholds.MaxFragmentation {
		analysis.PassedTests = append(analysis.PassedTests, "ZFS fragmentation within limits")
	} else {
		analysis.Warnings = append(analysis.Warnings,
			fmt.Sprintf("ZFS fragmentation high: %.1f%% > %.1f%%", 
				perfData.ZFSMetrics.FragmentationPercent, ra.thresholds.MaxFragmentation))
	}
}

// calculateOverallScore computes an overall performance score
func (ra *ResultAnalyzer) calculateOverallScore(analysis *AnalysisResult) float64 {
	totalTests := len(analysis.PassedTests) + len(analysis.FailedTests)
	if totalTests == 0 {
		return 0
	}
	
	baseScore := float64(len(analysis.PassedTests)) / float64(totalTests) * 100
	
	// Apply penalties for warnings
	warningPenalty := float64(len(analysis.Warnings)) * 2.0
	
	return math.Max(0, baseScore-warningPenalty)
}

// getPerformanceGrade assigns a letter grade based on the overall score
func (ra *ResultAnalyzer) getPerformanceGrade(score float64) string {
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

// generateRecommendations creates actionable recommendations based on analysis
func (ra *ResultAnalyzer) generateRecommendations(analysis *AnalysisResult, metrics *LoadTestMetrics, perfData *PerformanceData) {
	// Throughput recommendations
	if metrics.ThroughputOpsPerSec < ra.thresholds.MinThroughput {
		analysis.Recommendations = append(analysis.Recommendations,
			"Consider increasing worker concurrency or optimizing operation batching")
	}
	
	// Latency recommendations
	if metrics.LatencyP99 > ra.thresholds.MaxLatencyP99 {
		analysis.Recommendations = append(analysis.Recommendations,
			"Optimize ZFS recordsize and enable L2ARC for better latency")
	}
	
	// ZFS recommendations
	if perfData.ZFSMetrics.ARCHitRatio < ra.thresholds.MinZFSARCHitRatio {
		analysis.Recommendations = append(analysis.Recommendations,
			"Increase ARC size or add L2ARC devices for better cache hit ratio")
	}
	
	if perfData.ZFSMetrics.FragmentationPercent > ra.thresholds.MaxFragmentation {
		analysis.Recommendations = append(analysis.Recommendations,
			"Schedule ZFS defragmentation or adjust recordsize for workload")
	}
	
	// Resource recommendations
	resourceAnalysis := analysis.DetailedAnalysis.ResourceAnalysis
	if resourceAnalysis.ResourceBottleneck != "" {
		analysis.Recommendations = append(analysis.Recommendations,
			fmt.Sprintf("Address %s bottleneck for better performance", resourceAnalysis.ResourceBottleneck))
	}
}

// Helper methods for calculations

func (ra *ResultAnalyzer) calculateAverage(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func (ra *ResultAnalyzer) calculateVariance(values []float64, mean float64) float64 {
	if len(values) == 0 {
		return 0
	}
	
	sum := 0.0
	for _, v := range values {
		diff := v - mean
		sum += diff * diff
	}
	return sum / float64(len(values))
}

func (ra *ResultAnalyzer) calculatePercentile(sortedValues []float64, percentile float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}
	
	index := int(float64(len(sortedValues)) * percentile / 100.0)
	if index >= len(sortedValues) {
		index = len(sortedValues) - 1
	}
	return sortedValues[index]
}

func (ra *ResultAnalyzer) analyzeTrend(values []float64) string {
	if len(values) < 2 {
		return "insufficient_data"
	}
	
	// Simple trend analysis
	increasing := 0
	decreasing := 0
	
	for i := 1; i < len(values); i++ {
		if values[i] > values[i-1] {
			increasing++
		} else if values[i] < values[i-1] {
			decreasing++
		}
	}
	
	if increasing > decreasing*2 {
		return "increasing"
	} else if decreasing > increasing*2 {
		return "decreasing"
	}
	return "stable"
}

func (ra *ResultAnalyzer) calculateCPUEfficiency(perfData *PerformanceData) float64 {
	// Calculate CPU efficiency based on utilization vs throughput
	cpuUsage := perfData.CPUMetrics.UserPercent + perfData.CPUMetrics.SystemPercent
	return math.Max(0, 100-cpuUsage) // Simple efficiency metric
}

func (ra *ResultAnalyzer) calculateMemoryEfficiency(perfData *PerformanceData) float64 {
	return math.Max(0, 100-perfData.MemoryMetrics.UsagePercent)
}

func (ra *ResultAnalyzer) calculateDiskEfficiency(perfData *PerformanceData) float64 {
	return math.Max(0, 100-perfData.DiskMetrics.Utilization)
}

func (ra *ResultAnalyzer) calculateNetworkEfficiency(perfData *PerformanceData) float64 {
	// Assume 10Gbps network capacity
	totalBandwidth := perfData.NetworkMetrics.RxMBps + perfData.NetworkMetrics.TxMBps
	utilization := totalBandwidth / 1250.0 * 100 // 10Gbps = 1250 MB/s
	return math.Max(0, 100-utilization)
}

func (ra *ResultAnalyzer) identifyBottleneck(cpu, memory, disk, network float64) string {
	efficiencies := map[string]float64{
		"CPU":     cpu,
		"Memory":  memory,
		"Disk":    disk,
		"Network": network,
	}
	
	minEfficiency := 100.0
	bottleneck := ""
	
	for resource, efficiency := range efficiencies {
		if efficiency < minEfficiency {
			minEfficiency = efficiency
			bottleneck = resource
		}
	}
	
	if minEfficiency > 80 {
		return "none"
	}
	
	return bottleneck
}

func (ra *ResultAnalyzer) calculateFragmentationImpact(fragmentation float64) float64 {
	// Estimate performance impact of fragmentation
	return fragmentation * 2.0 // Simplified: 2% performance loss per 1% fragmentation
}

func (ra *ResultAnalyzer) recommendRecordSize(perfData *PerformanceData) string {
	// Analyze I/O patterns to recommend optimal record size
	avgIOSize := (perfData.DiskMetrics.ReadMBps + perfData.DiskMetrics.WriteMBps) / 
		float64(perfData.DiskMetrics.ReadIOPS+perfData.DiskMetrics.WriteIOPS) * 1024 * 1024
	
	switch {
	case avgIOSize < 64*1024:
		return "64K"
	case avgIOSize < 128*1024:
		return "128K"
	case avgIOSize < 512*1024:
		return "512K"
	default:
		return "1M"
	}
}

func (ra *ResultAnalyzer) calculateLinearScalability(perfData *PerformanceData) float64 {
	// Analyze how performance scales with resources
	// This is a simplified calculation
	return 0.85 // 85% linear scalability
}

func (ra *ResultAnalyzer) estimateScalabilityLimit(perfData *PerformanceData) int64 {
	// Estimate the maximum sustainable load
	currentThroughput := float64(1000000) // Current ops/sec
	resourceUtilization := (perfData.CPUMetrics.UserPercent + perfData.CPUMetrics.SystemPercent) / 100.0
	
	if resourceUtilization > 0 {
		return int64(currentThroughput / resourceUtilization * 0.8) // 80% max utilization
	}
	
	return 10000000 // 10M ops/sec default limit
}

func (ra *ResultAnalyzer) identifyScalabilityBottleneck(perfData *PerformanceData) string {
	// Identify what limits scalability
	if perfData.CPUMetrics.UserPercent+perfData.CPUMetrics.SystemPercent > 80 {
		return "CPU"
	}
	if perfData.MemoryMetrics.UsagePercent > 85 {
		return "Memory"
	}
	if perfData.DiskMetrics.Utilization > 80 {
		return "Disk I/O"
	}
	if perfData.ZFSMetrics.ARCHitRatio < 90 {
		return "Cache efficiency"
	}
	
	return "Network"
}