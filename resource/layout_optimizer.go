package resource

import (
	"fmt"
	"math"
	"time"
)

// LayoutOptimizer optimizes ZFS pool layout for maximum performance
type LayoutOptimizer struct {
	config *Config
}

// OptimizationAnalysis represents the result of layout analysis
type OptimizationAnalysis struct {
	PoolName        string                 `json:"pool_name"`
	CurrentScore    float64                `json:"current_score"`
	OptimalScore    float64                `json:"optimal_score"`
	ImprovementPct  float64                `json:"improvement_percent"`
	Actions         []*OptimizationAction  `json:"actions"`
	EstimatedTime   time.Duration          `json:"estimated_time"`
	RiskLevel       string                 `json:"risk_level"`
	Recommendations []string               `json:"recommendations"`
}

// OptimizationAction represents a specific optimization action
type OptimizationAction struct {
	Type        string                 `json:"type"`
	Description string                 `json:"description"`
	Priority    int                    `json:"priority"`
	Impact      string                 `json:"impact"`
	Parameters  map[string]interface{} `json:"parameters"`
	RiskLevel   string                 `json:"risk_level"`
}

// PerformanceMetrics holds performance-related metrics for optimization
type PerformanceMetrics struct {
	IOPSRead           int64     `json:"iops_read"`
	IOPSWrite          int64     `json:"iops_write"`
	ThroughputRead     int64     `json:"throughput_read"`
	ThroughputWrite    int64     `json:"throughput_write"`
	LatencyRead        float64   `json:"latency_read"`
	LatencyWrite       float64   `json:"latency_write"`
	QueueDepth         int       `json:"queue_depth"`
	CompressionRatio   float64   `json:"compression_ratio"`
	DeduplicationRatio float64   `json:"deduplication_ratio"`
	FragmentationLevel float64   `json:"fragmentation_level"`
	ARCHitRatio        float64   `json:"arc_hit_ratio"`
	L2ARCHitRatio      float64   `json:"l2arc_hit_ratio"`
	Timestamp          time.Time `json:"timestamp"`
}

// NewLayoutOptimizer creates a new layout optimizer
func NewLayoutOptimizer(config *Config) *LayoutOptimizer {
	return &LayoutOptimizer{
		config: config,
	}
}

// AnalyzePool analyzes a pool and provides optimization recommendations
func (lo *LayoutOptimizer) AnalyzePool(pool *PoolInfo) *OptimizationAnalysis {
	logger.Infof("Analyzing pool %s for optimization opportunities", pool.Name)

	analysis := &OptimizationAnalysis{
		PoolName:        pool.Name,
		Actions:         []*OptimizationAction{},
		Recommendations: []string{},
	}

	// Calculate current performance score
	analysis.CurrentScore = lo.calculatePerformanceScore(pool)

	// Analyze different optimization opportunities
	lo.analyzeRecordSize(pool, analysis)
	lo.analyzeCompression(pool, analysis)
	lo.analyzeFragmentation(pool, analysis)
	lo.analyzeLoadBalancing(pool, analysis)
	lo.analyzeCaching(pool, analysis)

	// Calculate potential optimal score
	analysis.OptimalScore = lo.calculateOptimalScore(pool, analysis.Actions)
	
	if analysis.OptimalScore > analysis.CurrentScore {
		analysis.ImprovementPct = ((analysis.OptimalScore - analysis.CurrentScore) / analysis.CurrentScore) * 100
	}

	// Estimate total optimization time
	analysis.EstimatedTime = lo.estimateOptimizationTime(analysis.Actions)

	// Determine overall risk level
	analysis.RiskLevel = lo.calculateOverallRisk(analysis.Actions)

	// Sort actions by priority
	lo.prioritizeActions(analysis.Actions)

	logger.Infof("Pool %s analysis complete: %.1f%% potential improvement", 
		pool.Name, analysis.ImprovementPct)

	return analysis
}

// calculatePerformanceScore calculates a performance score for the pool
func (lo *LayoutOptimizer) calculatePerformanceScore(pool *PoolInfo) float64 {
	score := 100.0 // Start with perfect score

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	// Penalize high fragmentation
	if pool.Fragmentation > 0.3 {
		score -= (pool.Fragmentation - 0.3) * 100
	}

	// Penalize low utilization (waste of resources)
	utilization := float64(pool.Used) / float64(pool.Size)
	if utilization < 0.2 {
		score -= (0.2 - utilization) * 50
	}

	// Penalize very high utilization (performance impact)
	if utilization > 0.8 {
		score -= (utilization - 0.8) * 200
	}

	// Factor in I/O performance
	if pool.ReadOps > 0 && pool.WriteOps > 0 {
		// Normalize IOPS (assuming 1000 IOPS is baseline)
		iopsScore := math.Min(float64(pool.ReadOps+pool.WriteOps)/1000.0, 2.0) * 10
		score += iopsScore
	}

	// Ensure score is within bounds
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score
}

// analyzeRecordSize analyzes and recommends optimal record size
func (lo *LayoutOptimizer) analyzeRecordSize(pool *PoolInfo, analysis *OptimizationAnalysis) {
	// Get current record size from pool properties
	currentRecordSize := pool.Properties["recordsize"]
	if currentRecordSize == "" {
		currentRecordSize = "128K" // ZFS default
	}

	// Analyze workload patterns to recommend optimal record size
	recommendedSize := lo.getOptimalRecordSize(pool)
	
	if recommendedSize != currentRecordSize {
		action := &OptimizationAction{
			Type:        "adjust_recordsize",
			Description: fmt.Sprintf("Adjust record size from %s to %s for better performance", 
				currentRecordSize, recommendedSize),
			Priority:    2,
			Impact:      "MEDIUM",
			RiskLevel:   "LOW",
			Parameters: map[string]interface{}{
				"current_size":     currentRecordSize,
				"recommended_size": recommendedSize,
			},
		}
		analysis.Actions = append(analysis.Actions, action)
		
		analysis.Recommendations = append(analysis.Recommendations,
			fmt.Sprintf("Consider adjusting record size to %s for workload optimization", recommendedSize))
	}
}

// analyzeCompression analyzes compression settings
func (lo *LayoutOptimizer) analyzeCompression(pool *PoolInfo, analysis *OptimizationAnalysis) {
	currentCompression := pool.Properties["compression"]
	if currentCompression == "" || currentCompression == "off" {
		// Recommend enabling compression
		action := &OptimizationAction{
			Type:        "enable_compression",
			Description: "Enable LZ4 compression to reduce storage usage and improve performance",
			Priority:    1,
			Impact:      "HIGH",
			RiskLevel:   "LOW",
			Parameters: map[string]interface{}{
				"algorithm": "lz4",
			},
		}
		analysis.Actions = append(analysis.Actions, action)
		
		analysis.Recommendations = append(analysis.Recommendations,
			"Enable LZ4 compression for better storage efficiency")
	} else if currentCompression == "gzip" {
		// Recommend switching to LZ4 for better performance
		action := &OptimizationAction{
			Type:        "change_compression",
			Description: "Switch from GZIP to LZ4 compression for better performance",
			Priority:    2,
			Impact:      "MEDIUM",
			RiskLevel:   "LOW",
			Parameters: map[string]interface{}{
				"current_algorithm": "gzip",
				"new_algorithm":     "lz4",
			},
		}
		analysis.Actions = append(analysis.Actions, action)
		
		analysis.Recommendations = append(analysis.Recommendations,
			"Consider switching to LZ4 compression for better CPU efficiency")
	}
}

// analyzeFragmentation analyzes fragmentation levels
func (lo *LayoutOptimizer) analyzeFragmentation(pool *PoolInfo, analysis *OptimizationAnalysis) {
	pool.mu.RLock()
	fragmentation := pool.Fragmentation
	pool.mu.RUnlock()

	if fragmentation > 0.3 {
		priority := 2
		impact := "MEDIUM"
		if fragmentation > 0.5 {
			priority = 1
			impact = "HIGH"
		}

		action := &OptimizationAction{
			Type:        "defragment_pool",
			Description: fmt.Sprintf("Defragment pool (current fragmentation: %.1f%%)", 
				fragmentation*100),
			Priority:    priority,
			Impact:      impact,
			RiskLevel:   "MEDIUM",
			Parameters: map[string]interface{}{
				"fragmentation_level": fragmentation,
				"method":             "scrub_and_rebalance",
			},
		}
		analysis.Actions = append(analysis.Actions, action)
		
		analysis.Recommendations = append(analysis.Recommendations,
			fmt.Sprintf("Pool fragmentation is %.1f%%, consider defragmentation", fragmentation*100))
	}
}

// analyzeLoadBalancing analyzes load distribution across devices
func (lo *LayoutOptimizer) analyzeLoadBalancing(pool *PoolInfo, analysis *OptimizationAnalysis) {
	pool.mu.RLock()
	diskCount := len(pool.Disks)
	pool.mu.RUnlock()

	if diskCount > 1 {
		// Check if load balancing could be improved
		action := &OptimizationAction{
			Type:        "rebalance_data",
			Description: "Rebalance data across pool devices for optimal performance",
			Priority:    3,
			Impact:      "MEDIUM",
			RiskLevel:   "MEDIUM",
			Parameters: map[string]interface{}{
				"disk_count": diskCount,
				"method":     "incremental_rebalance",
			},
		}
		analysis.Actions = append(analysis.Actions, action)
		
		analysis.Recommendations = append(analysis.Recommendations,
			"Consider rebalancing data across devices for better load distribution")
	}
}

// analyzeCaching analyzes ARC and L2ARC configuration
func (lo *LayoutOptimizer) analyzeCaching(pool *PoolInfo, analysis *OptimizationAnalysis) {
	// Check if L2ARC could be beneficial
	pool.mu.RLock()
	size := pool.Size
	pool.mu.RUnlock()

	// For large pools, recommend L2ARC if not already configured
	if size > 10*1024*1024*1024*1024 { // 10TB
		action := &OptimizationAction{
			Type:        "optimize_caching",
			Description: "Configure L2ARC for improved caching performance on large pool",
			Priority:    3,
			Impact:      "MEDIUM",
			RiskLevel:   "LOW",
			Parameters: map[string]interface{}{
				"pool_size": size,
				"cache_type": "l2arc",
			},
		}
		analysis.Actions = append(analysis.Actions, action)
		
		analysis.Recommendations = append(analysis.Recommendations,
			"Consider adding L2ARC devices for improved caching on this large pool")
	}
}

// getOptimalRecordSize determines optimal record size based on workload
func (lo *LayoutOptimizer) getOptimalRecordSize(pool *PoolInfo) string {
	pool.mu.RLock()
	readOps := pool.ReadOps
	writeOps := pool.WriteOps
	pool.mu.RUnlock()

	// Analyze I/O patterns
	if readOps > writeOps*2 {
		// Read-heavy workload, larger record size may be better
		return "1M"
	} else if writeOps > readOps*2 {
		// Write-heavy workload, smaller record size may be better
		return "64K"
	} else {
		// Balanced workload, use default optimized size
		return "128K"
	}
}

// calculateOptimalScore calculates the potential score after optimizations
func (lo *LayoutOptimizer) calculateOptimalScore(pool *PoolInfo, actions []*OptimizationAction) float64 {
	currentScore := lo.calculatePerformanceScore(pool)
	
	// Estimate improvement from each action
	totalImprovement := 0.0
	
	for _, action := range actions {
		switch action.Impact {
		case "HIGH":
			totalImprovement += 15.0
		case "MEDIUM":
			totalImprovement += 8.0
		case "LOW":
			totalImprovement += 3.0
		}
	}
	
	// Apply diminishing returns
	improvement := totalImprovement * (1.0 - math.Exp(-0.1*totalImprovement))
	
	optimalScore := currentScore + improvement
	if optimalScore > 100 {
		optimalScore = 100
	}
	
	return optimalScore
}

// estimateOptimizationTime estimates total time for all optimizations
func (lo *LayoutOptimizer) estimateOptimizationTime(actions []*OptimizationAction) time.Duration {
	totalMinutes := 0
	
	for _, action := range actions {
		switch action.Type {
		case "adjust_recordsize":
			totalMinutes += 5 // Quick property change
		case "enable_compression", "change_compression":
			totalMinutes += 10 // Property change with some I/O
		case "defragment_pool":
			totalMinutes += 120 // Long-running scrub operation
		case "rebalance_data":
			totalMinutes += 180 // Very long-running operation
		case "optimize_caching":
			totalMinutes += 30 // Device addition and configuration
		default:
			totalMinutes += 15 // Default estimate
		}
	}
	
	return time.Duration(totalMinutes) * time.Minute
}

// calculateOverallRisk determines the overall risk level
func (lo *LayoutOptimizer) calculateOverallRisk(actions []*OptimizationAction) string {
	highRiskCount := 0
	mediumRiskCount := 0
	
	for _, action := range actions {
		switch action.RiskLevel {
		case "HIGH":
			highRiskCount++
		case "MEDIUM":
			mediumRiskCount++
		}
	}
	
	if highRiskCount > 0 {
		return "HIGH"
	} else if mediumRiskCount > 1 {
		return "MEDIUM"
	} else {
		return "LOW"
	}
}

// prioritizeActions sorts actions by priority and impact
func (lo *LayoutOptimizer) prioritizeActions(actions []*OptimizationAction) {
	// Sort by priority (lower number = higher priority), then by impact
	for i := 0; i < len(actions)-1; i++ {
		for j := i + 1; j < len(actions); j++ {
			if actions[i].Priority > actions[j].Priority ||
				(actions[i].Priority == actions[j].Priority && 
				 lo.getImpactWeight(actions[i].Impact) < lo.getImpactWeight(actions[j].Impact)) {
				actions[i], actions[j] = actions[j], actions[i]
			}
		}
	}
}

// getImpactWeight returns numeric weight for impact levels
func (lo *LayoutOptimizer) getImpactWeight(impact string) int {
	switch impact {
	case "HIGH":
		return 3
	case "MEDIUM":
		return 2
	case "LOW":
		return 1
	default:
		return 0
	}
}