package resource

import (
	"math"
	"sort"
	"time"
)

// CostOptimizer optimizes storage costs across tiers
type CostOptimizer struct {
	config *TieredConfig
}

// CostAnalysis represents cost analysis results
type CostAnalysis struct {
	TotalMonthlyCost    float64                    `json:"total_monthly_cost"`
	CostByTier          map[string]float64         `json:"cost_by_tier"`
	CostBreakdown       *CostBreakdown             `json:"cost_breakdown"`
	OptimizationSavings float64                    `json:"optimization_savings"`
	Recommendations     []*CostRecommendation      `json:"recommendations"`
	AnalysisTimestamp   time.Time                  `json:"analysis_timestamp"`
}

// CostBreakdown breaks down costs by category
type CostBreakdown struct {
	StorageCosts   float64 `json:"storage_costs"`
	AccessCosts    float64 `json:"access_costs"`
	TransferCosts  float64 `json:"transfer_costs"`
	OperationalCosts float64 `json:"operational_costs"`
}

// CostRecommendation represents a cost optimization recommendation
type CostRecommendation struct {
	Type            string    `json:"type"`
	Description     string    `json:"description"`
	EstimatedSavings float64  `json:"estimated_savings"`
	ImplementationCost float64 `json:"implementation_cost"`
	ROI             float64   `json:"roi"`
	Priority        int       `json:"priority"`
	DataItems       []string  `json:"data_items"`
	TargetTier      string    `json:"target_tier"`
}

// NewCostOptimizer creates a new cost optimizer
func NewCostOptimizer(config *TieredConfig) *CostOptimizer {
	return &CostOptimizer{
		config: config,
	}
}

// OptimizeCosts analyzes and optimizes costs across storage tiers
func (co *CostOptimizer) OptimizeCosts(tiers map[string]*StorageTier, dataMap map[string]*DataItem) *CostAnalysis {
	tieredLogger.Info("Starting cost optimization analysis")

	analysis := &CostAnalysis{
		CostByTier:        make(map[string]float64),
		CostBreakdown:     &CostBreakdown{},
		Recommendations:   []*CostRecommendation{},
		AnalysisTimestamp: time.Now(),
	}

	// Calculate current costs
	co.calculateCurrentCosts(tiers, dataMap, analysis)

	// Generate optimization recommendations
	co.generateRecommendations(tiers, dataMap, analysis)

	// Calculate potential savings
	co.calculatePotentialSavings(analysis)

	tieredLogger.Infof("Cost optimization analysis complete: $%.2f potential monthly savings", 
		analysis.OptimizationSavings)

	return analysis
}

// CalculateDataItemCost calculates the monthly cost for a specific data item
func (co *CostOptimizer) CalculateDataItemCost(dataItem *DataItem, tier *StorageTier) float64 {
	dataItem.mu.RLock()
	size := dataItem.Size
	accessCount := dataItem.AccessCount
	dataItem.mu.RUnlock()

	sizeGB := float64(size) / (1024 * 1024 * 1024)
	
	// Storage cost
	storageCost := sizeGB * tier.Definition.StorageCost

	// Access cost (assuming monthly access pattern)
	accessCost := sizeGB * float64(accessCount) * tier.Definition.AccessCost / 30.0 // Daily to monthly

	return storageCost + accessCost
}

// GetCostProjection projects costs for a given time period
func (co *CostOptimizer) GetCostProjection(tiers map[string]*StorageTier, dataMap map[string]*DataItem, months int) map[string]float64 {
	projection := make(map[string]float64)

	for tierName, tier := range tiers {
		monthlyCost := 0.0

		tier.mu.RLock()
		for _, dataItem := range tier.DataItems {
			itemCost := co.CalculateDataItemCost(dataItem, tier)
			monthlyCost += itemCost
		}
		tier.mu.RUnlock()

		projection[tierName] = monthlyCost * float64(months)
	}

	return projection
}

// Private methods

func (co *CostOptimizer) calculateCurrentCosts(tiers map[string]*StorageTier, dataMap map[string]*DataItem, analysis *CostAnalysis) {
	totalCost := 0.0

	for tierName, tier := range tiers {
		tierCost := 0.0
		
		tier.mu.RLock()
		for _, dataItem := range tier.DataItems {
			itemCost := co.CalculateDataItemCost(dataItem, tier)
			tierCost += itemCost
			
			// Update cost breakdown
			dataItem.mu.RLock()
			sizeGB := float64(dataItem.Size) / (1024 * 1024 * 1024)
			accessCount := dataItem.AccessCount
			dataItem.mu.RUnlock()

			analysis.CostBreakdown.StorageCosts += sizeGB * tier.Definition.StorageCost
			analysis.CostBreakdown.AccessCosts += sizeGB * float64(accessCount) * tier.Definition.AccessCost / 30.0
		}
		tier.mu.RUnlock()

		analysis.CostByTier[tierName] = tierCost
		totalCost += tierCost
	}

	analysis.TotalMonthlyCost = totalCost
}

func (co *CostOptimizer) generateRecommendations(tiers map[string]*StorageTier, dataMap map[string]*DataItem, analysis *CostAnalysis) {
	// Analyze each data item for potential cost savings
	for cid, dataItem := range dataMap {
		recommendations := co.analyzeDataItemCostOptimization(cid, dataItem, tiers)
		analysis.Recommendations = append(analysis.Recommendations, recommendations...)
	}

	// Sort recommendations by ROI
	sort.Slice(analysis.Recommendations, func(i, j int) bool {
		return analysis.Recommendations[i].ROI > analysis.Recommendations[j].ROI
	})

	// Add tier consolidation recommendations
	consolidationRecs := co.analyzeTierConsolidation(tiers)
	analysis.Recommendations = append(analysis.Recommendations, consolidationRecs...)

	// Add capacity optimization recommendations
	capacityRecs := co.analyzeCapacityOptimization(tiers)
	analysis.Recommendations = append(analysis.Recommendations, capacityRecs...)
}

func (co *CostOptimizer) analyzeDataItemCostOptimization(cid string, dataItem *DataItem, tiers map[string]*StorageTier) []*CostRecommendation {
	var recommendations []*CostRecommendation

	dataItem.mu.RLock()
	currentTier := dataItem.CurrentTier
	accessPattern := dataItem.AccessPattern
	size := dataItem.Size
	dataItem.mu.RUnlock()

	currentTierDef := tiers[currentTier].Definition
	currentCost := co.CalculateDataItemCost(dataItem, tiers[currentTier])

	// Check each alternative tier
	for tierName, tier := range tiers {
		if tierName == currentTier {
			continue
		}

		// Calculate cost in alternative tier
		alternativeCost := co.calculateAlternativeCost(dataItem, tier)
		
		if alternativeCost < currentCost {
			savings := currentCost - alternativeCost
			
			// Calculate implementation cost (migration cost)
			implementationCost := co.calculateMigrationCost(dataItem, currentTierDef, tier.Definition)
			
			// Calculate ROI
			roi := 0.0
			if implementationCost > 0 {
				roi = (savings * 12) / implementationCost // Annual savings / implementation cost
			}

			// Only recommend if ROI meets threshold and access pattern is suitable
			if roi >= co.config.CostThresholds.ROIThreshold && co.isTierSuitableForAccessPattern(accessPattern, tier.Definition) {
				recommendation := &CostRecommendation{
					Type:               "tier_migration",
					Description:        fmt.Sprintf("Move data item %s from %s to %s tier", cid, currentTier, tierName),
					EstimatedSavings:   savings,
					ImplementationCost: implementationCost,
					ROI:                roi,
					Priority:           co.calculateRecommendationPriority(savings, roi),
					DataItems:          []string{cid},
					TargetTier:         tierName,
				}
				recommendations = append(recommendations, recommendation)
			}
		}
	}

	return recommendations
}

func (co *CostOptimizer) calculateAlternativeCost(dataItem *DataItem, tier *StorageTier) float64 {
	dataItem.mu.RLock()
	size := dataItem.Size
	accessCount := dataItem.AccessCount
	dataItem.mu.RUnlock()

	sizeGB := float64(size) / (1024 * 1024 * 1024)
	
	// Storage cost
	storageCost := sizeGB * tier.Definition.StorageCost

	// Access cost
	accessCost := sizeGB * float64(accessCount) * tier.Definition.AccessCost / 30.0

	return storageCost + accessCost
}

func (co *CostOptimizer) calculateMigrationCost(dataItem *DataItem, sourceTier, targetTier *TierDefinition) float64 {
	dataItem.mu.RLock()
	size := dataItem.Size
	dataItem.mu.RUnlock()

	sizeGB := float64(size) / (1024 * 1024 * 1024)
	
	// Transfer cost
	transferCost := sizeGB * sourceTier.TransferCost
	
	// Operational cost (estimated based on migration complexity)
	operationalCost := sizeGB * 0.01 // $0.01 per GB operational cost
	
	return transferCost + operationalCost
}

func (co *CostOptimizer) isTierSuitableForAccessPattern(pattern *AccessPattern, tierDef *TierDefinition) bool {
	// Check if access frequency is within tier's optimal range
	if pattern.Frequency < tierDef.MinAccessFreq || pattern.Frequency > tierDef.MaxAccessFreq {
		return false
	}

	// Check if data age is appropriate for tier
	if pattern.Recency > tierDef.RetentionPeriod {
		return false
	}

	return true
}

func (co *CostOptimizer) calculateRecommendationPriority(savings, roi float64) int {
	// Higher savings and ROI = higher priority (lower number)
	if savings > 100 && roi > 5.0 {
		return 1 // High priority
	} else if savings > 50 && roi > 2.0 {
		return 2 // Medium priority
	} else {
		return 3 // Low priority
	}
}

func (co *CostOptimizer) analyzeTierConsolidation(tiers map[string]*StorageTier) []*CostRecommendation {
	var recommendations []*CostRecommendation

	// Look for underutilized tiers that could be consolidated
	for tierName, tier := range tiers {
		tier.mu.RLock()
		utilization := tier.Metrics.Utilization
		itemCount := len(tier.DataItems)
		tier.mu.RUnlock()

		if utilization < 0.2 && itemCount > 0 { // Less than 20% utilized
			// Find a suitable target tier for consolidation
			targetTier := co.findConsolidationTarget(tier, tiers)
			if targetTier != "" {
				savings := co.calculateConsolidationSavings(tier, tiers[targetTier])
				
				recommendation := &CostRecommendation{
					Type:             "tier_consolidation",
					Description:      fmt.Sprintf("Consolidate underutilized tier %s into %s", tierName, targetTier),
					EstimatedSavings: savings,
					ImplementationCost: savings * 0.1, // 10% of savings as implementation cost
					ROI:              9.0, // High ROI for consolidation
					Priority:         2,
					TargetTier:       targetTier,
				}
				recommendations = append(recommendations, recommendation)
			}
		}
	}

	return recommendations
}

func (co *CostOptimizer) findConsolidationTarget(sourceTier *StorageTier, tiers map[string]*StorageTier) string {
	sourceTier.mu.RLock()
	sourceSize := sourceTier.Metrics.TotalSize
	sourceTier.mu.RUnlock()

	// Find a tier with similar characteristics and available capacity
	for tierName, tier := range tiers {
		if tierName == sourceTier.Definition.Name {
			continue
		}

		tier.mu.RLock()
		availableCapacity := int64(float64(tier.Definition.TotalCapacity)*tier.Definition.MaxUtilization) - tier.Definition.UsedCapacity
		tier.mu.RUnlock()

		if availableCapacity >= sourceSize {
			// Check if tier characteristics are compatible
			if co.aretiersCompatible(sourceTier.Definition, tier.Definition) {
				return tierName
			}
		}
	}

	return ""
}

func (co *CostOptimizer) aretiersCompatible(tier1, tier2 *TierDefinition) bool {
	// Check if tiers have similar performance characteristics
	latencyDiff := math.Abs(float64(tier1.ReadLatency - tier2.ReadLatency))
	throughputRatio := float64(tier1.Throughput) / float64(tier2.Throughput)

	// Compatible if latency difference is small and throughput is similar
	return latencyDiff < float64(10*time.Millisecond) && throughputRatio > 0.5 && throughputRatio < 2.0
}

func (co *CostOptimizer) calculateConsolidationSavings(sourceTier, targetTier *StorageTier) float64 {
	// Calculate savings from eliminating the source tier
	sourceTier.mu.RLock()
	sourceSize := sourceTier.Metrics.TotalSize
	sourceTier.mu.RUnlock()

	sizeGB := float64(sourceSize) / (1024 * 1024 * 1024)
	
	// Savings from reduced operational overhead
	operationalSavings := sizeGB * 0.05 // $0.05 per GB per month operational savings
	
	// Potential cost difference between tiers
	costDifference := (sourceTier.Definition.StorageCost - targetTier.Definition.StorageCost) * sizeGB
	
	return operationalSavings + math.Max(0, costDifference)
}

func (co *CostOptimizer) analyzeCapacityOptimization(tiers map[string]*StorageTier) []*CostRecommendation {
	var recommendations []*CostRecommendation

	for tierName, tier := range tiers {
		tier.mu.RLock()
		utilization := tier.Metrics.Utilization
		totalCapacity := tier.Definition.TotalCapacity
		tier.mu.RUnlock()

		// Recommend capacity reduction for overprovisioned tiers
		if utilization < 0.3 { // Less than 30% utilized
			optimalCapacity := int64(float64(tier.Definition.UsedCapacity) / 0.7) // Target 70% utilization
			capacityReduction := totalCapacity - optimalCapacity
			
			if capacityReduction > 0 {
				savings := float64(capacityReduction) / (1024*1024*1024) * tier.Definition.StorageCost
				
				recommendation := &CostRecommendation{
					Type:             "capacity_optimization",
					Description:      fmt.Sprintf("Reduce capacity for tier %s by %d GB", tierName, capacityReduction/(1024*1024*1024)),
					EstimatedSavings: savings,
					ImplementationCost: 0, // No implementation cost for capacity reduction
					ROI:              math.Inf(1), // Infinite ROI
					Priority:         1,
				}
				recommendations = append(recommendations, recommendation)
			}
		}
	}

	return recommendations
}

func (co *CostOptimizer) calculatePotentialSavings(analysis *CostAnalysis) {
	totalSavings := 0.0
	
	for _, recommendation := range analysis.Recommendations {
		// Only count high and medium priority recommendations
		if recommendation.Priority <= 2 {
			totalSavings += recommendation.EstimatedSavings
		}
	}
	
	analysis.OptimizationSavings = totalSavings
}