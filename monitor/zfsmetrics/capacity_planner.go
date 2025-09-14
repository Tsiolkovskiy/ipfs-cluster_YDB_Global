// Package zfsmetrics - Capacity Planning
// This module implements predictive capacity planning for ZFS storage systems,
// providing data growth predictions, resource recommendations, and early warning alerts.
package zfsmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var capacityLogger = logging.Logger("zfs-capacity")

// CapacityPlanner provides predictive capacity planning for ZFS systems
type CapacityPlanner struct {
	config          *CapacityPlannerConfig
	collector       *ZFSMetricsCollector
	
	// Historical data tracking
	mu                    sync.RWMutex
	storageHistory        []StorageDataPoint
	growthPatterns        map[string]*GrowthPattern
	capacityPredictions   []CapacityPrediction
	
	// Resource monitoring
	currentCapacity       *CapacityStatus
	resourceRecommendations []ResourceRecommendation
	alerts                []CapacityAlert
	
	// Background processing
	ctx                   context.Context
	cancel                context.CancelFunc
	planningTicker        *time.Ticker
	
	// Prediction models
	linearModel           *LinearGrowthModel
	seasonalModel         *SeasonalGrowthModel
	exponentialModel      *ExponentialGrowthModel
}

// CapacityPlannerConfig holds configuration for capacity planning
type CapacityPlannerConfig struct {
	// Planning intervals
	PlanningInterval      time.Duration `json:"planning_interval"`
	PredictionHorizon     time.Duration `json:"prediction_horizon"`
	
	// Data collection
	HistoryRetention      time.Duration `json:"history_retention"`
	DataPointInterval     time.Duration `json:"data_point_interval"`
	
	// Alert thresholds
	WarningThreshold      float64       `json:"warning_threshold"`      // 80% capacity
	CriticalThreshold     float64       `json:"critical_threshold"`     // 90% capacity
	GrowthRateThreshold   float64       `json:"growth_rate_threshold"`  // 10% per month
	
	// Prediction parameters
	MinHistoryPoints      int           `json:"min_history_points"`
	SeasonalityPeriod     time.Duration `json:"seasonality_period"`
	ConfidenceLevel       float64       `json:"confidence_level"`
	
	// Resource planning
	SafetyMargin          float64       `json:"safety_margin"`          // 20% safety margin
	LeadTime              time.Duration `json:"lead_time"`              // Time to provision new resources
	
	// Pool and dataset monitoring
	MonitoredPools        []string      `json:"monitored_pools"`
	MonitoredDatasets     []string      `json:"monitored_datasets"`
}

// StorageDataPoint represents a point-in-time storage measurement
type StorageDataPoint struct {
	Timestamp         time.Time `json:"timestamp"`
	PoolName          string    `json:"pool_name"`
	Dataset           string    `json:"dataset,omitempty"`
	UsedSpace         int64     `json:"used_space"`         // bytes
	AvailableSpace    int64     `json:"available_space"`    // bytes
	TotalSpace        int64     `json:"total_space"`        // bytes
	CompressionRatio  float64   `json:"compression_ratio"`
	DeduplicationRatio float64  `json:"deduplication_ratio"`
	FragmentationLevel float64  `json:"fragmentation_level"`
	PinCount          int64     `json:"pin_count,omitempty"`
}

// GrowthPattern represents detected growth patterns
type GrowthPattern struct {
	ResourceName      string    `json:"resource_name"`
	PatternType       string    `json:"pattern_type"`       // "linear", "exponential", "seasonal"
	GrowthRate        float64   `json:"growth_rate"`        // bytes per day
	Seasonality       []float64 `json:"seasonality,omitempty"`
	Confidence        float64   `json:"confidence"`
	LastUpdated       time.Time `json:"last_updated"`
	DataPoints        int       `json:"data_points"`
}

// CapacityPrediction represents a future capacity prediction
type CapacityPrediction struct {
	ResourceName      string    `json:"resource_name"`
	PredictionDate    time.Time `json:"prediction_date"`
	PredictedUsage    int64     `json:"predicted_usage"`    // bytes
	PredictedTotal    int64     `json:"predicted_total"`    // bytes
	UtilizationRate   float64   `json:"utilization_rate"`   // percentage
	Confidence        float64   `json:"confidence"`
	Model             string    `json:"model"`              // Which model was used
	UpperBound        int64     `json:"upper_bound"`        // Upper confidence bound
	LowerBound        int64     `json:"lower_bound"`        // Lower confidence bound
}

// CapacityStatus represents current capacity status
type CapacityStatus struct {
	PoolName           string    `json:"pool_name"`
	TotalCapacity      int64     `json:"total_capacity"`
	UsedCapacity       int64     `json:"used_capacity"`
	AvailableCapacity  int64     `json:"available_capacity"`
	UtilizationRate    float64   `json:"utilization_rate"`
	GrowthRate         float64   `json:"growth_rate"`        // bytes per day
	TimeToFull         time.Duration `json:"time_to_full"`
	LastUpdated        time.Time `json:"last_updated"`
}

// ResourceRecommendation represents a capacity planning recommendation
type ResourceRecommendation struct {
	ID                string    `json:"id"`
	ResourceName      string    `json:"resource_name"`
	RecommendationType string   `json:"recommendation_type"` // "add_storage", "optimize", "migrate"
	Priority          string    `json:"priority"`            // "low", "medium", "high", "critical"
	Description       string    `json:"description"`
	EstimatedCost     float64   `json:"estimated_cost,omitempty"`
	Implementation    string    `json:"implementation"`
	Timeline          time.Duration `json:"timeline"`
	ExpectedBenefit   string    `json:"expected_benefit"`
	CreatedAt         time.Time `json:"created_at"`
	Status            string    `json:"status"`              // "pending", "approved", "implemented", "rejected"
}

// CapacityAlert represents a capacity-related alert
type CapacityAlert struct {
	ID               string    `json:"id"`
	AlertType        string    `json:"alert_type"`        // "warning", "critical", "prediction"
	ResourceName     string    `json:"resource_name"`
	Message          string    `json:"message"`
	Threshold        float64   `json:"threshold"`
	CurrentValue     float64   `json:"current_value"`
	PredictedDate    time.Time `json:"predicted_date,omitempty"`
	Severity         string    `json:"severity"`
	CreatedAt        time.Time `json:"created_at"`
	Acknowledged     bool      `json:"acknowledged"`
	AcknowledgedBy   string    `json:"acknowledged_by,omitempty"`
	AcknowledgedAt   time.Time `json:"acknowledged_at,omitempty"`
}

// Growth models
type LinearGrowthModel struct {
	Slope     float64 `json:"slope"`
	Intercept float64 `json:"intercept"`
	RSquared  float64 `json:"r_squared"`
}

type SeasonalGrowthModel struct {
	Trend        float64   `json:"trend"`
	Seasonality  []float64 `json:"seasonality"`
	Period       int       `json:"period"`
	Amplitude    float64   `json:"amplitude"`
}

type ExponentialGrowthModel struct {
	InitialValue float64 `json:"initial_value"`
	GrowthRate   float64 `json:"growth_rate"`
	RSquared     float64 `json:"r_squared"`
}

// NewCapacityPlanner creates a new capacity planner
func NewCapacityPlanner(config *CapacityPlannerConfig, collector *ZFSMetricsCollector) (*CapacityPlanner, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid capacity planner configuration: %w", err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	planner := &CapacityPlanner{
		config:                  config,
		collector:               collector,
		storageHistory:          []StorageDataPoint{},
		growthPatterns:          make(map[string]*GrowthPattern),
		capacityPredictions:     []CapacityPrediction{},
		resourceRecommendations: []ResourceRecommendation{},
		alerts:                  []CapacityAlert{},
		ctx:                     ctx,
		cancel:                  cancel,
		linearModel:             &LinearGrowthModel{},
		seasonalModel:           &SeasonalGrowthModel{},
		exponentialModel:        &ExponentialGrowthModel{},
	}
	
	return planner, nil
}

// Start begins capacity planning operations
func (cp *CapacityPlanner) Start() error {
	interval := cp.config.PlanningInterval
	if interval == 0 {
		interval = 1 * time.Hour // Default to hourly planning
	}
	
	cp.planningTicker = time.NewTicker(interval)
	
	// Start planning loop
	go cp.planningLoop()
	
	// Start data collection loop
	go cp.dataCollectionLoop()
	
	capacityLogger.Infof("Started capacity planner with interval %v", interval)
	return nil
}

// Stop stops capacity planning operations
func (cp *CapacityPlanner) Stop() error {
	if cp.planningTicker != nil {
		cp.planningTicker.Stop()
	}
	cp.cancel()
	
	capacityLogger.Info("Stopped capacity planner")
	return nil
}

// planningLoop runs the main capacity planning loop
func (cp *CapacityPlanner) planningLoop() {
	for {
		select {
		case <-cp.planningTicker.C:
			cp.performCapacityPlanning()
		case <-cp.ctx.Done():
			return
		}
	}
}

// dataCollectionLoop collects storage data points
func (cp *CapacityPlanner) dataCollectionLoop() {
	interval := cp.config.DataPointInterval
	if interval == 0 {
		interval = 15 * time.Minute // Default to 15 minutes
	}
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			cp.collectStorageData()
		case <-cp.ctx.Done():
			return
		}
	}
}

// performCapacityPlanning performs a complete capacity planning cycle
func (cp *CapacityPlanner) performCapacityPlanning() {
	start := time.Now()
	
	capacityLogger.Info("Starting capacity planning cycle")
	
	// Update current capacity status
	cp.updateCapacityStatus()
	
	// Analyze growth patterns
	cp.analyzeGrowthPatterns()
	
	// Generate predictions
	cp.generateCapacityPredictions()
	
	// Check for alerts
	cp.checkCapacityAlerts()
	
	// Generate recommendations
	cp.generateResourceRecommendations()
	
	// Clean up old data
	cp.cleanupHistoricalData()
	
	duration := time.Since(start)
	capacityLogger.Infof("Completed capacity planning cycle in %v", duration)
}

// collectStorageData collects current storage data points
func (cp *CapacityPlanner) collectStorageData() {
	pools := cp.config.MonitoredPools
	if len(pools) == 0 {
		// Auto-discover pools
		discoveredPools, err := cp.discoverZFSPools()
		if err != nil {
			capacityLogger.Warnf("Failed to discover ZFS pools: %s", err)
			return
		}
		pools = discoveredPools
	}
	
	for _, pool := range pools {
		dataPoint, err := cp.collectPoolData(pool)
		if err != nil {
			capacityLogger.Warnf("Failed to collect data for pool %s: %s", pool, err)
			continue
		}
		
		cp.mu.Lock()
		cp.storageHistory = append(cp.storageHistory, dataPoint)
		cp.mu.Unlock()
	}
	
	// Collect dataset-specific data if configured
	for _, dataset := range cp.config.MonitoredDatasets {
		dataPoint, err := cp.collectDatasetData(dataset)
		if err != nil {
			capacityLogger.Warnf("Failed to collect data for dataset %s: %s", dataset, err)
			continue
		}
		
		cp.mu.Lock()
		cp.storageHistory = append(cp.storageHistory, dataPoint)
		cp.mu.Unlock()
	}
}

// collectPoolData collects storage data for a ZFS pool
func (cp *CapacityPlanner) collectPoolData(poolName string) (StorageDataPoint, error) {
	// Get pool statistics
	cmd := exec.Command("zpool", "list", "-H", "-p", poolName)
	output, err := cmd.Output()
	if err != nil {
		return StorageDataPoint{}, fmt.Errorf("failed to get pool stats: %w", err)
	}
	
	fields := strings.Fields(string(output))
	if len(fields) < 4 {
		return StorageDataPoint{}, fmt.Errorf("unexpected zpool output format")
	}
	
	totalSpace, _ := strconv.ParseInt(fields[1], 10, 64)
	usedSpace, _ := strconv.ParseInt(fields[2], 10, 64)
	availableSpace, _ := strconv.ParseInt(fields[3], 10, 64)
	
	// Get compression and deduplication ratios
	compressionRatio := cp.getPoolCompressionRatio(poolName)
	deduplicationRatio := cp.getPoolDeduplicationRatio(poolName)
	fragmentationLevel := cp.getPoolFragmentationLevel(poolName)
	
	return StorageDataPoint{
		Timestamp:          time.Now(),
		PoolName:           poolName,
		UsedSpace:          usedSpace,
		AvailableSpace:     availableSpace,
		TotalSpace:         totalSpace,
		CompressionRatio:   compressionRatio,
		DeduplicationRatio: deduplicationRatio,
		FragmentationLevel: fragmentationLevel,
	}, nil
}

// collectDatasetData collects storage data for a ZFS dataset
func (cp *CapacityPlanner) collectDatasetData(datasetName string) (StorageDataPoint, error) {
	// Get dataset statistics
	cmd := exec.Command("zfs", "list", "-H", "-p", datasetName)
	output, err := cmd.Output()
	if err != nil {
		return StorageDataPoint{}, fmt.Errorf("failed to get dataset stats: %w", err)
	}
	
	fields := strings.Fields(string(output))
	if len(fields) < 4 {
		return StorageDataPoint{}, fmt.Errorf("unexpected zfs output format")
	}
	
	usedSpace, _ := strconv.ParseInt(fields[1], 10, 64)
	availableSpace, _ := strconv.ParseInt(fields[2], 10, 64)
	
	// Extract pool name from dataset
	poolName := strings.Split(datasetName, "/")[0]
	
	return StorageDataPoint{
		Timestamp:      time.Now(),
		PoolName:       poolName,
		Dataset:        datasetName,
		UsedSpace:      usedSpace,
		AvailableSpace: availableSpace,
		TotalSpace:     usedSpace + availableSpace,
	}, nil
}

// analyzeGrowthPatterns analyzes historical data to detect growth patterns
func (cp *CapacityPlanner) analyzeGrowthPatterns() {
	cp.mu.RLock()
	history := make([]StorageDataPoint, len(cp.storageHistory))
	copy(history, cp.storageHistory)
	cp.mu.RUnlock()
	
	if len(history) < cp.config.MinHistoryPoints {
		capacityLogger.Debugf("Insufficient history points for analysis: %d < %d", 
			len(history), cp.config.MinHistoryPoints)
		return
	}
	
	// Group data by resource (pool or dataset)
	resourceData := make(map[string][]StorageDataPoint)
	for _, point := range history {
		resourceName := point.PoolName
		if point.Dataset != "" {
			resourceName = point.Dataset
		}
		resourceData[resourceName] = append(resourceData[resourceName], point)
	}
	
	// Analyze each resource
	for resourceName, data := range resourceData {
		if len(data) < cp.config.MinHistoryPoints {
			continue
		}
		
		// Sort by timestamp
		sort.Slice(data, func(i, j int) bool {
			return data[i].Timestamp.Before(data[j].Timestamp)
		})
		
		pattern := cp.detectGrowthPattern(resourceName, data)
		if pattern != nil {
			cp.mu.Lock()
			cp.growthPatterns[resourceName] = pattern
			cp.mu.Unlock()
		}
	}
}

// detectGrowthPattern detects the growth pattern for a resource
func (cp *CapacityPlanner) detectGrowthPattern(resourceName string, data []StorageDataPoint) *GrowthPattern {
	// Try different models and select the best one
	linearFit := cp.fitLinearModel(data)
	exponentialFit := cp.fitExponentialModel(data)
	seasonalFit := cp.fitSeasonalModel(data)
	
	// Select the model with the highest R-squared value
	bestModel := "linear"
	bestConfidence := linearFit.RSquared
	bestGrowthRate := linearFit.Slope
	
	if exponentialFit.RSquared > bestConfidence {
		bestModel = "exponential"
		bestConfidence = exponentialFit.RSquared
		bestGrowthRate = exponentialFit.GrowthRate
	}
	
	// For seasonal model, use a different confidence metric
	if len(seasonalFit.Seasonality) > 0 {
		seasonalConfidence := cp.calculateSeasonalConfidence(data, seasonalFit)
		if seasonalConfidence > bestConfidence {
			bestModel = "seasonal"
			bestConfidence = seasonalConfidence
			bestGrowthRate = seasonalFit.Trend
		}
	}
	
	return &GrowthPattern{
		ResourceName: resourceName,
		PatternType:  bestModel,
		GrowthRate:   bestGrowthRate,
		Confidence:   bestConfidence,
		LastUpdated:  time.Now(),
		DataPoints:   len(data),
	}
}

// fitLinearModel fits a linear growth model to the data
func (cp *CapacityPlanner) fitLinearModel(data []StorageDataPoint) *LinearGrowthModel {
	if len(data) < 2 {
		return &LinearGrowthModel{}
	}
	
	// Convert timestamps to days since first data point
	baseTime := data[0].Timestamp
	var x, y []float64
	
	for _, point := range data {
		days := point.Timestamp.Sub(baseTime).Hours() / 24
		x = append(x, days)
		y = append(y, float64(point.UsedSpace))
	}
	
	// Calculate linear regression
	slope, intercept, rSquared := cp.calculateLinearRegression(x, y)
	
	return &LinearGrowthModel{
		Slope:     slope,
		Intercept: intercept,
		RSquared:  rSquared,
	}
}

// fitExponentialModel fits an exponential growth model to the data
func (cp *CapacityPlanner) fitExponentialModel(data []StorageDataPoint) *ExponentialGrowthModel {
	if len(data) < 3 {
		return &ExponentialGrowthModel{}
	}
	
	// Transform data for exponential fitting (ln(y) = ln(a) + bx)
	baseTime := data[0].Timestamp
	var x, lnY []float64
	
	for _, point := range data {
		if point.UsedSpace <= 0 {
			continue // Skip zero or negative values
		}
		
		days := point.Timestamp.Sub(baseTime).Hours() / 24
		x = append(x, days)
		lnY = append(lnY, math.Log(float64(point.UsedSpace)))
	}
	
	if len(x) < 2 {
		return &ExponentialGrowthModel{}
	}
	
	// Calculate linear regression on transformed data
	growthRate, lnInitial, rSquared := cp.calculateLinearRegression(x, lnY)
	initialValue := math.Exp(lnInitial)
	
	return &ExponentialGrowthModel{
		InitialValue: initialValue,
		GrowthRate:   growthRate,
		RSquared:     rSquared,
	}
}

// fitSeasonalModel fits a seasonal growth model to the data
func (cp *CapacityPlanner) fitSeasonalModel(data []StorageDataPoint) *SeasonalGrowthModel {
	if len(data) < 30 { // Need at least 30 data points for seasonal analysis
		return &SeasonalGrowthModel{}
	}
	
	// Calculate seasonal period (default to weekly = 7 days)
	period := int(cp.config.SeasonalityPeriod.Hours() / 24)
	if period == 0 {
		period = 7 // Default to weekly seasonality
	}
	
	// Extract trend and seasonality
	trend := cp.calculateTrend(data)
	seasonality := cp.calculateSeasonality(data, period)
	amplitude := cp.calculateSeasonalAmplitude(seasonality)
	
	return &SeasonalGrowthModel{
		Trend:       trend,
		Seasonality: seasonality,
		Period:      period,
		Amplitude:   amplitude,
	}
}

// calculateLinearRegression calculates linear regression coefficients
func (cp *CapacityPlanner) calculateLinearRegression(x, y []float64) (slope, intercept, rSquared float64) {
	if len(x) != len(y) || len(x) < 2 {
		return 0, 0, 0
	}
	
	n := float64(len(x))
	
	// Calculate means
	var sumX, sumY float64
	for i := 0; i < len(x); i++ {
		sumX += x[i]
		sumY += y[i]
	}
	meanX := sumX / n
	meanY := sumY / n
	
	// Calculate slope and intercept
	var numerator, denominator float64
	for i := 0; i < len(x); i++ {
		numerator += (x[i] - meanX) * (y[i] - meanY)
		denominator += (x[i] - meanX) * (x[i] - meanX)
	}
	
	if denominator == 0 {
		return 0, meanY, 0
	}
	
	slope = numerator / denominator
	intercept = meanY - slope*meanX
	
	// Calculate R-squared
	var ssRes, ssTot float64
	for i := 0; i < len(x); i++ {
		predicted := slope*x[i] + intercept
		ssRes += (y[i] - predicted) * (y[i] - predicted)
		ssTot += (y[i] - meanY) * (y[i] - meanY)
	}
	
	if ssTot == 0 {
		rSquared = 1.0
	} else {
		rSquared = 1.0 - (ssRes / ssTot)
	}
	
	return slope, intercept, rSquared
}

// calculateTrend calculates the overall trend in the data
func (cp *CapacityPlanner) calculateTrend(data []StorageDataPoint) float64 {
	if len(data) < 2 {
		return 0
	}
	
	// Simple trend calculation: (last - first) / time_diff
	first := data[0]
	last := data[len(data)-1]
	
	timeDiff := last.Timestamp.Sub(first.Timestamp).Hours() / 24 // days
	if timeDiff == 0 {
		return 0
	}
	
	spaceDiff := float64(last.UsedSpace - first.UsedSpace)
	return spaceDiff / timeDiff // bytes per day
}

// calculateSeasonality calculates seasonal patterns in the data
func (cp *CapacityPlanner) calculateSeasonality(data []StorageDataPoint, period int) []float64 {
	if len(data) < period*2 {
		return []float64{}
	}
	
	seasonality := make([]float64, period)
	counts := make([]int, period)
	
	baseTime := data[0].Timestamp
	
	for _, point := range data {
		daysSinceBase := int(point.Timestamp.Sub(baseTime).Hours() / 24)
		seasonalIndex := daysSinceBase % period
		
		seasonality[seasonalIndex] += float64(point.UsedSpace)
		counts[seasonalIndex]++
	}
	
	// Calculate averages
	for i := 0; i < period; i++ {
		if counts[i] > 0 {
			seasonality[i] /= float64(counts[i])
		}
	}
	
	return seasonality
}

// calculateSeasonalAmplitude calculates the amplitude of seasonal variation
func (cp *CapacityPlanner) calculateSeasonalAmplitude(seasonality []float64) float64 {
	if len(seasonality) == 0 {
		return 0
	}
	
	min := seasonality[0]
	max := seasonality[0]
	
	for _, value := range seasonality {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	
	return max - min
}

// calculateSeasonalConfidence calculates confidence for seasonal model
func (cp *CapacityPlanner) calculateSeasonalConfidence(data []StorageDataPoint, model *SeasonalGrowthModel) float64 {
	// Simplified confidence calculation based on seasonal amplitude vs trend
	if model.Trend == 0 {
		return 0
	}
	
	// Higher amplitude relative to trend indicates stronger seasonality
	relativeAmplitude := model.Amplitude / math.Abs(model.Trend)
	
	// Convert to confidence score (0-1)
	confidence := math.Min(relativeAmplitude/100, 1.0)
	return confidence
}// gener
ateCapacityPredictions generates future capacity predictions
func (cp *CapacityPlanner) generateCapacityPredictions() {
	cp.mu.RLock()
	patterns := make(map[string]*GrowthPattern)
	for k, v := range cp.growthPatterns {
		patterns[k] = v
	}
	cp.mu.RUnlock()
	
	var predictions []CapacityPrediction
	
	// Generate predictions for each resource
	for resourceName, pattern := range patterns {
		resourcePredictions := cp.generateResourcePredictions(resourceName, pattern)
		predictions = append(predictions, resourcePredictions...)
	}
	
	cp.mu.Lock()
	cp.capacityPredictions = predictions
	cp.mu.Unlock()
	
	capacityLogger.Infof("Generated %d capacity predictions", len(predictions))
}

// generateResourcePredictions generates predictions for a specific resource
func (cp *CapacityPlanner) generateResourcePredictions(resourceName string, pattern *GrowthPattern) []CapacityPrediction {
	var predictions []CapacityPrediction
	
	// Get current capacity
	currentCapacity := cp.getCurrentResourceCapacity(resourceName)
	if currentCapacity == nil {
		return predictions
	}
	
	// Generate predictions for different time horizons
	horizons := []time.Duration{
		7 * 24 * time.Hour,   // 1 week
		30 * 24 * time.Hour,  // 1 month
		90 * 24 * time.Hour,  // 3 months
		365 * 24 * time.Hour, // 1 year
	}
	
	if cp.config.PredictionHorizon > 0 {
		horizons = append(horizons, cp.config.PredictionHorizon)
	}
	
	for _, horizon := range horizons {
		prediction := cp.generateSinglePrediction(resourceName, pattern, currentCapacity, horizon)
		if prediction != nil {
			predictions = append(predictions, *prediction)
		}
	}
	
	return predictions
}

// generateSinglePrediction generates a single prediction
func (cp *CapacityPlanner) generateSinglePrediction(resourceName string, pattern *GrowthPattern, 
	currentCapacity *CapacityStatus, horizon time.Duration) *CapacityPrediction {
	
	days := horizon.Hours() / 24
	var predictedUsage int64
	var confidence float64
	
	switch pattern.PatternType {
	case "linear":
		predictedUsage = currentCapacity.UsedCapacity + int64(pattern.GrowthRate*days)
		confidence = pattern.Confidence
		
	case "exponential":
		growth := math.Exp(pattern.GrowthRate * days / 365) // Annual growth rate
		predictedUsage = int64(float64(currentCapacity.UsedCapacity) * growth)
		confidence = pattern.Confidence * 0.8 // Lower confidence for exponential
		
	case "seasonal":
		// Apply trend and seasonal adjustment
		trendGrowth := pattern.GrowthRate * days
		predictedUsage = currentCapacity.UsedCapacity + int64(trendGrowth)
		confidence = pattern.Confidence * 0.9 // Slightly lower confidence
		
	default:
		return nil
	}
	
	// Calculate confidence bounds
	errorMargin := float64(predictedUsage) * (1.0 - confidence) * 0.5
	upperBound := int64(float64(predictedUsage) + errorMargin)
	lowerBound := int64(float64(predictedUsage) - errorMargin)
	
	// Ensure bounds are reasonable
	if lowerBound < currentCapacity.UsedCapacity {
		lowerBound = currentCapacity.UsedCapacity
	}
	
	utilizationRate := float64(predictedUsage) / float64(currentCapacity.TotalCapacity) * 100
	
	return &CapacityPrediction{
		ResourceName:    resourceName,
		PredictionDate:  time.Now().Add(horizon),
		PredictedUsage:  predictedUsage,
		PredictedTotal:  currentCapacity.TotalCapacity,
		UtilizationRate: utilizationRate,
		Confidence:      confidence,
		Model:           pattern.PatternType,
		UpperBound:      upperBound,
		LowerBound:      lowerBound,
	}
}

// updateCapacityStatus updates current capacity status
func (cp *CapacityPlanner) updateCapacityStatus() {
	pools := cp.config.MonitoredPools
	if len(pools) == 0 {
		discoveredPools, err := cp.discoverZFSPools()
		if err != nil {
			capacityLogger.Warnf("Failed to discover pools: %s", err)
			return
		}
		pools = discoveredPools
	}
	
	for _, pool := range pools {
		status, err := cp.calculateCapacityStatus(pool)
		if err != nil {
			capacityLogger.Warnf("Failed to calculate capacity status for %s: %s", pool, err)
			continue
		}
		
		cp.mu.Lock()
		cp.currentCapacity = status
		cp.mu.Unlock()
	}
}

// calculateCapacityStatus calculates current capacity status for a pool
func (cp *CapacityPlanner) calculateCapacityStatus(poolName string) (*CapacityStatus, error) {
	dataPoint, err := cp.collectPoolData(poolName)
	if err != nil {
		return nil, err
	}
	
	utilizationRate := float64(dataPoint.UsedSpace) / float64(dataPoint.TotalSpace) * 100
	
	// Calculate growth rate from recent history
	growthRate := cp.calculateRecentGrowthRate(poolName)
	
	// Calculate time to full
	var timeToFull time.Duration
	if growthRate > 0 {
		remainingSpace := dataPoint.AvailableSpace
		daysToFull := float64(remainingSpace) / growthRate
		timeToFull = time.Duration(daysToFull * 24 * float64(time.Hour))
	}
	
	return &CapacityStatus{
		PoolName:          poolName,
		TotalCapacity:     dataPoint.TotalSpace,
		UsedCapacity:      dataPoint.UsedSpace,
		AvailableCapacity: dataPoint.AvailableSpace,
		UtilizationRate:   utilizationRate,
		GrowthRate:        growthRate,
		TimeToFull:        timeToFull,
		LastUpdated:       time.Now(),
	}, nil
}

// calculateRecentGrowthRate calculates recent growth rate for a resource
func (cp *CapacityPlanner) calculateRecentGrowthRate(resourceName string) float64 {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	// Get recent data points (last 7 days)
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	var recentPoints []StorageDataPoint
	
	for _, point := range cp.storageHistory {
		if point.PoolName == resourceName && point.Timestamp.After(cutoff) {
			recentPoints = append(recentPoints, point)
		}
	}
	
	if len(recentPoints) < 2 {
		return 0
	}
	
	// Sort by timestamp
	sort.Slice(recentPoints, func(i, j int) bool {
		return recentPoints[i].Timestamp.Before(recentPoints[j].Timestamp)
	})
	
	// Calculate growth rate
	first := recentPoints[0]
	last := recentPoints[len(recentPoints)-1]
	
	timeDiff := last.Timestamp.Sub(first.Timestamp).Hours() / 24 // days
	if timeDiff == 0 {
		return 0
	}
	
	spaceDiff := float64(last.UsedSpace - first.UsedSpace)
	return spaceDiff / timeDiff // bytes per day
}

// checkCapacityAlerts checks for capacity-related alerts
func (cp *CapacityPlanner) checkCapacityAlerts() {
	var newAlerts []CapacityAlert
	
	// Check current capacity alerts
	cp.mu.RLock()
	currentCapacity := cp.currentCapacity
	predictions := make([]CapacityPrediction, len(cp.capacityPredictions))
	copy(predictions, cp.capacityPredictions)
	cp.mu.RUnlock()
	
	if currentCapacity != nil {
		// Check utilization thresholds
		if currentCapacity.UtilizationRate >= cp.config.CriticalThreshold {
			alert := CapacityAlert{
				ID:           fmt.Sprintf("critical_%s_%d", currentCapacity.PoolName, time.Now().Unix()),
				AlertType:    "critical",
				ResourceName: currentCapacity.PoolName,
				Message:      fmt.Sprintf("Pool %s is %.1f%% full (critical threshold: %.1f%%)", 
					currentCapacity.PoolName, currentCapacity.UtilizationRate, cp.config.CriticalThreshold),
				Threshold:    cp.config.CriticalThreshold,
				CurrentValue: currentCapacity.UtilizationRate,
				Severity:     "critical",
				CreatedAt:    time.Now(),
			}
			newAlerts = append(newAlerts, alert)
		} else if currentCapacity.UtilizationRate >= cp.config.WarningThreshold {
			alert := CapacityAlert{
				ID:           fmt.Sprintf("warning_%s_%d", currentCapacity.PoolName, time.Now().Unix()),
				AlertType:    "warning",
				ResourceName: currentCapacity.PoolName,
				Message:      fmt.Sprintf("Pool %s is %.1f%% full (warning threshold: %.1f%%)", 
					currentCapacity.PoolName, currentCapacity.UtilizationRate, cp.config.WarningThreshold),
				Threshold:    cp.config.WarningThreshold,
				CurrentValue: currentCapacity.UtilizationRate,
				Severity:     "warning",
				CreatedAt:    time.Now(),
			}
			newAlerts = append(newAlerts, alert)
		}
		
		// Check growth rate alerts
		monthlyGrowthRate := currentCapacity.GrowthRate * 30 // Convert to monthly
		if monthlyGrowthRate > 0 {
			monthlyGrowthPercent := (monthlyGrowthRate / float64(currentCapacity.TotalCapacity)) * 100
			if monthlyGrowthPercent >= cp.config.GrowthRateThreshold {
				alert := CapacityAlert{
					ID:           fmt.Sprintf("growth_%s_%d", currentCapacity.PoolName, time.Now().Unix()),
					AlertType:    "warning",
					ResourceName: currentCapacity.PoolName,
					Message:      fmt.Sprintf("Pool %s growth rate is %.1f%% per month (threshold: %.1f%%)", 
						currentCapacity.PoolName, monthlyGrowthPercent, cp.config.GrowthRateThreshold),
					Threshold:    cp.config.GrowthRateThreshold,
					CurrentValue: monthlyGrowthPercent,
					Severity:     "warning",
					CreatedAt:    time.Now(),
				}
				newAlerts = append(newAlerts, alert)
			}
		}
	}
	
	// Check prediction alerts
	for _, prediction := range predictions {
		if prediction.UtilizationRate >= cp.config.CriticalThreshold {
			alert := CapacityAlert{
				ID:           fmt.Sprintf("prediction_%s_%d", prediction.ResourceName, time.Now().Unix()),
				AlertType:    "prediction",
				ResourceName: prediction.ResourceName,
				Message:      fmt.Sprintf("Pool %s predicted to reach %.1f%% capacity by %s", 
					prediction.ResourceName, prediction.UtilizationRate, 
					prediction.PredictionDate.Format("2006-01-02")),
				Threshold:    cp.config.CriticalThreshold,
				CurrentValue: prediction.UtilizationRate,
				PredictedDate: prediction.PredictionDate,
				Severity:     "warning",
				CreatedAt:    time.Now(),
			}
			newAlerts = append(newAlerts, alert)
		}
	}
	
	// Add new alerts
	cp.mu.Lock()
	cp.alerts = append(cp.alerts, newAlerts...)
	cp.mu.Unlock()
	
	if len(newAlerts) > 0 {
		capacityLogger.Infof("Generated %d new capacity alerts", len(newAlerts))
	}
}

// generateResourceRecommendations generates resource planning recommendations
func (cp *CapacityPlanner) generateResourceRecommendations() {
	var recommendations []ResourceRecommendation
	
	cp.mu.RLock()
	currentCapacity := cp.currentCapacity
	predictions := make([]CapacityPrediction, len(cp.capacityPredictions))
	copy(predictions, cp.capacityPredictions)
	cp.mu.RUnlock()
	
	if currentCapacity == nil {
		return
	}
	
	// Recommendation 1: Add storage if utilization is high
	if currentCapacity.UtilizationRate >= cp.config.WarningThreshold {
		priority := "medium"
		if currentCapacity.UtilizationRate >= cp.config.CriticalThreshold {
			priority = "critical"
		}
		
		recommendation := ResourceRecommendation{
			ID:                fmt.Sprintf("add_storage_%s_%d", currentCapacity.PoolName, time.Now().Unix()),
			ResourceName:      currentCapacity.PoolName,
			RecommendationType: "add_storage",
			Priority:          priority,
			Description:       fmt.Sprintf("Add storage to pool %s (current utilization: %.1f%%)", 
				currentCapacity.PoolName, currentCapacity.UtilizationRate),
			Implementation:    "Add additional vdevs to the ZFS pool",
			Timeline:          cp.config.LeadTime,
			ExpectedBenefit:   "Increase available capacity and reduce utilization",
			CreatedAt:         time.Now(),
			Status:            "pending",
		}
		recommendations = append(recommendations, recommendation)
	}
	
	// Recommendation 2: Optimize compression if ratio is low
	if currentCapacity.UtilizationRate >= cp.config.WarningThreshold {
		// Check if we can get compression ratio from recent data
		compressionRatio := cp.getRecentCompressionRatio(currentCapacity.PoolName)
		if compressionRatio < 1.5 {
			recommendation := ResourceRecommendation{
				ID:                fmt.Sprintf("optimize_compression_%s_%d", currentCapacity.PoolName, time.Now().Unix()),
				ResourceName:      currentCapacity.PoolName,
				RecommendationType: "optimize",
				Priority:          "medium",
				Description:       fmt.Sprintf("Optimize compression for pool %s (current ratio: %.2fx)", 
					currentCapacity.PoolName, compressionRatio),
				Implementation:    "Enable or upgrade compression algorithm (lz4, gzip, zstd)",
				Timeline:          24 * time.Hour, // Can be done quickly
				ExpectedBenefit:   "Reduce storage usage by 20-50% through better compression",
				CreatedAt:         time.Now(),
				Status:            "pending",
			}
			recommendations = append(recommendations, recommendation)
		}
	}
	
	// Recommendation 3: Migrate data if growth is very high
	monthlyGrowthPercent := (currentCapacity.GrowthRate * 30 / float64(currentCapacity.TotalCapacity)) * 100
	if monthlyGrowthPercent > cp.config.GrowthRateThreshold*2 {
		recommendation := ResourceRecommendation{
			ID:                fmt.Sprintf("migrate_data_%s_%d", currentCapacity.PoolName, time.Now().Unix()),
			ResourceName:      currentCapacity.PoolName,
			RecommendationType: "migrate",
			Priority:          "high",
			Description:       fmt.Sprintf("Consider migrating cold data from pool %s (growth rate: %.1f%%/month)", 
				currentCapacity.PoolName, monthlyGrowthPercent),
			Implementation:    "Implement tiered storage and migrate cold data to cheaper storage",
			Timeline:          7 * 24 * time.Hour,
			ExpectedBenefit:   "Reduce growth rate and optimize storage costs",
			CreatedAt:         time.Now(),
			Status:            "pending",
		}
		recommendations = append(recommendations, recommendation)
	}
	
	// Add recommendations
	cp.mu.Lock()
	cp.resourceRecommendations = append(cp.resourceRecommendations, recommendations...)
	cp.mu.Unlock()
	
	if len(recommendations) > 0 {
		capacityLogger.Infof("Generated %d new resource recommendations", len(recommendations))
	}
}

// Helper methods

func (cp *CapacityPlanner) discoverZFSPools() ([]string, error) {
	cmd := exec.Command("zpool", "list", "-H", "-o", "name")
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var pools []string
	for _, line := range lines {
		if line != "" {
			pools = append(pools, strings.TrimSpace(line))
		}
	}
	
	return pools, nil
}

func (cp *CapacityPlanner) getPoolCompressionRatio(poolName string) float64 {
	cmd := exec.Command("zfs", "get", "-H", "-p", "compressratio", poolName)
	output, err := cmd.Output()
	if err != nil {
		return 1.0
	}
	
	fields := strings.Fields(string(output))
	if len(fields) >= 3 {
		ratioStr := strings.TrimSuffix(fields[2], "x")
		if ratio, err := strconv.ParseFloat(ratioStr, 64); err == nil {
			return ratio
		}
	}
	
	return 1.0
}

func (cp *CapacityPlanner) getPoolDeduplicationRatio(poolName string) float64 {
	cmd := exec.Command("zpool", "get", "-H", "-p", "dedupratio", poolName)
	output, err := cmd.Output()
	if err != nil {
		return 1.0
	}
	
	fields := strings.Fields(string(output))
	if len(fields) >= 3 {
		ratioStr := strings.TrimSuffix(fields[2], "x")
		if ratio, err := strconv.ParseFloat(ratioStr, 64); err == nil {
			return ratio
		}
	}
	
	return 1.0
}

func (cp *CapacityPlanner) getPoolFragmentationLevel(poolName string) float64 {
	cmd := exec.Command("zpool", "get", "-H", "-p", "fragmentation", poolName)
	output, err := cmd.Output()
	if err != nil {
		return 0.0
	}
	
	fields := strings.Fields(string(output))
	if len(fields) >= 3 {
		fragStr := strings.TrimSuffix(fields[2], "%")
		if frag, err := strconv.ParseFloat(fragStr, 64); err == nil {
			return frag
		}
	}
	
	return 0.0
}

func (cp *CapacityPlanner) getCurrentResourceCapacity(resourceName string) *CapacityStatus {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	if cp.currentCapacity != nil && cp.currentCapacity.PoolName == resourceName {
		return cp.currentCapacity
	}
	
	return nil
}

func (cp *CapacityPlanner) getRecentCompressionRatio(poolName string) float64 {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	// Find most recent data point for this pool
	for i := len(cp.storageHistory) - 1; i >= 0; i-- {
		point := cp.storageHistory[i]
		if point.PoolName == poolName {
			return point.CompressionRatio
		}
	}
	
	return 1.0 // Default if no data found
}

func (cp *CapacityPlanner) cleanupHistoricalData() {
	if cp.config.HistoryRetention == 0 {
		return
	}
	
	cutoff := time.Now().Add(-cp.config.HistoryRetention)
	
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	// Remove old storage history
	var filteredHistory []StorageDataPoint
	for _, point := range cp.storageHistory {
		if point.Timestamp.After(cutoff) {
			filteredHistory = append(filteredHistory, point)
		}
	}
	cp.storageHistory = filteredHistory
	
	// Remove old alerts
	var filteredAlerts []CapacityAlert
	for _, alert := range cp.alerts {
		if alert.CreatedAt.After(cutoff) {
			filteredAlerts = append(filteredAlerts, alert)
		}
	}
	cp.alerts = filteredAlerts
	
	capacityLogger.Debugf("Cleaned up historical data older than %v", cp.config.HistoryRetention)
}

// Public API methods

func (cp *CapacityPlanner) GetCapacityStatus() *CapacityStatus {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	if cp.currentCapacity == nil {
		return nil
	}
	
	// Return a copy
	status := *cp.currentCapacity
	return &status
}

func (cp *CapacityPlanner) GetCapacityPredictions() []CapacityPrediction {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	predictions := make([]CapacityPrediction, len(cp.capacityPredictions))
	copy(predictions, cp.capacityPredictions)
	return predictions
}

func (cp *CapacityPlanner) GetResourceRecommendations() []ResourceRecommendation {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	recommendations := make([]ResourceRecommendation, len(cp.resourceRecommendations))
	copy(recommendations, cp.resourceRecommendations)
	return recommendations
}

func (cp *CapacityPlanner) GetCapacityAlerts() []CapacityAlert {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	alerts := make([]CapacityAlert, len(cp.alerts))
	copy(alerts, cp.alerts)
	return alerts
}

func (cp *CapacityPlanner) GetGrowthPatterns() map[string]*GrowthPattern {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	patterns := make(map[string]*GrowthPattern)
	for k, v := range cp.growthPatterns {
		pattern := *v
		patterns[k] = &pattern
	}
	return patterns
}

func (cp *CapacityPlanner) AcknowledgeAlert(alertID, acknowledgedBy string) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	
	for i := range cp.alerts {
		if cp.alerts[i].ID == alertID {
			cp.alerts[i].Acknowledged = true
			cp.alerts[i].AcknowledgedBy = acknowledgedBy
			cp.alerts[i].AcknowledgedAt = time.Now()
			return nil
		}
	}
	
	return fmt.Errorf("alert %s not found", alertID)
}

func (cp *CapacityPlanner) GetPlanningStats() map[string]interface{} {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	
	activeAlerts := 0
	for _, alert := range cp.alerts {
		if !alert.Acknowledged {
			activeAlerts++
		}
	}
	
	pendingRecommendations := 0
	for _, rec := range cp.resourceRecommendations {
		if rec.Status == "pending" {
			pendingRecommendations++
		}
	}
	
	return map[string]interface{}{
		"storage_history_points":    len(cp.storageHistory),
		"growth_patterns":          len(cp.growthPatterns),
		"capacity_predictions":     len(cp.capacityPredictions),
		"active_alerts":           activeAlerts,
		"total_alerts":            len(cp.alerts),
		"pending_recommendations": pendingRecommendations,
		"total_recommendations":   len(cp.resourceRecommendations),
	}
}

// Validate validates the capacity planner configuration
func (config *CapacityPlannerConfig) Validate() error {
	if config.PlanningInterval <= 0 {
		config.PlanningInterval = 1 * time.Hour
	}
	
	if config.DataPointInterval <= 0 {
		config.DataPointInterval = 15 * time.Minute
	}
	
	if config.HistoryRetention <= 0 {
		config.HistoryRetention = 90 * 24 * time.Hour // 90 days
	}
	
	if config.WarningThreshold <= 0 || config.WarningThreshold > 100 {
		config.WarningThreshold = 80.0
	}
	
	if config.CriticalThreshold <= 0 || config.CriticalThreshold > 100 {
		config.CriticalThreshold = 90.0
	}
	
	if config.GrowthRateThreshold <= 0 {
		config.GrowthRateThreshold = 10.0 // 10% per month
	}
	
	if config.MinHistoryPoints <= 0 {
		config.MinHistoryPoints = 10
	}
	
	if config.ConfidenceLevel <= 0 || config.ConfidenceLevel > 1 {
		config.ConfidenceLevel = 0.95
	}
	
	if config.SafetyMargin <= 0 {
		config.SafetyMargin = 0.2 // 20%
	}
	
	if config.LeadTime <= 0 {
		config.LeadTime = 7 * 24 * time.Hour // 1 week
	}
	
	return nil
}