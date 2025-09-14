// Package resource provides tiered storage management for IPFS Cluster ZFS integration
// It implements automatic data movement between storage tiers based on access patterns
// and ML-driven predictions to optimize cost and performance.
package resource

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"
)

var tieredLogger = logging.Logger("tiered-storage")

// TieredStorageManager manages automatic data movement between storage tiers
type TieredStorageManager struct {
	config          *TieredConfig
	tiers           map[string]*StorageTier
	accessPredictor *AccessPredictor
	migrationEngine *MigrationEngine
	costOptimizer   *CostOptimizer
	
	// Data tracking
	dataMap         map[string]*DataItem
	accessHistory   map[string]*AccessHistory
	
	// Synchronization
	mu              sync.RWMutex
	
	// Background processes
	analysisTicker  *time.Ticker
	stopAnalysis    chan struct{}
	migrationTicker *time.Ticker
	stopMigration   chan struct{}
}

// TieredConfig holds configuration for tiered storage
type TieredConfig struct {
	// Tier definitions
	Tiers               []TierDefinition  `json:"tiers"`
	
	// Migration settings
	MigrationInterval   time.Duration     `json:"migration_interval"`
	MaxConcurrentMigrations int           `json:"max_concurrent_migrations"`
	MigrationBandwidth  int64             `json:"migration_bandwidth"` // bytes/sec
	
	// Analysis settings
	AnalysisInterval    time.Duration     `json:"analysis_interval"`
	AccessWindowSize    time.Duration     `json:"access_window_size"`
	PredictionHorizon   time.Duration     `json:"prediction_horizon"`
	
	// Cost optimization
	CostOptimizationEnabled bool          `json:"cost_optimization_enabled"`
	CostThresholds      CostThresholds    `json:"cost_thresholds"`
	
	// ML settings
	MLModelEnabled      bool              `json:"ml_model_enabled"`
	ModelUpdateInterval time.Duration     `json:"model_update_interval"`
	TrainingDataSize    int               `json:"training_data_size"`
}

// TierDefinition defines a storage tier
type TierDefinition struct {
	Name            string            `json:"name"`
	Type            string            `json:"type"`            // "hot", "warm", "cold", "archive"
	MediaType       string            `json:"media_type"`      // "nvme", "ssd", "hdd", "tape"
	ZFSPool         string            `json:"zfs_pool"`
	Properties      map[string]string `json:"properties"`
	
	// Performance characteristics
	ReadLatency     time.Duration     `json:"read_latency"`
	WriteLatency    time.Duration     `json:"write_latency"`
	Throughput      int64             `json:"throughput"`      // bytes/sec
	IOPS            int64             `json:"iops"`
	
	// Cost characteristics
	StorageCost     float64           `json:"storage_cost"`    // $/GB/month
	AccessCost      float64           `json:"access_cost"`     // $/GB accessed
	TransferCost    float64           `json:"transfer_cost"`   // $/GB transferred
	
	// Capacity and limits
	TotalCapacity   int64             `json:"total_capacity"`
	UsedCapacity    int64             `json:"used_capacity"`
	MaxUtilization  float64           `json:"max_utilization"` // 0.9 = 90%
	
	// Access patterns for tier selection
	MinAccessFreq   float64           `json:"min_access_freq"` // accesses/day
	MaxAccessFreq   float64           `json:"max_access_freq"` // accesses/day
	RetentionPeriod time.Duration     `json:"retention_period"`
}

// StorageTier represents an active storage tier
type StorageTier struct {
	Definition      *TierDefinition   `json:"definition"`
	DataItems       map[string]*DataItem `json:"data_items"`
	Metrics         *TierMetrics      `json:"metrics"`
	LastOptimized   time.Time         `json:"last_optimized"`
	
	mu sync.RWMutex `json:"-"`
}

// DataItem represents a piece of data in the tiered storage system
type DataItem struct {
	CID             string            `json:"cid"`
	Size            int64             `json:"size"`
	CurrentTier     string            `json:"current_tier"`
	OptimalTier     string            `json:"optimal_tier"`
	CreatedAt       time.Time         `json:"created_at"`
	LastAccessed    time.Time         `json:"last_accessed"`
	AccessCount     int64             `json:"access_count"`
	AccessPattern   *AccessPattern    `json:"access_pattern"`
	MigrationHistory []MigrationRecord `json:"migration_history"`
	
	// Cost tracking
	StorageCost     float64           `json:"storage_cost"`
	AccessCost      float64           `json:"access_cost"`
	TotalCost       float64           `json:"total_cost"`
	
	mu sync.RWMutex `json:"-"`
}

// AccessPattern represents the access pattern of a data item
type AccessPattern struct {
	Frequency       float64           `json:"frequency"`        // accesses per day
	Recency         time.Duration     `json:"recency"`          // time since last access
	Seasonality     []float64         `json:"seasonality"`      // hourly access pattern
	Trend           float64           `json:"trend"`            // access trend (increasing/decreasing)
	Predictability  float64           `json:"predictability"`   // how predictable the pattern is
}

// AccessHistory tracks access history for ML training
type AccessHistory struct {
	CID             string            `json:"cid"`
	AccessTimes     []time.Time       `json:"access_times"`
	AccessSizes     []int64           `json:"access_sizes"`
	WindowStart     time.Time         `json:"window_start"`
	WindowEnd       time.Time         `json:"window_end"`
}

// MigrationRecord tracks data migrations
type MigrationRecord struct {
	FromTier        string            `json:"from_tier"`
	ToTier          string            `json:"to_tier"`
	Timestamp       time.Time         `json:"timestamp"`
	Reason          string            `json:"reason"`
	Duration        time.Duration     `json:"duration"`
	BytesTransferred int64            `json:"bytes_transferred"`
	Success         bool              `json:"success"`
	Error           string            `json:"error,omitempty"`
}

// TierMetrics holds metrics for a storage tier
type TierMetrics struct {
	TotalItems      int64             `json:"total_items"`
	TotalSize       int64             `json:"total_size"`
	Utilization     float64           `json:"utilization"`
	AccessRate      float64           `json:"access_rate"`      // accesses/sec
	MigrationRate   float64           `json:"migration_rate"`   // items/hour
	CostPerGB       float64           `json:"cost_per_gb"`
	PerformanceScore float64          `json:"performance_score"`
	LastUpdated     time.Time         `json:"last_updated"`
}

// CostThresholds defines cost optimization thresholds
type CostThresholds struct {
	MaxMonthlyCost      float64       `json:"max_monthly_cost"`
	CostPerGBThreshold  float64       `json:"cost_per_gb_threshold"`
	ROIThreshold        float64       `json:"roi_threshold"`
}

// NewTieredStorageManager creates a new tiered storage manager
func NewTieredStorageManager(config *TieredConfig) (*TieredStorageManager, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid configuration")
	}

	tsm := &TieredStorageManager{
		config:          config,
		tiers:           make(map[string]*StorageTier),
		accessPredictor: NewAccessPredictor(config),
		migrationEngine: NewMigrationEngine(config),
		costOptimizer:   NewCostOptimizer(config),
		dataMap:         make(map[string]*DataItem),
		accessHistory:   make(map[string]*AccessHistory),
		stopAnalysis:    make(chan struct{}),
		stopMigration:   make(chan struct{}),
	}

	// Initialize storage tiers
	for _, tierDef := range config.Tiers {
		tier := &StorageTier{
			Definition: &tierDef,
			DataItems:  make(map[string]*DataItem),
			Metrics:    &TierMetrics{},
		}
		tsm.tiers[tierDef.Name] = tier
	}

	tieredLogger.Info("Tiered storage manager initialized successfully")
	return tsm, nil
}

// Start starts the tiered storage manager
func (tsm *TieredStorageManager) Start(ctx context.Context) error {
	tieredLogger.Info("Starting tiered storage manager")

	// Start analysis process
	if tsm.config.AnalysisInterval > 0 {
		tsm.startAnalysis()
	}

	// Start migration process
	if tsm.config.MigrationInterval > 0 {
		tsm.startMigration()
	}

	// Initialize ML model if enabled
	if tsm.config.MLModelEnabled {
		if err := tsm.accessPredictor.Initialize(ctx); err != nil {
			tieredLogger.Warnf("Failed to initialize ML model: %v", err)
		}
	}

	tieredLogger.Info("Tiered storage manager started successfully")
	return nil
}

// Stop stops the tiered storage manager
func (tsm *TieredStorageManager) Stop() error {
	tieredLogger.Info("Stopping tiered storage manager")

	// Stop analysis
	if tsm.analysisTicker != nil {
		tsm.analysisTicker.Stop()
		close(tsm.stopAnalysis)
	}

	// Stop migration
	if tsm.migrationTicker != nil {
		tsm.migrationTicker.Stop()
		close(tsm.stopMigration)
	}

	tieredLogger.Info("Tiered storage manager stopped")
	return nil
}

// AddDataItem adds a new data item to the tiered storage system
func (tsm *TieredStorageManager) AddDataItem(ctx context.Context, cid string, size int64) error {
	tieredLogger.Debugf("Adding data item %s (size: %d bytes)", cid, size)

	// Determine optimal tier for new data
	optimalTier := tsm.selectOptimalTierForNewData(size)
	if optimalTier == "" {
		return fmt.Errorf("no suitable tier found for data item %s", cid)
	}

	// Create data item
	dataItem := &DataItem{
		CID:         cid,
		Size:        size,
		CurrentTier: optimalTier,
		OptimalTier: optimalTier,
		CreatedAt:   time.Now(),
		LastAccessed: time.Now(),
		AccessCount: 0,
		AccessPattern: &AccessPattern{
			Frequency:      1.0, // Initial assumption
			Recency:        0,
			Seasonality:    make([]float64, 24), // 24 hours
			Trend:          0,
			Predictability: 0.5,
		},
		MigrationHistory: []MigrationRecord{},
	}

	// Add to tier and data map
	tsm.mu.Lock()
	tsm.dataMap[cid] = dataItem
	
	if tier, exists := tsm.tiers[optimalTier]; exists {
		tier.mu.Lock()
		tier.DataItems[cid] = dataItem
		tier.mu.Unlock()
	}
	tsm.mu.Unlock()

	// Initialize access history
	tsm.initializeAccessHistory(cid)

	tieredLogger.Debugf("Data item %s added to tier %s", cid, optimalTier)
	return nil
}

// RecordAccess records an access to a data item
func (tsm *TieredStorageManager) RecordAccess(cid string, accessSize int64) error {
	tsm.mu.RLock()
	dataItem, exists := tsm.dataMap[cid]
	tsm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("data item %s not found", cid)
	}

	// Update data item access information
	dataItem.mu.Lock()
	dataItem.LastAccessed = time.Now()
	dataItem.AccessCount++
	dataItem.mu.Unlock()

	// Update access history
	tsm.updateAccessHistory(cid, accessSize)

	// Update access pattern
	tsm.updateAccessPattern(dataItem)

	return nil
}

// MigrateDataItem migrates a data item to a different tier
func (tsm *TieredStorageManager) MigrateDataItem(ctx context.Context, cid, targetTier string) error {
	tieredLogger.Infof("Migrating data item %s to tier %s", cid, targetTier)

	tsm.mu.RLock()
	dataItem, exists := tsm.dataMap[cid]
	tsm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("data item %s not found", cid)
	}

	dataItem.mu.RLock()
	currentTier := dataItem.CurrentTier
	dataItem.mu.RUnlock()

	if currentTier == targetTier {
		return nil // Already in target tier
	}

	// Perform migration
	startTime := time.Now()
	err := tsm.migrationEngine.MigrateData(ctx, cid, currentTier, targetTier)
	duration := time.Since(startTime)

	// Record migration
	migrationRecord := MigrationRecord{
		FromTier:         currentTier,
		ToTier:           targetTier,
		Timestamp:        startTime,
		Reason:           "manual_migration",
		Duration:         duration,
		BytesTransferred: dataItem.Size,
		Success:          err == nil,
	}

	if err != nil {
		migrationRecord.Error = err.Error()
		tieredLogger.Errorf("Migration failed for %s: %v", cid, err)
		return err
	}

	// Update data item
	dataItem.mu.Lock()
	dataItem.CurrentTier = targetTier
	dataItem.MigrationHistory = append(dataItem.MigrationHistory, migrationRecord)
	dataItem.mu.Unlock()

	// Update tier mappings
	tsm.mu.Lock()
	if sourceTier, exists := tsm.tiers[currentTier]; exists {
		sourceTier.mu.Lock()
		delete(sourceTier.DataItems, cid)
		sourceTier.mu.Unlock()
	}
	
	if destTier, exists := tsm.tiers[targetTier]; exists {
		destTier.mu.Lock()
		destTier.DataItems[cid] = dataItem
		destTier.mu.Unlock()
	}
	tsm.mu.Unlock()

	tieredLogger.Infof("Successfully migrated %s from %s to %s", cid, currentTier, targetTier)
	return nil
}

// OptimizePlacement analyzes and optimizes data placement across tiers
func (tsm *TieredStorageManager) OptimizePlacement(ctx context.Context) error {
	tieredLogger.Info("Starting placement optimization")

	// Analyze all data items
	optimizations := tsm.analyzeOptimizationOpportunities()
	
	if len(optimizations) == 0 {
		tieredLogger.Info("No optimization opportunities found")
		return nil
	}

	// Sort by priority/benefit
	sort.Slice(optimizations, func(i, j int) bool {
		return optimizations[i].Benefit > optimizations[j].Benefit
	})

	// Execute optimizations
	executed := 0
	for _, opt := range optimizations {
		if executed >= tsm.config.MaxConcurrentMigrations {
			break
		}

		if err := tsm.MigrateDataItem(ctx, opt.CID, opt.TargetTier); err != nil {
			tieredLogger.Errorf("Failed to execute optimization for %s: %v", opt.CID, err)
			continue
		}

		executed++
	}

	tieredLogger.Infof("Executed %d optimizations out of %d opportunities", executed, len(optimizations))
	return nil
}

// GetTierMetrics returns metrics for all tiers
func (tsm *TieredStorageManager) GetTierMetrics() map[string]*TierMetrics {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	metrics := make(map[string]*TierMetrics)
	for name, tier := range tsm.tiers {
		tier.mu.RLock()
		// Create a copy to avoid race conditions
		metricsCopy := *tier.Metrics
		tier.mu.RUnlock()
		metrics[name] = &metricsCopy
	}

	return metrics
}

// GetDataItemInfo returns information about a specific data item
func (tsm *TieredStorageManager) GetDataItemInfo(cid string) (*DataItem, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	dataItem, exists := tsm.dataMap[cid]
	if !exists {
		return nil, fmt.Errorf("data item %s not found", cid)
	}

	// Return a copy to avoid race conditions
	dataItem.mu.RLock()
	itemCopy := *dataItem
	dataItem.mu.RUnlock()

	return &itemCopy, nil
}

// Private methods

func (tsm *TieredStorageManager) selectOptimalTierForNewData(size int64) string {
	// For new data, typically start with hot tier if available
	for _, tierName := range []string{"hot", "warm", "cold"} {
		if tier, exists := tsm.tiers[tierName]; exists {
			tier.mu.RLock()
			hasCapacity := tier.Definition.UsedCapacity+size <= 
				int64(float64(tier.Definition.TotalCapacity)*tier.Definition.MaxUtilization)
			tier.mu.RUnlock()

			if hasCapacity {
				return tierName
			}
		}
	}

	// If no preferred tier has capacity, find any available tier
	for name, tier := range tsm.tiers {
		tier.mu.RLock()
		hasCapacity := tier.Definition.UsedCapacity+size <= 
			int64(float64(tier.Definition.TotalCapacity)*tier.Definition.MaxUtilization)
		tier.mu.RUnlock()

		if hasCapacity {
			return name
		}
	}

	return ""
}

func (tsm *TieredStorageManager) initializeAccessHistory(cid string) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	tsm.accessHistory[cid] = &AccessHistory{
		CID:         cid,
		AccessTimes: []time.Time{time.Now()},
		AccessSizes: []int64{0},
		WindowStart: time.Now().Add(-tsm.config.AccessWindowSize),
		WindowEnd:   time.Now(),
	}
}

func (tsm *TieredStorageManager) updateAccessHistory(cid string, accessSize int64) {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	history, exists := tsm.accessHistory[cid]
	if !exists {
		tsm.initializeAccessHistory(cid)
		history = tsm.accessHistory[cid]
	}

	now := time.Now()
	history.AccessTimes = append(history.AccessTimes, now)
	history.AccessSizes = append(history.AccessSizes, accessSize)

	// Trim old access records outside the window
	windowStart := now.Add(-tsm.config.AccessWindowSize)
	for i, accessTime := range history.AccessTimes {
		if accessTime.After(windowStart) {
			history.AccessTimes = history.AccessTimes[i:]
			history.AccessSizes = history.AccessSizes[i:]
			break
		}
	}

	history.WindowStart = windowStart
	history.WindowEnd = now
}

func (tsm *TieredStorageManager) updateAccessPattern(dataItem *DataItem) {
	tsm.mu.RLock()
	history, exists := tsm.accessHistory[dataItem.CID]
	tsm.mu.RUnlock()

	if !exists {
		return
	}

	dataItem.mu.Lock()
	defer dataItem.mu.Unlock()

	// Calculate frequency (accesses per day)
	windowDuration := history.WindowEnd.Sub(history.WindowStart)
	if windowDuration > 0 {
		accessesPerDay := float64(len(history.AccessTimes)) * float64(24*time.Hour) / float64(windowDuration)
		dataItem.AccessPattern.Frequency = accessesPerDay
	}

	// Calculate recency
	dataItem.AccessPattern.Recency = time.Since(dataItem.LastAccessed)

	// Calculate seasonality (simplified - hourly pattern)
	hourlyAccesses := make([]int, 24)
	for _, accessTime := range history.AccessTimes {
		hour := accessTime.Hour()
		hourlyAccesses[hour]++
	}

	// Normalize to percentages
	totalAccesses := len(history.AccessTimes)
	if totalAccesses > 0 {
		for i, count := range hourlyAccesses {
			dataItem.AccessPattern.Seasonality[i] = float64(count) / float64(totalAccesses)
		}
	}

	// Calculate trend (simplified - compare first and second half of window)
	if len(history.AccessTimes) >= 4 {
		midPoint := len(history.AccessTimes) / 2
		firstHalf := float64(midPoint)
		secondHalf := float64(len(history.AccessTimes) - midPoint)
		dataItem.AccessPattern.Trend = (secondHalf - firstHalf) / firstHalf
	}

	// Calculate predictability based on variance in access intervals
	if len(history.AccessTimes) >= 3 {
		intervals := make([]float64, len(history.AccessTimes)-1)
		for i := 1; i < len(history.AccessTimes); i++ {
			intervals[i-1] = history.AccessTimes[i].Sub(history.AccessTimes[i-1]).Seconds()
		}

		// Calculate coefficient of variation
		mean := 0.0
		for _, interval := range intervals {
			mean += interval
		}
		mean /= float64(len(intervals))

		variance := 0.0
		for _, interval := range intervals {
			variance += math.Pow(interval-mean, 2)
		}
		variance /= float64(len(intervals))

		if mean > 0 {
			cv := math.Sqrt(variance) / mean
			dataItem.AccessPattern.Predictability = 1.0 / (1.0 + cv) // Higher CV = lower predictability
		}
	}
}

func (tsm *TieredStorageManager) startAnalysis() {
	tsm.analysisTicker = time.NewTicker(tsm.config.AnalysisInterval)
	
	go func() {
		for {
			select {
			case <-tsm.analysisTicker.C:
				tsm.performAnalysis()
			case <-tsm.stopAnalysis:
				return
			}
		}
	}()
}

func (tsm *TieredStorageManager) startMigration() {
	tsm.migrationTicker = time.NewTicker(tsm.config.MigrationInterval)
	
	go func() {
		for {
			select {
			case <-tsm.migrationTicker.C:
				ctx := context.Background()
				if err := tsm.OptimizePlacement(ctx); err != nil {
					tieredLogger.Errorf("Automatic optimization failed: %v", err)
				}
			case <-tsm.stopMigration:
				return
			}
		}
	}()
}

func (tsm *TieredStorageManager) performAnalysis() {
	tieredLogger.Debug("Performing tiered storage analysis")

	// Update tier metrics
	tsm.updateTierMetrics()

	// Update ML model if enabled
	if tsm.config.MLModelEnabled {
		ctx := context.Background()
		if err := tsm.accessPredictor.UpdateModel(ctx, tsm.accessHistory); err != nil {
			tieredLogger.Warnf("Failed to update ML model: %v", err)
		}
	}

	// Perform cost optimization if enabled
	if tsm.config.CostOptimizationEnabled {
		tsm.costOptimizer.OptimizeCosts(tsm.tiers, tsm.dataMap)
	}
}

func (tsm *TieredStorageManager) updateTierMetrics() {
	for _, tier := range tsm.tiers {
		tier.mu.Lock()
		
		tier.Metrics.TotalItems = int64(len(tier.DataItems))
		tier.Metrics.TotalSize = 0
		tier.Metrics.LastUpdated = time.Now()

		for _, dataItem := range tier.DataItems {
			dataItem.mu.RLock()
			tier.Metrics.TotalSize += dataItem.Size
			dataItem.mu.RUnlock()
		}

		if tier.Definition.TotalCapacity > 0 {
			tier.Metrics.Utilization = float64(tier.Metrics.TotalSize) / float64(tier.Definition.TotalCapacity)
		}

		tier.mu.Unlock()
	}
}

// OptimizationOpportunity represents a potential optimization
type OptimizationOpportunity struct {
	CID        string  `json:"cid"`
	CurrentTier string `json:"current_tier"`
	TargetTier string  `json:"target_tier"`
	Benefit    float64 `json:"benefit"`
	Reason     string  `json:"reason"`
}

func (tsm *TieredStorageManager) analyzeOptimizationOpportunities() []*OptimizationOpportunity {
	var opportunities []*OptimizationOpportunity

	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	for cid, dataItem := range tsm.dataMap {
		dataItem.mu.RLock()
		currentTier := dataItem.CurrentTier
		accessPattern := dataItem.AccessPattern
		dataItem.mu.RUnlock()

		// Determine optimal tier based on access pattern
		optimalTier := tsm.determineOptimalTier(accessPattern)
		
		if optimalTier != currentTier {
			benefit := tsm.calculateMigrationBenefit(dataItem, currentTier, optimalTier)
			
			if benefit > 0 {
				opportunities = append(opportunities, &OptimizationOpportunity{
					CID:         cid,
					CurrentTier: currentTier,
					TargetTier:  optimalTier,
					Benefit:     benefit,
					Reason:      tsm.getMigrationReason(accessPattern, currentTier, optimalTier),
				})
			}
		}
	}

	return opportunities
}

func (tsm *TieredStorageManager) determineOptimalTier(pattern *AccessPattern) string {
	// High frequency = hot tier
	if pattern.Frequency > 10 { // More than 10 accesses per day
		return "hot"
	}
	
	// Medium frequency = warm tier
	if pattern.Frequency > 1 { // More than 1 access per day
		return "warm"
	}
	
	// Low frequency but recent = warm tier
	if pattern.Recency < 24*time.Hour {
		return "warm"
	}
	
	// Old and infrequent = cold tier
	if pattern.Recency < 7*24*time.Hour {
		return "cold"
	}
	
	// Very old = archive tier
	return "archive"
}

func (tsm *TieredStorageManager) calculateMigrationBenefit(dataItem *DataItem, fromTier, toTier string) float64 {
	// Simplified benefit calculation based on cost savings and performance impact
	fromTierDef := tsm.tiers[fromTier].Definition
	toTierDef := tsm.tiers[toTier].Definition
	
	// Cost benefit (positive if moving to cheaper tier)
	costBenefit := (fromTierDef.StorageCost - toTierDef.StorageCost) * float64(dataItem.Size) / (1024*1024*1024) // per GB
	
	// Performance penalty (negative if moving to slower tier)
	performancePenalty := 0.0
	if toTierDef.ReadLatency > fromTierDef.ReadLatency {
		performancePenalty = float64(toTierDef.ReadLatency-fromTierDef.ReadLatency) / float64(time.Millisecond) * 0.01
	}
	
	return costBenefit - performancePenalty
}

func (tsm *TieredStorageManager) getMigrationReason(pattern *AccessPattern, fromTier, toTier string) string {
	if pattern.Frequency > 10 {
		return "high_access_frequency"
	} else if pattern.Frequency < 0.1 {
		return "low_access_frequency"
	} else if pattern.Recency > 7*24*time.Hour {
		return "data_aging"
	} else {
		return "cost_optimization"
	}
}

// Validate validates the tiered storage configuration
func (c *TieredConfig) Validate() error {
	if len(c.Tiers) == 0 {
		return fmt.Errorf("at least one tier must be defined")
	}
	
	if c.MigrationInterval <= 0 {
		return fmt.Errorf("migration_interval must be positive")
	}
	
	if c.MaxConcurrentMigrations <= 0 {
		return fmt.Errorf("max_concurrent_migrations must be positive")
	}
	
	if c.AnalysisInterval <= 0 {
		return fmt.Errorf("analysis_interval must be positive")
	}
	
	if c.AccessWindowSize <= 0 {
		return fmt.Errorf("access_window_size must be positive")
	}
	
	// Validate tier definitions
	tierNames := make(map[string]bool)
	for _, tier := range c.Tiers {
		if tier.Name == "" {
			return fmt.Errorf("tier name cannot be empty")
		}
		
		if tierNames[tier.Name] {
			return fmt.Errorf("duplicate tier name: %s", tier.Name)
		}
		tierNames[tier.Name] = true
		
		if tier.TotalCapacity <= 0 {
			return fmt.Errorf("tier %s total_capacity must be positive", tier.Name)
		}
		
		if tier.MaxUtilization <= 0 || tier.MaxUtilization > 1 {
			return fmt.Errorf("tier %s max_utilization must be between 0 and 1", tier.Name)
		}
	}
	
	return nil
}