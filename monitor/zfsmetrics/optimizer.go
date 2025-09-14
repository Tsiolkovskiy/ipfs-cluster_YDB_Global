// Package zfsmetrics - Performance Optimizer
// This module implements automatic ZFS parameter optimization using ML models
// and A/B testing to find optimal configurations for different workloads.
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

var optimizerLogger = logging.Logger("zfs-optimizer")

// PerformanceOptimizer automatically optimizes ZFS parameters based on workload patterns
type PerformanceOptimizer struct {
	config          *OptimizerConfig
	collector       *ZFSMetricsCollector
	mlModel         *MLModel
	abTester        *ABTester
	
	// Current optimization state
	mu                    sync.RWMutex
	currentConfiguration  *ZFSConfiguration
	optimizationHistory   []OptimizationResult
	
	// Background optimization
	ctx                   context.Context
	cancel                context.CancelFunc
	optimizationTicker    *time.Ticker
	
	// Performance tracking
	baselineMetrics       map[string]float64
	currentMetrics        map[string]float64
	optimizationCount     int64
}

// OptimizerConfig holds configuration for the performance optimizer
type OptimizerConfig struct {
	// Optimization interval
	OptimizationInterval time.Duration `json:"optimization_interval"`
	
	// ML model configuration
	MLModelPath          string        `json:"ml_model_path"`
	TrainingDataPath     string        `json:"training_data_path"`
	ModelUpdateInterval  time.Duration `json:"model_update_interval"`
	
	// A/B testing configuration
	ABTestDuration       time.Duration `json:"ab_test_duration"`
	ABTestConfidence     float64       `json:"ab_test_confidence"`
	MaxConcurrentTests   int           `json:"max_concurrent_tests"`
	
	// Safety limits
	MaxParameterChange   float64       `json:"max_parameter_change"`
	RollbackThreshold    float64       `json:"rollback_threshold"`
	SafetyCheckInterval  time.Duration `json:"safety_check_interval"`
	
	// Optimization targets
	OptimizationTargets  []string      `json:"optimization_targets"`
	WeightedTargets      map[string]float64 `json:"weighted_targets"`
}

// ZFSConfiguration represents a complete ZFS configuration
type ZFSConfiguration struct {
	PoolName     string                 `json:"pool_name"`
	Parameters   map[string]interface{} `json:"parameters"`
	Timestamp    time.Time              `json:"timestamp"`
	Source       string                 `json:"source"` // "baseline", "ml", "ab_test"
	Version      int                    `json:"version"`
}

// OptimizationResult tracks the results of an optimization attempt
type OptimizationResult struct {
	Timestamp         time.Time                `json:"timestamp"`
	Configuration     *ZFSConfiguration        `json:"configuration"`
	MetricsBefore     map[string]float64       `json:"metrics_before"`
	MetricsAfter      map[string]float64       `json:"metrics_after"`
	PerformanceGain   float64                  `json:"performance_gain"`
	OptimizationType  string                   `json:"optimization_type"`
	Success           bool                     `json:"success"`
	RollbackReason    string                   `json:"rollback_reason,omitempty"`
}

// MLModel represents a machine learning model for ZFS optimization
type MLModel struct {
	ModelPath        string                 `json:"model_path"`
	Features         []string               `json:"features"`
	Targets          []string               `json:"targets"`
	TrainingData     []TrainingExample      `json:"training_data"`
	ModelAccuracy    float64                `json:"model_accuracy"`
	LastTrained      time.Time              `json:"last_trained"`
	PredictionCache  map[string]Prediction  `json:"prediction_cache"`
	mu               sync.RWMutex
}

// TrainingExample represents a training data point
type TrainingExample struct {
	Features    map[string]float64 `json:"features"`
	Targets     map[string]float64 `json:"targets"`
	Timestamp   time.Time          `json:"timestamp"`
	WorkloadType string            `json:"workload_type"`
}

// Prediction represents a model prediction
type Prediction struct {
	Parameters      map[string]interface{} `json:"parameters"`
	ExpectedGain    float64                `json:"expected_gain"`
	Confidence      float64                `json:"confidence"`
	Timestamp       time.Time              `json:"timestamp"`
}

// ABTester manages A/B testing of different configurations
type ABTester struct {
	config           *OptimizerConfig
	activeTests      map[string]*ABTest
	testHistory      []ABTestResult
	mu               sync.RWMutex
}

// ABTest represents an active A/B test
type ABTest struct {
	ID               string                 `json:"id"`
	Name             string                 `json:"name"`
	ControlConfig    *ZFSConfiguration      `json:"control_config"`
	TestConfig       *ZFSConfiguration      `json:"test_config"`
	StartTime        time.Time              `json:"start_time"`
	Duration         time.Duration          `json:"duration"`
	ControlMetrics   []map[string]float64   `json:"control_metrics"`
	TestMetrics      []map[string]float64   `json:"test_metrics"`
	Status           string                 `json:"status"` // "running", "completed", "failed"
}

// ABTestResult represents the result of an A/B test
type ABTestResult struct {
	TestID           string                 `json:"test_id"`
	TestName         string                 `json:"test_name"`
	StartTime        time.Time              `json:"start_time"`
	EndTime          time.Time              `json:"end_time"`
	Winner           string                 `json:"winner"` // "control", "test", "inconclusive"
	PerformanceGain  float64                `json:"performance_gain"`
	StatisticalSignificance float64         `json:"statistical_significance"`
	ControlConfig    *ZFSConfiguration      `json:"control_config"`
	TestConfig       *ZFSConfiguration      `json:"test_config"`
}

// NewPerformanceOptimizer creates a new ZFS performance optimizer
func NewPerformanceOptimizer(config *OptimizerConfig, collector *ZFSMetricsCollector) (*PerformanceOptimizer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid optimizer configuration: %w", err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	// Initialize ML model
	mlModel, err := NewMLModel(config)
	if err != nil {
		optimizerLogger.Warnf("Failed to initialize ML model: %s", err)
		mlModel = &MLModel{
			Features:        []string{"arc_hit_ratio", "compression_ratio", "fragmentation_level", "iops"},
			Targets:         []string{"performance_score"},
			TrainingData:    []TrainingExample{},
			PredictionCache: make(map[string]Prediction),
		}
	}
	
	// Initialize A/B tester
	abTester := &ABTester{
		config:      config,
		activeTests: make(map[string]*ABTest),
		testHistory: []ABTestResult{},
	}
	
	optimizer := &PerformanceOptimizer{
		config:               config,
		collector:            collector,
		mlModel:              mlModel,
		abTester:             abTester,
		optimizationHistory:  []OptimizationResult{},
		baselineMetrics:      make(map[string]float64),
		currentMetrics:       make(map[string]float64),
		ctx:                  ctx,
		cancel:               cancel,
	}
	
	// Get current ZFS configuration as baseline
	if err := optimizer.captureBaselineConfiguration(); err != nil {
		optimizerLogger.Warnf("Failed to capture baseline configuration: %s", err)
	}
	
	return optimizer, nil
}

// Start begins the optimization process
func (po *PerformanceOptimizer) Start() error {
	interval := po.config.OptimizationInterval
	if interval == 0 {
		interval = 1 * time.Hour // Default to hourly optimization
	}
	
	po.optimizationTicker = time.NewTicker(interval)
	
	// Start optimization loop
	go po.optimizationLoop()
	
	// Start ML model training loop
	go po.mlTrainingLoop()
	
	// Start A/B testing loop
	go po.abTestingLoop()
	
	optimizerLogger.Infof("Started ZFS performance optimizer with interval %v", interval)
	return nil
}

// Stop stops the optimization process
func (po *PerformanceOptimizer) Stop() error {
	if po.optimizationTicker != nil {
		po.optimizationTicker.Stop()
	}
	po.cancel()
	
	optimizerLogger.Info("Stopped ZFS performance optimizer")
	return nil
}

// optimizationLoop runs the main optimization loop
func (po *PerformanceOptimizer) optimizationLoop() {
	for {
		select {
		case <-po.optimizationTicker.C:
			po.performOptimization()
		case <-po.ctx.Done():
			return
		}
	}
}

// performOptimization performs a single optimization cycle
func (po *PerformanceOptimizer) performOptimization() {
	start := time.Now()
	
	po.mu.Lock()
	po.optimizationCount++
	po.mu.Unlock()
	
	optimizerLogger.Infof("Starting optimization cycle #%d", po.optimizationCount)
	
	// Collect current metrics
	currentMetrics := po.collectCurrentMetrics()
	
	// Update ML model with current data
	po.updateMLModel(currentMetrics)
	
	// Get ML prediction for optimal configuration
	prediction, err := po.mlModel.Predict(currentMetrics)
	if err != nil {
		optimizerLogger.Warnf("ML prediction failed: %s", err)
		return
	}
	
	// Create new configuration based on prediction
	newConfig := po.createOptimizedConfiguration(prediction)
	
	// Validate configuration safety
	if !po.validateConfigurationSafety(newConfig) {
		optimizerLogger.Warn("New configuration failed safety validation")
		return
	}
	
	// Apply configuration and measure results
	result := po.applyAndMeasureConfiguration(newConfig, "ml")
	
	// Store optimization result
	po.mu.Lock()
	po.optimizationHistory = append(po.optimizationHistory, result)
	po.mu.Unlock()
	
	duration := time.Since(start)
	optimizerLogger.Infof("Completed optimization cycle in %v, gain: %.2f%%", 
		duration, result.PerformanceGain*100)
}

// collectCurrentMetrics collects current ZFS metrics
func (po *PerformanceOptimizer) collectCurrentMetrics() map[string]float64 {
	metrics := make(map[string]float64)
	
	zfsMetrics := po.collector.GetZFSMetrics()
	for _, metric := range zfsMetrics {
		if value, err := strconv.ParseFloat(metric.Value, 64); err == nil {
			metrics[metric.Name] = value
		}
	}
	
	return metrics
}

// updateMLModel updates the ML model with new training data
func (po *PerformanceOptimizer) updateMLModel(metrics map[string]float64) {
	po.mlModel.mu.Lock()
	defer po.mlModel.mu.Unlock()
	
	// Create training example from current metrics
	example := TrainingExample{
		Features:     metrics,
		Targets:      po.calculatePerformanceTargets(metrics),
		Timestamp:    time.Now(),
		WorkloadType: po.detectWorkloadType(metrics),
	}
	
	po.mlModel.TrainingData = append(po.mlModel.TrainingData, example)
	
	// Limit training data size
	maxTrainingData := 10000
	if len(po.mlModel.TrainingData) > maxTrainingData {
		po.mlModel.TrainingData = po.mlModel.TrainingData[len(po.mlModel.TrainingData)-maxTrainingData:]
	}
}

// calculatePerformanceTargets calculates performance targets from metrics
func (po *PerformanceOptimizer) calculatePerformanceTargets(metrics map[string]float64) map[string]float64 {
	targets := make(map[string]float64)
	
	// Calculate composite performance score
	score := 0.0
	weights := po.config.WeightedTargets
	
	if weights == nil {
		weights = map[string]float64{
			"zfs_arc_hit_ratio":      0.3,
			"zfs_compression_ratio":  0.2,
			"zfs_fragmentation_level": -0.2, // Negative because lower is better
			"zfs_read_iops":          0.15,
			"zfs_write_iops":         0.15,
		}
	}
	
	for metricName, weight := range weights {
		if value, exists := metrics[metricName]; exists {
			score += value * weight
		}
	}
	
	targets["performance_score"] = score
	return targets
}

// detectWorkloadType detects the current workload type based on metrics
func (po *PerformanceOptimizer) detectWorkloadType(metrics map[string]float64) string {
	readIOPS := metrics["zfs_read_iops"]
	writeIOPS := metrics["zfs_write_iops"]
	
	if readIOPS > writeIOPS*3 {
		return "read_heavy"
	} else if writeIOPS > readIOPS*3 {
		return "write_heavy"
	} else {
		return "balanced"
	}
}

// createOptimizedConfiguration creates a new configuration based on ML prediction
func (po *PerformanceOptimizer) createOptimizedConfiguration(prediction Prediction) *ZFSConfiguration {
	po.mu.RLock()
	currentConfig := po.currentConfiguration
	po.mu.RUnlock()
	
	newConfig := &ZFSConfiguration{
		PoolName:   currentConfig.PoolName,
		Parameters: make(map[string]interface{}),
		Timestamp:  time.Now(),
		Source:     "ml",
		Version:    currentConfig.Version + 1,
	}
	
	// Copy current parameters
	for k, v := range currentConfig.Parameters {
		newConfig.Parameters[k] = v
	}
	
	// Apply ML predictions with safety limits
	for param, value := range prediction.Parameters {
		if po.isParameterSafeToChange(param, value) {
			newConfig.Parameters[param] = value
		}
	}
	
	return newConfig
}

// validateConfigurationSafety validates that a configuration is safe to apply
func (po *PerformanceOptimizer) validateConfigurationSafety(config *ZFSConfiguration) bool {
	po.mu.RLock()
	currentConfig := po.currentConfiguration
	po.mu.RUnlock()
	
	// Check parameter change limits
	for param, newValue := range config.Parameters {
		if currentValue, exists := currentConfig.Parameters[param]; exists {
			if !po.isParameterChangeSafe(param, currentValue, newValue) {
				optimizerLogger.Warnf("Parameter %s change from %v to %v exceeds safety limits", 
					param, currentValue, newValue)
				return false
			}
		}
	}
	
	return true
}

// isParameterSafeToChange checks if a parameter is safe to change
func (po *PerformanceOptimizer) isParameterSafeToChange(param string, value interface{}) bool {
	// Define safe parameters that can be changed automatically
	safeParameters := map[string]bool{
		"recordsize":           true,
		"compression":          true,
		"primarycache":         true,
		"secondarycache":       true,
		"logbias":             true,
		"sync":                false, // Sync is critical, don't auto-change
		"dedup":               false, // Dedup is expensive, don't auto-change
		"atime":               true,
		"relatime":            true,
	}
	
	return safeParameters[param]
}

// isParameterChangeSafe checks if a parameter change is within safe limits
func (po *PerformanceOptimizer) isParameterChangeSafe(param string, oldValue, newValue interface{}) bool {
	maxChange := po.config.MaxParameterChange
	if maxChange == 0 {
		maxChange = 0.5 // Default to 50% max change
	}
	
	// For numeric parameters, check percentage change
	if oldFloat, ok := oldValue.(float64); ok {
		if newFloat, ok := newValue.(float64); ok {
			change := math.Abs(newFloat-oldFloat) / oldFloat
			return change <= maxChange
		}
	}
	
	// For string parameters, allow change if it's a known safe value
	safeValues := map[string][]string{
		"compression": {"off", "lz4", "gzip", "zstd"},
		"logbias":     {"latency", "throughput"},
		"primarycache": {"all", "none", "metadata"},
		"secondarycache": {"all", "none", "metadata"},
	}
	
	if values, exists := safeValues[param]; exists {
		newStr := fmt.Sprintf("%v", newValue)
		for _, safeValue := range values {
			if newStr == safeValue {
				return true
			}
		}
		return false
	}
	
	return true
}

// applyAndMeasureConfiguration applies a configuration and measures its impact
func (po *PerformanceOptimizer) applyAndMeasureConfiguration(config *ZFSConfiguration, optimizationType string) OptimizationResult {
	// Collect metrics before change
	metricsBefore := po.collectCurrentMetrics()
	
	// Apply configuration
	err := po.applyConfiguration(config)
	if err != nil {
		optimizerLogger.Errorf("Failed to apply configuration: %s", err)
		return OptimizationResult{
			Timestamp:        time.Now(),
			Configuration:    config,
			MetricsBefore:    metricsBefore,
			MetricsAfter:     metricsBefore,
			PerformanceGain:  0,
			OptimizationType: optimizationType,
			Success:          false,
			RollbackReason:   err.Error(),
		}
	}
	
	// Wait for metrics to stabilize
	time.Sleep(5 * time.Minute)
	
	// Collect metrics after change
	metricsAfter := po.collectCurrentMetrics()
	
	// Calculate performance gain
	gain := po.calculatePerformanceGain(metricsBefore, metricsAfter)
	
	// Check if rollback is needed
	rollbackReason := ""
	if gain < po.config.RollbackThreshold {
		rollbackReason = fmt.Sprintf("Performance gain %.2f%% below threshold %.2f%%", 
			gain*100, po.config.RollbackThreshold*100)
		
		// Rollback to previous configuration
		po.mu.RLock()
		previousConfig := po.currentConfiguration
		po.mu.RUnlock()
		
		if err := po.applyConfiguration(previousConfig); err != nil {
			optimizerLogger.Errorf("Failed to rollback configuration: %s", err)
		} else {
			optimizerLogger.Infof("Rolled back configuration due to poor performance")
		}
	} else {
		// Update current configuration
		po.mu.Lock()
		po.currentConfiguration = config
		po.mu.Unlock()
	}
	
	return OptimizationResult{
		Timestamp:        time.Now(),
		Configuration:    config,
		MetricsBefore:    metricsBefore,
		MetricsAfter:     metricsAfter,
		PerformanceGain:  gain,
		OptimizationType: optimizationType,
		Success:          rollbackReason == "",
		RollbackReason:   rollbackReason,
	}
}

// applyConfiguration applies a ZFS configuration
func (po *PerformanceOptimizer) applyConfiguration(config *ZFSConfiguration) error {
	for param, value := range config.Parameters {
		cmd := exec.Command("zfs", "set", fmt.Sprintf("%s=%v", param, value), config.PoolName)
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to set %s=%v: %w", param, value, err)
		}
	}
	
	optimizerLogger.Infof("Applied configuration version %d to pool %s", 
		config.Version, config.PoolName)
	return nil
}

// calculatePerformanceGain calculates the performance gain between two metric sets
func (po *PerformanceOptimizer) calculatePerformanceGain(before, after map[string]float64) float64 {
	beforeScore := po.calculatePerformanceTargets(before)["performance_score"]
	afterScore := po.calculatePerformanceTargets(after)["performance_score"]
	
	if beforeScore == 0 {
		return 0
	}
	
	return (afterScore - beforeScore) / beforeScore
}

// captureBaselineConfiguration captures the current ZFS configuration as baseline
func (po *PerformanceOptimizer) captureBaselineConfiguration() error {
	poolName := po.collector.poolName
	
	// Get current ZFS properties
	cmd := exec.Command("zfs", "get", "-H", "-p", "all", poolName)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get ZFS properties: %w", err)
	}
	
	parameters := make(map[string]interface{})
	lines := strings.Split(string(output), "\n")
	
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			property := fields[1]
			value := fields[2]
			
			// Try to parse as number, otherwise keep as string
			if numValue, err := strconv.ParseFloat(value, 64); err == nil {
				parameters[property] = numValue
			} else {
				parameters[property] = value
			}
		}
	}
	
	po.currentConfiguration = &ZFSConfiguration{
		PoolName:   poolName,
		Parameters: parameters,
		Timestamp:  time.Now(),
		Source:     "baseline",
		Version:    1,
	}
	
	optimizerLogger.Infof("Captured baseline configuration for pool %s with %d parameters", 
		poolName, len(parameters))
	return nil
}

// GetOptimizationHistory returns the optimization history
func (po *PerformanceOptimizer) GetOptimizationHistory() []OptimizationResult {
	po.mu.RLock()
	defer po.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	history := make([]OptimizationResult, len(po.optimizationHistory))
	copy(history, po.optimizationHistory)
	return history
}

// GetCurrentConfiguration returns the current ZFS configuration
func (po *PerformanceOptimizer) GetCurrentConfiguration() *ZFSConfiguration {
	po.mu.RLock()
	defer po.mu.RUnlock()
	
	if po.currentConfiguration == nil {
		return nil
	}
	
	// Return a copy
	config := *po.currentConfiguration
	config.Parameters = make(map[string]interface{})
	for k, v := range po.currentConfiguration.Parameters {
		config.Parameters[k] = v
	}
	
	return &config
}

// GetOptimizationStats returns optimization statistics
func (po *PerformanceOptimizer) GetOptimizationStats() map[string]interface{} {
	po.mu.RLock()
	defer po.mu.RUnlock()
	
	successCount := 0
	totalGain := 0.0
	
	for _, result := range po.optimizationHistory {
		if result.Success {
			successCount++
			totalGain += result.PerformanceGain
		}
	}
	
	avgGain := 0.0
	if successCount > 0 {
		avgGain = totalGain / float64(successCount)
	}
	
	return map[string]interface{}{
		"optimization_count":     po.optimizationCount,
		"successful_optimizations": successCount,
		"success_rate":          float64(successCount) / float64(len(po.optimizationHistory)),
		"average_performance_gain": avgGain,
		"total_optimizations":   len(po.optimizationHistory),
		"ml_model_accuracy":     po.mlModel.ModelAccuracy,
		"active_ab_tests":       len(po.abTester.activeTests),
	}
}