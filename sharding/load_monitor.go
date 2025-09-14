package sharding

import (
	"sync"
	"time"
)

// LoadMonitor tracks shard load and performance metrics in real-time
type LoadMonitor struct {
	shardMetrics    map[string]*ShardLoadInfo
	alertThresholds *AlertThresholds
	mutex           sync.RWMutex
	alertCallbacks  []AlertCallback
}

// ShardLoadInfo contains real-time load information for a shard
type ShardLoadInfo struct {
	ShardID           string    `json:"shard_id"`
	LastUpdate        time.Time `json:"last_update"`
	PinCount          int64     `json:"pin_count"`
	OperationsPerSec  float64   `json:"operations_per_sec"`
	UtilizationPercent float64  `json:"utilization_percent"`
	LoadScore         float64   `json:"load_score"`
	
	// Performance metrics
	AvgLatency        float64   `json:"avg_latency"`
	ErrorRate         float64   `json:"error_rate"`
	ThroughputMBps    float64   `json:"throughput_mbps"`
	
	// ZFS metrics
	CompressionRatio   float64  `json:"compression_ratio"`
	DeduplicationRatio float64  `json:"deduplication_ratio"`
	FragmentationLevel float64  `json:"fragmentation_level"`
	
	// Historical data for trend analysis
	OperationHistory  []float64 `json:"operation_history"`
	LatencyHistory    []float64 `json:"latency_history"`
}

// AlertCallback is called when an alert condition is met
type AlertCallback func(alert *Alert)

// Alert represents a monitoring alert
type Alert struct {
	ShardID     string    `json:"shard_id"`
	Type        AlertType `json:"type"`
	Severity    Severity  `json:"severity"`
	Message     string    `json:"message"`
	Timestamp   time.Time `json:"timestamp"`
	Value       float64   `json:"value"`
	Threshold   float64   `json:"threshold"`
}

// AlertType defines the type of alert
type AlertType int

const (
	AlertHighUtilization AlertType = iota
	AlertLowUtilization
	AlertHighLatency
	AlertHighErrorRate
	AlertLowCompression
	AlertHighFragmentation
)

// Severity defines alert severity levels
type Severity int

const (
	SeverityInfo Severity = iota
	SeverityWarning
	SeverityCritical
)

// NewLoadMonitor creates a new load monitor instance
func NewLoadMonitor() *LoadMonitor {
	return &LoadMonitor{
		shardMetrics: make(map[string]*ShardLoadInfo),
		alertThresholds: &AlertThresholds{
			HighUtilization:     0.85,
			LowUtilization:      0.10,
			HighLatency:         100.0, // ms
			HighErrorRate:       0.01,  // 1%
			LowCompressionRatio: 1.2,
		},
		alertCallbacks: make([]AlertCallback, 0),
	}
}

// InitializeShard initializes monitoring for a new shard
func (lm *LoadMonitor) InitializeShard(shardID string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	lm.shardMetrics[shardID] = &ShardLoadInfo{
		ShardID:           shardID,
		LastUpdate:        time.Now(),
		OperationHistory:  make([]float64, 0, 100), // Keep last 100 samples
		LatencyHistory:    make([]float64, 0, 100),
		CompressionRatio:  1.0,
		DeduplicationRatio: 1.0,
	}

	logger.Infof("Initialized monitoring for shard: %s", shardID)
}

// RemoveShard removes monitoring for a shard
func (lm *LoadMonitor) RemoveShard(shardID string) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	delete(lm.shardMetrics, shardID)
	logger.Infof("Removed monitoring for shard: %s", shardID)
}

// UpdateLoad updates the load information for a shard
func (lm *LoadMonitor) UpdateLoad(shardID string, pinCount int64, operations int64) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	loadInfo, exists := lm.shardMetrics[shardID]
	if !exists {
		return
	}

	now := time.Now()
	timeDiff := now.Sub(loadInfo.LastUpdate).Seconds()
	
	if timeDiff > 0 {
		// Calculate operations per second
		loadInfo.OperationsPerSec = float64(operations) / timeDiff
		
		// Update operation history
		loadInfo.OperationHistory = append(loadInfo.OperationHistory, loadInfo.OperationsPerSec)
		if len(loadInfo.OperationHistory) > 100 {
			loadInfo.OperationHistory = loadInfo.OperationHistory[1:]
		}
	}

	loadInfo.PinCount = pinCount
	loadInfo.LastUpdate = now
	
	// Calculate load score (weighted combination of metrics)
	loadInfo.LoadScore = lm.calculateLoadScore(loadInfo)
	
	// Check for alerts
	lm.checkAlertsUnlocked(shardID, loadInfo)
}

// UpdateMetrics updates detailed metrics for a shard
func (lm *LoadMonitor) UpdateMetrics(shardID string, metrics *ShardMetrics) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	loadInfo, exists := lm.shardMetrics[shardID]
	if !exists {
		return
	}

	loadInfo.UtilizationPercent = metrics.UtilizationPercent
	loadInfo.AvgLatency = metrics.LatencyAvg
	loadInfo.ErrorRate = metrics.ErrorRate
	loadInfo.CompressionRatio = metrics.CompressionRatio
	loadInfo.DeduplicationRatio = metrics.DeduplicationRatio
	loadInfo.FragmentationLevel = metrics.FragmentationLevel
	
	// Update latency history
	loadInfo.LatencyHistory = append(loadInfo.LatencyHistory, loadInfo.AvgLatency)
	if len(loadInfo.LatencyHistory) > 100 {
		loadInfo.LatencyHistory = loadInfo.LatencyHistory[1:]
	}
	
	// Recalculate load score
	loadInfo.LoadScore = lm.calculateLoadScore(loadInfo)
	
	// Check for alerts
	lm.checkAlertsUnlocked(shardID, loadInfo)
}

// GetShardLoad returns the current load information for a shard
func (lm *LoadMonitor) GetShardLoad(shardID string) (*ShardLoadInfo, bool) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	loadInfo, exists := lm.shardMetrics[shardID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	loadInfoCopy := *loadInfo
	return &loadInfoCopy, true
}

// GetAllShardLoads returns load information for all monitored shards
func (lm *LoadMonitor) GetAllShardLoads() map[string]*ShardLoadInfo {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	result := make(map[string]*ShardLoadInfo)
	for shardID, loadInfo := range lm.shardMetrics {
		loadInfoCopy := *loadInfo
		result[shardID] = &loadInfoCopy
	}

	return result
}

// RegisterAlertCallback registers a callback for alerts
func (lm *LoadMonitor) RegisterAlertCallback(callback AlertCallback) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	lm.alertCallbacks = append(lm.alertCallbacks, callback)
}

// SetAlertThresholds updates the alert thresholds
func (lm *LoadMonitor) SetAlertThresholds(thresholds *AlertThresholds) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()

	lm.alertThresholds = thresholds
}

// GetLoadTrends analyzes load trends for capacity planning
func (lm *LoadMonitor) GetLoadTrends(shardID string) *LoadTrends {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()

	loadInfo, exists := lm.shardMetrics[shardID]
	if !exists {
		return nil
	}

	trends := &LoadTrends{
		ShardID:   shardID,
		Timestamp: time.Now(),
	}

	// Analyze operation trends
	if len(loadInfo.OperationHistory) >= 2 {
		trends.OperationTrend = lm.calculateTrend(loadInfo.OperationHistory)
	}

	// Analyze latency trends
	if len(loadInfo.LatencyHistory) >= 2 {
		trends.LatencyTrend = lm.calculateTrend(loadInfo.LatencyHistory)
	}

	// Predict future utilization
	trends.PredictedUtilization = lm.predictUtilization(loadInfo)

	return trends
}

// LoadTrends contains trend analysis for a shard
type LoadTrends struct {
	ShardID              string    `json:"shard_id"`
	Timestamp            time.Time `json:"timestamp"`
	OperationTrend       float64   `json:"operation_trend"`       // Positive = increasing
	LatencyTrend         float64   `json:"latency_trend"`         // Positive = increasing
	PredictedUtilization float64   `json:"predicted_utilization"` // Predicted utilization in 1 hour
}

// Helper methods

func (lm *LoadMonitor) calculateLoadScore(loadInfo *ShardLoadInfo) float64 {
	// Weighted combination of various metrics
	utilizationWeight := 0.4
	latencyWeight := 0.3
	errorWeight := 0.2
	fragmentationWeight := 0.1

	utilizationScore := loadInfo.UtilizationPercent / 100.0
	latencyScore := loadInfo.AvgLatency / 1000.0 // Normalize to seconds
	errorScore := loadInfo.ErrorRate
	fragmentationScore := loadInfo.FragmentationLevel / 100.0

	return (utilizationScore * utilizationWeight) +
		   (latencyScore * latencyWeight) +
		   (errorScore * errorWeight) +
		   (fragmentationScore * fragmentationWeight)
}

func (lm *LoadMonitor) checkAlertsUnlocked(shardID string, loadInfo *ShardLoadInfo) {
	alerts := make([]*Alert, 0)

	// Check utilization alerts
	if loadInfo.UtilizationPercent > lm.alertThresholds.HighUtilization*100 {
		alerts = append(alerts, &Alert{
			ShardID:   shardID,
			Type:      AlertHighUtilization,
			Severity:  SeverityCritical,
			Message:   "Shard utilization is critically high",
			Timestamp: time.Now(),
			Value:     loadInfo.UtilizationPercent,
			Threshold: lm.alertThresholds.HighUtilization * 100,
		})
	} else if loadInfo.UtilizationPercent < lm.alertThresholds.LowUtilization*100 {
		alerts = append(alerts, &Alert{
			ShardID:   shardID,
			Type:      AlertLowUtilization,
			Severity:  SeverityWarning,
			Message:   "Shard utilization is very low",
			Timestamp: time.Now(),
			Value:     loadInfo.UtilizationPercent,
			Threshold: lm.alertThresholds.LowUtilization * 100,
		})
	}

	// Check latency alerts
	if loadInfo.AvgLatency > lm.alertThresholds.HighLatency {
		alerts = append(alerts, &Alert{
			ShardID:   shardID,
			Type:      AlertHighLatency,
			Severity:  SeverityWarning,
			Message:   "Shard latency is high",
			Timestamp: time.Now(),
			Value:     loadInfo.AvgLatency,
			Threshold: lm.alertThresholds.HighLatency,
		})
	}

	// Check error rate alerts
	if loadInfo.ErrorRate > lm.alertThresholds.HighErrorRate {
		alerts = append(alerts, &Alert{
			ShardID:   shardID,
			Type:      AlertHighErrorRate,
			Severity:  SeverityCritical,
			Message:   "Shard error rate is high",
			Timestamp: time.Now(),
			Value:     loadInfo.ErrorRate,
			Threshold: lm.alertThresholds.HighErrorRate,
		})
	}

	// Check compression alerts
	if loadInfo.CompressionRatio < lm.alertThresholds.LowCompressionRatio {
		alerts = append(alerts, &Alert{
			ShardID:   shardID,
			Type:      AlertLowCompression,
			Severity:  SeverityInfo,
			Message:   "Shard compression ratio is low",
			Timestamp: time.Now(),
			Value:     loadInfo.CompressionRatio,
			Threshold: lm.alertThresholds.LowCompressionRatio,
		})
	}

	// Fire alert callbacks
	for _, alert := range alerts {
		for _, callback := range lm.alertCallbacks {
			go callback(alert) // Fire callbacks asynchronously
		}
	}
}

func (lm *LoadMonitor) calculateTrend(values []float64) float64 {
	if len(values) < 2 {
		return 0.0
	}

	// Simple linear regression to calculate trend
	n := float64(len(values))
	sumX, sumY, sumXY, sumX2 := 0.0, 0.0, 0.0, 0.0

	for i, y := range values {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	// Calculate slope (trend)
	denominator := n*sumX2 - sumX*sumX
	if denominator == 0 {
		return 0.0
	}

	slope := (n*sumXY - sumX*sumY) / denominator
	return slope
}

func (lm *LoadMonitor) predictUtilization(loadInfo *ShardLoadInfo) float64 {
	if len(loadInfo.OperationHistory) < 5 {
		return loadInfo.UtilizationPercent // Not enough data for prediction
	}

	// Use simple trend extrapolation
	trend := lm.calculateTrend(loadInfo.OperationHistory)
	
	// Predict utilization 1 hour from now (assuming 1 sample per minute)
	futureOperations := loadInfo.OperationsPerSec + (trend * 60) // 60 minutes
	
	// Convert to utilization percentage (simplified)
	maxOperationsPerSec := 1000.0 // Assumed maximum
	predictedUtilization := (futureOperations / maxOperationsPerSec) * 100
	
	// Cap at 100%
	if predictedUtilization > 100 {
		predictedUtilization = 100
	}
	
	return predictedUtilization
}