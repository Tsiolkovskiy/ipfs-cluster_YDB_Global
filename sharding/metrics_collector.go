package sharding

import (
	"fmt"
	"sync"
	"time"
)

// MetricsCollector collects and aggregates shard metrics for analysis
type MetricsCollector struct {
	metrics         map[string]*ShardMetrics
	historicalData  map[string][]*ShardMetrics
	mutex           sync.RWMutex
	maxHistorySize  int
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics:        make(map[string]*ShardMetrics),
		historicalData: make(map[string][]*ShardMetrics),
		maxHistorySize: 1440, // Keep 24 hours of data (1 sample per minute)
	}
}

// UpdateMetrics updates metrics for a shard
func (mc *MetricsCollector) UpdateMetrics(shardID string, metrics *ShardMetrics) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Update current metrics
	mc.metrics[shardID] = metrics

	// Add to historical data
	if mc.historicalData[shardID] == nil {
		mc.historicalData[shardID] = make([]*ShardMetrics, 0, mc.maxHistorySize)
	}

	mc.historicalData[shardID] = append(mc.historicalData[shardID], metrics)

	// Trim historical data if it exceeds max size
	if len(mc.historicalData[shardID]) > mc.maxHistorySize {
		mc.historicalData[shardID] = mc.historicalData[shardID][1:]
	}
}

// GetMetrics returns current metrics for a shard
func (mc *MetricsCollector) GetMetrics(shardID string) (*ShardMetrics, error) {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	metrics, exists := mc.metrics[shardID]
	if !exists {
		return nil, fmt.Errorf("metrics not found for shard %s", shardID)
	}

	// Return a copy
	metricsCopy := *metrics
	return &metricsCopy, nil
}

// GetAllMetrics returns current metrics for all shards
func (mc *MetricsCollector) GetAllMetrics() map[string]*ShardMetrics {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	result := make(map[string]*ShardMetrics)
	for shardID, metrics := range mc.metrics {
		metricsCopy := *metrics
		result[shardID] = &metricsCopy
	}

	return result
}

// GetHistoricalMetrics returns historical metrics for a shard
func (mc *MetricsCollector) GetHistoricalMetrics(shardID string, duration time.Duration) ([]*ShardMetrics, error) {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	history, exists := mc.historicalData[shardID]
	if !exists {
		return nil, fmt.Errorf("no historical data for shard %s", shardID)
	}

	cutoff := time.Now().Add(-duration)
	var result []*ShardMetrics

	for _, metrics := range history {
		if metrics.Timestamp.After(cutoff) {
			metricsCopy := *metrics
			result = append(result, &metricsCopy)
		}
	}

	return result, nil
}

// GetAggregatedMetrics returns aggregated metrics across all shards
func (mc *MetricsCollector) GetAggregatedMetrics() *AggregatedMetrics {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	if len(mc.metrics) == 0 {
		return &AggregatedMetrics{Timestamp: time.Now()}
	}

	agg := &AggregatedMetrics{
		Timestamp:   time.Now(),
		ShardCount:  len(mc.metrics),
	}

	var totalPins int64
	var totalUtilization, totalIOPSRead, totalIOPSWrite float64
	var totalThroughputRead, totalThroughputWrite, totalLatency float64
	var totalErrorRate, totalCompression, totalDeduplication float64

	for _, metrics := range mc.metrics {
		totalPins += metrics.PinCount
		totalUtilization += metrics.UtilizationPercent
		totalIOPSRead += metrics.IOPSRead
		totalIOPSWrite += metrics.IOPSWrite
		totalThroughputRead += metrics.ThroughputRead
		totalThroughputWrite += metrics.ThroughputWrite
		totalLatency += metrics.LatencyAvg
		totalErrorRate += metrics.ErrorRate
		totalCompression += metrics.CompressionRatio
		totalDeduplication += metrics.DeduplicationRatio
	}

	shardCount := float64(len(mc.metrics))
	agg.TotalPinCount = totalPins
	agg.AvgUtilization = totalUtilization / shardCount
	agg.TotalIOPSRead = totalIOPSRead
	agg.TotalIOPSWrite = totalIOPSWrite
	agg.TotalThroughputRead = totalThroughputRead
	agg.TotalThroughputWrite = totalThroughputWrite
	agg.AvgLatency = totalLatency / shardCount
	agg.AvgErrorRate = totalErrorRate / shardCount
	agg.AvgCompressionRatio = totalCompression / shardCount
	agg.AvgDeduplicationRatio = totalDeduplication / shardCount

	return agg
}

// AggregatedMetrics contains system-wide aggregated metrics
type AggregatedMetrics struct {
	Timestamp              time.Time `json:"timestamp"`
	ShardCount             int       `json:"shard_count"`
	TotalPinCount          int64     `json:"total_pin_count"`
	AvgUtilization         float64   `json:"avg_utilization"`
	TotalIOPSRead          float64   `json:"total_iops_read"`
	TotalIOPSWrite         float64   `json:"total_iops_write"`
	TotalThroughputRead    float64   `json:"total_throughput_read"`
	TotalThroughputWrite   float64   `json:"total_throughput_write"`
	AvgLatency             float64   `json:"avg_latency"`
	AvgErrorRate           float64   `json:"avg_error_rate"`
	AvgCompressionRatio    float64   `json:"avg_compression_ratio"`
	AvgDeduplicationRatio  float64   `json:"avg_deduplication_ratio"`
}

// RemoveShardMetrics removes all metrics for a shard
func (mc *MetricsCollector) RemoveShardMetrics(shardID string) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	delete(mc.metrics, shardID)
	delete(mc.historicalData, shardID)
}

// GetPerformanceSummary returns a performance summary for analysis
func (mc *MetricsCollector) GetPerformanceSummary(duration time.Duration) *PerformanceSummary {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	summary := &PerformanceSummary{
		Timestamp: time.Now(),
		Duration:  duration,
		ShardSummaries: make(map[string]*ShardPerformanceSummary),
	}

	cutoff := time.Now().Add(-duration)

	for shardID, history := range mc.historicalData {
		shardSummary := &ShardPerformanceSummary{
			ShardID: shardID,
		}

		var samples []*ShardMetrics
		for _, metrics := range history {
			if metrics.Timestamp.After(cutoff) {
				samples = append(samples, metrics)
			}
		}

		if len(samples) > 0 {
			shardSummary.SampleCount = len(samples)
			shardSummary.calculateSummaryStats(samples)
		}

		summary.ShardSummaries[shardID] = shardSummary
	}

	return summary
}

// PerformanceSummary contains performance analysis over a time period
type PerformanceSummary struct {
	Timestamp      time.Time                           `json:"timestamp"`
	Duration       time.Duration                       `json:"duration"`
	ShardSummaries map[string]*ShardPerformanceSummary `json:"shard_summaries"`
}

// ShardPerformanceSummary contains performance summary for a single shard
type ShardPerformanceSummary struct {
	ShardID     string  `json:"shard_id"`
	SampleCount int     `json:"sample_count"`
	
	// Utilization stats
	MinUtilization float64 `json:"min_utilization"`
	MaxUtilization float64 `json:"max_utilization"`
	AvgUtilization float64 `json:"avg_utilization"`
	
	// Latency stats
	MinLatency float64 `json:"min_latency"`
	MaxLatency float64 `json:"max_latency"`
	AvgLatency float64 `json:"avg_latency"`
	
	// Throughput stats
	MinThroughput float64 `json:"min_throughput"`
	MaxThroughput float64 `json:"max_throughput"`
	AvgThroughput float64 `json:"avg_throughput"`
	
	// Error stats
	MinErrorRate float64 `json:"min_error_rate"`
	MaxErrorRate float64 `json:"max_error_rate"`
	AvgErrorRate float64 `json:"avg_error_rate"`
}

func (sps *ShardPerformanceSummary) calculateSummaryStats(samples []*ShardMetrics) {
	if len(samples) == 0 {
		return
	}

	// Initialize with first sample
	first := samples[0]
	sps.MinUtilization = first.UtilizationPercent
	sps.MaxUtilization = first.UtilizationPercent
	sps.MinLatency = first.LatencyAvg
	sps.MaxLatency = first.LatencyAvg
	sps.MinThroughput = first.ThroughputRead + first.ThroughputWrite
	sps.MaxThroughput = first.ThroughputRead + first.ThroughputWrite
	sps.MinErrorRate = first.ErrorRate
	sps.MaxErrorRate = first.ErrorRate

	var sumUtilization, sumLatency, sumThroughput, sumErrorRate float64

	for _, sample := range samples {
		// Utilization
		if sample.UtilizationPercent < sps.MinUtilization {
			sps.MinUtilization = sample.UtilizationPercent
		}
		if sample.UtilizationPercent > sps.MaxUtilization {
			sps.MaxUtilization = sample.UtilizationPercent
		}
		sumUtilization += sample.UtilizationPercent

		// Latency
		if sample.LatencyAvg < sps.MinLatency {
			sps.MinLatency = sample.LatencyAvg
		}
		if sample.LatencyAvg > sps.MaxLatency {
			sps.MaxLatency = sample.LatencyAvg
		}
		sumLatency += sample.LatencyAvg

		// Throughput
		throughput := sample.ThroughputRead + sample.ThroughputWrite
		if throughput < sps.MinThroughput {
			sps.MinThroughput = throughput
		}
		if throughput > sps.MaxThroughput {
			sps.MaxThroughput = throughput
		}
		sumThroughput += throughput

		// Error rate
		if sample.ErrorRate < sps.MinErrorRate {
			sps.MinErrorRate = sample.ErrorRate
		}
		if sample.ErrorRate > sps.MaxErrorRate {
			sps.MaxErrorRate = sample.ErrorRate
		}
		sumErrorRate += sample.ErrorRate
	}

	// Calculate averages
	count := float64(len(samples))
	sps.AvgUtilization = sumUtilization / count
	sps.AvgLatency = sumLatency / count
	sps.AvgThroughput = sumThroughput / count
	sps.AvgErrorRate = sumErrorRate / count
}