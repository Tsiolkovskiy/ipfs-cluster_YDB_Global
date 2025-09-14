package sharding

import "time"

// ShardMetrics contains detailed metrics for a shard
type ShardMetrics struct {
	ShardID            string    `json:"shard_id"`
	Timestamp          time.Time `json:"timestamp"`
	PinCount           int64     `json:"pin_count"`
	UtilizationPercent float64   `json:"utilization_percent"`
	IOPSRead           float64   `json:"iops_read"`
	IOPSWrite          float64   `json:"iops_write"`
	ThroughputRead     float64   `json:"throughput_read"`
	ThroughputWrite    float64   `json:"throughput_write"`
	LatencyAvg         float64   `json:"latency_avg"`
	ErrorRate          float64   `json:"error_rate"`
	CompressionRatio   float64   `json:"compression_ratio"`
	DeduplicationRatio float64   `json:"deduplication_ratio"`
	FragmentationLevel float64   `json:"fragmentation_level"`
}

// AlertThresholds defines thresholds for monitoring alerts
type AlertThresholds struct {
	HighUtilization     float64 `json:"high_utilization"`
	LowUtilization      float64 `json:"low_utilization"`
	HighLatency         float64 `json:"high_latency"`
	HighErrorRate       float64 `json:"high_error_rate"`
	LowCompressionRatio float64 `json:"low_compression_ratio"`
}