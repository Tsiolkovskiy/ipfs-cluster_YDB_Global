// Package zfsmetrics implements a ZFS-specific metrics collector for IPFS Cluster.
// It collects ZFS performance metrics like ARC hit ratio, compression ratio, IOPS,
// and integrates with IPFS Cluster informers for comprehensive monitoring.
package zfsmetrics

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/monitor/metrics"

	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	rpc "github.com/libp2p/go-libp2p-gorpc"
)

var logger = logging.Logger("zfsmetrics")

// ZFSMetricsCollector collects ZFS-specific performance metrics
type ZFSMetricsCollector struct {
	config    *Config
	rpcClient *rpc.Client
	store     *metrics.Store
	
	// Metrics collection state
	mu              sync.RWMutex
	lastCollection  time.Time
	collectionCount int64
	
	// ZFS system information
	poolName     string
	datasets     []string
	
	// Performance tracking
	metricsHistory map[string]*MetricHistory
	
	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
	
	// Collection ticker
	ticker *time.Ticker
}

// Config holds configuration for ZFS metrics collector
type Config struct {
	// ZFS pool name to monitor
	PoolName string `json:"pool_name"`
	
	// Datasets to monitor (empty means all)
	Datasets []string `json:"datasets"`
	
	// Collection interval
	CollectionInterval time.Duration `json:"collection_interval"`
	
	// Metric TTL
	MetricTTL time.Duration `json:"metric_ttl"`
	
	// Enable detailed IOPS monitoring
	EnableIOPSMonitoring bool `json:"enable_iops_monitoring"`
	
	// Enable ARC statistics
	EnableARCStats bool `json:"enable_arc_stats"`
	
	// Enable compression monitoring
	EnableCompressionStats bool `json:"enable_compression_stats"`
	
	// Enable deduplication monitoring
	EnableDeduplicationStats bool `json:"enable_deduplication_stats"`
	
	// Enable fragmentation monitoring
	EnableFragmentationStats bool `json:"enable_fragmentation_stats"`
}

// MetricHistory tracks historical values for trend analysis
type MetricHistory struct {
	Values    []float64
	Timestamps []time.Time
	MaxSize   int
	mu        sync.RWMutex
}

// ZFSMetric represents a ZFS-specific metric
type ZFSMetric struct {
	api.Metric
	
	// ZFS-specific fields
	PoolName    string  `json:"pool_name"`
	Dataset     string  `json:"dataset,omitempty"`
	MetricType  string  `json:"metric_type"`
	RawValue    string  `json:"raw_value"`
	Unit        string  `json:"unit"`
	Trend       string  `json:"trend,omitempty"` // "increasing", "decreasing", "stable"
}

// NewZFSMetricsCollector creates a new ZFS metrics collector
func NewZFSMetricsCollector(cfg *Config) (*ZFSMetricsCollector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	collector := &ZFSMetricsCollector{
		config:         cfg,
		store:          metrics.NewStore(),
		poolName:       cfg.PoolName,
		datasets:       cfg.Datasets,
		metricsHistory: make(map[string]*MetricHistory),
		ctx:            ctx,
		cancel:         cancel,
	}
	
	// Initialize metric histories
	collector.initializeMetricHistories()
	
	// Discover datasets if not specified
	if len(collector.datasets) == 0 {
		datasets, err := collector.discoverDatasets()
		if err != nil {
			logger.Warnf("Failed to discover datasets: %s", err)
		} else {
			collector.datasets = datasets
		}
	}
	
	return collector, nil
}

// Start begins metrics collection
func (zmc *ZFSMetricsCollector) Start() {
	interval := zmc.config.CollectionInterval
	if interval == 0 {
		interval = 30 * time.Second // Default to 30 seconds
	}
	
	zmc.ticker = time.NewTicker(interval)
	
	go zmc.collectionLoop()
	
	logger.Infof("Started ZFS metrics collection for pool %s with interval %v", 
		zmc.poolName, interval)
}

// Stop stops metrics collection
func (zmc *ZFSMetricsCollector) Stop() {
	if zmc.ticker != nil {
		zmc.ticker.Stop()
	}
	zmc.cancel()
	
	logger.Info("Stopped ZFS metrics collection")
}

// SetClient sets the RPC client for cluster integration
func (zmc *ZFSMetricsCollector) SetClient(c *rpc.Client) {
	zmc.mu.Lock()
	defer zmc.mu.Unlock()
	zmc.rpcClient = c
}

// GetMetrics returns current ZFS metrics as IPFS Cluster metrics
func (zmc *ZFSMetricsCollector) GetMetrics(ctx context.Context) []api.Metric {
	zmc.mu.RLock()
	defer zmc.mu.RUnlock()
	
	var clusterMetrics []api.Metric
	
	// Collect all ZFS metrics
	zfsMetrics := zmc.collectAllMetrics()
	
	// Convert to cluster metrics format
	for _, zfsMetric := range zfsMetrics {
		clusterMetric := api.Metric{
			Name:          zfsMetric.Name,
			Value:         zfsMetric.Value,
			Valid:         zfsMetric.Valid,
			Weight:        zfsMetric.Weight,
			Partitionable: false, // ZFS metrics are not partitionable
		}
		
		clusterMetric.SetTTL(zmc.config.MetricTTL)
		clusterMetrics = append(clusterMetrics, clusterMetric)
	}
	
	return clusterMetrics
}

// GetZFSMetrics returns detailed ZFS metrics
func (zmc *ZFSMetricsCollector) GetZFSMetrics() []ZFSMetric {
	zmc.mu.RLock()
	defer zmc.mu.RUnlock()
	
	return zmc.collectAllMetrics()
}

// GetMetricHistory returns historical data for a specific metric
func (zmc *ZFSMetricsCollector) GetMetricHistory(metricName string) *MetricHistory {
	zmc.mu.RLock()
	defer zmc.mu.RUnlock()
	
	history, exists := zmc.metricsHistory[metricName]
	if !exists {
		return nil
	}
	
	// Return a copy to avoid race conditions
	history.mu.RLock()
	defer history.mu.RUnlock()
	
	return &MetricHistory{
		Values:     append([]float64(nil), history.Values...),
		Timestamps: append([]time.Time(nil), history.Timestamps...),
		MaxSize:    history.MaxSize,
	}
}//
 collectionLoop runs the main metrics collection loop
func (zmc *ZFSMetricsCollector) collectionLoop() {
	for {
		select {
		case <-zmc.ticker.C:
			zmc.performCollection()
		case <-zmc.ctx.Done():
			return
		}
	}
}

// performCollection collects all enabled metrics
func (zmc *ZFSMetricsCollector) performCollection() {
	start := time.Now()
	
	zmc.mu.Lock()
	zmc.collectionCount++
	zmc.lastCollection = start
	zmc.mu.Unlock()
	
	// Collect metrics based on configuration
	var metrics []ZFSMetric
	
	if zmc.config.EnableARCStats {
		if arcMetrics, err := zmc.collectARCMetrics(); err == nil {
			metrics = append(metrics, arcMetrics...)
		} else {
			logger.Warnf("Failed to collect ARC metrics: %s", err)
		}
	}
	
	if zmc.config.EnableCompressionStats {
		if compressionMetrics, err := zmc.collectCompressionMetrics(); err == nil {
			metrics = append(metrics, compressionMetrics...)
		} else {
			logger.Warnf("Failed to collect compression metrics: %s", err)
		}
	}
	
	if zmc.config.EnableDeduplicationStats {
		if dedupMetrics, err := zmc.collectDeduplicationMetrics(); err == nil {
			metrics = append(metrics, dedupMetrics...)
		} else {
			logger.Warnf("Failed to collect deduplication metrics: %s", err)
		}
	}
	
	if zmc.config.EnableFragmentationStats {
		if fragMetrics, err := zmc.collectFragmentationMetrics(); err == nil {
			metrics = append(metrics, fragMetrics...)
		} else {
			logger.Warnf("Failed to collect fragmentation metrics: %s", err)
		}
	}
	
	if zmc.config.EnableIOPSMonitoring {
		if iopsMetrics, err := zmc.collectIOPSMetrics(); err == nil {
			metrics = append(metrics, iopsMetrics...)
		} else {
			logger.Warnf("Failed to collect IOPS metrics: %s", err)
		}
	}
	
	// Store metrics in history and cluster store
	for _, metric := range metrics {
		zmc.updateMetricHistory(metric)
		
		// Convert to cluster metric and store
		clusterMetric := api.Metric{
			Name:          metric.Name,
			Value:         metric.Value,
			Valid:         metric.Valid,
			Weight:        metric.Weight,
			Partitionable: false,
		}
		clusterMetric.SetTTL(zmc.config.MetricTTL)
		
		zmc.store.Add(clusterMetric)
	}
	
	duration := time.Since(start)
	logger.Debugf("Collected %d ZFS metrics in %v", len(metrics), duration)
}

// collectAllMetrics collects all current ZFS metrics
func (zmc *ZFSMetricsCollector) collectAllMetrics() []ZFSMetric {
	var allMetrics []ZFSMetric
	
	if zmc.config.EnableARCStats {
		if metrics, err := zmc.collectARCMetrics(); err == nil {
			allMetrics = append(allMetrics, metrics...)
		}
	}
	
	if zmc.config.EnableCompressionStats {
		if metrics, err := zmc.collectCompressionMetrics(); err == nil {
			allMetrics = append(allMetrics, metrics...)
		}
	}
	
	if zmc.config.EnableDeduplicationStats {
		if metrics, err := zmc.collectDeduplicationMetrics(); err == nil {
			allMetrics = append(allMetrics, metrics...)
		}
	}
	
	if zmc.config.EnableFragmentationStats {
		if metrics, err := zmc.collectFragmentationMetrics(); err == nil {
			allMetrics = append(allMetrics, metrics...)
		}
	}
	
	if zmc.config.EnableIOPSMonitoring {
		if metrics, err := zmc.collectIOPSMetrics(); err == nil {
			allMetrics = append(allMetrics, metrics...)
		}
	}
	
	return allMetrics
}

// collectARCMetrics collects ZFS ARC (Adaptive Replacement Cache) metrics
func (zmc *ZFSMetricsCollector) collectARCMetrics() ([]ZFSMetric, error) {
	cmd := exec.Command("cat", "/proc/spl/kstat/zfs/arcstats")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to read ARC stats: %w", err)
	}
	
	lines := strings.Split(string(output), "\n")
	arcStats := make(map[string]int64)
	
	// Parse ARC statistics
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			if value, err := strconv.ParseInt(fields[2], 10, 64); err == nil {
				arcStats[fields[0]] = value
			}
		}
	}
	
	var metrics []ZFSMetric
	
	// Calculate ARC hit ratio
	hits := arcStats["hits"]
	misses := arcStats["misses"]
	total := hits + misses
	
	if total > 0 {
		hitRatio := float64(hits) / float64(total) * 100
		metrics = append(metrics, ZFSMetric{
			Metric: api.Metric{
				Name:   "zfs_arc_hit_ratio",
				Value:  fmt.Sprintf("%.2f", hitRatio),
				Valid:  true,
				Weight: int64(hitRatio * 1000), // Higher hit ratio = higher weight
			},
			PoolName:   zmc.poolName,
			MetricType: "arc_performance",
			RawValue:   fmt.Sprintf("%d/%d", hits, total),
			Unit:       "percentage",
		})
	}
	
	// ARC size metrics
	if arcSize, exists := arcStats["size"]; exists {
		metrics = append(metrics, ZFSMetric{
			Metric: api.Metric{
				Name:   "zfs_arc_size",
				Value:  fmt.Sprintf("%d", arcSize),
				Valid:  true,
				Weight: -arcSize, // Smaller ARC size = higher weight (more available memory)
			},
			PoolName:   zmc.poolName,
			MetricType: "arc_memory",
			RawValue:   fmt.Sprintf("%d", arcSize),
			Unit:       "bytes",
		})
	}
	
	// ARC target size
	if arcTargetSize, exists := arcStats["c"]; exists {
		metrics = append(metrics, ZFSMetric{
			Metric: api.Metric{
				Name:   "zfs_arc_target_size",
				Value:  fmt.Sprintf("%d", arcTargetSize),
				Valid:  true,
				Weight: -arcTargetSize,
			},
			PoolName:   zmc.poolName,
			MetricType: "arc_memory",
			RawValue:   fmt.Sprintf("%d", arcTargetSize),
			Unit:       "bytes",
		})
	}
	
	return metrics, nil
}

// collectCompressionMetrics collects ZFS compression metrics
func (zmc *ZFSMetricsCollector) collectCompressionMetrics() ([]ZFSMetric, error) {
	var metrics []ZFSMetric
	
	// Collect compression ratio for pool
	cmd := exec.Command("zfs", "get", "-H", "-p", "compressratio", zmc.poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get compression ratio: %w", err)
	}
	
	fields := strings.Fields(string(output))
	if len(fields) >= 3 {
		ratioStr := strings.TrimSuffix(fields[2], "x")
		if ratio, err := strconv.ParseFloat(ratioStr, 64); err == nil {
			metrics = append(metrics, ZFSMetric{
				Metric: api.Metric{
					Name:   "zfs_compression_ratio",
					Value:  fmt.Sprintf("%.2f", ratio),
					Valid:  true,
					Weight: int64(ratio * 1000), // Higher compression = higher weight
				},
				PoolName:   zmc.poolName,
				MetricType: "compression",
				RawValue:   ratioStr,
				Unit:       "ratio",
			})
		}
	}
	
	// Collect compression ratios for individual datasets
	for _, dataset := range zmc.datasets {
		cmd := exec.Command("zfs", "get", "-H", "-p", "compressratio", dataset)
		output, err := cmd.Output()
		if err != nil {
			continue // Skip failed datasets
		}
		
		fields := strings.Fields(string(output))
		if len(fields) >= 3 {
			ratioStr := strings.TrimSuffix(fields[2], "x")
			if ratio, err := strconv.ParseFloat(ratioStr, 64); err == nil {
				metrics = append(metrics, ZFSMetric{
					Metric: api.Metric{
						Name:   fmt.Sprintf("zfs_dataset_compression_ratio_%s", strings.ReplaceAll(dataset, "/", "_")),
						Value:  fmt.Sprintf("%.2f", ratio),
						Valid:  true,
						Weight: int64(ratio * 1000),
					},
					PoolName:   zmc.poolName,
					Dataset:    dataset,
					MetricType: "compression",
					RawValue:   ratioStr,
					Unit:       "ratio",
				})
			}
		}
	}
	
	return metrics, nil
}

// collectDeduplicationMetrics collects ZFS deduplication metrics
func (zmc *ZFSMetricsCollector) collectDeduplicationMetrics() ([]ZFSMetric, error) {
	var metrics []ZFSMetric
	
	// Get deduplication ratio for pool
	cmd := exec.Command("zpool", "get", "-H", "-p", "dedupratio", zmc.poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get deduplication ratio: %w", err)
	}
	
	fields := strings.Fields(string(output))
	if len(fields) >= 3 {
		ratioStr := strings.TrimSuffix(fields[2], "x")
		if ratio, err := strconv.ParseFloat(ratioStr, 64); err == nil {
			metrics = append(metrics, ZFSMetric{
				Metric: api.Metric{
					Name:   "zfs_deduplication_ratio",
					Value:  fmt.Sprintf("%.2f", ratio),
					Valid:  true,
					Weight: int64(ratio * 1000), // Higher dedup ratio = higher weight
				},
				PoolName:   zmc.poolName,
				MetricType: "deduplication",
				RawValue:   ratioStr,
				Unit:       "ratio",
			})
		}
	}
	
	return metrics, nil
}

// collectFragmentationMetrics collects ZFS fragmentation metrics
func (zmc *ZFSMetricsCollector) collectFragmentationMetrics() ([]ZFSMetric, error) {
	var metrics []ZFSMetric
	
	// Get fragmentation level for pool
	cmd := exec.Command("zpool", "get", "-H", "-p", "fragmentation", zmc.poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get fragmentation level: %w", err)
	}
	
	fields := strings.Fields(string(output))
	if len(fields) >= 3 {
		fragStr := strings.TrimSuffix(fields[2], "%")
		if frag, err := strconv.ParseFloat(fragStr, 64); err == nil {
			metrics = append(metrics, ZFSMetric{
				Metric: api.Metric{
					Name:   "zfs_fragmentation_level",
					Value:  fmt.Sprintf("%.2f", frag),
					Valid:  true,
					Weight: -int64(frag * 1000), // Lower fragmentation = higher weight
				},
				PoolName:   zmc.poolName,
				MetricType: "fragmentation",
				RawValue:   fragStr,
				Unit:       "percentage",
			})
		}
	}
	
	return metrics, nil
}

// collectIOPSMetrics collects ZFS IOPS metrics
func (zmc *ZFSMetricsCollector) collectIOPSMetrics() ([]ZFSMetric, error) {
	var metrics []ZFSMetric
	
	// Get pool I/O statistics
	cmd := exec.Command("zpool", "iostat", "-v", zmc.poolName, "1", "2")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get IOPS metrics: %w", err)
	}
	
	lines := strings.Split(string(output), "\n")
	
	// Parse iostat output (simplified parsing)
	for i, line := range lines {
		if strings.Contains(line, zmc.poolName) && i+1 < len(lines) {
			// The next line should contain the actual statistics
			statsLine := lines[i+1]
			fields := strings.Fields(statsLine)
			
			if len(fields) >= 6 {
				// Parse read and write operations per second
				if readOps, err := strconv.ParseFloat(fields[4], 64); err == nil {
					metrics = append(metrics, ZFSMetric{
						Metric: api.Metric{
							Name:   "zfs_read_iops",
							Value:  fmt.Sprintf("%.2f", readOps),
							Valid:  true,
							Weight: int64(readOps),
						},
						PoolName:   zmc.poolName,
						MetricType: "iops",
						RawValue:   fields[4],
						Unit:       "ops/sec",
					})
				}
				
				if writeOps, err := strconv.ParseFloat(fields[5], 64); err == nil {
					metrics = append(metrics, ZFSMetric{
						Metric: api.Metric{
							Name:   "zfs_write_iops",
							Value:  fmt.Sprintf("%.2f", writeOps),
							Valid:  true,
							Weight: int64(writeOps),
						},
						PoolName:   zmc.poolName,
						MetricType: "iops",
						RawValue:   fields[5],
						Unit:       "ops/sec",
					})
				}
			}
			break
		}
	}
	
	return metrics, nil
}

// updateMetricHistory updates the historical data for a metric
func (zmc *ZFSMetricsCollector) updateMetricHistory(metric ZFSMetric) {
	history, exists := zmc.metricsHistory[metric.Name]
	if !exists {
		history = &MetricHistory{
			MaxSize: 100, // Keep last 100 values
		}
		zmc.metricsHistory[metric.Name] = history
	}
	
	history.mu.Lock()
	defer history.mu.Unlock()
	
	// Parse metric value
	if value, err := strconv.ParseFloat(metric.Value, 64); err == nil {
		history.Values = append(history.Values, value)
		history.Timestamps = append(history.Timestamps, time.Now())
		
		// Trim to max size
		if len(history.Values) > history.MaxSize {
			history.Values = history.Values[1:]
			history.Timestamps = history.Timestamps[1:]
		}
	}
}

// initializeMetricHistories initializes metric history tracking
func (zmc *ZFSMetricsCollector) initializeMetricHistories() {
	metricNames := []string{
		"zfs_arc_hit_ratio",
		"zfs_arc_size",
		"zfs_arc_target_size",
		"zfs_compression_ratio",
		"zfs_deduplication_ratio",
		"zfs_fragmentation_level",
		"zfs_read_iops",
		"zfs_write_iops",
	}
	
	for _, name := range metricNames {
		zmc.metricsHistory[name] = &MetricHistory{
			MaxSize: 100,
		}
	}
}

// discoverDatasets discovers all datasets in the ZFS pool
func (zmc *ZFSMetricsCollector) discoverDatasets() ([]string, error) {
	cmd := exec.Command("zfs", "list", "-H", "-o", "name", "-r", zmc.poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list datasets: %w", err)
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var datasets []string
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && line != zmc.poolName {
			datasets = append(datasets, line)
		}
	}
	
	return datasets, nil
}

// GetCollectionStats returns statistics about metrics collection
func (zmc *ZFSMetricsCollector) GetCollectionStats() map[string]interface{} {
	zmc.mu.RLock()
	defer zmc.mu.RUnlock()
	
	return map[string]interface{}{
		"collection_count":    zmc.collectionCount,
		"last_collection":     zmc.lastCollection,
		"pool_name":          zmc.poolName,
		"monitored_datasets": len(zmc.datasets),
		"metric_histories":   len(zmc.metricsHistory),
	}
}