package zfsmetrics

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/observations"

	rpc "github.com/libp2p/go-libp2p-gorpc"
	"go.opencensus.io/stats"
	"go.opencensus.io/trace"
)

// ZFSInformer implements the ipfscluster.Informer interface for ZFS metrics
type ZFSInformer struct {
	config    *Config
	collector *ZFSMetricsCollector
	
	mu        sync.Mutex
	rpcClient *rpc.Client
}

// NewZFSInformer creates a new ZFS informer
func NewZFSInformer(cfg *Config) (*ZFSInformer, error) {
	collector, err := NewZFSMetricsCollector(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics collector: %w", err)
	}
	
	informer := &ZFSInformer{
		config:    cfg,
		collector: collector,
	}
	
	// Start metrics collection
	collector.Start()
	
	return informer, nil
}

// Name returns the name of this informer
func (zfi *ZFSInformer) Name() string {
	return "zfs-metrics"
}

// SetClient provides the informer with an RPC client
func (zfi *ZFSInformer) SetClient(c *rpc.Client) {
	zfi.mu.Lock()
	defer zfi.mu.Unlock()
	
	zfi.rpcClient = c
	zfi.collector.SetClient(c)
}

// Shutdown gracefully shuts down the ZFS informer
func (zfi *ZFSInformer) Shutdown(ctx context.Context) error {
	_, span := trace.StartSpan(ctx, "informer/zfs/Shutdown")
	defer span.End()
	
	zfi.mu.Lock()
	defer zfi.mu.Unlock()
	
	// Stop metrics collection
	zfi.collector.Stop()
	
	zfi.rpcClient = nil
	return nil
}

// GetMetrics returns ZFS metrics as IPFS Cluster metrics
func (zfi *ZFSInformer) GetMetrics(ctx context.Context) []api.Metric {
	ctx, span := trace.StartSpan(ctx, "informer/zfs/GetMetrics")
	defer span.End()
	
	zfi.mu.Lock()
	rpcClient := zfi.rpcClient
	zfi.mu.Unlock()
	
	if rpcClient == nil {
		return []api.Metric{
			{
				Name:  zfi.Name(),
				Valid: false,
			},
		}
	}
	
	// Get metrics from collector
	metrics := zfi.collector.GetMetrics(ctx)
	
	// Record metrics for observability
	for _, metric := range metrics {
		if metric.Valid {
			stats.Record(ctx, observations.InformerDisk.M(metric.Weight))
		}
	}
	
	return metrics
}

// GetZFSMetrics returns detailed ZFS metrics
func (zfi *ZFSInformer) GetZFSMetrics() []ZFSMetric {
	return zfi.collector.GetZFSMetrics()
}

// GetMetricHistory returns historical data for a specific metric
func (zfi *ZFSInformer) GetMetricHistory(metricName string) *MetricHistory {
	return zfi.collector.GetMetricHistory(metricName)
}

// GetCollectionStats returns statistics about metrics collection
func (zfi *ZFSInformer) GetCollectionStats() map[string]interface{} {
	return zfi.collector.GetCollectionStats()
}

// CreateSnapshot creates a ZFS snapshot for metrics consistency
func (zfi *ZFSInformer) CreateSnapshot(snapshotName string) error {
	// This would integrate with the ZFS snapshot functionality
	// For now, return a placeholder implementation
	logger.Infof("Creating ZFS snapshot: %s", snapshotName)
	return nil
}

// GetPoolHealth returns the health status of the ZFS pool
func (zfi *ZFSInformer) GetPoolHealth() (string, error) {
	// This would check ZFS pool health
	// For now, return a placeholder implementation
	return "ONLINE", nil
}

// GetDatasetInfo returns information about monitored datasets
func (zfi *ZFSInformer) GetDatasetInfo() map[string]interface{} {
	return map[string]interface{}{
		"pool_name":          zfi.config.PoolName,
		"monitored_datasets": zfi.config.Datasets,
		"enabled_metrics":    zfi.config.GetEnabledMetrics(),
	}
}