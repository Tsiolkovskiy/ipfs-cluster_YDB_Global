# ZFS Metrics Monitoring and Optimization System

This package provides comprehensive ZFS metrics collection, performance optimization, and capacity planning for IPFS Cluster deployments handling trillion-scale pin operations.

## Overview

The ZFS metrics system consists of four main components:

1. **Metrics Collector** - Real-time collection of ZFS performance metrics
2. **Performance Optimizer** - ML-based automatic optimization with A/B testing
3. **Capacity Planner** - Predictive capacity planning and resource recommendations
4. **Dashboard** - Web-based visualization and monitoring interface

## Features

### Metrics Collection
- **ARC Statistics**: Hit ratio, size, target size
- **Compression Metrics**: Compression ratios for pools and datasets
- **Deduplication Metrics**: Deduplication ratios and savings
- **Fragmentation Monitoring**: Pool fragmentation levels
- **IOPS Monitoring**: Read/write operations per second
- **Historical Tracking**: Configurable retention and trend analysis

### Performance Optimization
- **ML-Based Optimization**: Machine learning models for parameter tuning
- **A/B Testing**: Statistical testing of configuration changes
- **Automatic Rollback**: Safety mechanisms for poor-performing changes
- **Workload Detection**: Automatic detection of read/write patterns
- **Parameter Safety**: Configurable limits on parameter changes

### Capacity Planning
- **Growth Pattern Detection**: Linear, exponential, and seasonal patterns
- **Predictive Analytics**: Multi-horizon capacity predictions
- **Resource Recommendations**: Automated suggestions for capacity expansion
- **Early Warning Alerts**: Configurable thresholds and notifications
- **Cost Optimization**: Recommendations for storage tiering and migration

### Dashboard and Visualization
- **Real-time Metrics**: Live performance monitoring
- **Historical Charts**: Trend visualization and analysis
- **Alert Management**: Alert acknowledgment and tracking
- **Recommendation Tracking**: Implementation status and outcomes
- **Mobile-Friendly**: Responsive design for mobile access

## Quick Start

### Basic Setup

```go
package main

import (
    "time"
    "github.com/ipfs-cluster/ipfs-cluster/monitor/zfsmetrics"
)

func main() {
    // Configure metrics collection
    config := &zfsmetrics.Config{
        PoolName:                 "tank",
        CollectionInterval:       30 * time.Second,
        MetricTTL:               5 * time.Minute,
        EnableARCStats:          true,
        EnableCompressionStats:   true,
        EnableDeduplicationStats: true,
        EnableFragmentationStats: true,
        EnableIOPSMonitoring:    true,
    }
    
    // Create and start metrics collector
    collector, err := zfsmetrics.NewZFSMetricsCollector(config)
    if err != nil {
        panic(err)
    }
    collector.Start()
    defer collector.Stop()
    
    // Create informer for IPFS Cluster integration
    informer, err := zfsmetrics.NewZFSInformer(config)
    if err != nil {
        panic(err)
    }
    defer informer.Shutdown(context.Background())
    
    // Start dashboard
    dashboardConfig := &zfsmetrics.DashboardConfig{
        Port:         8080,
        Title:        "ZFS Metrics Dashboard",
        RefreshRate:  30,
        EnableAlerts: true,
    }
    
    dashboard := zfsmetrics.NewDashboard(collector, informer, dashboardConfig)
    go dashboard.Start()
    
    // Keep running
    select {}
}
```

### Performance Optimization

```go
// Configure performance optimizer
optimizerConfig := &zfsmetrics.OptimizerConfig{
    OptimizationInterval: 1 * time.Hour,
    ABTestDuration:      24 * time.Hour,
    ABTestConfidence:    0.95,
    MaxConcurrentTests:  3,
    MaxParameterChange:  0.5,
    RollbackThreshold:   -0.05,
}

// Create and start optimizer
optimizer, err := zfsmetrics.NewPerformanceOptimizer(optimizerConfig, collector)
if err != nil {
    panic(err)
}

err = optimizer.Start()
if err != nil {
    panic(err)
}
defer optimizer.Stop()

// Get optimization statistics
stats := optimizer.GetOptimizationStats()
fmt.Printf("Optimizations: %d, Success rate: %.1f%%\n", 
    stats["optimization_count"], stats["success_rate"].(float64)*100)
```

### Capacity Planning

```go
// Configure capacity planner
capacityConfig := &zfsmetrics.CapacityPlannerConfig{
    PlanningInterval:    1 * time.Hour,
    DataPointInterval:   15 * time.Minute,
    WarningThreshold:    80.0,
    CriticalThreshold:   90.0,
    GrowthRateThreshold: 10.0,
    MonitoredPools:      []string{"tank", "backup"},
}

// Create and start capacity planner
planner, err := zfsmetrics.NewCapacityPlanner(capacityConfig, collector)
if err != nil {
    panic(err)
}

err = planner.Start()
if err != nil {
    panic(err)
}
defer planner.Stop()

// Get capacity predictions
predictions := planner.GetCapacityPredictions()
for _, pred := range predictions {
    fmt.Printf("Pool %s: %.1f%% full by %s (confidence: %.1f%%)\n",
        pred.ResourceName, pred.UtilizationRate, 
        pred.PredictionDate.Format("2006-01-02"), pred.Confidence*100)
}
```

## Configuration

### Metrics Collector Configuration

```go
type Config struct {
    PoolName                 string        `json:"pool_name"`
    Datasets                 []string      `json:"datasets"`
    CollectionInterval       time.Duration `json:"collection_interval"`
    MetricTTL               time.Duration `json:"metric_ttl"`
    EnableIOPSMonitoring     bool          `json:"enable_iops_monitoring"`
    EnableARCStats          bool          `json:"enable_arc_stats"`
    EnableCompressionStats   bool          `json:"enable_compression_stats"`
    EnableDeduplicationStats bool          `json:"enable_deduplication_stats"`
    EnableFragmentationStats bool          `json:"enable_fragmentation_stats"`
}
```

### Performance Optimizer Configuration

```go
type OptimizerConfig struct {
    OptimizationInterval time.Duration `json:"optimization_interval"`
    MLModelPath          string        `json:"ml_model_path"`
    TrainingDataPath     string        `json:"training_data_path"`
    ABTestDuration       time.Duration `json:"ab_test_duration"`
    ABTestConfidence     float64       `json:"ab_test_confidence"`
    MaxConcurrentTests   int           `json:"max_concurrent_tests"`
    MaxParameterChange   float64       `json:"max_parameter_change"`
    RollbackThreshold    float64       `json:"rollback_threshold"`
}
```

### Capacity Planner Configuration

```go
type CapacityPlannerConfig struct {
    PlanningInterval      time.Duration `json:"planning_interval"`
    PredictionHorizon     time.Duration `json:"prediction_horizon"`
    HistoryRetention      time.Duration `json:"history_retention"`
    WarningThreshold      float64       `json:"warning_threshold"`
    CriticalThreshold     float64       `json:"critical_threshold"`
    GrowthRateThreshold   float64       `json:"growth_rate_threshold"`
    MinHistoryPoints      int           `json:"min_history_points"`
    MonitoredPools        []string      `json:"monitored_pools"`
    MonitoredDatasets     []string      `json:"monitored_datasets"`
}
```

## API Reference

### Metrics Collection

```go
// Get current ZFS metrics
metrics := collector.GetZFSMetrics()

// Get metric history for trend analysis
history := collector.GetMetricHistory("zfs_arc_hit_ratio")

// Get collection statistics
stats := collector.GetCollectionStats()
```

### Performance Optimization

```go
// Get optimization history
history := optimizer.GetOptimizationHistory()

// Get current configuration
config := optimizer.GetCurrentConfiguration()

// Start manual A/B test
testID, err := optimizer.StartABTest("test_compression", testConfig, 24*time.Hour)

// Get A/B test results
results := optimizer.GetABTestHistory()
```

### Capacity Planning

```go
// Get current capacity status
status := planner.GetCapacityStatus()

// Get capacity predictions
predictions := planner.GetCapacityPredictions()

// Get resource recommendations
recommendations := planner.GetResourceRecommendations()

// Get capacity alerts
alerts := planner.GetCapacityAlerts()

// Acknowledge an alert
err := planner.AcknowledgeAlert("alert_id", "admin_user")
```

## Dashboard Access

The web dashboard is available at `http://localhost:8080` (or configured port) and provides:

- **Overview**: Current system status and key metrics
- **Metrics**: Real-time and historical performance data
- **Optimization**: ML model performance and A/B test results
- **Capacity**: Growth predictions and resource recommendations
- **Alerts**: Active alerts and acknowledgment interface

## Monitoring Integration

### IPFS Cluster Integration

The system integrates with IPFS Cluster through the informer interface:

```go
// Register ZFS informer with IPFS Cluster
informer := zfsmetrics.NewZFSInformer(config)
cluster.RegisterInformer(informer)
```

### Prometheus Integration

Export metrics to Prometheus:

```go
// Export ZFS metrics to Prometheus
prometheus.MustRegister(zfsmetrics.NewPrometheusExporter(collector))
```

### Grafana Dashboards

Pre-built Grafana dashboards are available for:
- ZFS performance metrics
- Capacity utilization trends
- Optimization effectiveness
- Alert status and history

## Performance Considerations

### Resource Usage

- **Memory**: ~50MB base + ~1KB per metric per hour of history
- **CPU**: <1% on modern systems during normal operation
- **Disk**: ~10MB per day for metric storage (configurable retention)
- **Network**: Minimal (only ZFS command execution)

### Scalability

- Supports monitoring of multiple pools and datasets
- Horizontal scaling through multiple collector instances
- Efficient metric aggregation and storage
- Configurable collection intervals for performance tuning

### High Availability

- Stateless design allows for easy failover
- Persistent storage of historical data and ML models
- Graceful degradation when ZFS commands fail
- Automatic recovery from temporary issues

## Troubleshooting

### Common Issues

1. **ZFS Commands Failing**
   - Ensure ZFS utilities are installed and accessible
   - Check user permissions for ZFS operations
   - Verify pool and dataset names are correct

2. **High Memory Usage**
   - Reduce history retention period
   - Increase collection intervals
   - Limit number of monitored datasets

3. **Inaccurate Predictions**
   - Ensure sufficient historical data (>30 days recommended)
   - Check for data quality issues
   - Adjust ML model parameters

4. **Dashboard Not Loading**
   - Check port availability and firewall settings
   - Verify dashboard service is running
   - Check browser console for JavaScript errors

### Debug Logging

Enable debug logging for detailed troubleshooting:

```go
import logging "github.com/ipfs/go-log/v2"

logging.SetLogLevel("zfsmetrics", "DEBUG")
logging.SetLogLevel("zfs-optimizer", "DEBUG")
logging.SetLogLevel("zfs-capacity", "DEBUG")
```

## Contributing

Contributions are welcome! Please see the main IPFS Cluster contributing guidelines.

### Development Setup

1. Install ZFS utilities for testing
2. Run tests: `go test ./monitor/zfsmetrics/...`
3. Run integration tests: `go test -tags=integration ./monitor/zfsmetrics/...`
4. Check code coverage: `go test -cover ./monitor/zfsmetrics/...`

### Testing

The package includes comprehensive tests:
- Unit tests for individual components
- Integration tests for system interaction
- Performance benchmarks
- Mock ZFS environments for CI/CD

## License

This project is licensed under the same terms as IPFS Cluster.