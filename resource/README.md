# Resource Management Package

This package provides comprehensive resource management for IPFS Cluster ZFS integration, supporting trillion-scale pin operations through automated resource management, tiered storage, and maintenance operations.

## Overview

The resource management package consists of three main components:

1. **Resource Manager** - Automated ZFS pool and disk management
2. **Tiered Storage Manager** - Intelligent data placement across storage tiers
3. **Automated Maintenance** - Scheduled ZFS maintenance operations

## Components

### Resource Manager (`manager.go`)

Manages ZFS pools and disks automatically:

- **Automatic disk addition** to pools when capacity thresholds are reached
- **Health monitoring** of disks with SMART data analysis
- **Performance optimization** through layout analysis
- **Automatic replacement** of failed disks

#### Key Features:

- Capacity threshold monitoring (default 80%)
- Disk health monitoring with temperature and SMART attributes
- Automatic pool expansion when needed
- Layout optimization for maximum performance
- Failed disk detection and replacement

#### Usage:

```go
config := resource.DefaultConfig()
rm, err := resource.NewResourceManager(config)
if err != nil {
    log.Fatal(err)
}

ctx := context.Background()
if err := rm.Start(ctx); err != nil {
    log.Fatal(err)
}

// Add disk to pool
err = rm.AddDiskToPool(ctx, "main-pool", "/dev/sdb")

// Get pool information
poolInfo, err := rm.GetPoolInfo("main-pool")
```

### Tiered Storage Manager (`tiered_storage.go`)

Implements intelligent data placement across storage tiers:

- **Hot tier** - NVMe SSDs for frequently accessed data
- **Warm tier** - SATA SSDs for moderately accessed data  
- **Cold tier** - HDDs for infrequently accessed data
- **Archive tier** - Tape/object storage for long-term retention

#### Key Features:

- **ML-based access prediction** using historical patterns
- **Automatic data migration** between tiers based on access patterns
- **Cost optimization** to minimize storage costs
- **Performance optimization** to maximize access speed

#### Usage:

```go
config := &resource.TieredConfig{
    Tiers: []resource.TierDefinition{
        {
            Name: "hot",
            Type: "hot", 
            MediaType: "nvme",
            StorageCost: 0.10, // $0.10/GB/month
            // ... other properties
        },
        // ... other tiers
    },
    // ... other config
}

tsm, err := resource.NewTieredStorageManager(config)
if err != nil {
    log.Fatal(err)
}

// Add data item
err = tsm.AddDataItem(ctx, "QmHash123", 1024*1024) // 1MB

// Record access
err = tsm.RecordAccess("QmHash123", 512)

// Get metrics
metrics := tsm.GetTierMetrics()
```

### Automated Maintenance (`automated_maintenance.go`)

Provides scheduled ZFS maintenance operations:

- **Automatic scrubbing** to detect and repair data corruption
- **Defragmentation** to optimize storage layout
- **Repair operations** for failed devices and pools
- **Scheduled maintenance** during low-usage windows

#### Key Features:

- **Configurable maintenance windows** to minimize impact
- **Performance impact limiting** to maintain system responsiveness
- **Automatic error detection and repair**
- **Progress monitoring** for all operations

#### Usage:

```go
config := &resource.MaintenanceConfig{
    ScrubInterval: 7 * 24 * time.Hour, // Weekly scrubs
    DefragThreshold: 0.3, // 30% fragmentation threshold
    MaintenanceWindow: resource.TimeWindow{
        StartHour: 2, // 2 AM
        EndHour: 6,   // 6 AM
        Days: []int{0, 6}, // Weekends only
    },
    // ... other config
}

am, err := resource.NewAutomatedMaintenance(config)
if err != nil {
    log.Fatal(err)
}

// Schedule operations
scrubOp, err := am.ScheduleScrub("main-pool", 2)
defragOp, err := am.ScheduleDefragmentation("main-pool", 3)

// Get operation status
status, err := am.GetOperationStatus(scrubOp.ID)
```

## Configuration

### Resource Manager Configuration

```go
config := &resource.Config{
    AutoExpandPools: true,
    CapacityThreshold: 0.8, // 80%
    MinFreeSpace: 100 * 1024 * 1024 * 1024, // 100GB
    AutoAddDisks: true,
    DiskScanInterval: 5 * time.Minute,
    HealthCheckInterval: 1 * time.Minute,
    AutoOptimizeLayout: true,
    OptimizationInterval: 1 * time.Hour,
    AutoReplaceFailed: true,
    AlertThresholds: resource.AlertThresholds{
        CapacityWarning: 0.8,
        CapacityCritical: 0.9,
        FragmentationWarning: 0.3,
        TemperatureWarning: 50,
        TemperatureCritical: 60,
    },
}
```

### Tiered Storage Configuration

```go
config := &resource.TieredConfig{
    Tiers: []resource.TierDefinition{
        {
            Name: "hot",
            MediaType: "nvme",
            TotalCapacity: 1024 * 1024 * 1024 * 1024, // 1TB
            StorageCost: 0.10, // $0.10/GB/month
            AccessCost: 0.01,  // $0.01/GB accessed
            ReadLatency: 1 * time.Millisecond,
            MinAccessFreq: 10, // 10+ accesses/day
        },
        // ... more tiers
    },
    MigrationInterval: 1 * time.Hour,
    MaxConcurrentMigrations: 2,
    CostOptimizationEnabled: true,
    MLModelEnabled: true,
}
```

### Maintenance Configuration

```go
config := &resource.MaintenanceConfig{
    ScrubInterval: 7 * 24 * time.Hour,
    DefragInterval: 30 * 24 * time.Hour,
    DefragThreshold: 0.3,
    AutoRepairEnabled: true,
    RepairTimeout: 2 * time.Hour,
    MaxRepairAttempts: 3,
    MaintenanceWindow: resource.TimeWindow{
        StartHour: 2,
        EndHour: 6,
        Days: []int{0, 6}, // Weekends
    },
    MaxConcurrentOps: 2,
    PerformanceImpactLimit: 0.1, // 10%
}
```

## Integration with IPFS Cluster

The resource management package integrates with IPFS Cluster through:

1. **ZFS Dataset Management** - Automatic creation and management of datasets for pin storage
2. **Performance Monitoring** - Integration with cluster metrics and informers
3. **Capacity Planning** - Automatic scaling based on pin growth patterns
4. **Data Lifecycle** - Tiered storage based on pin access patterns

## Monitoring and Metrics

### Resource Manager Metrics

- Pool utilization and capacity
- Disk health and temperature
- Performance optimization scores
- Failed disk detection events

### Tiered Storage Metrics

- Data distribution across tiers
- Migration rates and success
- Cost optimization savings
- Access pattern predictions

### Maintenance Metrics

- Scrub completion rates and errors found
- Defragmentation effectiveness
- Repair success rates
- Maintenance window utilization

## Performance Considerations

### Resource Manager

- Health checks run every minute by default
- Optimization analysis runs hourly
- Disk operations are throttled to prevent I/O impact

### Tiered Storage

- ML model updates are batched and scheduled
- Migrations are bandwidth-limited
- Cost analysis runs during low-usage periods

### Automated Maintenance

- Operations respect maintenance windows
- Performance impact is monitored and limited
- Concurrent operations are limited to prevent overload

## Error Handling and Recovery

### Automatic Recovery

- Failed disk replacement
- Pool degradation repair
- Data corruption detection and repair
- Migration failure retry with exponential backoff

### Manual Intervention

- Critical errors trigger alerts
- Manual override capabilities for all operations
- Detailed logging for troubleshooting
- Emergency stop functionality

## Testing

Each component includes comprehensive unit tests:

```bash
# Run all tests
go test ./resource/...

# Run specific component tests
go test ./resource/ -run TestResourceManager
go test ./resource/ -run TestTieredStorage
go test ./resource/ -run TestAutomatedMaintenance

# Run with coverage
go test ./resource/... -cover
```

## Dependencies

- ZFS utilities (`zpool`, `zfs`)
- SMART monitoring tools (`smartctl`)
- Go standard library
- IPFS Cluster logging framework

## Requirements Satisfied

This implementation satisfies the following requirements from the specification:

- **5.1** - Automatic resource management and capacity planning
- **5.2** - Tiered storage with cost optimization
- **5.3** - Automated maintenance with minimal performance impact
- **5.4** - Comprehensive monitoring and alerting

The system is designed to handle trillion-scale pin operations through intelligent resource management, automated optimization, and proactive maintenance.