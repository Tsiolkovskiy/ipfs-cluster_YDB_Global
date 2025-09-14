# ZFS-Optimized IPFS Connector

This package implements a ZFS-optimized IPFS Connector for IPFS Cluster that provides enhanced functionality for working with ZFS storage systems.

## Features

### Core Components

1. **ZFSConnector** - Main connector that extends the standard HTTP connector with ZFS optimizations
2. **DatasetManager** - Manages ZFS datasets, sharding, and auto-scaling
3. **ShardingStrategies** - Multiple strategies for distributing data across ZFS datasets
4. **ZFSConnectorRPCAPI** - Extended RPC API with ZFS-specific operations
5. **Metrics Collection** - Real-time ZFS performance monitoring

### ZFS Optimizations

- **Compression**: Automatic compression with configurable algorithms (lz4, gzip, zstd)
- **Deduplication**: ZFS native deduplication support
- **Snapshots**: Automated snapshot creation and management
- **Sharding**: Intelligent data distribution across multiple ZFS datasets
- **Auto-scaling**: Automatic dataset creation and load balancing

### Sharding Strategies

1. **Consistent Hash** - Distributes data using consistent hashing for even distribution
2. **Round Robin** - Simple round-robin distribution across datasets
3. **Size-based** - Optimizes placement based on available space and compression ratios

## Configuration

```json
{
  "zfs_connector": {
    "http_config": {
      "node_multiaddress": "/ip4/127.0.0.1/tcp/5001"
    },
    "zfs_config": {
      "pool_name": "ipfs-cluster",
      "compression_type": "zstd",
      "enable_deduplication": true,
      "max_pins_per_dataset": 1000000,
      "sharding_strategy": "consistent_hash",
      "auto_optimization": true
    }
  }
}
```

## Usage

See `example_integration.go` for detailed usage examples.

## Testing

Run tests with: `go test -v ./ipfsconn/zfshttp/`

All tests pass successfully, covering:
- Configuration validation
- Sharding strategies
- Dataset management
- Metrics collection
- RPC API functionality