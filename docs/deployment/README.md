# IPFS Cluster ZFS Integration - Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the IPFS Cluster ZFS Integration system capable of handling trillion-scale pins. The deployment includes ZFS-optimized IPFS connectors, automated sharding, replication services, and performance monitoring.

## Prerequisites

### Hardware Requirements

#### Minimum Configuration (Development)
- CPU: 16 cores, 2.4GHz+
- RAM: 64GB
- Storage: 2TB NVMe SSD + 10TB HDD
- Network: 10Gbps Ethernet

#### Production Configuration (Trillion Pins)
- CPU: 64 cores, 3.0GHz+
- RAM: 512GB
- Storage: 100TB NVMe SSD (hot tier) + 1PB SSD (warm tier) + 10PB HDD (cold tier)
- Network: 100Gbps InfiniBand/Ethernet

### Software Requirements

- Ubuntu 22.04 LTS or CentOS 8+
- ZFS 2.1.0+
- Go 1.19+
- IPFS Kubo 0.17.0+
- Docker 20.10+
- Ansible 2.12+ (for automated deployment)

## Quick Start

### 1. System Preparation

```bash
# Install ZFS
sudo apt update
sudo apt install zfsutils-linux

# Create ZFS pool
sudo zpool create -f ipfs-cluster-pool /dev/sdb /dev/sdc /dev/sdd
sudo zfs set compression=lz4 ipfs-cluster-pool
sudo zfs set dedup=on ipfs-cluster-pool
```

### 2. Clone and Build

```bash
git clone https://github.com/ipfs/ipfs-cluster.git
cd ipfs-cluster
make build
```

### 3. Initialize Configuration

```bash
./ipfs-cluster-service init --config-dir ~/.ipfs-cluster
```

## Detailed Deployment Steps

### Step 1: ZFS Pool Configuration

Create optimized ZFS pools for different storage tiers:

```bash
# Hot tier (NVMe SSDs)
sudo zpool create -f hot-tier \
  -o ashift=12 \
  -O compression=lz4 \
  -O recordsize=128K \
  -O atime=off \
  /dev/nvme0n1 /dev/nvme1n1

# Warm tier (SATA SSDs)  
sudo zpool create -f warm-tier \
  -o ashift=12 \
  -O compression=gzip-6 \
  -O recordsize=1M \
  -O atime=off \
  /dev/sda /dev/sdb /dev/sdc /dev/sdd

# Cold tier (HDDs)
sudo zpool create -f cold-tier \
  -o ashift=12 \
  -O compression=zstd \
  -O recordsize=1M \
  -O atime=off \
  /dev/sde /dev/sdf /dev/sdg /dev/sdh
```

### Step 2: Dataset Creation

```bash
# Create datasets for different shard types
sudo zfs create hot-tier/ipfs-cluster
sudo zfs create hot-tier/ipfs-cluster/metadata
sudo zfs create hot-tier/ipfs-cluster/hot-shards

sudo zfs create warm-tier/ipfs-cluster
sudo zfs create warm-tier/ipfs-cluster/warm-shards

sudo zfs create cold-tier/ipfs-cluster  
sudo zfs create cold-tier/ipfs-cluster/cold-shards
sudo zfs create cold-tier/ipfs-cluster/archive
```

### Step 3: IPFS Cluster Configuration

Create the main configuration file:

```bash
cat > ~/.ipfs-cluster/service.json << 'EOF'
{
  "cluster": {
    "secret": "$(openssl rand -hex 32)",
    "leave_on_shutdown": false,
    "listen_multiaddress": "/ip4/0.0.0.0/tcp/9096",
    "state_sync_interval": "5m0s",
    "ipfs_sync_interval": "2m10s",
    "replication_factor_min": 2,
    "replication_factor_max": 3,
    "monitor_ping_interval": "15s"
  },
  "consensus": {
    "crdt": {
      "cluster_name": "ipfs-cluster-zfs",
      "trusted_peers": ["*"],
      "batching": {
        "max_batch_size": 0,
        "max_batch_age": "0s"
      },
      "repair_interval": "1h0m0s"
    }
  },
  "api": {
    "ipfsproxy": {
      "listen_multiaddress": "/ip4/127.0.0.1/tcp/9095",
      "node_multiaddress": "/ip4/127.0.0.1/tcp/5001",
      "log_level": "info"
    },
    "restapi": {
      "http_listen_multiaddress": "/ip4/0.0.0.0/tcp/9094",
      "read_timeout": "30s",
      "read_header_timeout": "5s",
      "write_timeout": "60s",
      "idle_timeout": "120s"
    }
  },
  "ipfs_connector": {
    "zfshttp": {
      "node_multiaddress": "/ip4/127.0.0.1/tcp/5001",
      "connect_swarms_delay": "30s",
      "pin_timeout": "2m0s",
      "unpin_timeout": "3m0s",
      "zfs_config": {
        "hot_tier_pool": "hot-tier/ipfs-cluster",
        "warm_tier_pool": "warm-tier/ipfs-cluster", 
        "cold_tier_pool": "cold-tier/ipfs-cluster",
        "sharding_strategy": "consistent_hash",
        "max_pins_per_shard": 1000000000,
        "compression_enabled": true,
        "deduplication_enabled": true,
        "snapshot_interval": "1h",
        "replication_factor": 3
      }
    }
  },
  "pin_tracker": {
    "stateless": {
      "max_pin_queue_size": 1000000,
      "concurrent_pins": 256
    }
  },
  "monitor": {
    "zfsmetrics": {
      "check_interval": "15s",
      "failure_threshold": 3,
      "metrics_collection_interval": "30s",
      "performance_optimization": true,
      "auto_tuning_enabled": true
    }
  },
  "allocator": {
    "balanced": {
      "allocate_by": ["tag:tier", "freespace"]
    }
  },
  "informer": {
    "disk": {
      "metric_ttl": "30s",
      "metric_type": "freespace"
    },
    "tags": {
      "metric_ttl": "30s",
      "tags": {
        "tier": "auto-detect"
      }
    }
  }
}
EOF
```

### Step 4: Start Services

```bash
# Start IPFS daemon
ipfs daemon --enable-gc &

# Start IPFS Cluster
./ipfs-cluster-service daemon &
```

## Configuration Examples

### Multi-Node Cluster Setup

For a 3-node cluster configuration:

#### Node 1 (Bootstrap node)
```json
{
  "cluster": {
    "secret": "shared-secret-32-bytes-hex",
    "listen_multiaddress": "/ip4/0.0.0.0/tcp/9096"
  }
}
```

#### Node 2 & 3 (Follower nodes)
```json
{
  "cluster": {
    "secret": "shared-secret-32-bytes-hex", 
    "listen_multiaddress": "/ip4/0.0.0.0/tcp/9096",
    "bootstrap": ["/ip4/NODE1_IP/tcp/9096/p2p/NODE1_PEER_ID"]
  }
}
```

### Performance Tuning Configuration

```json
{
  "zfs_optimization": {
    "arc_max": "256G",
    "arc_min": "64G", 
    "l2arc_write_max": "1G",
    "recordsize_optimization": true,
    "compression_algorithm": "lz4",
    "sync_mode": "standard",
    "atime": false,
    "prefetch": "metadata"
  },
  "sharding_config": {
    "strategy": "consistent_hash_with_load_balancing",
    "hash_function": "sha256",
    "virtual_nodes": 150,
    "rebalance_threshold": 0.1,
    "migration_batch_size": 10000
  }
}
```

## Automated Deployment

### Using Ansible

The repository includes Ansible playbooks for automated deployment:

```bash
# Install Ansible
pip install ansible

# Configure inventory
cp ansible/inventory.example ansible/inventory
# Edit ansible/inventory with your server details

# Deploy full cluster
ansible-playbook -i ansible/inventory ansible/deploy-cluster.yml

# Deploy single node
ansible-playbook -i ansible/inventory ansible/deploy-node.yml --limit node1
```

### Using Docker Compose

For containerized deployment:

```bash
# Start the cluster
docker-compose -f docker/docker-compose.zfs.yml up -d

# Scale to multiple nodes
docker-compose -f docker/docker-compose.zfs.yml up -d --scale cluster-node=3
```

## Verification

### Health Checks

```bash
# Check cluster status
./ipfs-cluster-ctl status

# Check ZFS pool health
sudo zpool status

# Check performance metrics
./ipfs-cluster-ctl metrics zfs

# Verify replication
./ipfs-cluster-ctl pin ls | head -10
```

### Performance Validation

```bash
# Run benchmark suite
cd test/integration
go test -v -run TestTrillionPinsWorkload -timeout 24h

# Monitor during load test
watch -n 5 './ipfs-cluster-ctl metrics zfs'
```

## Troubleshooting

### Common Issues

1. **ZFS Pool Import Failures**
   ```bash
   sudo zpool import -f pool-name
   ```

2. **High Memory Usage**
   ```bash
   echo $((64 * 1024 * 1024 * 1024)) | sudo tee /sys/module/zfs/parameters/zfs_arc_max
   ```

3. **Slow Pin Operations**
   ```bash
   sudo zfs set recordsize=128K dataset-name
   sudo zfs set compression=lz4 dataset-name
   ```

## Next Steps

After successful deployment:

1. Review the [Operations Runbook](../operations/README.md)
2. Set up monitoring dashboards
3. Configure backup and disaster recovery
4. Implement capacity planning procedures

## Support

For deployment issues:
- Check logs: `journalctl -u ipfs-cluster`
- Review ZFS status: `sudo zpool status -v`
- Monitor performance: `./ipfs-cluster-ctl metrics`