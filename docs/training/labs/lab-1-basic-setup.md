# Lab 1: Basic Setup and Configuration

## Lab Overview

**Duration:** 4 hours  
**Difficulty:** Beginner  
**Prerequisites:** Module 1 completion

In this lab, you will set up a basic IPFS Cluster with ZFS integration from scratch. You'll learn the fundamental configuration steps and verify the system is working correctly.

## Learning Objectives

By completing this lab, you will be able to:
- Install and configure ZFS on Linux
- Set up IPFS and IPFS Cluster
- Configure ZFS integration
- Verify system functionality
- Perform basic operations

## Lab Environment

### Hardware Requirements
- Virtual machine with 8GB RAM, 4 CPU cores
- 3 virtual disks (10GB each) for ZFS pools
- Ubuntu 22.04 LTS

### Software Requirements
- Root access to the system
- Internet connectivity for package downloads

## Exercise 1: ZFS Installation and Setup (60 minutes)

### Step 1.1: Install ZFS

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install ZFS utilities
sudo apt install -y zfsutils-linux

# Verify ZFS installation
zfs version
zpool version
```

**Expected Output:**
```
zfs-2.1.5-1ubuntu6~22.04.1
zfs-kmod-2.1.5-1ubuntu6~22.04.1
```

### Step 1.2: Create ZFS Pools

```bash
# Identify available disks
lsblk

# Create hot tier pool (assuming /dev/sdb)
sudo zpool create -f hot-tier \
  -o ashift=12 \
  -O compression=lz4 \
  -O recordsize=128K \
  -O atime=off \
  /dev/sdb

# Create warm tier pool (assuming /dev/sdc)
sudo zpool create -f warm-tier \
  -o ashift=12 \
  -O compression=gzip-6 \
  -O recordsize=1M \
  -O atime=off \
  /dev/sdc

# Create cold tier pool (assuming /dev/sdd)
sudo zpool create -f cold-tier \
  -o ashift=12 \
  -O compression=zstd \
  -O recordsize=1M \
  -O atime=off \
  /dev/sdd

# Verify pool creation
zpool list
zpool status
```

**Expected Output:**
```
NAME        SIZE  ALLOC   FREE  CKPOINT  EXPANDSZ   FRAG    CAP  DEDUP    HEALTH  ALTROOT
cold-tier  9.50G    96K  9.50G        -         -     0%     0%  1.00x    ONLINE  -
hot-tier   9.50G    96K  9.50G        -         -     0%     0%  1.00x    ONLINE  -
warm-tier  9.50G    96K  9.50G        -         -     0%     0%  1.00x    ONLINE  -
```

### Step 1.3: Create ZFS Datasets

```bash
# Create datasets for IPFS Cluster
sudo zfs create hot-tier/ipfs-cluster
sudo zfs create hot-tier/ipfs-cluster/metadata
sudo zfs create hot-tier/ipfs-cluster/hot-shards

sudo zfs create warm-tier/ipfs-cluster
sudo zfs create warm-tier/ipfs-cluster/warm-shards

sudo zfs create cold-tier/ipfs-cluster
sudo zfs create cold-tier/ipfs-cluster/cold-shards
sudo zfs create cold-tier/ipfs-cluster/archive

# Set mount points
sudo zfs set mountpoint=/mnt/hot-tier hot-tier/ipfs-cluster
sudo zfs set mountpoint=/mnt/warm-tier warm-tier/ipfs-cluster
sudo zfs set mountpoint=/mnt/cold-tier cold-tier/ipfs-cluster

# Verify dataset creation
zfs list
```

**Checkpoint 1:** Verify that all pools are ONLINE and datasets are mounted correctly.

## Exercise 2: IPFS Installation (30 minutes)

### Step 2.1: Install Go

```bash
# Download and install Go
wget https://golang.org/dl/go1.19.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.19.5.linux-amd64.tar.gz

# Add Go to PATH
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# Verify Go installation
go version
```

### Step 2.2: Install IPFS (Kubo)

```bash
# Download IPFS
wget https://github.com/ipfs/kubo/releases/download/v0.17.0/kubo_v0.17.0_linux-amd64.tar.gz
tar -xzf kubo_v0.17.0_linux-amd64.tar.gz

# Install IPFS
sudo cp kubo/ipfs /usr/local/bin/
sudo chmod +x /usr/local/bin/ipfs

# Verify installation
ipfs version
```

### Step 2.3: Initialize IPFS

```bash
# Create IPFS user
sudo useradd -r -s /bin/bash -d /var/lib/ipfs ipfs
sudo mkdir -p /var/lib/ipfs
sudo chown ipfs:ipfs /var/lib/ipfs

# Initialize IPFS as the ipfs user
sudo -u ipfs ipfs init --profile server

# Configure IPFS for cluster use
sudo -u ipfs ipfs config Addresses.API /ip4/0.0.0.0/tcp/5001
sudo -u ipfs ipfs config Addresses.Gateway /ip4/0.0.0.0/tcp/8080
sudo -u ipfs ipfs config --json Swarm.ConnMgr.HighWater 2000
sudo -u ipfs ipfs config --json Swarm.ConnMgr.LowWater 500
sudo -u ipfs ipfs config --json Datastore.StorageMax '"100GB"'
```

**Checkpoint 2:** Verify IPFS is initialized and configured correctly.

## Exercise 3: IPFS Cluster Installation (45 minutes)

### Step 3.1: Build IPFS Cluster

```bash
# Clone IPFS Cluster repository
git clone https://github.com/ipfs/ipfs-cluster.git
cd ipfs-cluster

# Build IPFS Cluster
make build

# Install binaries
sudo cp ipfs-cluster-service /usr/local/bin/
sudo cp ipfs-cluster-ctl /usr/local/bin/
sudo cp ipfs-cluster-follow /usr/local/bin/
sudo chmod +x /usr/local/bin/ipfs-cluster-*

# Verify installation
ipfs-cluster-service --version
ipfs-cluster-ctl --version
```

### Step 3.2: Create IPFS Cluster User and Directories

```bash
# Create cluster user
sudo useradd -r -s /bin/bash -d /var/lib/ipfs-cluster ipfs-cluster
sudo mkdir -p /var/lib/ipfs-cluster /var/log/ipfs-cluster
sudo chown ipfs-cluster:ipfs-cluster /var/lib/ipfs-cluster /var/log/ipfs-cluster

# Initialize cluster configuration
sudo -u ipfs-cluster ipfs-cluster-service init --config-dir /var/lib/ipfs-cluster
```

### Step 3.3: Configure ZFS Integration

Create the ZFS-optimized configuration:

```bash
sudo -u ipfs-cluster cat > /var/lib/ipfs-cluster/service.json << 'EOF'
{
  "cluster": {
    "secret": "REPLACE_WITH_GENERATED_SECRET",
    "leave_on_shutdown": false,
    "listen_multiaddress": "/ip4/0.0.0.0/tcp/9096",
    "state_sync_interval": "5m0s",
    "ipfs_sync_interval": "2m10s",
    "replication_factor_min": 1,
    "replication_factor_max": 1,
    "monitor_ping_interval": "15s"
  },
  "consensus": {
    "crdt": {
      "cluster_name": "lab-zfs-cluster",
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
        "max_pins_per_shard": 1000000,
        "compression_enabled": true,
        "deduplication_enabled": true,
        "snapshot_interval": "1h",
        "replication_factor": 1
      }
    }
  },
  "pin_tracker": {
    "stateless": {
      "max_pin_queue_size": 100000,
      "concurrent_pins": 32
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
        "tier": "lab-environment"
      }
    }
  }
}
EOF

# Generate cluster secret
CLUSTER_SECRET=$(openssl rand -hex 32)
sudo -u ipfs-cluster sed -i "s/REPLACE_WITH_GENERATED_SECRET/$CLUSTER_SECRET/" /var/lib/ipfs-cluster/service.json
```

**Checkpoint 3:** Verify configuration file is created and secret is generated.

## Exercise 4: Service Configuration (30 minutes)

### Step 4.1: Create Systemd Services

Create IPFS service:

```bash
sudo cat > /etc/systemd/system/ipfs.service << 'EOF'
[Unit]
Description=IPFS Daemon
After=network.target

[Service]
Type=simple
User=ipfs
Group=ipfs
ExecStart=/usr/local/bin/ipfs daemon --enable-gc
Restart=always
RestartSec=10
Environment=IPFS_PATH=/var/lib/ipfs/.ipfs
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ipfs

[Install]
WantedBy=multi-user.target
EOF
```

Create IPFS Cluster service:

```bash
sudo cat > /etc/systemd/system/ipfs-cluster.service << 'EOF'
[Unit]
Description=IPFS Cluster Service
After=network.target ipfs.service
Requires=ipfs.service

[Service]
Type=simple
User=ipfs-cluster
Group=ipfs-cluster
ExecStart=/usr/local/bin/ipfs-cluster-service daemon --config-dir /var/lib/ipfs-cluster
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ipfs-cluster

[Install]
WantedBy=multi-user.target
EOF
```

### Step 4.2: Start Services

```bash
# Reload systemd
sudo systemctl daemon-reload

# Enable and start IPFS
sudo systemctl enable ipfs
sudo systemctl start ipfs

# Wait for IPFS to start
sleep 10

# Enable and start IPFS Cluster
sudo systemctl enable ipfs-cluster
sudo systemctl start ipfs-cluster

# Check service status
sudo systemctl status ipfs
sudo systemctl status ipfs-cluster
```

**Checkpoint 4:** Verify both services are running without errors.

## Exercise 5: System Verification (45 minutes)

### Step 5.1: Basic Functionality Tests

```bash
# Test IPFS connectivity
ipfs id

# Test IPFS Cluster connectivity
ipfs-cluster-ctl id

# Check cluster status
ipfs-cluster-ctl status

# Check peers
ipfs-cluster-ctl peers ls
```

### Step 5.2: ZFS Integration Tests

```bash
# Check ZFS pool status
zpool status

# Check ZFS datasets
zfs list

# Test ZFS compression
echo "This is a test file for compression" | sudo tee /mnt/hot-tier/test.txt
zfs get compressratio hot-tier/ipfs-cluster

# Check ZFS metrics
cat /proc/spl/kstat/zfs/arcstats | grep -E "hits|misses|size"
```

### Step 5.3: Pin Operations Test

```bash
# Add a test file to IPFS
echo "Hello, IPFS Cluster with ZFS!" > test-file.txt
CID=$(ipfs add -q test-file.txt)
echo "Added file with CID: $CID"

# Pin the file through cluster
ipfs-cluster-ctl pin add $CID

# Check pin status
ipfs-cluster-ctl status $CID

# Verify file is accessible
ipfs cat $CID

# Check ZFS dataset usage
zfs list -o name,used,refer
```

### Step 5.4: Performance Baseline

```bash
# Create performance test script
cat > performance-test.sh << 'EOF'
#!/bin/bash

echo "=== Performance Baseline Test ==="
echo "Starting at: $(date)"

# Test 1: Pin 100 small files
echo "Test 1: Pinning 100 small files..."
start_time=$(date +%s)

for i in {1..100}; do
    echo "Test file $i content" > "test-$i.txt"
    CID=$(ipfs add -q "test-$i.txt")
    ipfs-cluster-ctl pin add $CID > /dev/null 2>&1
done

end_time=$(date +%s)
duration=$((end_time - start_time))
echo "Completed in $duration seconds"
echo "Rate: $(echo "scale=2; 100 / $duration" | bc) pins/second"

# Test 2: Check ZFS compression
echo -e "\nTest 2: ZFS Compression Analysis..."
zfs get compressratio hot-tier/ipfs-cluster
zfs get used,logicalused hot-tier/ipfs-cluster

# Test 3: Memory usage
echo -e "\nTest 3: Memory Usage..."
free -h

# Test 4: ZFS ARC statistics
echo -e "\nTest 4: ZFS ARC Statistics..."
arc_hits=$(awk '/^hits/ {print $3}' /proc/spl/kstat/zfs/arcstats)
arc_misses=$(awk '/^misses/ {print $3}' /proc/spl/kstat/zfs/arcstats)
if [[ $arc_hits -gt 0 && $arc_misses -gt 0 ]]; then
    hit_ratio=$(echo "scale=2; $arc_hits * 100 / ($arc_hits + $arc_misses)" | bc)
    echo "ARC Hit Ratio: ${hit_ratio}%"
fi

echo "=== Performance Test Complete ==="
EOF

chmod +x performance-test.sh
./performance-test.sh
```

**Checkpoint 5:** All tests should pass and show reasonable performance metrics.

## Exercise 6: Monitoring Setup (30 minutes)

### Step 6.1: Install Monitoring Tools

```bash
# Install monitoring dependencies
sudo apt install -y htop iotop nethogs

# Create monitoring script
cat > monitor-cluster.sh << 'EOF'
#!/bin/bash

echo "=== IPFS Cluster ZFS Monitoring ==="
echo "Timestamp: $(date)"
echo

# Cluster status
echo "=== Cluster Status ==="
ipfs-cluster-ctl status | head -10
echo

# ZFS pool status
echo "=== ZFS Pool Status ==="
zpool list
echo

# ZFS I/O statistics
echo "=== ZFS I/O Statistics ==="
zpool iostat -v 1 1
echo

# System resources
echo "=== System Resources ==="
echo "Memory Usage:"
free -h
echo
echo "CPU Load:"
uptime
echo

# Disk usage
echo "=== Disk Usage ==="
df -h | grep -E "(Filesystem|/mnt)"
echo

# Network connections
echo "=== Network Connections ==="
ss -tuln | grep -E "(5001|9094|9095|9096)"
EOF

chmod +x monitor-cluster.sh
./monitor-cluster.sh
```

### Step 6.2: Create Health Check Script

```bash
# Create health check script
cat > health-check.sh << 'EOF'
#!/bin/bash

echo "=== IPFS Cluster ZFS Health Check ==="

# Check services
echo "Service Status:"
systemctl is-active ipfs && echo "✓ IPFS: Running" || echo "✗ IPFS: Not running"
systemctl is-active ipfs-cluster && echo "✓ Cluster: Running" || echo "✗ Cluster: Not running"
echo

# Check ZFS pools
echo "ZFS Pool Health:"
for pool in hot-tier warm-tier cold-tier; do
    if zpool list "$pool" > /dev/null 2>&1; then
        health=$(zpool list -H -o health "$pool")
        if [[ "$health" == "ONLINE" ]]; then
            echo "✓ $pool: $health"
        else
            echo "✗ $pool: $health"
        fi
    else
        echo "✗ $pool: Not found"
    fi
done
echo

# Check API endpoints
echo "API Endpoints:"
curl -s http://localhost:5001/api/v0/id > /dev/null && echo "✓ IPFS API: Responding" || echo "✗ IPFS API: Not responding"
curl -s http://localhost:9094/api/v0/id > /dev/null && echo "✓ Cluster API: Responding" || echo "✗ Cluster API: Not responding"
echo

# Check disk usage
echo "Disk Usage Warnings:"
for pool in hot-tier warm-tier cold-tier; do
    if zpool list "$pool" > /dev/null 2>&1; then
        usage=$(zpool list -H -o capacity "$pool" | tr -d '%')
        if [[ $usage -gt 80 ]]; then
            echo "⚠ $pool: ${usage}% full"
        fi
    fi
done

echo "=== Health Check Complete ==="
EOF

chmod +x health-check.sh
./health-check.sh
```

## Lab Completion Checklist

Mark each item as complete:

- [ ] ZFS pools created and online
- [ ] ZFS datasets created with proper mount points
- [ ] IPFS installed and initialized
- [ ] IPFS Cluster installed and configured
- [ ] ZFS integration configured
- [ ] Services running without errors
- [ ] Basic pin operations working
- [ ] Performance baseline established
- [ ] Monitoring scripts created
- [ ] Health check script working

## Troubleshooting Common Issues

### Issue: ZFS pool creation fails

**Solution:**
```bash
# Check if disks are available
lsblk

# Clear any existing partitions
sudo wipefs -a /dev/sdX

# Try creating pool again
```

### Issue: IPFS Cluster fails to start

**Solution:**
```bash
# Check logs
journalctl -u ipfs-cluster -n 50

# Verify IPFS is running first
systemctl status ipfs

# Check configuration syntax
ipfs-cluster-service --config-dir /var/lib/ipfs-cluster --dry-run
```

### Issue: Pin operations fail

**Solution:**
```bash
# Check IPFS connectivity
ipfs swarm peers

# Verify cluster peer connectivity
ipfs-cluster-ctl peers ls

# Check ZFS dataset permissions
ls -la /mnt/*/
```

## Lab Summary

In this lab, you have successfully:

1. **Installed and configured ZFS** with three storage tiers
2. **Set up IPFS and IPFS Cluster** with ZFS integration
3. **Configured services** to run automatically
4. **Verified functionality** through comprehensive testing
5. **Established monitoring** and health checking procedures

### Key Takeaways

- ZFS provides powerful features for large-scale storage
- IPFS Cluster coordinates distributed pin management
- Proper configuration is critical for optimal performance
- Monitoring and health checks are essential for operations

### Next Steps

- Proceed to Lab 2: Performance Testing
- Review Module 2: Advanced Configuration
- Practice troubleshooting scenarios

## Additional Resources

- [ZFS Administration Guide](https://openzfs.github.io/openzfs-docs/)
- [IPFS Cluster Configuration Reference](https://cluster.ipfs.io/documentation/reference/configuration/)
- [Performance Tuning Guide](../operations/performance-tuning.md)

## Lab Evaluation

**Self-Assessment Questions:**

1. What are the three ZFS pools you created and their purposes?
2. How does ZFS compression benefit IPFS storage?
3. What is the role of the cluster secret in IPFS Cluster?
4. How would you check if a pin operation was successful?

**Practical Assessment:**

Demonstrate the following to your instructor:
1. Show all ZFS pools are healthy
2. Pin a new file and verify it's stored
3. Run the health check script
4. Explain the monitoring output

**Time to Complete:** Most students complete this lab in 3-4 hours.

**Instructor Notes:** This lab provides hands-on experience with the core components. Students should be comfortable with basic operations before proceeding to advanced topics.