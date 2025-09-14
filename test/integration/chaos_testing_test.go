package integration

import (
	"context"
	"testing"
	"time"
)

func TestChaosTestingFramework(t *testing.T) {
	config := &ChaosTestConfig{
		TestDuration:       time.Minute * 2,
		FailureInterval:    time.Second * 10,
		RecoveryTimeout:    time.Second * 30,
		MaxConcurrentFails: 3,
		
		NetworkFailures: &NetworkFailureConfig{
			Enabled:           true,
			PartitionDuration: time.Second * 30,
			PacketLossPercent: 10.0,
			LatencyIncrease:   time.Millisecond * 100,
			BandwidthLimit:    100,
			DNSFailures:       true,
		},
		
		DiskFailures: &DiskFailureConfig{
			Enabled:         true,
			DiskFullPercent: 95.0,
			IOErrors:        true,
			SlowIO:          true,
			DiskCorruption:  false, // Disabled for safety
			FailureDuration: time.Second * 45,
		},
		
		NodeFailures: &NodeFailureConfig{
			Enabled:         true,
			CrashNodes:      true,
			MemoryPressure:  true,
			CPUStarvation:   true,
			ProcessKills:    true,
			FailureDuration: time.Second * 60,
		},
		
		ZFSFailures: &ZFSFailureConfig{
			Enabled:           true,
			PoolDegradation:   true,
			SnapshotFailures:  true,
			ReplicationFails:  true,
			ChecksumErrors:    false, // Disabled for safety
			FailureDuration:   time.Second * 90,
		},
		
		AutoRecovery:       true,
		RecoveryStrategies: []string{"network", "disk", "node", "zfs"},
		ValidationChecks:   []string{"cluster_health", "data_integrity", "performance"},
		
		ClusterNodes:     []string{"node1", "node2", "node3"},
		ZFSPools:        []string{"pool1", "pool2"},
		CriticalServices: []string{"ipfs-cluster", "ipfs", "zfs"},
	}
	
	framework := NewChaosTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()
	
	results, err := framework.RunChaosTest(ctx)
	if err != nil {
		t.Fatalf("Chaos test failed: %v", err)
	}
	
	// Validate results
	if results.Metrics.TotalFailuresInjected == 0 {
		t.Error("No failures were injected")
	}
	
	if results.Metrics.TestDuration == 0 {
		t.Error("Test duration not recorded")
	}
	
	if results.FinalValidation == nil {
		t.Error("Final validation not performed")
	}
	
	t.Logf("Chaos test completed successfully:")
	t.Logf("- Total failures injected: %d", results.Metrics.TotalFailuresInjected)
	t.Logf("- Successful recoveries: %d", results.Metrics.SuccessfulRecoveries)
	t.Logf("- Failed recoveries: %d", results.Metrics.FailedRecoveries)
	t.Logf("- Test duration: %v", results.Metrics.TestDuration)
	t.Logf("- System availability: %.2f%%", results.Metrics.SystemAvailability)
}

func TestNetworkFailureInjection(t *testing.T) {
	config := &ChaosTestConfig{
		TestDuration:       time.Second * 30,
		FailureInterval:    time.Second * 5,
		RecoveryTimeout:    time.Second * 15,
		MaxConcurrentFails: 1,
		
		NetworkFailures: &NetworkFailureConfig{
			Enabled:           true,
			PartitionDuration: time.Second * 10,
			PacketLossPercent: 5.0,
			LatencyIncrease:   time.Millisecond * 50,
		},
		
		AutoRecovery:     true,
		ClusterNodes:     []string{"node1", "node2"},
		ZFSPools:        []string{"pool1"},
		CriticalServices: []string{"ipfs-cluster"},
	}
	
	framework := NewChaosTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	results, err := framework.RunChaosTest(ctx)
	if err != nil {
		t.Fatalf("Network failure test failed: %v", err)
	}
	
	// Check that network failures were injected
	networkFailures := results.Metrics.FailuresByType["network_partition"] +
		results.Metrics.FailuresByType["network_latency"] +
		results.Metrics.FailuresByType["network_packet_loss"]
	
	if networkFailures == 0 {
		t.Error("No network failures were injected")
	}
	
	t.Logf("Network failure test completed with %d network failures", networkFailures)
}

func TestDiskFailureInjection(t *testing.T) {
	config := &ChaosTestConfig{
		TestDuration:       time.Second * 30,
		FailureInterval:    time.Second * 5,
		RecoveryTimeout:    time.Second * 20,
		MaxConcurrentFails: 1,
		
		DiskFailures: &DiskFailureConfig{
			Enabled:         true,
			DiskFullPercent: 90.0,
			IOErrors:        true,
			SlowIO:          true,
			FailureDuration: time.Second * 15,
		},
		
		AutoRecovery:     true,
		ClusterNodes:     []string{"node1"},
		ZFSPools:        []string{"pool1"},
		CriticalServices: []string{"ipfs-cluster"},
	}
	
	framework := NewChaosTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	results, err := framework.RunChaosTest(ctx)
	if err != nil {
		t.Fatalf("Disk failure test failed: %v", err)
	}
	
	// Check that disk failures were injected
	diskFailures := results.Metrics.FailuresByType["disk_full"] +
		results.Metrics.FailuresByType["disk_io_error"]
	
	if diskFailures == 0 {
		t.Error("No disk failures were injected")
	}
	
	t.Logf("Disk failure test completed with %d disk failures", diskFailures)
}

func TestZFSFailureInjection(t *testing.T) {
	config := &ChaosTestConfig{
		TestDuration:       time.Second * 30,
		FailureInterval:    time.Second * 5,
		RecoveryTimeout:    time.Second * 30,
		MaxConcurrentFails: 1,
		
		ZFSFailures: &ZFSFailureConfig{
			Enabled:           true,
			PoolDegradation:   true,
			SnapshotFailures:  true,
			ReplicationFails:  true,
			FailureDuration:   time.Second * 20,
		},
		
		AutoRecovery:     true,
		ClusterNodes:     []string{"node1"},
		ZFSPools:        []string{"pool1", "pool2"},
		CriticalServices: []string{"ipfs-cluster"},
	}
	
	framework := NewChaosTestingFramework(config)
	
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	
	results, err := framework.RunChaosTest(ctx)
	if err != nil {
		t.Fatalf("ZFS failure test failed: %v", err)
	}
	
	// Check that ZFS failures were injected
	zfsFailures := results.Metrics.FailuresByType["zfs_pool_degradation"] +
		results.Metrics.FailuresByType["zfs_snapshot_fail"] +
		results.Metrics.FailuresByType["zfs_replication_fail"]
	
	if zfsFailures == 0 {
		t.Error("No ZFS failures were injected")
	}
	
	t.Logf("ZFS failure test completed with %d ZFS failures", zfsFailures)
}

func TestRecoveryStrategies(t *testing.T) {
	// Test Network Partition Recovery
	t.Run("NetworkPartitionRecovery", func(t *testing.T) {
		recovery := &NetworkPartitionRecovery{}
		
		failure := &ActiveFailure{
			ID:        "test-network-partition",
			Type:      FailureTypeNetworkPartition,
			Target:    "node1",
			StartTime: time.Now(),
			Duration:  time.Second * 30,
			Status:    FailureStatusActive,
		}
		
		if !recovery.CanRecover(failure) {
			t.Error("NetworkPartitionRecovery should be able to recover network partition failures")
		}
		
		err := recovery.Recover(failure)
		if err != nil {
			t.Errorf("Network partition recovery failed: %v", err)
		}
		
		estimatedTime := recovery.EstimateRecoveryTime(failure)
		if estimatedTime <= 0 {
			t.Error("Recovery time estimate should be positive")
		}
		
		err = recovery.ValidateRecovery(failure)
		if err != nil {
			t.Errorf("Network partition recovery validation failed: %v", err)
		}
	})
	
	// Test Disk Failure Recovery
	t.Run("DiskFailureRecovery", func(t *testing.T) {
		recovery := &DiskFailureRecovery{}
		
		failure := &ActiveFailure{
			ID:        "test-disk-full",
			Type:      FailureTypeDiskFull,
			Target:    "node1",
			StartTime: time.Now(),
			Duration:  time.Second * 60,
			Status:    FailureStatusActive,
		}
		
		if !recovery.CanRecover(failure) {
			t.Error("DiskFailureRecovery should be able to recover disk failures")
		}
		
		err := recovery.Recover(failure)
		if err != nil {
			t.Errorf("Disk failure recovery failed: %v", err)
		}
	})
	
	// Test ZFS Failure Recovery
	t.Run("ZFSFailureRecovery", func(t *testing.T) {
		recovery := &ZFSFailureRecovery{}
		
		failure := &ActiveFailure{
			ID:        "test-zfs-degradation",
			Type:      FailureTypeZFSPoolDegradation,
			Target:    "pool1",
			StartTime: time.Now(),
			Duration:  time.Second * 120,
			Status:    FailureStatusActive,
		}
		
		if !recovery.CanRecover(failure) {
			t.Error("ZFSFailureRecovery should be able to recover ZFS failures")
		}
		
		err := recovery.Recover(failure)
		if err != nil {
			t.Errorf("ZFS failure recovery failed: %v", err)
		}
	})
}

func TestValidationChecks(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	pools := []string{"pool1", "pool2"}
	services := []string{"ipfs-cluster", "ipfs"}
	
	// Test Cluster Health Check
	t.Run("ClusterHealthCheck", func(t *testing.T) {
		check := NewClusterHealthCheck(nodes)
		
		result, err := check.Execute()
		if err != nil {
			t.Fatalf("Cluster health check failed: %v", err)
		}
		
		if result.CheckName != "cluster_health" {
			t.Errorf("Expected check name 'cluster_health', got '%s'", result.CheckName)
		}
		
		if !check.IsHealthy(result) {
			t.Error("Cluster should be healthy in test environment")
		}
		
		t.Logf("Cluster health check passed with score: %.1f", result.Score)
	})
	
	// Test Data Integrity Check
	t.Run("DataIntegrityCheck", func(t *testing.T) {
		check := NewDataIntegrityCheck(pools)
		
		result, err := check.Execute()
		if err != nil {
			t.Fatalf("Data integrity check failed: %v", err)
		}
		
		if result.CheckName != "data_integrity" {
			t.Errorf("Expected check name 'data_integrity', got '%s'", result.CheckName)
		}
		
		if !check.IsHealthy(result) {
			t.Error("Data integrity should be healthy in test environment")
		}
		
		t.Logf("Data integrity check passed with score: %.1f", result.Score)
	})
	
	// Test ZFS Health Check
	t.Run("ZFSHealthCheck", func(t *testing.T) {
		check := NewZFSHealthCheck(pools)
		
		result, err := check.Execute()
		if err != nil {
			t.Fatalf("ZFS health check failed: %v", err)
		}
		
		if result.CheckName != "zfs_health" {
			t.Errorf("Expected check name 'zfs_health', got '%s'", result.CheckName)
		}
		
		if !check.IsHealthy(result) {
			t.Error("ZFS should be healthy in test environment")
		}
		
		t.Logf("ZFS health check passed with score: %.1f", result.Score)
	})
	
	// Test Network Connectivity Check
	t.Run("NetworkConnectivityCheck", func(t *testing.T) {
		check := NewNetworkConnectivityCheck(nodes)
		
		result, err := check.Execute()
		if err != nil {
			t.Fatalf("Network connectivity check failed: %v", err)
		}
		
		if result.CheckName != "network_connectivity" {
			t.Errorf("Expected check name 'network_connectivity', got '%s'", result.CheckName)
		}
		
		if !check.IsHealthy(result) {
			t.Error("Network connectivity should be healthy in test environment")
		}
		
		t.Logf("Network connectivity check passed with score: %.1f", result.Score)
	})
	
	// Test Service Availability Check
	t.Run("ServiceAvailabilityCheck", func(t *testing.T) {
		check := NewServiceAvailabilityCheck(services, nodes)
		
		result, err := check.Execute()
		if err != nil {
			t.Fatalf("Service availability check failed: %v", err)
		}
		
		if result.CheckName != "service_availability" {
			t.Errorf("Expected check name 'service_availability', got '%s'", result.CheckName)
		}
		
		if !check.IsHealthy(result) {
			t.Error("Services should be available in test environment")
		}
		
		t.Logf("Service availability check passed with score: %.1f", result.Score)
	})
	
	// Test Composite Validation Check
	t.Run("CompositeValidationCheck", func(t *testing.T) {
		check := NewCompositeValidationCheck(nodes, pools, services)
		
		result, err := check.Execute()
		if err != nil {
			t.Fatalf("Composite validation check failed: %v", err)
		}
		
		if result.CheckName != "composite_validation" {
			t.Errorf("Expected check name 'composite_validation', got '%s'", result.CheckName)
		}
		
		if !check.IsHealthy(result) {
			t.Error("Composite validation should pass in test environment")
		}
		
		t.Logf("Composite validation check passed with score: %.1f", result.Score)
	})
}

func TestFailureTypeString(t *testing.T) {
	testCases := []struct {
		failureType FailureType
		expected    string
	}{
		{FailureTypeNetworkPartition, "network_partition"},
		{FailureTypeNetworkLatency, "network_latency"},
		{FailureTypeDiskFull, "disk_full"},
		{FailureTypeNodeCrash, "node_crash"},
		{FailureTypeZFSPoolDegradation, "zfs_pool_degradation"},
	}
	
	for _, tc := range testCases {
		result := tc.failureType.String()
		if result != tc.expected {
			t.Errorf("Expected %s, got %s", tc.expected, result)
		}
	}
}

func TestCompositeRecoveryStrategy(t *testing.T) {
	strategy := NewCompositeRecoveryStrategy()
	
	// Test network partition recovery
	networkFailure := &ActiveFailure{
		Type: FailureTypeNetworkPartition,
	}
	
	if !strategy.CanRecover(networkFailure) {
		t.Error("Composite strategy should be able to recover network partitions")
	}
	
	err := strategy.Recover(networkFailure)
	if err != nil {
		t.Errorf("Network partition recovery failed: %v", err)
	}
	
	// Test disk failure recovery
	diskFailure := &ActiveFailure{
		Type: FailureTypeDiskFull,
	}
	
	if !strategy.CanRecover(diskFailure) {
		t.Error("Composite strategy should be able to recover disk failures")
	}
	
	err = strategy.Recover(diskFailure)
	if err != nil {
		t.Errorf("Disk failure recovery failed: %v", err)
	}
	
	// Test unknown failure type
	unknownFailure := &ActiveFailure{
		Type: FailureType(999),
	}
	
	if strategy.CanRecover(unknownFailure) {
		t.Error("Composite strategy should not be able to recover unknown failure types")
	}
}

// Benchmark tests for chaos testing performance

func BenchmarkChaosTestingFramework(b *testing.B) {
	config := &ChaosTestConfig{
		TestDuration:       time.Second * 10,
		FailureInterval:    time.Second * 2,
		RecoveryTimeout:    time.Second * 5,
		MaxConcurrentFails: 2,
		
		NetworkFailures: &NetworkFailureConfig{
			Enabled:           true,
			PartitionDuration: time.Second * 5,
		},
		
		AutoRecovery:     true,
		ClusterNodes:     []string{"node1", "node2"},
		ZFSPools:        []string{"pool1"},
		CriticalServices: []string{"ipfs-cluster"},
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		framework := NewChaosTestingFramework(config)
		
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		
		_, err := framework.RunChaosTest(ctx)
		if err != nil {
			b.Fatalf("Benchmark chaos test failed: %v", err)
		}
		
		cancel()
	}
}

func BenchmarkValidationChecks(b *testing.B) {
	nodes := []string{"node1", "node2", "node3"}
	pools := []string{"pool1", "pool2"}
	services := []string{"ipfs-cluster", "ipfs"}
	
	check := NewCompositeValidationCheck(nodes, pools, services)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := check.Execute()
		if err != nil {
			b.Fatalf("Benchmark validation check failed: %v", err)
		}
	}
}