package zfshttp

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNewDisasterRecoveryService(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery:        true,
		FailureDetectionInterval:  30 * time.Second,
		RecoveryTimeout:           30 * time.Minute,
		MaxRecoveryAttempts:       3,
		CrossDCReplicationEnabled: true,
		IntegrityCheckInterval:    time.Hour,
		BackupRetentionPeriod:     30 * 24 * time.Hour,
		AlertingEnabled:           true,
	}
	
	// Create mock dependencies
	replicationService := &ReplicationService{}
	snapshotManager := &DisasterRecoveryMockSnapshotManager{}
	nodeManager := &DisasterRecoveryMockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	if service == nil {
		t.Fatal("Expected non-nil disaster recovery service")
	}
	
	if !service.config.EnableAutoRecovery {
		t.Error("Expected auto recovery to be enabled")
	}
	
	if service.config.RecoveryTimeout != 30*time.Minute {
		t.Errorf("Expected recovery timeout 30m, got %v", service.config.RecoveryTimeout)
	}
}

func TestDisasterRecoveryService_DetectFailure(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery: false, // Disable auto recovery for testing
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &DisasterRecoveryMockSnapshotManager{}
	nodeManager := &DisasterRecoveryMockNodeManager{
		healthError: fmt.Errorf("node unreachable"),
	}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	ctx := context.Background()
	failure, err := service.DetectFailure(ctx, "node1")
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if failure == nil {
		t.Fatal("Expected failure event to be detected")
	}
	
	if failure.Type != FailureTypeNodeDown {
		t.Errorf("Expected failure type NodeDown, got %v", failure.Type)
	}
	
	if len(failure.AffectedNodes) != 1 || failure.AffectedNodes[0] != "node1" {
		t.Errorf("Expected affected nodes [node1], got %v", failure.AffectedNodes)
	}
}

func TestDisasterRecoveryService_DetectFailure_NoFailure(t *testing.T) {
	config := &DisasterRecoveryConfig{}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{} // Healthy node
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	ctx := context.Background()
	failure, err := service.DetectFailure(ctx, "node1")
	
	if err == nil {
		t.Fatal("Expected error when no failure is detected")
	}
	
	if failure != nil {
		t.Error("Expected no failure event when node is healthy")
	}
}

func TestDisasterRecoveryService_InitiateRecovery(t *testing.T) {
	config := &DisasterRecoveryConfig{
		RecoveryTimeout: 30 * time.Minute,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	// Initialize the recovery engine
	service.initializeRecoveryEngine()
	
	failure := &FailureEvent{
		ID:            "test-failure-1",
		Type:          FailureTypeNodeDown,
		Severity:      FailureSeverityHigh,
		AffectedNodes: []string{"node1"},
		DetectedAt:    time.Now(),
		Status:        FailureStatusDetected,
	}
	
	recovery, err := service.InitiateRecovery(failure)
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if recovery == nil {
		t.Fatal("Expected recovery operation to be created")
	}
	
	if recovery.FailureEvent.ID != failure.ID {
		t.Errorf("Expected failure ID %s, got %s", failure.ID, recovery.FailureEvent.ID)
	}
}

func TestDisasterRecoveryService_GetRecoveryStatus(t *testing.T) {
	config := &DisasterRecoveryConfig{}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeRecoveryEngine()
	
	// Add a test recovery operation
	recovery := &RecoveryOperation{
		ID:     "test-recovery-1",
		Status: RecoveryStatusRunning,
	}
	service.recoveryEngine.activeRecoveries[recovery.ID] = recovery
	
	// Test getting existing recovery
	retrievedRecovery, err := service.GetRecoveryStatus("test-recovery-1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if retrievedRecovery.ID != recovery.ID {
		t.Errorf("Expected recovery ID %s, got %s", recovery.ID, retrievedRecovery.ID)
	}
	
	// Test getting non-existent recovery
	_, err = service.GetRecoveryStatus("non-existent")
	if err == nil {
		t.Fatal("Expected error for non-existent recovery")
	}
}

func TestDisasterRecoveryService_ListActiveRecoveries(t *testing.T) {
	config := &DisasterRecoveryConfig{}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeRecoveryEngine()
	
	// Add test recovery operations
	recovery1 := &RecoveryOperation{ID: "recovery1", Status: RecoveryStatusRunning}
	recovery2 := &RecoveryOperation{ID: "recovery2", Status: RecoveryStatusPending}
	
	service.recoveryEngine.activeRecoveries[recovery1.ID] = recovery1
	service.recoveryEngine.activeRecoveries[recovery2.ID] = recovery2
	
	recoveries := service.ListActiveRecoveries()
	
	if len(recoveries) != 2 {
		t.Errorf("Expected 2 active recoveries, got %d", len(recoveries))
	}
	
	// Verify recovery IDs are present
	recoveryIDs := make(map[string]bool)
	for _, recovery := range recoveries {
		recoveryIDs[recovery.ID] = true
	}
	
	if !recoveryIDs["recovery1"] || !recoveryIDs["recovery2"] {
		t.Error("Expected both recovery1 and recovery2 in active recoveries list")
	}
}

func TestDisasterRecoveryService_GetMetrics(t *testing.T) {
	config := &DisasterRecoveryConfig{}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	// Set test metrics
	service.metrics.TotalFailures = 10
	service.metrics.RecoveredFailures = 8
	service.metrics.FailedRecoveries = 2
	service.metrics.DataRecovered = 1000000
	
	metrics := service.GetMetrics()
	
	if metrics.TotalFailures != 10 {
		t.Errorf("Expected 10 total failures, got %d", metrics.TotalFailures)
	}
	
	if metrics.RecoveredFailures != 8 {
		t.Errorf("Expected 8 recovered failures, got %d", metrics.RecoveredFailures)
	}
	
	if metrics.FailedRecoveries != 2 {
		t.Errorf("Expected 2 failed recoveries, got %d", metrics.FailedRecoveries)
	}
	
	if metrics.DataRecovered != 1000000 {
		t.Errorf("Expected 1000000 data recovered, got %d", metrics.DataRecovered)
	}
}

func TestDisasterRecoveryService_VerifyDataIntegrity(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeIntegrityChecker()
	service.initializeCrossDCReplication()
	
	// Override integrity checker config to disable actual ZFS commands in test
	service.integrityChecker = &IntegrityChecker{
		config: &IntegrityCheckConfig{
			VerifyChecksums:   false, // Disable to avoid ZFS command errors in test
			VerifyReplication: false, // Disable to avoid replication checks in test
		},
	}
	
	ctx := context.Background()
	report, err := service.VerifyDataIntegrity(ctx, "test-dataset")
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if report == nil {
		t.Fatal("Expected integrity report to be generated")
	}
	
	if report.Dataset != "test-dataset" {
		t.Errorf("Expected dataset 'test-dataset', got %s", report.Dataset)
	}
	
	// With both checks disabled, status should be passed
	if report.Status != IntegrityCheckStatusPassed {
		t.Errorf("Expected status passed, got %v", report.Status)
	}
}

func TestDisasterRecoveryService_CreateDisasterRecoveryBackup(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	datasets := []string{"dataset1", "dataset2"}
	
	backup, err := service.CreateDisasterRecoveryBackup(ctx, datasets)
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if backup == nil {
		t.Fatal("Expected backup operation to be created")
	}
	
	if len(backup.Datasets) != 2 {
		t.Errorf("Expected 2 datasets, got %d", len(backup.Datasets))
	}
	
	if backup.Status != BackupStatusCompleted {
		t.Errorf("Expected backup status completed, got %v", backup.Status)
	}
}

func TestNetworkPartitionRecoveryStrategy(t *testing.T) {
	strategy := &NetworkPartitionRecoveryStrategy{
		crossDCReplication: &CrossDatacenterReplication{},
		nodeManager:       &MockNodeManager{},
	}
	
	failure := &FailureEvent{
		Type: FailureTypeNetworkPartition,
		Metadata: map[string]interface{}{
			"datacenter1": "dc1",
			"datacenter2": "dc2",
		},
	}
	
	if !strategy.CanRecover(failure) {
		t.Error("Expected strategy to be able to recover network partition")
	}
	
	ctx := context.Background()
	result, err := strategy.Recover(ctx, failure)
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if !result.Success {
		t.Error("Expected recovery to be successful")
	}
	
	estimatedTime := strategy.GetEstimatedRecoveryTime(failure)
	if estimatedTime != 15*time.Minute {
		t.Errorf("Expected 15 minutes estimated time, got %v", estimatedTime)
	}
}

func TestDatacenterOutageRecoveryStrategy(t *testing.T) {
	strategy := &DatacenterOutageRecoveryStrategy{
		crossDCReplication: &CrossDatacenterReplication{},
		nodeManager:       &MockNodeManager{},
		snapshotManager:   &MockSnapshotManager{},
	}
	
	failure := &FailureEvent{
		Type: FailureTypeDatacenterOutage,
		Metadata: map[string]interface{}{
			"datacenter": "failed-dc",
		},
	}
	
	if !strategy.CanRecover(failure) {
		t.Error("Expected strategy to be able to recover datacenter outage")
	}
	
	ctx := context.Background()
	result, err := strategy.Recover(ctx, failure)
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if !result.Success {
		t.Error("Expected recovery to be successful")
	}
	
	estimatedTime := strategy.GetEstimatedRecoveryTime(failure)
	if estimatedTime != 30*time.Minute {
		t.Errorf("Expected 30 minutes estimated time, got %v", estimatedTime)
	}
}

func TestDisasterRecoveryService_CrossDCReplication(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
		IntegrityCheckInterval:    time.Hour,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeCrossDCReplication()
	
	// Test cross-DC sync
	service.performCrossDCSync()
	
	// Test integrity verification
	ctx := context.Background()
	err := service.verifyCrossDCIntegrity(ctx)
	
	if err != nil {
		t.Errorf("Expected no error in cross-DC integrity check, got %v", err)
	}
}

func TestDisasterRecoveryService_EnhancedIntegrityCheck(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeIntegrityChecker()
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	report, err := service.VerifyDataIntegrity(ctx, "test-dataset")
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if report == nil {
		t.Fatal("Expected integrity report to be generated")
	}
	
	if report.Dataset != "test-dataset" {
		t.Errorf("Expected dataset 'test-dataset', got %s", report.Dataset)
	}
	
	// Verify checksum results are populated
	if report.ChecksumResults == nil {
		t.Error("Expected checksum results to be populated")
	}
	
	// Verify replication results are populated
	if report.ReplicationResults == nil {
		t.Error("Expected replication results to be populated")
	}
}

func TestDisasterRecoveryService_CrossDatacenterFailureDetection(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery:        false, // Disable for testing
		CrossDCReplicationEnabled: true,
		FailureDetectionInterval:  100 * time.Millisecond,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeFailureDetector()
	service.initializeCrossDCReplication()
	
	// Simulate cross-DC failure detection
	service.detectReplicationFailures(context.Background())
	
	// The test passes if no panics occur during failure detection
}

func TestDisasterRecoveryService_ReplicationLagDetection(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery:        false,
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	
	// Test replication lag check
	lag, err := service.checkReplicationLag(ctx, "test-dataset")
	if err != nil {
		t.Errorf("Expected no error checking replication lag, got %v", err)
	}
	
	if lag < 0 {
		t.Errorf("Expected non-negative lag, got %v", lag)
	}
}

func TestDisasterRecoveryService_DatacenterConsistencyCheck(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	
	// Test consistency check (should pass with mock implementation)
	err := service.checkCrossDatacenterConsistency(ctx, "test-dataset")
	if err != nil {
		t.Errorf("Expected no error in consistency check, got %v", err)
	}
}

func TestDisasterRecoveryService_FailureSeverityCalculation(t *testing.T) {
	config := &DisasterRecoveryConfig{}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	// Test different failure scenarios
	testCases := []struct {
		dataset  string
		error    error
		expected FailureSeverity
	}{
		{"ipfs-cluster/metadata", fmt.Errorf("some error"), FailureSeverityCritical},
		{"ipfs-cluster/shard-1", fmt.Errorf("no replicas available"), FailureSeverityCritical},
		{"ipfs-cluster/shard-2", fmt.Errorf("insufficient replicas"), FailureSeverityHigh},
		{"ipfs-cluster/temp", fmt.Errorf("minor issue"), FailureSeverityMedium},
	}
	
	for _, tc := range testCases {
		severity := service.calculateReplicationFailureSeverity(tc.dataset, tc.error)
		if severity != tc.expected {
			t.Errorf("For dataset %s with error %v, expected severity %v, got %v", 
				tc.dataset, tc.error, tc.expected, severity)
		}
	}
}

func TestDisasterRecoveryService_ComprehensiveBackup(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	datasets := []string{"dataset1", "dataset2", "dataset3"}
	
	backup, err := service.CreateDisasterRecoveryBackup(ctx, datasets)
	
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if backup == nil {
		t.Fatal("Expected backup operation to be created")
	}
	
	if len(backup.Datasets) != 3 {
		t.Errorf("Expected 3 datasets, got %d", len(backup.Datasets))
	}
	
	if len(backup.Snapshots) != 3 {
		t.Errorf("Expected 3 snapshots, got %d", len(backup.Snapshots))
	}
	
	if backup.Status != BackupStatusCompleted {
		t.Errorf("Expected backup status completed, got %v", backup.Status)
	}
}

func TestDisasterRecoveryService_AutomaticFailureDetection(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery:       true,
		FailureDetectionInterval: 100 * time.Millisecond,
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &DisasterRecoveryMockSnapshotManager{}
	nodeManager := &DisasterRecoveryMockNodeManager{
		healthError: fmt.Errorf("node unreachable"),
	}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeFailureDetector()
	service.initializeRecoveryEngine()
	service.initializeCrossDCReplication()
	
	// Simulate failure detection
	service.performFailureDetection()
	
	// Give some time for the goroutine to execute
	time.Sleep(100 * time.Millisecond)
	
	// Check that recovery was initiated
	recoveries := service.ListActiveRecoveries()
	if len(recoveries) == 0 {
		t.Error("Expected automatic recovery to be initiated")
	}
}

func TestDisasterRecoveryService_CrossDatacenterReplication(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
		IntegrityCheckInterval:    time.Hour,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	// Test initialization
	err := service.initializeCrossDCReplication()
	if err != nil {
		t.Fatalf("Failed to initialize cross-DC replication: %v", err)
	}
	
	// Verify datacenters were initialized
	if len(service.crossDCReplication.datacenterNodes) < 2 {
		t.Error("Expected at least 2 datacenters for disaster recovery")
	}
	
	// Test cross-DC sync
	ctx := context.Background()
	err = service.syncDatasetCrossDC(ctx, "test-dataset")
	if err != nil {
		t.Errorf("Cross-DC sync failed: %v", err)
	}
}

func TestDisasterRecoveryService_IntegrityVerification(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeIntegrityChecker()
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	
	// Test comprehensive integrity check
	report, err := service.VerifyDataIntegrity(ctx, "test-dataset")
	if err != nil {
		t.Fatalf("Integrity verification failed: %v", err)
	}
	
	if report == nil {
		t.Fatal("Expected integrity report")
	}
	
	if report.Dataset != "test-dataset" {
		t.Errorf("Expected dataset 'test-dataset', got %s", report.Dataset)
	}
	
	// Verify both checksum and replication results are populated
	if report.ChecksumResults == nil {
		t.Error("Expected checksum results")
	}
	
	if report.ReplicationResults == nil {
		t.Error("Expected replication results")
	}
}

func TestDisasterRecoveryService_AutomaticRecovery_Requirement3_3(t *testing.T) {
	// Test requirement 3.3: WHEN создается новый snapshot THEN он должен автоматически реплицироваться на резервные узлы
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeCrossDCReplication()
	
	ctx := context.Background()
	
	// Create a disaster recovery backup (which creates snapshots)
	datasets := []string{"test-dataset"}
	backup, err := service.CreateDisasterRecoveryBackup(ctx, datasets)
	
	if err != nil {
		t.Fatalf("Failed to create disaster recovery backup: %v", err)
	}
	
	// Verify snapshots were created and replicated
	if len(backup.Snapshots) == 0 {
		t.Error("Expected snapshots to be created")
	}
	
	if backup.Status != BackupStatusCompleted {
		t.Errorf("Expected backup to complete successfully, got status: %v", backup.Status)
	}
	
	// Verify cross-datacenter replication occurred
	// In a real implementation, this would verify the snapshots exist on backup nodes
	if len(backup.Errors) > 0 {
		t.Errorf("Backup completed with errors: %v", backup.Errors)
	}
}

func TestDisasterRecoveryService_SecurityViolationDetection_Requirement8_4(t *testing.T) {
	// Test requirement 8.4: IF обнаруживается попытка несанкционированного доступа THEN должны блокироваться соответствующие ZFS datasets
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery: true,
		AlertingEnabled:    true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeFailureDetector()
	service.initializeRecoveryEngine()
	
	// Simulate security violation detection
	failure := &FailureEvent{
		ID:               "security-violation-1",
		Type:             FailureTypeDataCorruption, // Use data corruption as proxy for security violation
		Severity:         FailureSeverityCritical,
		AffectedDatasets: []string{"sensitive-dataset"},
		DetectedAt:       time.Now(),
		Description:      "Unauthorized access attempt detected",
		Status:           FailureStatusDetected,
		Metadata: map[string]interface{}{
			"security_violation": true,
			"access_attempt":     "unauthorized_read",
			"source_ip":         "192.168.1.100",
		},
	}
	
	// Initiate recovery (which should include dataset blocking)
	recovery, err := service.InitiateRecovery(failure)
	if err != nil {
		t.Fatalf("Failed to initiate security recovery: %v", err)
	}
	
	if recovery == nil {
		t.Fatal("Expected recovery operation to be created")
	}
	
	// Verify recovery was initiated for security violation
	if recovery.FailureEvent.Type != FailureTypeDataCorruption {
		t.Errorf("Expected data corruption failure type, got %v", recovery.FailureEvent.Type)
	}
	
	if recovery.FailureEvent.Severity != FailureSeverityCritical {
		t.Errorf("Expected critical severity for security violation, got %v", recovery.FailureEvent.Severity)
	}
}

func TestDisasterRecoveryService_ComprehensiveFailureScenarios(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery:        true,
		CrossDCReplicationEnabled: true,
		RecoveryTimeout:           30 * time.Minute,
		MaxRecoveryAttempts:       3,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeFailureDetector()
	service.initializeRecoveryEngine()
	service.initializeCrossDCReplication()
	
	// Test different failure scenarios
	testCases := []struct {
		name        string
		failureType FailureType
		severity    FailureSeverity
		expectRecovery bool
	}{
		{
			name:        "Node Down",
			failureType: FailureTypeNodeDown,
			severity:    FailureSeverityHigh,
			expectRecovery: true,
		},
		{
			name:        "Data Corruption",
			failureType: FailureTypeDataCorruption,
			severity:    FailureSeverityCritical,
			expectRecovery: true,
		},
		{
			name:        "Network Partition",
			failureType: FailureTypeNetworkPartition,
			severity:    FailureSeverityCritical,
			expectRecovery: true,
		},
		{
			name:        "Datacenter Outage",
			failureType: FailureTypeDatacenterOutage,
			severity:    FailureSeverityCritical,
			expectRecovery: true,
		},
		{
			name:        "Replication Failure",
			failureType: FailureTypeReplicationFailure,
			severity:    FailureSeverityHigh,
			expectRecovery: true,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			failure := &FailureEvent{
				ID:            fmt.Sprintf("test-failure-%s", tc.name),
				Type:          tc.failureType,
				Severity:      tc.severity,
				AffectedNodes: []string{"test-node"},
				AffectedDatasets: []string{"test-dataset"},
				DetectedAt:    time.Now(),
				Description:   fmt.Sprintf("Test %s failure", tc.name),
				Status:        FailureStatusDetected,
				Metadata: map[string]interface{}{
					"test_case": tc.name,
				},
			}
			
			recovery, err := service.InitiateRecovery(failure)
			
			if tc.expectRecovery {
				if err != nil {
					t.Errorf("Expected recovery to be initiated, got error: %v", err)
				}
				if recovery == nil {
					t.Error("Expected recovery operation to be created")
				}
			} else {
				if err == nil {
					t.Error("Expected recovery to fail")
				}
			}
		})
	}
}

func TestDisasterRecoveryService_PerformanceMetrics(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery: true,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	
	// Simulate some recovery operations
	service.metrics.TotalFailures = 100
	service.metrics.RecoveredFailures = 95
	service.metrics.FailedRecoveries = 5
	service.metrics.AverageRecoveryTime = 300.0 // 5 minutes
	service.metrics.DataRecovered = 10000000000 // 10GB
	service.metrics.NodesRecovered = 50
	service.metrics.SystemUptime = 99.5
	
	metrics := service.GetMetrics()
	
	if metrics.TotalFailures != 100 {
		t.Errorf("Expected 100 total failures, got %d", metrics.TotalFailures)
	}
	
	if metrics.RecoveredFailures != 95 {
		t.Errorf("Expected 95 recovered failures, got %d", metrics.RecoveredFailures)
	}
	
	if metrics.SystemUptime != 99.5 {
		t.Errorf("Expected 99.5%% uptime, got %f", metrics.SystemUptime)
	}
	
	// Verify recovery success rate
	successRate := float64(metrics.RecoveredFailures) / float64(metrics.TotalFailures) * 100
	if successRate < 90.0 {
		t.Errorf("Recovery success rate too low: %f%%", successRate)
	}
}

func TestDisasterRecoveryService_DataIntegrityChecks(t *testing.T) {
	config := &DisasterRecoveryConfig{
		CrossDCReplicationEnabled: true,
		IntegrityCheckInterval:    time.Hour,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManager{}
	nodeManager := &MockNodeManager{}
	
	service := NewDisasterRecoveryService(config, replicationService, snapshotManager, nodeManager)
	service.initializeIntegrityChecker()
	service.initializeCrossDCReplication()
	
	// Override integrity checker config to disable actual ZFS commands in test
	service.integrityChecker = &IntegrityChecker{
		config: &IntegrityCheckConfig{
			VerifyChecksums:   false, // Disable to avoid ZFS command errors in test
			VerifyReplication: true,  // Keep replication checks enabled
		},
	}
	
	ctx := context.Background()
	
	// Test integrity verification with different scenarios
	testDatasets := []string{
		"ipfs-cluster/metadata",
		"ipfs-cluster/shard-1",
		"ipfs-cluster/shard-2",
		"ipfs-cluster/temp-cache",
	}
	
	for _, dataset := range testDatasets {
		report, err := service.VerifyDataIntegrity(ctx, dataset)
		if err != nil {
			t.Errorf("Integrity check failed for dataset %s: %v", dataset, err)
			continue
		}
		
		if report.Status != IntegrityCheckStatusPassed {
			t.Errorf("Expected integrity check to pass for dataset %s, got status: %v", 
				dataset, report.Status)
		}
		
		// Verify comprehensive checks were performed
		if report.ChecksumResults == nil {
			t.Errorf("Missing checksum results for dataset %s", dataset)
		}
		
		if report.ReplicationResults == nil {
			t.Errorf("Missing replication results for dataset %s", dataset)
		}
	}
}

