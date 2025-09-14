package zfshttp

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Test the disaster recovery service in isolation
func TestDisasterRecoveryStandalone(t *testing.T) {
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
	snapshotManager := &MockSnapshotManagerStandalone{}
	nodeManager := &MockNodeManagerStandalone{}
	
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

func TestDisasterRecoveryFailureDetection(t *testing.T) {
	config := &DisasterRecoveryConfig{
		EnableAutoRecovery: false, // Disable auto recovery for testing
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManagerStandalone{}
	nodeManager := &MockNodeManagerStandalone{
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

func TestDisasterRecoveryInitiateRecovery(t *testing.T) {
	config := &DisasterRecoveryConfig{
		RecoveryTimeout: 30 * time.Minute,
	}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManagerStandalone{}
	nodeManager := &MockNodeManagerStandalone{}
	
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

func TestDisasterRecoveryMetrics(t *testing.T) {
	config := &DisasterRecoveryConfig{}
	
	replicationService := &ReplicationService{}
	snapshotManager := &MockSnapshotManagerStandalone{}
	nodeManager := &MockNodeManagerStandalone{}
	
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

// Standalone mock implementations that don't conflict with existing ones
type MockSnapshotManagerStandalone struct {
	snapshots map[string][]string
	error     error
}

func (m *MockSnapshotManagerStandalone) CreateSnapshot(ctx context.Context, dataset, name string) error {
	if m.error != nil {
		return m.error
	}
	
	if m.snapshots == nil {
		m.snapshots = make(map[string][]string)
	}
	
	m.snapshots[dataset] = append(m.snapshots[dataset], name)
	return nil
}

func (m *MockSnapshotManagerStandalone) ListSnapshots(ctx context.Context, dataset string) ([]string, error) {
	if m.error != nil {
		return nil, m.error
	}
	
	if m.snapshots == nil {
		return []string{"snapshot1", "snapshot2"}, nil
	}
	
	return m.snapshots[dataset], nil
}

func (m *MockSnapshotManagerStandalone) RollbackToSnapshot(ctx context.Context, dataset, snapshot string) error {
	return m.error
}

func (m *MockSnapshotManagerStandalone) DeleteSnapshot(ctx context.Context, dataset, snapshot string) error {
	return m.error
}

func (m *MockSnapshotManagerStandalone) GetSnapshotInfo(ctx context.Context, dataset, snapshot string) (*SnapshotInfo, error) {
	if m.error != nil {
		return nil, m.error
	}
	
	return &SnapshotInfo{
		Name:         snapshot,
		Dataset:      dataset,
		CreationTime: time.Now(),
		Size:         1000000,
	}, nil
}

func (m *MockSnapshotManagerStandalone) GetPreviousSnapshot(ctx context.Context, dataset, currentSnapshot string) (string, error) {
	if m.error != nil {
		return "", m.error
	}
	return "prev-snapshot", nil
}

type MockNodeManagerStandalone struct {
	nodes       []Node
	healthError error
}

func (m *MockNodeManagerStandalone) GetAvailableNodes(ctx context.Context) ([]Node, error) {
	if m.nodes != nil {
		return m.nodes, nil
	}
	
	return []Node{
		{ID: "node1", Address: "192.168.1.1", Datacenter: "dc1", Status: NodeStatusOnline},
		{ID: "node2", Address: "192.168.1.2", Datacenter: "dc2", Status: NodeStatusOnline},
	}, nil
}

func (m *MockNodeManagerStandalone) CheckNodeHealth(ctx context.Context, nodeID string) (*NodeHealth, error) {
	if m.healthError != nil {
		return &NodeHealth{
			IsHealthy: false,
			LastCheck: time.Now(),
		}, m.healthError
	}
	
	return &NodeHealth{
		IsHealthy: true,
		LastCheck: time.Now(),
	}, nil
}

func (m *MockNodeManagerStandalone) GetNodeByID(ctx context.Context, nodeID string) (*Node, error) {
	return &Node{
		ID:         nodeID,
		Address:    "192.168.1.1",
		Datacenter: "dc1",
		Status:     NodeStatusOnline,
	}, nil
}

func (m *MockNodeManagerStandalone) UpdateNodeStatus(ctx context.Context, nodeID string, status NodeStatus) error {
	return nil
}

func (m *MockNodeManagerStandalone) GetNodeNetworkInfo(ctx context.Context, nodeID string) (*NodeNetworkInfo, error) {
	return &NodeNetworkInfo{
		NodeID:    nodeID,
		IPAddress: "192.168.1.10",
		Port:      22,
		Protocol:  "ssh",
	}, nil
}