package zfshttp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// MockConsensusClient implements ConsensusClient for testing
type MockConsensusClient struct {
	coordinationError error
	clusterState      *ClusterState
	events           []*ReplicationEvent
}

func (m *MockConsensusClient) CoordinateReplication(ctx context.Context, job *ReplicationJob) error {
	return m.coordinationError
}

func (m *MockConsensusClient) GetClusterState(ctx context.Context) (*ClusterState, error) {
	if m.clusterState == nil {
		return &ClusterState{
			Nodes:       []Node{{ID: "node1", Status: NodeStatusOnline}},
			ActivePins:  1000,
			LastUpdated: time.Now(),
		}, nil
	}
	return m.clusterState, nil
}

func (m *MockConsensusClient) RegisterReplicationEvent(ctx context.Context, event *ReplicationEvent) error {
	m.events = append(m.events, event)
	return nil
}

// MockNodeManager implements NodeManager for testing
type MockNodeManager struct {
	nodes       []Node
	healthError error
	networkInfo *NodeNetworkInfo
}

func (m *MockNodeManager) GetAvailableNodes(ctx context.Context) ([]Node, error) {
	if m.nodes == nil {
		return []Node{
			{ID: "node1", Status: NodeStatusOnline, Address: "192.168.1.10"},
			{ID: "node2", Status: NodeStatusOnline, Address: "192.168.1.11"},
		}, nil
	}
	return m.nodes, nil
}

func (m *MockNodeManager) CheckNodeHealth(ctx context.Context, nodeID string) (*NodeHealth, error) {
	if m.healthError != nil {
		return nil, m.healthError
	}
	return &NodeHealth{
		NodeID:    nodeID,
		IsHealthy: true,
		LastCheck: time.Now(),
	}, nil
}

func (m *MockNodeManager) GetNodeNetworkInfo(ctx context.Context, nodeID string) (*NodeNetworkInfo, error) {
	if m.networkInfo == nil {
		return &NodeNetworkInfo{
			NodeID:    nodeID,
			IPAddress: "192.168.1.10",
			Port:      22,
			Protocol:  "ssh",
		}, nil
	}
	return m.networkInfo, nil
}

func (m *MockNodeManager) GetNodeByID(ctx context.Context, nodeID string) (*Node, error) {
	return &Node{
		ID:         nodeID,
		Address:    "192.168.1.1",
		Datacenter: "dc1",
		Status:     NodeStatusOnline,
	}, nil
}

func (m *MockNodeManager) UpdateNodeStatus(ctx context.Context, nodeID string, status NodeStatus) error {
	return nil
}

// MockSnapshotManager implements SnapshotManager for testing
type MockSnapshotManager struct {
	createError        error
	previousSnapshot   string
	previousSnapError  error
}

func (m *MockSnapshotManager) CreateSnapshot(ctx context.Context, dataset, name string) error {
	return m.createError
}

func (m *MockSnapshotManager) GetPreviousSnapshot(ctx context.Context, dataset, currentSnapshot string) (string, error) {
	if m.previousSnapError != nil {
		return "", m.previousSnapError
	}
	if m.previousSnapshot == "" {
		return "prev-snapshot", nil
	}
	return m.previousSnapshot, nil
}

func (m *MockSnapshotManager) ListSnapshots(ctx context.Context, dataset string) ([]string, error) {
	return []string{"snapshot1", "snapshot2"}, nil
}

func (m *MockSnapshotManager) DeleteSnapshot(ctx context.Context, dataset, snapshot string) error {
	return nil
}

func (m *MockSnapshotManager) RollbackToSnapshot(ctx context.Context, dataset, snapshot string) error {
	return nil
}

func (m *MockSnapshotManager) GetSnapshotInfo(ctx context.Context, dataset, snapshot string) (*SnapshotInfo, error) {
	return &SnapshotInfo{
		Name:         snapshot,
		Dataset:      dataset,
		CreationTime: time.Now(),
		Size:         1000000,
	}, nil
}

func TestNewReplicationService(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	if service == nil {
		t.Fatal("Expected non-nil replication service")
	}
	
	if service.config.ReplicationFactor != 2 {
		t.Errorf("Expected replication factor 2, got %d", service.config.ReplicationFactor)
	}
	
	if len(service.activeJobs) != 0 {
		t.Errorf("Expected empty active jobs map, got %d jobs", len(service.activeJobs))
	}
}

func TestReplicationService_ReplicateDataset(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	ctx := context.Background()
	dataset := "tank/ipfs/shard1"
	targetNodes := []string{"node1", "node2"}
	
	job, err := service.ReplicateDataset(ctx, dataset, targetNodes)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if job == nil {
		t.Fatal("Expected non-nil replication job")
	}
	
	if job.SourceDataset != dataset {
		t.Errorf("Expected dataset %s, got %s", dataset, job.SourceDataset)
	}
	
	if len(job.TargetNodes) != len(targetNodes) {
		t.Errorf("Expected %d target nodes, got %d", len(targetNodes), len(job.TargetNodes))
	}
	
	if job.Status != ReplicationStatusPending {
		t.Errorf("Expected status pending, got %v", job.Status)
	}
}

func TestReplicationService_ReplicateDataset_SnapshotError(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{
		createError: fmt.Errorf("snapshot creation failed"),
	}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	ctx := context.Background()
	dataset := "tank/ipfs/shard1"
	targetNodes := []string{"node1", "node2"}
	
	_, err := service.ReplicateDataset(ctx, dataset, targetNodes)
	if err == nil {
		t.Fatal("Expected error due to snapshot creation failure")
	}
	
	if !strings.Contains(err.Error(), "failed to create snapshot") {
		t.Errorf("Expected snapshot error, got %v", err)
	}
}

func TestReplicationService_GetJobStatus(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	// Add a test job
	job := &ReplicationJob{
		ID:            "test-job-1",
		SourceDataset: "tank/ipfs/shard1",
		Status:        ReplicationStatusRunning,
	}
	service.activeJobs[job.ID] = job
	
	// Test getting existing job
	retrievedJob, err := service.GetJobStatus("test-job-1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if retrievedJob.ID != job.ID {
		t.Errorf("Expected job ID %s, got %s", job.ID, retrievedJob.ID)
	}
	
	// Test getting non-existent job
	_, err = service.GetJobStatus("non-existent")
	if err == nil {
		t.Fatal("Expected error for non-existent job")
	}
}

func TestReplicationService_CancelJob(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	// Add a test job
	job := &ReplicationJob{
		ID:            "test-job-1",
		SourceDataset: "tank/ipfs/shard1",
		Status:        ReplicationStatusRunning,
	}
	service.activeJobs[job.ID] = job
	
	// Test cancelling running job
	err := service.CancelJob("test-job-1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	
	if job.Status != ReplicationStatusCancelled {
		t.Errorf("Expected status cancelled, got %v", job.Status)
	}
	
	// Test cancelling non-existent job
	err = service.CancelJob("non-existent")
	if err == nil {
		t.Fatal("Expected error for non-existent job")
	}
}

func TestReplicationService_ListActiveJobs(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	// Add test jobs
	job1 := &ReplicationJob{ID: "job1", Status: ReplicationStatusRunning}
	job2 := &ReplicationJob{ID: "job2", Status: ReplicationStatusPending}
	
	service.activeJobs[job1.ID] = job1
	service.activeJobs[job2.ID] = job2
	
	jobs := service.ListActiveJobs()
	
	if len(jobs) != 2 {
		t.Errorf("Expected 2 active jobs, got %d", len(jobs))
	}
	
	// Verify job IDs are present
	jobIDs := make(map[string]bool)
	for _, job := range jobs {
		jobIDs[job.ID] = true
	}
	
	if !jobIDs["job1"] || !jobIDs["job2"] {
		t.Error("Expected both job1 and job2 in active jobs list")
	}
}

func TestReplicationService_GetMetrics(t *testing.T) {
	config := &ReplicationConfig{
		ReplicationFactor: 2,
		MaxConcurrentJobs: 4,
		EnableIncremental: true,
	}
	
	consensusClient := &MockConsensusClient{}
	nodeManager := &MockNodeManager{}
	snapshotManager := &MockSnapshotManager{}
	
	service := NewReplicationService(config, snapshotManager, consensusClient, nodeManager)
	
	// Set some test metrics
	service.metrics.TotalJobs = 10
	service.metrics.CompletedJobs = 8
	service.metrics.FailedJobs = 2
	
	metrics := service.GetMetrics()
	
	if metrics.TotalJobs != 10 {
		t.Errorf("Expected 10 total jobs, got %d", metrics.TotalJobs)
	}
	
	if metrics.CompletedJobs != 8 {
		t.Errorf("Expected 8 completed jobs, got %d", metrics.CompletedJobs)
	}
	
	if metrics.FailedJobs != 2 {
		t.Errorf("Expected 2 failed jobs, got %d", metrics.FailedJobs)
	}
}