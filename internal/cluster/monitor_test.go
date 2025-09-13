package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestOperationMonitor_TrackOperation(t *testing.T) {
	monitor := createTestMonitor(t)

	operationID := monitor.TrackOperation(OperationTypePin, "cluster-1", "QmTest123")

	assert.NotEmpty(t, operationID)

	status, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, operationID, status.ID)
	assert.Equal(t, OperationTypePin, status.Type)
	assert.Equal(t, "cluster-1", status.ClusterID)
	assert.Equal(t, "QmTest123", status.CID)
	assert.Equal(t, models.ContentStatusPending, status.Status)
	assert.Equal(t, 0.0, status.Progress)
	assert.Nil(t, status.CompletedAt)
}

func TestOperationMonitor_CompleteOperation(t *testing.T) {
	monitor := createTestMonitor(t)

	operationID := monitor.TrackOperation(OperationTypePin, "cluster-1", "QmTest123")

	// Complete the operation successfully
	monitor.CompleteOperation(operationID, models.ContentStatusPinned, nil)

	status, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, models.ContentStatusPinned, status.Status)
	assert.Equal(t, 1.0, status.Progress)
	assert.NotNil(t, status.CompletedAt)
	assert.Empty(t, status.Error)
}

func TestOperationMonitor_CompleteOperationWithError(t *testing.T) {
	monitor := createTestMonitor(t)

	operationID := monitor.TrackOperation(OperationTypePin, "cluster-1", "QmTest123")

	// Complete the operation with error
	testError := assert.AnError
	monitor.CompleteOperation(operationID, models.ContentStatusFailed, testError)

	status, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, models.ContentStatusFailed, status.Status)
	assert.Equal(t, 1.0, status.Progress)
	assert.NotNil(t, status.CompletedAt)
	assert.Equal(t, testError.Error(), status.Error)
}

func TestOperationMonitor_ListOperations(t *testing.T) {
	monitor := createTestMonitor(t)

	// Track multiple operations with small delays to ensure unique IDs
	op1 := monitor.TrackOperation(OperationTypePin, "cluster-1", "QmTest123")
	time.Sleep(1 * time.Millisecond)
	op2 := monitor.TrackOperation(OperationTypeUnpin, "cluster-2", "QmTest456")
	time.Sleep(1 * time.Millisecond)
	op3 := monitor.TrackOperation(OperationTypeHealth, "cluster-1", "")

	operations := monitor.ListOperations()
	assert.GreaterOrEqual(t, len(operations), 3, "Should have at least 3 operations")

	// Check that all operations are present
	operationIDs := make(map[string]bool)
	for _, op := range operations {
		operationIDs[op.ID] = true
	}

	assert.True(t, operationIDs[op1], "Pin operation should be present")
	assert.True(t, operationIDs[op2], "Unpin operation should be present")
	assert.True(t, operationIDs[op3], "Health operation should be present")
}

func TestOperationMonitor_GetNonExistentOperation(t *testing.T) {
	monitor := createTestMonitor(t)

	status, exists := monitor.GetOperationStatus("non-existent-id")
	assert.False(t, exists)
	assert.Nil(t, status)
}

func TestOperationMonitor_CheckPinOperation(t *testing.T) {
	// Create mock adapter
	mockAdapter := &mockControlAdapter{
		getPinStatusFunc: func(ctx context.Context, clusterID, cid string) (*models.PinStatus, error) {
			return &models.PinStatus{
				CID:       cid,
				Status:    models.ContentStatusPinned,
				Progress:  1.0,
				UpdatedAt: time.Now(),
			}, nil
		},
	}

	monitor := &OperationMonitor{
		adapter:       mockAdapter,
		logger:        zaptest.NewLogger(t),
		operations:    make(map[string]*OperationStatus),
		checkInterval: 1 * time.Second,
	}

	// Track an operation
	operationID := monitor.TrackOperation(OperationTypePin, "cluster-1", "QmTest123")

	// Simulate checking the operation
	operation, _ := monitor.GetOperationStatus(operationID)
	monitor.checkSingleOperation(context.Background(), operation)

	// Check that the operation was updated
	updatedOperation, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, models.ContentStatusPinned, updatedOperation.Status)
	assert.Equal(t, 1.0, updatedOperation.Progress)
	assert.NotNil(t, updatedOperation.CompletedAt)
}

func TestOperationMonitor_CheckHealthOperation(t *testing.T) {
	// Create mock adapter
	mockAdapter := &mockControlAdapter{
		healthCheckFunc: func(ctx context.Context, clusterID string) error {
			return nil // Healthy
		},
	}

	monitor := &OperationMonitor{
		adapter:       mockAdapter,
		logger:        zaptest.NewLogger(t),
		operations:    make(map[string]*OperationStatus),
		checkInterval: 1 * time.Second,
	}

	// Track a health check operation
	operationID := monitor.TrackOperation(OperationTypeHealth, "cluster-1", "")

	// Simulate checking the operation
	operation, _ := monitor.GetOperationStatus(operationID)
	monitor.checkSingleOperation(context.Background(), operation)

	// Check that the operation was completed successfully
	updatedOperation, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, models.ContentStatusPinned, updatedOperation.Status) // Using as "healthy" status
	assert.Equal(t, 1.0, updatedOperation.Progress)
	assert.NotNil(t, updatedOperation.CompletedAt)
	assert.Empty(t, updatedOperation.Error)
}

func TestOperationMonitor_CheckHealthOperationFailure(t *testing.T) {
	// Create mock adapter that returns error
	mockAdapter := &mockControlAdapter{
		healthCheckFunc: func(ctx context.Context, clusterID string) error {
			return assert.AnError
		},
	}

	monitor := &OperationMonitor{
		adapter:       mockAdapter,
		logger:        zaptest.NewLogger(t),
		operations:    make(map[string]*OperationStatus),
		checkInterval: 1 * time.Second,
	}

	// Track a health check operation
	operationID := monitor.TrackOperation(OperationTypeHealth, "cluster-1", "")

	// Simulate checking the operation
	operation, _ := monitor.GetOperationStatus(operationID)
	monitor.checkSingleOperation(context.Background(), operation)

	// Check that the operation failed
	updatedOperation, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, models.ContentStatusFailed, updatedOperation.Status)
	assert.Equal(t, 1.0, updatedOperation.Progress)
	assert.NotNil(t, updatedOperation.CompletedAt)
	assert.Equal(t, assert.AnError.Error(), updatedOperation.Error)
}

func TestOperationMonitor_CleanupOldOperations(t *testing.T) {
	monitor := createTestMonitor(t)

	// Track an operation and complete it
	operationID := monitor.TrackOperation(OperationTypePin, "cluster-1", "QmTest123")
	monitor.CompleteOperation(operationID, models.ContentStatusPinned, nil)

	// Manually set the completion time to be old
	monitor.mutex.Lock()
	oldTime := time.Now().Add(-25 * time.Hour) // 25 hours ago
	monitor.operations[operationID].CompletedAt = &oldTime
	monitor.mutex.Unlock()

	// Run cleanup
	monitor.cleanupOldOperations()

	// Check that the old operation was removed
	_, exists := monitor.GetOperationStatus(operationID)
	assert.False(t, exists)
}

func TestOperationMonitor_StartStop(t *testing.T) {
	monitor := createTestMonitor(t)
	monitor.checkInterval = 10 * time.Millisecond // Fast interval for testing

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start monitoring
	monitor.Start(ctx)

	// Let it run for a short time
	time.Sleep(50 * time.Millisecond)

	// Stop monitoring
	monitor.Stop()

	// Should not panic or hang
}

func TestGenerateOperationID(t *testing.T) {
	id1 := generateOperationID()
	time.Sleep(1 * time.Millisecond) // Ensure different timestamps
	id2 := generateOperationID()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2) // Should be unique
	assert.Contains(t, id1, "op_")
	assert.Contains(t, id2, "op_")
}

// Helper functions and mocks

func createTestMonitor(t *testing.T) *OperationMonitor {
	mockAdapter := &mockControlAdapter{}
	config := &MonitorConfig{
		Adapter:       mockAdapter,
		Logger:        zaptest.NewLogger(t),
		CheckInterval: 1 * time.Second,
	}

	monitor, err := NewOperationMonitor(config)
	require.NoError(t, err)
	return monitor
}

// mockControlAdapter is a mock implementation of ControlAdapter for testing
type mockControlAdapter struct {
	submitPinPlanFunc    func(ctx context.Context, clusterID string, plan *models.PinPlan) error
	getPinStatusFunc     func(ctx context.Context, clusterID, cid string) (*models.PinStatus, error)
	unpinContentFunc     func(ctx context.Context, clusterID, cid string) error
	getClusterStatusFunc func(ctx context.Context, clusterID string) (*models.ClusterStatus, error)
	getNodeMetricsFunc   func(ctx context.Context, clusterID string) ([]*models.NodeMetrics, error)
	addNodeFunc          func(ctx context.Context, clusterID string, nodeConfig *models.NodeConfig) error
	removeNodeFunc       func(ctx context.Context, clusterID, nodeID string) error
	healthCheckFunc      func(ctx context.Context, clusterID string) error
}

func (m *mockControlAdapter) SubmitPinPlan(ctx context.Context, clusterID string, plan *models.PinPlan) error {
	if m.submitPinPlanFunc != nil {
		return m.submitPinPlanFunc(ctx, clusterID, plan)
	}
	return nil
}

func (m *mockControlAdapter) GetPinStatus(ctx context.Context, clusterID, cid string) (*models.PinStatus, error) {
	if m.getPinStatusFunc != nil {
		return m.getPinStatusFunc(ctx, clusterID, cid)
	}
	return &models.PinStatus{
		CID:       cid,
		Status:    models.ContentStatusPinned,
		Progress:  1.0,
		UpdatedAt: time.Now(),
	}, nil
}

func (m *mockControlAdapter) UnpinContent(ctx context.Context, clusterID, cid string) error {
	if m.unpinContentFunc != nil {
		return m.unpinContentFunc(ctx, clusterID, cid)
	}
	return nil
}

func (m *mockControlAdapter) GetClusterStatus(ctx context.Context, clusterID string) (*models.ClusterStatus, error) {
	if m.getClusterStatusFunc != nil {
		return m.getClusterStatusFunc(ctx, clusterID)
	}
	status := models.ClusterStatusHealthy
	return &status, nil
}

func (m *mockControlAdapter) GetNodeMetrics(ctx context.Context, clusterID string) ([]*models.NodeMetrics, error) {
	if m.getNodeMetricsFunc != nil {
		return m.getNodeMetricsFunc(ctx, clusterID)
	}
	return []*models.NodeMetrics{}, nil
}

func (m *mockControlAdapter) AddNode(ctx context.Context, clusterID string, nodeConfig *models.NodeConfig) error {
	if m.addNodeFunc != nil {
		return m.addNodeFunc(ctx, clusterID, nodeConfig)
	}
	return nil
}

func (m *mockControlAdapter) RemoveNode(ctx context.Context, clusterID, nodeID string) error {
	if m.removeNodeFunc != nil {
		return m.removeNodeFunc(ctx, clusterID, nodeID)
	}
	return nil
}

func (m *mockControlAdapter) HealthCheck(ctx context.Context, clusterID string) error {
	if m.healthCheckFunc != nil {
		return m.healthCheckFunc(ctx, clusterID)
	}
	return nil
}

func (m *mockControlAdapter) Close() error {
	return nil
}