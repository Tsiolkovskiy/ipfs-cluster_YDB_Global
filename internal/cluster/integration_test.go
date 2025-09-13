package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestClusterControlAdapterIntegration tests the full integration of the cluster control adapter
func TestClusterControlAdapterIntegration(t *testing.T) {
	// Create adapter with test configuration
	config := &AdapterConfig{
		Timeout:     5 * time.Second,
		RetryConfig: DefaultRetryConfig(),
		Logger:      zaptest.NewLogger(t),
	}
	adapter := NewIPFSClusterAdapter(config)
	defer adapter.Close()

	// Create operation monitor
	monitorConfig := &MonitorConfig{
		Adapter:       adapter,
		Logger:        zaptest.NewLogger(t),
		CheckInterval: 100 * time.Millisecond, // Fast interval for testing
	}
	monitor, err := NewOperationMonitor(monitorConfig)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start monitoring
	monitor.Start(ctx)
	defer monitor.Stop()

	clusterID := "test-cluster-1"
	cid := "QmTestIntegration123"

	// Set up mock client for the cluster
	mockClient := &mockIPFSClusterServiceClient{
		submitPinFunc: func(ctx context.Context, req *PinRequest) (*PinResponse, error) {
			assert.Equal(t, cid, req.Cid)
			return &PinResponse{Success: true, Message: "Pin submitted successfully"}, nil
		},
		getPinStatusFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
			return &PinStatusResponse{
				Cid:       req.Cid,
				Status:    "pinned",
				Progress:  1.0,
				UpdatedAt: time.Now().Unix(),
			}, nil
		},
		healthCheckFunc: func(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error) {
			return &HealthCheckResponse{Healthy: true, Message: "Cluster is healthy"}, nil
		},
	}

	// Override connection for testing
	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	// Test 1: Submit pin plan
	plan := &models.PinPlan{
		ID:  "integration-plan-1",
		CID: cid,
		Assignments: []*models.NodeAssignment{
			{
				NodeID:    "node-1",
				ClusterID: clusterID,
				ZoneID:    "zone-msk",
				Priority:  1,
			},
			{
				NodeID:    "node-2",
				ClusterID: clusterID,
				ZoneID:    "zone-nn",
				Priority:  2,
			},
		},
	}

	// Track the operation
	operationID := monitor.TrackOperation(OperationTypePin, clusterID, cid)

	// Submit the pin plan
	err = adapter.SubmitPinPlan(ctx, clusterID, plan)
	require.NoError(t, err)

	// Test 2: Check pin status
	status, err := adapter.GetPinStatus(ctx, clusterID, cid)
	require.NoError(t, err)
	assert.Equal(t, cid, status.CID)
	assert.Equal(t, models.ContentStatusPinned, status.Status)
	assert.Equal(t, 1.0, status.Progress)

	// Complete the operation
	monitor.CompleteOperation(operationID, status.Status, nil)

	// Test 3: Verify operation tracking
	trackedOperation, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	assert.Equal(t, models.ContentStatusPinned, trackedOperation.Status)
	assert.Equal(t, 1.0, trackedOperation.Progress)
	assert.NotNil(t, trackedOperation.CompletedAt)

	// Test 4: Health check
	healthOperationID := monitor.TrackOperation(OperationTypeHealth, clusterID, "")
	err = adapter.HealthCheck(ctx, clusterID)
	require.NoError(t, err)
	monitor.CompleteOperation(healthOperationID, models.ContentStatusPinned, nil) // Using as "healthy"

	// Test 5: List all operations
	operations := monitor.ListOperations()
	assert.GreaterOrEqual(t, len(operations), 2, "Should have at least 2 operations")

	// Verify both operations are tracked
	operationIDs := make(map[string]bool)
	for _, op := range operations {
		operationIDs[op.ID] = true
	}
	assert.True(t, operationIDs[operationID], "Pin operation should be tracked")
	assert.True(t, operationIDs[healthOperationID], "Health operation should be tracked")
}

// TestClusterControlAdapterFailureScenarios tests various failure scenarios
func TestClusterControlAdapterFailureScenarios(t *testing.T) {
	config := &AdapterConfig{
		Timeout: 1 * time.Second, // Short timeout for testing
		RetryConfig: &RetryConfig{
			MaxAttempts:   2,
			InitialDelay:  10 * time.Millisecond,
			MaxDelay:      100 * time.Millisecond,
			BackoffFactor: 2.0,
			Jitter:        false,
		},
		Logger: zaptest.NewLogger(t),
	}
	adapter := NewIPFSClusterAdapter(config)
	defer adapter.Close()

	clusterID := "failing-cluster"
	cid := "QmTestFailure123"

	t.Run("network failure with retry", func(t *testing.T) {
		callCount := 0
		mockClient := &mockIPFSClusterServiceClient{
			getPinStatusFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
				callCount++
				if callCount == 1 {
					return nil, assert.AnError // First call fails
				}
				// Second call succeeds
				return &PinStatusResponse{
					Cid:       req.Cid,
					Status:    "pinned",
					Progress:  1.0,
					UpdatedAt: time.Now().Unix(),
				}, nil
			},
		}

		adapter.connections[clusterID] = &ClusterConnection{
			ID:       clusterID,
			Endpoint: "test-endpoint",
			Client:   mockClient,
			LastUsed: time.Now(),
		}

		status, err := adapter.GetPinStatus(context.Background(), clusterID, cid)
		require.NoError(t, err)
		assert.Equal(t, 2, callCount) // Should have retried once
		assert.Equal(t, cid, status.CID)
	})

	t.Run("persistent failure", func(t *testing.T) {
		callCount := 0
		mockClient := &mockIPFSClusterServiceClient{
			submitPinFunc: func(ctx context.Context, req *PinRequest) (*PinResponse, error) {
				callCount++
				return nil, assert.AnError // Always fails
			},
		}

		adapter.connections[clusterID] = &ClusterConnection{
			ID:       clusterID,
			Endpoint: "test-endpoint",
			Client:   mockClient,
			LastUsed: time.Now(),
		}

		plan := &models.PinPlan{
			ID:  "failing-plan",
			CID: cid,
		}

		err := adapter.SubmitPinPlan(context.Background(), clusterID, plan)
		require.Error(t, err)
		assert.Equal(t, 2, callCount) // Should have retried once (MaxAttempts = 2)
	})

	t.Run("context cancellation", func(t *testing.T) {
		mockClient := &mockIPFSClusterServiceClient{
			healthCheckFunc: func(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error) {
				// Simulate slow operation
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(2 * time.Second):
					return &HealthCheckResponse{Healthy: true}, nil
				}
			},
		}

		adapter.connections[clusterID] = &ClusterConnection{
			ID:       clusterID,
			Endpoint: "test-endpoint",
			Client:   mockClient,
			LastUsed: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := adapter.HealthCheck(ctx, clusterID)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})
}

// TestClusterControlAdapterConcurrency tests concurrent operations
func TestClusterControlAdapterConcurrency(t *testing.T) {
	config := &AdapterConfig{
		Timeout:     5 * time.Second,
		RetryConfig: DefaultRetryConfig(),
		Logger:      zaptest.NewLogger(t),
	}
	adapter := NewIPFSClusterAdapter(config)
	defer adapter.Close()

	clusterID := "concurrent-cluster"

	// Set up mock client that handles concurrent requests
	requestCount := 0
	mockClient := &mockIPFSClusterServiceClient{
		getPinStatusFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
			requestCount++
			// Simulate some processing time
			time.Sleep(10 * time.Millisecond)
			return &PinStatusResponse{
				Cid:       req.Cid,
				Status:    "pinned",
				Progress:  1.0,
				UpdatedAt: time.Now().Unix(),
			}, nil
		},
	}

	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	// Launch multiple concurrent requests
	const numRequests = 10
	results := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(index int) {
			cid := fmt.Sprintf("QmConcurrent%d", index)
			_, err := adapter.GetPinStatus(context.Background(), clusterID, cid)
			results <- err
		}(i)
	}

	// Wait for all requests to complete
	for i := 0; i < numRequests; i++ {
		err := <-results
		assert.NoError(t, err)
	}

	// Verify all requests were processed
	assert.Equal(t, numRequests, requestCount)
}

// TestOperationMonitorIntegration tests the operation monitor with real adapter
func TestOperationMonitorIntegration(t *testing.T) {
	// Create adapter
	config := &AdapterConfig{
		Timeout:     5 * time.Second,
		RetryConfig: DefaultRetryConfig(),
		Logger:      zaptest.NewLogger(t),
	}
	adapter := NewIPFSClusterAdapter(config)
	defer adapter.Close()

	// Create monitor
	monitorConfig := &MonitorConfig{
		Adapter:       adapter,
		Logger:        zaptest.NewLogger(t),
		CheckInterval: 50 * time.Millisecond, // Fast for testing
	}
	monitor, err := NewOperationMonitor(monitorConfig)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	monitor.Start(ctx)
	defer monitor.Stop()

	clusterID := "monitor-test-cluster"
	cid := "QmMonitorTest123"

	// Set up mock client that simulates operation progress
	statusCallCount := 0
	mockClient := &mockIPFSClusterServiceClient{
		submitPinFunc: func(ctx context.Context, req *PinRequest) (*PinResponse, error) {
			return &PinResponse{Success: true, Message: "Pin submitted"}, nil
		},
		getPinStatusFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
			statusCallCount++
			progress := float32(statusCallCount) * 0.25
			if progress > 1.0 {
				progress = 1.0
			}
			
			status := "pending"
			if progress >= 1.0 {
				status = "pinned"
			}

			return &PinStatusResponse{
				Cid:       req.Cid,
				Status:    status,
				Progress:  progress,
				UpdatedAt: time.Now().Unix(),
			}, nil
		},
	}

	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	// Submit pin plan and track operation
	plan := &models.PinPlan{
		ID:  "monitor-plan-1",
		CID: cid,
	}

	operationID := monitor.TrackOperation(OperationTypePin, clusterID, cid)
	err = adapter.SubmitPinPlan(ctx, clusterID, plan)
	require.NoError(t, err)

	// Wait for monitor to check the operation multiple times
	time.Sleep(200 * time.Millisecond)

	// Check that the operation was updated by the monitor
	operation, exists := monitor.GetOperationStatus(operationID)
	require.True(t, exists)
	
	// The monitor should have checked the status and updated the operation
	assert.True(t, statusCallCount > 0, "Monitor should have checked pin status")
	
	// The monitor should have checked the status and updated the operation
	// Since we're using a fast check interval, the operation should have been updated
	if statusCallCount >= 4 {
		assert.Equal(t, models.ContentStatusPinned, operation.Status)
		assert.Equal(t, 1.0, operation.Progress)
		assert.NotNil(t, operation.CompletedAt)
	} else {
		// If not enough checks happened, just verify it's being monitored
		assert.True(t, operation.Progress >= 0.0 && operation.Progress <= 1.0)
	}
}