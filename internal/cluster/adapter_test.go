package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIPFSClusterAdapter_SubmitPinPlan(t *testing.T) {
	tests := []struct {
		name        string
		clusterID   string
		plan        *models.PinPlan
		mockFunc    func(ctx context.Context, req *PinRequest) (*PinResponse, error)
		expectError bool
	}{
		{
			name:      "successful pin submission",
			clusterID: "cluster-1",
			plan: &models.PinPlan{
				ID:  "plan-1",
				CID: "QmTest123",
				Assignments: []*models.NodeAssignment{
					{
						NodeID:    "node-1",
						ClusterID: "cluster-1",
						ZoneID:    "zone-msk",
						Priority:  1,
					},
				},
			},
			mockFunc: func(ctx context.Context, req *PinRequest) (*PinResponse, error) {
				assert.Equal(t, "QmTest123", req.Cid)
				assert.Equal(t, "plan-1", req.PlanId)
				assert.Len(t, req.Assignments, 1)
				return &PinResponse{Success: true, Message: "Pin submitted"}, nil
			},
			expectError: false,
		},
		{
			name:      "pin submission rejected",
			clusterID: "cluster-1",
			plan: &models.PinPlan{
				ID:  "plan-2",
				CID: "QmTest456",
			},
			mockFunc: func(ctx context.Context, req *PinRequest) (*PinResponse, error) {
				return &PinResponse{Success: false, Message: "Insufficient space"}, nil
			},
			expectError: true,
		},
		{
			name:      "network error with retry",
			clusterID: "cluster-1",
			plan: &models.PinPlan{
				ID:  "plan-3",
				CID: "QmTest789",
			},
			mockFunc: func(ctx context.Context, req *PinRequest) (*PinResponse, error) {
				return nil, status.Error(codes.Unavailable, "network error")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := createTestAdapter(t)
			
			// Set up mock client
			mockClient := &mockIPFSClusterServiceClient{
				submitPinFunc: tt.mockFunc,
			}
			
			// Override the connection creation for testing
			// We create a mock connection that bypasses the gRPC connection check
			adapter.connections[tt.clusterID] = &ClusterConnection{
				ID:       tt.clusterID,
				Endpoint: "test-endpoint",
				Conn:     nil, // We'll bypass the connection check in tests
				Client:   mockClient,
				LastUsed: time.Now(),
			}

			err := adapter.SubmitPinPlan(context.Background(), tt.clusterID, tt.plan)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIPFSClusterAdapter_GetPinStatus(t *testing.T) {
	tests := []struct {
		name        string
		clusterID   string
		cid         string
		mockFunc    func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error)
		expectError bool
		expectedStatus *models.PinStatus
	}{
		{
			name:      "successful status retrieval",
			clusterID: "cluster-1",
			cid:       "QmTest123",
			mockFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
				assert.Equal(t, "QmTest123", req.Cid)
				return &PinStatusResponse{
					Cid:       "QmTest123",
					Status:    "pinned",
					Progress:  1.0,
					UpdatedAt: 1640995200,
				}, nil
			},
			expectError: false,
			expectedStatus: &models.PinStatus{
				CID:       "QmTest123",
				Status:    models.ContentStatusPinned,
				Progress:  1.0,
				UpdatedAt: time.Unix(1640995200, 0),
			},
		},
		{
			name:      "pin in progress",
			clusterID: "cluster-1",
			cid:       "QmTest456",
			mockFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
				return &PinStatusResponse{
					Cid:       "QmTest456",
					Status:    "pending",
					Progress:  0.5,
					UpdatedAt: 1640995200,
				}, nil
			},
			expectError: false,
			expectedStatus: &models.PinStatus{
				CID:       "QmTest456",
				Status:    models.ContentStatusPending,
				Progress:  0.5,
				UpdatedAt: time.Unix(1640995200, 0),
			},
		},
		{
			name:      "network error",
			clusterID: "cluster-1",
			cid:       "QmTest789",
			mockFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
				return nil, status.Error(codes.DeadlineExceeded, "timeout")
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := createTestAdapter(t)
			
			mockClient := &mockIPFSClusterServiceClient{
				getPinStatusFunc: tt.mockFunc,
			}
			
			adapter.connections[tt.clusterID] = &ClusterConnection{
				ID:       tt.clusterID,
				Endpoint: "test-endpoint",
				Client:   mockClient,
				LastUsed: time.Now(),
			}

			status, err := adapter.GetPinStatus(context.Background(), tt.clusterID, tt.cid)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, status)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStatus.CID, status.CID)
				assert.Equal(t, tt.expectedStatus.Status, status.Status)
				assert.Equal(t, tt.expectedStatus.Progress, status.Progress)
			}
		})
	}
}

func TestIPFSClusterAdapter_UnpinContent(t *testing.T) {
	tests := []struct {
		name        string
		clusterID   string
		cid         string
		mockFunc    func(ctx context.Context, req *UnpinRequest) (*UnpinResponse, error)
		expectError bool
	}{
		{
			name:      "successful unpin",
			clusterID: "cluster-1",
			cid:       "QmTest123",
			mockFunc: func(ctx context.Context, req *UnpinRequest) (*UnpinResponse, error) {
				assert.Equal(t, "QmTest123", req.Cid)
				return &UnpinResponse{Success: true, Message: "Content unpinned"}, nil
			},
			expectError: false,
		},
		{
			name:      "unpin failed",
			clusterID: "cluster-1",
			cid:       "QmTest456",
			mockFunc: func(ctx context.Context, req *UnpinRequest) (*UnpinResponse, error) {
				return &UnpinResponse{Success: false, Message: "Content not found"}, nil
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := createTestAdapter(t)
			
			mockClient := &mockIPFSClusterServiceClient{
				unpinFunc: tt.mockFunc,
			}
			
			adapter.connections[tt.clusterID] = &ClusterConnection{
				ID:       tt.clusterID,
				Endpoint: "test-endpoint",
				Client:   mockClient,
				LastUsed: time.Now(),
			}

			err := adapter.UnpinContent(context.Background(), tt.clusterID, tt.cid)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIPFSClusterAdapter_GetClusterStatus(t *testing.T) {
	tests := []struct {
		name           string
		clusterID      string
		mockFunc       func(ctx context.Context, req *ClusterStatusRequest) (*ClusterStatusResponse, error)
		expectError    bool
		expectedStatus *models.ClusterStatus
	}{
		{
			name:      "healthy cluster",
			clusterID: "cluster-1",
			mockFunc: func(ctx context.Context, req *ClusterStatusRequest) (*ClusterStatusResponse, error) {
				return &ClusterStatusResponse{Status: "healthy"}, nil
			},
			expectError: false,
			expectedStatus: func() *models.ClusterStatus {
				status := models.ClusterStatusHealthy
				return &status
			}(),
		},
		{
			name:      "degraded cluster",
			clusterID: "cluster-1",
			mockFunc: func(ctx context.Context, req *ClusterStatusRequest) (*ClusterStatusResponse, error) {
				return &ClusterStatusResponse{Status: "degraded"}, nil
			},
			expectError: false,
			expectedStatus: func() *models.ClusterStatus {
				status := models.ClusterStatusDegraded
				return &status
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := createTestAdapter(t)
			
			mockClient := &mockIPFSClusterServiceClient{
				getClusterStatusFunc: tt.mockFunc,
			}
			
			adapter.connections[tt.clusterID] = &ClusterConnection{
				ID:       tt.clusterID,
				Endpoint: "test-endpoint",
				Client:   mockClient,
				LastUsed: time.Now(),
			}

			status, err := adapter.GetClusterStatus(context.Background(), tt.clusterID)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, *tt.expectedStatus, *status)
			}
		})
	}
}

func TestIPFSClusterAdapter_GetNodeMetrics(t *testing.T) {
	adapter := createTestAdapter(t)
	clusterID := "cluster-1"
	
	mockClient := &mockIPFSClusterServiceClient{
		getNodeMetricsFunc: func(ctx context.Context, req *NodeMetricsRequest) (*NodeMetricsResponse, error) {
			return &NodeMetricsResponse{
				Metrics: []*NodeMetric{
					{
						CpuUsage:       0.5,
						MemoryUsage:    0.6,
						StorageUsage:   0.7,
						NetworkInMbps:  100.0,
						NetworkOutMbps: 80.0,
						PinnedObjects:  1000,
						LastUpdated:    1640995200,
					},
					{
						CpuUsage:       0.3,
						MemoryUsage:    0.4,
						StorageUsage:   0.5,
						NetworkInMbps:  120.0,
						NetworkOutMbps: 90.0,
						PinnedObjects:  800,
						LastUpdated:    1640995200,
					},
				},
			}, nil
		},
	}
	
	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	metrics, err := adapter.GetNodeMetrics(context.Background(), clusterID)

	require.NoError(t, err)
	assert.Len(t, metrics, 2)
	
	assert.InDelta(t, 0.5, metrics[0].CPUUsage, 0.01)
	assert.InDelta(t, 0.6, metrics[0].MemoryUsage, 0.01)
	assert.InDelta(t, 0.7, metrics[0].StorageUsage, 0.01)
	assert.Equal(t, int64(1000), metrics[0].PinnedObjects)
	
	assert.InDelta(t, 0.3, metrics[1].CPUUsage, 0.01)
	assert.Equal(t, int64(800), metrics[1].PinnedObjects)
}

func TestIPFSClusterAdapter_AddNode(t *testing.T) {
	adapter := createTestAdapter(t)
	clusterID := "cluster-1"
	
	nodeConfig := &models.NodeConfig{
		Address: "192.168.1.100:4001",
		Resources: &models.NodeResources{
			CPUCores:     8,
			MemoryBytes:  16 * 1024 * 1024 * 1024, // 16GB
			StorageBytes: 1024 * 1024 * 1024 * 1024, // 1TB
			NetworkMbps:  1000,
		},
	}
	
	mockClient := &mockIPFSClusterServiceClient{
		addNodeFunc: func(ctx context.Context, req *AddNodeRequest) (*AddNodeResponse, error) {
			assert.Equal(t, clusterID, req.ClusterId)
			assert.Equal(t, nodeConfig.Address, req.Address)
			assert.Equal(t, int32(8), req.Resources.CpuCores)
			return &AddNodeResponse{
				Success: true,
				NodeId:  "node-new-123",
				Message: "Node added successfully",
			}, nil
		},
	}
	
	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	err := adapter.AddNode(context.Background(), clusterID, nodeConfig)
	assert.NoError(t, err)
}

func TestIPFSClusterAdapter_RemoveNode(t *testing.T) {
	adapter := createTestAdapter(t)
	clusterID := "cluster-1"
	nodeID := "node-123"
	
	mockClient := &mockIPFSClusterServiceClient{
		removeNodeFunc: func(ctx context.Context, req *RemoveNodeRequest) (*RemoveNodeResponse, error) {
			assert.Equal(t, clusterID, req.ClusterId)
			assert.Equal(t, nodeID, req.NodeId)
			return &RemoveNodeResponse{
				Success: true,
				Message: "Node removed successfully",
			}, nil
		},
	}
	
	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	err := adapter.RemoveNode(context.Background(), clusterID, nodeID)
	assert.NoError(t, err)
}

func TestIPFSClusterAdapter_HealthCheck(t *testing.T) {
	tests := []struct {
		name        string
		clusterID   string
		mockFunc    func(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error)
		expectError bool
	}{
		{
			name:      "healthy cluster",
			clusterID: "cluster-1",
			mockFunc: func(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error) {
				return &HealthCheckResponse{Healthy: true, Message: "All systems operational"}, nil
			},
			expectError: false,
		},
		{
			name:      "unhealthy cluster",
			clusterID: "cluster-1",
			mockFunc: func(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error) {
				return &HealthCheckResponse{Healthy: false, Message: "Some nodes are down"}, nil
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := createTestAdapter(t)
			
			mockClient := &mockIPFSClusterServiceClient{
				healthCheckFunc: tt.mockFunc,
			}
			
			adapter.connections[tt.clusterID] = &ClusterConnection{
				ID:       tt.clusterID,
				Endpoint: "test-endpoint",
				Client:   mockClient,
				LastUsed: time.Now(),
			}

			err := adapter.HealthCheck(context.Background(), tt.clusterID)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRetryConfig_NextDelay(t *testing.T) {
	config := &RetryConfig{
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        false,
	}

	// Test exponential backoff
	delay1 := config.NextDelay(0)
	delay2 := config.NextDelay(1)
	delay3 := config.NextDelay(2)

	assert.Equal(t, 100*time.Millisecond, delay1)
	assert.Equal(t, 200*time.Millisecond, delay2)
	assert.Equal(t, 400*time.Millisecond, delay3)

	// Test max delay cap
	delay10 := config.NextDelay(10)
	assert.Equal(t, 5*time.Second, delay10)
}

func TestIPFSClusterAdapter_RetryLogic(t *testing.T) {
	adapter := createTestAdapter(t)
	adapter.retryConfig.MaxAttempts = 3
	adapter.retryConfig.InitialDelay = 10 * time.Millisecond
	
	clusterID := "cluster-1"
	cid := "QmTest123"
	
	callCount := 0
	mockClient := &mockIPFSClusterServiceClient{
		getPinStatusFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
			callCount++
			if callCount < 3 {
				return nil, status.Error(codes.Unavailable, "temporary failure")
			}
			return &PinStatusResponse{
				Cid:       cid,
				Status:    "pinned",
				Progress:  1.0,
				UpdatedAt: 1640995200,
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

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount) // Should have retried twice
	assert.Equal(t, cid, status.CID)
	assert.Equal(t, models.ContentStatusPinned, status.Status)
}

func TestIPFSClusterAdapter_NonRetryableError(t *testing.T) {
	adapter := createTestAdapter(t)
	adapter.retryConfig.MaxAttempts = 3
	
	clusterID := "cluster-1"
	cid := "QmTest123"
	
	callCount := 0
	mockClient := &mockIPFSClusterServiceClient{
		getPinStatusFunc: func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
			callCount++
			return nil, status.Error(codes.NotFound, "content not found")
		},
	}
	
	adapter.connections[clusterID] = &ClusterConnection{
		ID:       clusterID,
		Endpoint: "test-endpoint",
		Client:   mockClient,
		LastUsed: time.Now(),
	}

	_, err := adapter.GetPinStatus(context.Background(), clusterID, cid)

	assert.Error(t, err)
	assert.Equal(t, 1, callCount) // Should not have retried
}

func TestIPFSClusterAdapter_Close(t *testing.T) {
	adapter := createTestAdapter(t)
	
	// For this test, we'll just verify the close logic without actual connections
	// since we can't easily mock grpc.ClientConn in tests
	
	// Test that Close() doesn't panic with empty connections
	err := adapter.Close()
	assert.NoError(t, err)
	assert.Empty(t, adapter.connections)
}

// Helper functions and mocks

func createTestAdapter(t *testing.T) *IPFSClusterAdapter {
	config := &AdapterConfig{
		Timeout:     5 * time.Second,
		RetryConfig: DefaultRetryConfig(),
		Logger:      zaptest.NewLogger(t),
	}
	return NewIPFSClusterAdapter(config)
}

// Note: We don't need mockGRPCConn anymore since we simplified the Close test