package cluster

import (
	"context"
)

// This file contains the gRPC service definitions for IPFS Cluster communication
// In a real implementation, these would be generated from protobuf files

// IPFSClusterServiceClient defines the gRPC client interface for IPFS Cluster
type IPFSClusterServiceClient interface {
	SubmitPin(ctx context.Context, req *PinRequest) (*PinResponse, error)
	GetPinStatus(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error)
	Unpin(ctx context.Context, req *UnpinRequest) (*UnpinResponse, error)
	GetClusterStatus(ctx context.Context, req *ClusterStatusRequest) (*ClusterStatusResponse, error)
	GetNodeMetrics(ctx context.Context, req *NodeMetricsRequest) (*NodeMetricsResponse, error)
	AddNode(ctx context.Context, req *AddNodeRequest) (*AddNodeResponse, error)
	RemoveNode(ctx context.Context, req *RemoveNodeRequest) (*RemoveNodeResponse, error)
	HealthCheck(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error)
}

// Mock implementation for testing
type mockIPFSClusterServiceClient struct {
	submitPinFunc      func(ctx context.Context, req *PinRequest) (*PinResponse, error)
	getPinStatusFunc   func(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error)
	unpinFunc          func(ctx context.Context, req *UnpinRequest) (*UnpinResponse, error)
	getClusterStatusFunc func(ctx context.Context, req *ClusterStatusRequest) (*ClusterStatusResponse, error)
	getNodeMetricsFunc func(ctx context.Context, req *NodeMetricsRequest) (*NodeMetricsResponse, error)
	addNodeFunc        func(ctx context.Context, req *AddNodeRequest) (*AddNodeResponse, error)
	removeNodeFunc     func(ctx context.Context, req *RemoveNodeRequest) (*RemoveNodeResponse, error)
	healthCheckFunc    func(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error)
}

func (m *mockIPFSClusterServiceClient) SubmitPin(ctx context.Context, req *PinRequest) (*PinResponse, error) {
	if m.submitPinFunc != nil {
		return m.submitPinFunc(ctx, req)
	}
	return &PinResponse{Success: true, Message: "Pin submitted successfully"}, nil
}

func (m *mockIPFSClusterServiceClient) GetPinStatus(ctx context.Context, req *PinStatusRequest) (*PinStatusResponse, error) {
	if m.getPinStatusFunc != nil {
		return m.getPinStatusFunc(ctx, req)
	}
	return &PinStatusResponse{
		Cid:       req.Cid,
		Status:    "pinned",
		Progress:  1.0,
		UpdatedAt: 1640995200, // 2022-01-01 00:00:00 UTC
	}, nil
}

func (m *mockIPFSClusterServiceClient) Unpin(ctx context.Context, req *UnpinRequest) (*UnpinResponse, error) {
	if m.unpinFunc != nil {
		return m.unpinFunc(ctx, req)
	}
	return &UnpinResponse{Success: true, Message: "Content unpinned successfully"}, nil
}

func (m *mockIPFSClusterServiceClient) GetClusterStatus(ctx context.Context, req *ClusterStatusRequest) (*ClusterStatusResponse, error) {
	if m.getClusterStatusFunc != nil {
		return m.getClusterStatusFunc(ctx, req)
	}
	return &ClusterStatusResponse{Status: "healthy"}, nil
}

func (m *mockIPFSClusterServiceClient) GetNodeMetrics(ctx context.Context, req *NodeMetricsRequest) (*NodeMetricsResponse, error) {
	if m.getNodeMetricsFunc != nil {
		return m.getNodeMetricsFunc(ctx, req)
	}
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
		},
	}, nil
}

func (m *mockIPFSClusterServiceClient) AddNode(ctx context.Context, req *AddNodeRequest) (*AddNodeResponse, error) {
	if m.addNodeFunc != nil {
		return m.addNodeFunc(ctx, req)
	}
	return &AddNodeResponse{Success: true, NodeId: "node-123", Message: "Node added successfully"}, nil
}

func (m *mockIPFSClusterServiceClient) RemoveNode(ctx context.Context, req *RemoveNodeRequest) (*RemoveNodeResponse, error) {
	if m.removeNodeFunc != nil {
		return m.removeNodeFunc(ctx, req)
	}
	return &RemoveNodeResponse{Success: true, Message: "Node removed successfully"}, nil
}

func (m *mockIPFSClusterServiceClient) HealthCheck(ctx context.Context, req *HealthCheckRequest) (*HealthCheckResponse, error) {
	if m.healthCheckFunc != nil {
		return m.healthCheckFunc(ctx, req)
	}
	return &HealthCheckResponse{Healthy: true, Message: "Cluster is healthy"}, nil
}

// NewIPFSClusterServiceClient creates a new gRPC client
// In a real implementation, this would create an actual gRPC client from the connection
func NewIPFSClusterServiceClient(conn interface{}) IPFSClusterServiceClient {
	// For now, return a mock client for testing
	return &mockIPFSClusterServiceClient{}
}