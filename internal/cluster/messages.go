package cluster

// This file contains the message types for gRPC communication with IPFS Cluster
// In a real implementation, these would be generated from protobuf files

// PinRequest represents a request to pin content
type PinRequest struct {
	Cid         string            `json:"cid"`
	PlanId      string            `json:"plan_id"`
	Assignments []*NodeAssignment `json:"assignments"`
}

// PinResponse represents the response to a pin request
type PinResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// NodeAssignment represents assignment of content to a specific node
type NodeAssignment struct {
	NodeId    string `json:"node_id"`
	ClusterId string `json:"cluster_id"`
	ZoneId    string `json:"zone_id"`
	Priority  int32  `json:"priority"`
}

// PinStatusRequest represents a request for pin status
type PinStatusRequest struct {
	Cid string `json:"cid"`
}

// PinStatusResponse represents the response with pin status
type PinStatusResponse struct {
	Cid       string  `json:"cid"`
	Status    string  `json:"status"`
	Progress  float32 `json:"progress"`
	Error     string  `json:"error,omitempty"`
	UpdatedAt int64   `json:"updated_at"`
}

// UnpinRequest represents a request to unpin content
type UnpinRequest struct {
	Cid string `json:"cid"`
}

// UnpinResponse represents the response to an unpin request
type UnpinResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// ClusterStatusRequest represents a request for cluster status
type ClusterStatusRequest struct {
	ClusterId string `json:"cluster_id"`
}

// ClusterStatusResponse represents the response with cluster status
type ClusterStatusResponse struct {
	Status string `json:"status"`
}

// NodeMetricsRequest represents a request for node metrics
type NodeMetricsRequest struct {
	ClusterId string `json:"cluster_id"`
}

// NodeMetricsResponse represents the response with node metrics
type NodeMetricsResponse struct {
	Metrics []*NodeMetric `json:"metrics"`
}

// NodeMetric represents metrics for a single node
type NodeMetric struct {
	CpuUsage       float32 `json:"cpu_usage"`
	MemoryUsage    float32 `json:"memory_usage"`
	StorageUsage   float32 `json:"storage_usage"`
	NetworkInMbps  float32 `json:"network_in_mbps"`
	NetworkOutMbps float32 `json:"network_out_mbps"`
	PinnedObjects  int64   `json:"pinned_objects"`
	LastUpdated    int64   `json:"last_updated"`
}

// AddNodeRequest represents a request to add a node
type AddNodeRequest struct {
	ClusterId string         `json:"cluster_id"`
	Address   string         `json:"address"`
	Resources *NodeResources `json:"resources"`
}

// NodeResources represents node resource specifications
type NodeResources struct {
	CpuCores     int32 `json:"cpu_cores"`
	MemoryBytes  int64 `json:"memory_bytes"`
	StorageBytes int64 `json:"storage_bytes"`
	NetworkMbps  int32 `json:"network_mbps"`
}

// AddNodeResponse represents the response to an add node request
type AddNodeResponse struct {
	Success bool   `json:"success"`
	NodeId  string `json:"node_id"`
	Message string `json:"message"`
}

// RemoveNodeRequest represents a request to remove a node
type RemoveNodeRequest struct {
	ClusterId string `json:"cluster_id"`
	NodeId    string `json:"node_id"`
}

// RemoveNodeResponse represents the response to a remove node request
type RemoveNodeResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// HealthCheckRequest represents a health check request
type HealthCheckRequest struct {
	ClusterId string `json:"cluster_id"`
}

// HealthCheckResponse represents the response to a health check
type HealthCheckResponse struct {
	Healthy bool   `json:"healthy"`
	Message string `json:"message"`
}