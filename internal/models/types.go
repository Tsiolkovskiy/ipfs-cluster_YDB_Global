package models

import (
	"time"
)

// Zone represents a geographic zone containing IPFS clusters
type Zone struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Region      string            `json:"region"`
	Status      ZoneStatus        `json:"status"`
	Capabilities map[string]string `json:"capabilities"`
	Coordinates *GeoCoordinates   `json:"coordinates,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// ZoneStatus represents the operational status of a zone
type ZoneStatus string

const (
	ZoneStatusActive      ZoneStatus = "active"
	ZoneStatusDegraded    ZoneStatus = "degraded"
	ZoneStatusMaintenance ZoneStatus = "maintenance"
	ZoneStatusOffline     ZoneStatus = "offline"
)

// GeoCoordinates represents geographic coordinates
type GeoCoordinates struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Cluster represents an IPFS cluster within a zone
type Cluster struct {
	ID          string            `json:"id"`
	ZoneID      string            `json:"zone_id"`
	Name        string            `json:"name"`
	Endpoint    string            `json:"endpoint"`
	Status      ClusterStatus     `json:"status"`
	Version     string            `json:"version"`
	Nodes       []*Node           `json:"nodes"`
	Capabilities map[string]string `json:"capabilities"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// ClusterStatus represents the operational status of a cluster
type ClusterStatus string

const (
	ClusterStatusHealthy  ClusterStatus = "healthy"
	ClusterStatusDegraded ClusterStatus = "degraded"
	ClusterStatusOffline  ClusterStatus = "offline"
)

// Node represents a single node within an IPFS cluster
type Node struct {
	ID          string            `json:"id"`
	ClusterID   string            `json:"cluster_id"`
	Address     string            `json:"address"`
	Status      NodeStatus        `json:"status"`
	Resources   *NodeResources    `json:"resources"`
	Metrics     *NodeMetrics      `json:"metrics,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// NodeStatus represents the operational status of a node
type NodeStatus string

const (
	NodeStatusOnline   NodeStatus = "online"
	NodeStatusOffline  NodeStatus = "offline"
	NodeStatusDraining NodeStatus = "draining"
)

// NodeResources represents the resource capacity of a node
type NodeResources struct {
	CPUCores      int   `json:"cpu_cores"`
	MemoryBytes   int64 `json:"memory_bytes"`
	StorageBytes  int64 `json:"storage_bytes"`
	NetworkMbps   int   `json:"network_mbps"`
}

// NodeMetrics represents current metrics for a node
type NodeMetrics struct {
	CPUUsage       float64 `json:"cpu_usage"`        // 0.0 to 1.0
	MemoryUsage    float64 `json:"memory_usage"`     // 0.0 to 1.0
	StorageUsage   float64 `json:"storage_usage"`    // 0.0 to 1.0
	NetworkInMbps  float64 `json:"network_in_mbps"`
	NetworkOutMbps float64 `json:"network_out_mbps"`
	PinnedObjects  int64   `json:"pinned_objects"`
	LastUpdated    time.Time `json:"last_updated"`
}

// Content represents content stored in the global data graph
type Content struct {
	CID         string            `json:"cid"`
	Size        int64             `json:"size"`
	Type        string            `json:"type"`
	Replicas    []*Replica        `json:"replicas"`
	Policies    []string          `json:"policies"`
	Status      ContentStatus     `json:"status"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// ContentStatus represents the status of content in the system
type ContentStatus string

const (
	ContentStatusPending    ContentStatus = "pending"
	ContentStatusPinned     ContentStatus = "pinned"
	ContentStatusReplicated ContentStatus = "replicated"
	ContentStatusFailed     ContentStatus = "failed"
)

// Replica represents a single replica of content
type Replica struct {
	NodeID      string        `json:"node_id"`
	ClusterID   string        `json:"cluster_id"`
	ZoneID      string        `json:"zone_id"`
	Status      ReplicaStatus `json:"status"`
	CreatedAt   time.Time     `json:"created_at"`
	VerifiedAt  *time.Time    `json:"verified_at,omitempty"`
}

// ReplicaStatus represents the status of a content replica
type ReplicaStatus string

const (
	ReplicaStatusPending  ReplicaStatus = "pending"
	ReplicaStatusPinned   ReplicaStatus = "pinned"
	ReplicaStatusFailed   ReplicaStatus = "failed"
	ReplicaStatusVerified ReplicaStatus = "verified"
)

// Topology represents the global topology of zones and clusters
type Topology struct {
	Zones    map[string]*Zone    `json:"zones"`
	Clusters map[string]*Cluster `json:"clusters"`
	UpdatedAt time.Time          `json:"updated_at"`
}

// Policy represents a replication and placement policy
type Policy struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Version     int               `json:"version"`
	Rules       map[string]string `json:"rules"`  // Rego rules
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	CreatedBy   string            `json:"created_by"`
}

// PinPlan represents a plan for pinning content to specific nodes
type PinPlan struct {
	ID          string              `json:"id"`
	CID         string              `json:"cid"`
	Assignments []*NodeAssignment   `json:"assignments"`
	Cost        *CostEstimate       `json:"cost"`
	CreatedAt   time.Time           `json:"created_at"`
}

// NodeAssignment represents assignment of content to a specific node
type NodeAssignment struct {
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
	ZoneID    string `json:"zone_id"`
	Priority  int    `json:"priority"`
}

// CostEstimate represents estimated costs for an operation
type CostEstimate struct {
	StorageCost  float64 `json:"storage_cost"`
	EgressCost   float64 `json:"egress_cost"`
	ComputeCost  float64 `json:"compute_cost"`
	TotalCost    float64 `json:"total_cost"`
	Currency     string  `json:"currency"`
}

// PinStatus represents the status of a pin operation
type PinStatus struct {
	CID       string        `json:"cid"`
	Status    ContentStatus `json:"status"`
	Progress  float64       `json:"progress"` // 0.0 to 1.0
	Error     string        `json:"error,omitempty"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// NodeConfig represents configuration for adding a new node
type NodeConfig struct {
	Address     string            `json:"address"`
	Resources   *NodeResources    `json:"resources"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ShardAssignment represents ownership of a shard by a zone
type ShardAssignment struct {
	ShardID    string    `json:"shard_id"`
	ZoneID     string    `json:"zone_id"`
	Status     string    `json:"status"`
	AssignedAt time.Time `json:"assigned_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ShardDistribution represents the current distribution of shards
type ShardDistribution struct {
	Assignments map[string]*ShardAssignment `json:"assignments"`
	UpdatedAt   time.Time                   `json:"updated_at"`
}