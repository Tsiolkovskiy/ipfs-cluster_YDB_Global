package topology

import (
	"context"
	"fmt"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/global-data-controller/gdc/internal/storage"
)

// Service provides topology management functionality
type Service interface {
	// Zone management
	RegisterZone(ctx context.Context, zone *models.Zone) error
	GetZone(ctx context.Context, zoneID string) (*models.Zone, error)
	ListZones(ctx context.Context) ([]*models.Zone, error)
	UpdateZoneStatus(ctx context.Context, zoneID string, status models.ZoneStatus) error
	DeleteZone(ctx context.Context, zoneID string) error

	// Cluster management
	RegisterCluster(ctx context.Context, cluster *models.Cluster) error
	GetCluster(ctx context.Context, clusterID string) (*models.Cluster, error)
	ListClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error)
	UpdateClusterStatus(ctx context.Context, clusterID string, status models.ClusterStatus) error
	DeleteCluster(ctx context.Context, clusterID string) error

	// Node management
	AddNode(ctx context.Context, node *models.Node) error
	RemoveNode(ctx context.Context, nodeID string) error
	UpdateNodeMetrics(ctx context.Context, nodeID string, metrics *models.NodeMetrics) error

	// Topology information
	GetTopology(ctx context.Context) (*models.Topology, error)
	GetZoneHealth(ctx context.Context, zoneID string) (*ZoneHealth, error)
	GetClusterHealth(ctx context.Context, clusterID string) (*ClusterHealth, error)
}

// ZoneHealth represents the health status of a zone
type ZoneHealth struct {
	ZoneID           string                       `json:"zone_id"`
	Status           models.ZoneStatus            `json:"status"`
	TotalClusters    int                          `json:"total_clusters"`
	HealthyClusters  int                          `json:"healthy_clusters"`
	DegradedClusters int                          `json:"degraded_clusters"`
	OfflineClusters  int                          `json:"offline_clusters"`
	TotalNodes       int                          `json:"total_nodes"`
	OnlineNodes      int                          `json:"online_nodes"`
	OfflineNodes     int                          `json:"offline_nodes"`
	DrainingNodes    int                          `json:"draining_nodes"`
	ClusterHealth    map[string]*ClusterHealth    `json:"cluster_health"`
	LastUpdated      time.Time                    `json:"last_updated"`
}

// ClusterHealth represents the health status of a cluster
type ClusterHealth struct {
	ClusterID     string                    `json:"cluster_id"`
	Status        models.ClusterStatus      `json:"status"`
	TotalNodes    int                       `json:"total_nodes"`
	OnlineNodes   int                       `json:"online_nodes"`
	OfflineNodes  int                       `json:"offline_nodes"`
	DrainingNodes int                       `json:"draining_nodes"`
	NodeHealth    map[string]*NodeHealth    `json:"node_health"`
	LastUpdated   time.Time                 `json:"last_updated"`
}

// NodeHealth represents the health status of a node
type NodeHealth struct {
	NodeID      string             `json:"node_id"`
	Status      models.NodeStatus  `json:"status"`
	Metrics     *models.NodeMetrics `json:"metrics,omitempty"`
	LastSeen    time.Time          `json:"last_seen"`
}

// topologyService implements the Service interface
type topologyService struct {
	store storage.MetadataStore
}

// NewService creates a new topology service
func NewService(store storage.MetadataStore) Service {
	return &topologyService{
		store: store,
	}
}

// RegisterZone registers a new zone
func (s *topologyService) RegisterZone(ctx context.Context, zone *models.Zone) error {
	if zone.ID == "" {
		return fmt.Errorf("zone ID is required")
	}
	if zone.Name == "" {
		return fmt.Errorf("zone name is required")
	}
	if zone.Region == "" {
		return fmt.Errorf("zone region is required")
	}

	// Set default status if not provided
	if zone.Status == "" {
		zone.Status = models.ZoneStatusActive
	}

	// Set timestamps
	now := time.Now()
	if zone.CreatedAt.IsZero() {
		zone.CreatedAt = now
	}
	zone.UpdatedAt = now

	// Initialize capabilities if nil
	if zone.Capabilities == nil {
		zone.Capabilities = make(map[string]string)
	}

	return s.store.RegisterZone(ctx, zone)
}

// GetZone retrieves a zone by ID
func (s *topologyService) GetZone(ctx context.Context, zoneID string) (*models.Zone, error) {
	if zoneID == "" {
		return nil, fmt.Errorf("zone ID is required")
	}

	// Try to get zone from store (assuming GetZone method exists)
	topology, err := s.store.GetZoneTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	zone, exists := topology.Zones[zoneID]
	if !exists {
		return nil, fmt.Errorf("zone not found: %s", zoneID)
	}

	return zone, nil
}

// ListZones retrieves all zones
func (s *topologyService) ListZones(ctx context.Context) ([]*models.Zone, error) {
	topology, err := s.store.GetZoneTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	zones := make([]*models.Zone, 0, len(topology.Zones))
	for _, zone := range topology.Zones {
		zones = append(zones, zone)
	}

	return zones, nil
}

// UpdateZoneStatus updates the status of a zone
func (s *topologyService) UpdateZoneStatus(ctx context.Context, zoneID string, status models.ZoneStatus) error {
	if zoneID == "" {
		return fmt.Errorf("zone ID is required")
	}

	// Validate status
	switch status {
	case models.ZoneStatusActive, models.ZoneStatusDegraded, models.ZoneStatusMaintenance, models.ZoneStatusOffline:
		// Valid status
	default:
		return fmt.Errorf("invalid zone status: %s", status)
	}

	// Check if we have UpdateZoneStatus method in store, if not we'll need to implement it
	if updater, ok := s.store.(interface {
		UpdateZoneStatus(ctx context.Context, zoneID string, status models.ZoneStatus) error
	}); ok {
		return updater.UpdateZoneStatus(ctx, zoneID, status)
	}

	// Fallback: get zone, update status, and re-register
	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return fmt.Errorf("failed to get zone: %w", err)
	}

	zone.Status = status
	zone.UpdatedAt = time.Now()

	return s.store.RegisterZone(ctx, zone)
}

// DeleteZone removes a zone (soft delete by setting status to offline)
func (s *topologyService) DeleteZone(ctx context.Context, zoneID string) error {
	if zoneID == "" {
		return fmt.Errorf("zone ID is required")
	}

	// Check if zone has active clusters
	clusters, err := s.ListClusters(ctx, zoneID)
	if err != nil {
		return fmt.Errorf("failed to check zone clusters: %w", err)
	}

	for _, cluster := range clusters {
		if cluster.Status != models.ClusterStatusOffline {
			return fmt.Errorf("cannot delete zone with active clusters: cluster %s is %s", cluster.ID, cluster.Status)
		}
	}

	// Soft delete by setting status to offline
	return s.UpdateZoneStatus(ctx, zoneID, models.ZoneStatusOffline)
}

// RegisterCluster registers a new cluster
func (s *topologyService) RegisterCluster(ctx context.Context, cluster *models.Cluster) error {
	if cluster.ID == "" {
		return fmt.Errorf("cluster ID is required")
	}
	if cluster.ZoneID == "" {
		return fmt.Errorf("cluster zone ID is required")
	}
	if cluster.Name == "" {
		return fmt.Errorf("cluster name is required")
	}
	if cluster.Endpoint == "" {
		return fmt.Errorf("cluster endpoint is required")
	}

	// Verify zone exists
	_, err := s.GetZone(ctx, cluster.ZoneID)
	if err != nil {
		return fmt.Errorf("zone not found: %w", err)
	}

	// Set default status if not provided
	if cluster.Status == "" {
		cluster.Status = models.ClusterStatusHealthy
	}

	// Set timestamps
	now := time.Now()
	if cluster.CreatedAt.IsZero() {
		cluster.CreatedAt = now
	}
	cluster.UpdatedAt = now

	// Initialize capabilities if nil
	if cluster.Capabilities == nil {
		cluster.Capabilities = make(map[string]string)
	}

	// Initialize nodes if nil
	if cluster.Nodes == nil {
		cluster.Nodes = []*models.Node{}
	}

	return s.store.RegisterCluster(ctx, cluster)
}

// GetCluster retrieves a cluster by ID
func (s *topologyService) GetCluster(ctx context.Context, clusterID string) (*models.Cluster, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("cluster ID is required")
	}

	topology, err := s.store.GetZoneTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	cluster, exists := topology.Clusters[clusterID]
	if !exists {
		return nil, fmt.Errorf("cluster not found: %s", clusterID)
	}

	return cluster, nil
}

// ListClusters retrieves clusters, optionally filtered by zone
func (s *topologyService) ListClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error) {
	topology, err := s.store.GetZoneTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	clusters := make([]*models.Cluster, 0)
	for _, cluster := range topology.Clusters {
		if zoneID == "" || cluster.ZoneID == zoneID {
			clusters = append(clusters, cluster)
		}
	}

	return clusters, nil
}

// UpdateClusterStatus updates the status of a cluster
func (s *topologyService) UpdateClusterStatus(ctx context.Context, clusterID string, status models.ClusterStatus) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID is required")
	}

	// Validate status
	switch status {
	case models.ClusterStatusHealthy, models.ClusterStatusDegraded, models.ClusterStatusOffline:
		// Valid status
	default:
		return fmt.Errorf("invalid cluster status: %s", status)
	}

	// Check if we have UpdateClusterStatus method in store
	if updater, ok := s.store.(interface {
		UpdateClusterStatus(ctx context.Context, clusterID string, status models.ClusterStatus) error
	}); ok {
		return updater.UpdateClusterStatus(ctx, clusterID, status)
	}

	// Fallback: get cluster, update status, and re-register
	cluster, err := s.GetCluster(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("failed to get cluster: %w", err)
	}

	cluster.Status = status
	cluster.UpdatedAt = time.Now()

	return s.store.RegisterCluster(ctx, cluster)
}

// DeleteCluster removes a cluster (soft delete by setting status to offline)
func (s *topologyService) DeleteCluster(ctx context.Context, clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID is required")
	}

	// Check if cluster has active nodes
	cluster, err := s.GetCluster(ctx, clusterID)
	if err != nil {
		return fmt.Errorf("failed to get cluster: %w", err)
	}

	for _, node := range cluster.Nodes {
		if node.Status == models.NodeStatusOnline {
			return fmt.Errorf("cannot delete cluster with online nodes: node %s is online", node.ID)
		}
	}

	// Soft delete by setting status to offline
	return s.UpdateClusterStatus(ctx, clusterID, models.ClusterStatusOffline)
}

// AddNode adds a node to a cluster
func (s *topologyService) AddNode(ctx context.Context, node *models.Node) error {
	if node.ID == "" {
		return fmt.Errorf("node ID is required")
	}
	if node.ClusterID == "" {
		return fmt.Errorf("node cluster ID is required")
	}
	if node.Address == "" {
		return fmt.Errorf("node address is required")
	}

	// Verify cluster exists
	cluster, err := s.GetCluster(ctx, node.ClusterID)
	if err != nil {
		return fmt.Errorf("cluster not found: %w", err)
	}

	// Set default status if not provided
	if node.Status == "" {
		node.Status = models.NodeStatusOnline
	}

	// Set timestamps
	now := time.Now()
	if node.CreatedAt.IsZero() {
		node.CreatedAt = now
	}
	node.UpdatedAt = now

	// Add node to cluster
	cluster.Nodes = append(cluster.Nodes, node)
	cluster.UpdatedAt = time.Now()

	return s.store.RegisterCluster(ctx, cluster)
}

// RemoveNode removes a node from a cluster
func (s *topologyService) RemoveNode(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("node ID is required")
	}

	// Find the cluster containing this node
	topology, err := s.store.GetZoneTopology(ctx)
	if err != nil {
		return fmt.Errorf("failed to get topology: %w", err)
	}

	var targetCluster *models.Cluster
	var nodeIndex int = -1

	for _, cluster := range topology.Clusters {
		for i, node := range cluster.Nodes {
			if node.ID == nodeID {
				targetCluster = cluster
				nodeIndex = i
				break
			}
		}
		if targetCluster != nil {
			break
		}
	}

	if targetCluster == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// Remove node from cluster
	targetCluster.Nodes = append(targetCluster.Nodes[:nodeIndex], targetCluster.Nodes[nodeIndex+1:]...)
	targetCluster.UpdatedAt = time.Now()

	return s.store.RegisterCluster(ctx, targetCluster)
}

// UpdateNodeMetrics updates metrics for a node
func (s *topologyService) UpdateNodeMetrics(ctx context.Context, nodeID string, metrics *models.NodeMetrics) error {
	if nodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if metrics == nil {
		return fmt.Errorf("metrics are required")
	}

	// Check if we have UpdateNodeMetrics method in store
	if updater, ok := s.store.(interface {
		UpdateNodeMetrics(ctx context.Context, nodeID string, metrics *models.NodeMetrics) error
	}); ok {
		return updater.UpdateNodeMetrics(ctx, nodeID, metrics)
	}

	// Fallback: find node, update metrics, and re-register cluster
	topology, err := s.store.GetZoneTopology(ctx)
	if err != nil {
		return fmt.Errorf("failed to get topology: %w", err)
	}

	var targetCluster *models.Cluster
	var targetNode *models.Node

	for _, cluster := range topology.Clusters {
		for _, node := range cluster.Nodes {
			if node.ID == nodeID {
				targetCluster = cluster
				targetNode = node
				break
			}
		}
		if targetNode != nil {
			break
		}
	}

	if targetNode == nil {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// Update metrics
	metrics.LastUpdated = time.Now()
	targetNode.Metrics = metrics
	targetNode.UpdatedAt = time.Now()
	targetCluster.UpdatedAt = time.Now()

	return s.store.RegisterCluster(ctx, targetCluster)
}

// GetTopology retrieves the complete topology
func (s *topologyService) GetTopology(ctx context.Context) (*models.Topology, error) {
	return s.store.GetZoneTopology(ctx)
}

// GetZoneHealth calculates and returns zone health information
func (s *topologyService) GetZoneHealth(ctx context.Context, zoneID string) (*ZoneHealth, error) {
	if zoneID == "" {
		return nil, fmt.Errorf("zone ID is required")
	}

	zone, err := s.GetZone(ctx, zoneID)
	if err != nil {
		return nil, fmt.Errorf("failed to get zone: %w", err)
	}

	clusters, err := s.ListClusters(ctx, zoneID)
	if err != nil {
		return nil, fmt.Errorf("failed to get clusters: %w", err)
	}

	health := &ZoneHealth{
		ZoneID:        zoneID,
		Status:        zone.Status,
		ClusterHealth: make(map[string]*ClusterHealth),
		LastUpdated:   time.Now(),
	}

	// Calculate cluster and node statistics
	for _, cluster := range clusters {
		clusterHealth, err := s.GetClusterHealth(ctx, cluster.ID)
		if err != nil {
			continue // Skip clusters with errors
		}

		health.ClusterHealth[cluster.ID] = clusterHealth
		health.TotalClusters++
		health.TotalNodes += clusterHealth.TotalNodes
		health.OnlineNodes += clusterHealth.OnlineNodes
		health.OfflineNodes += clusterHealth.OfflineNodes
		health.DrainingNodes += clusterHealth.DrainingNodes

		switch cluster.Status {
		case models.ClusterStatusHealthy:
			health.HealthyClusters++
		case models.ClusterStatusDegraded:
			health.DegradedClusters++
		case models.ClusterStatusOffline:
			health.OfflineClusters++
		}
	}

	return health, nil
}

// GetClusterHealth calculates and returns cluster health information
func (s *topologyService) GetClusterHealth(ctx context.Context, clusterID string) (*ClusterHealth, error) {
	if clusterID == "" {
		return nil, fmt.Errorf("cluster ID is required")
	}

	cluster, err := s.GetCluster(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster: %w", err)
	}

	health := &ClusterHealth{
		ClusterID:   clusterID,
		Status:      cluster.Status,
		NodeHealth:  make(map[string]*NodeHealth),
		LastUpdated: time.Now(),
	}

	// Calculate node statistics
	for _, node := range cluster.Nodes {
		nodeHealth := &NodeHealth{
			NodeID:   node.ID,
			Status:   node.Status,
			Metrics:  node.Metrics,
			LastSeen: node.UpdatedAt,
		}

		health.NodeHealth[node.ID] = nodeHealth
		health.TotalNodes++

		switch node.Status {
		case models.NodeStatusOnline:
			health.OnlineNodes++
		case models.NodeStatusOffline:
			health.OfflineNodes++
		case models.NodeStatusDraining:
			health.DrainingNodes++
		}
	}

	return health, nil
}