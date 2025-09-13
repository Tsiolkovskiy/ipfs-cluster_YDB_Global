package api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"go.uber.org/zap"
)

// ExampleServices provides a simple implementation of the Services interface for demonstration
type ExampleServices struct {
	zones    map[string]*models.Zone
	clusters map[string]*models.Cluster
	content  map[string]*models.Content
	policies map[string]*models.Policy
}

// NewExampleServices creates a new example services implementation
func NewExampleServices() *ExampleServices {
	return &ExampleServices{
		zones:    make(map[string]*models.Zone),
		clusters: make(map[string]*models.Cluster),
		content:  make(map[string]*models.Content),
		policies: make(map[string]*models.Policy),
	}
}

// Zone management methods
func (s *ExampleServices) GetZones(ctx context.Context) ([]*models.Zone, error) {
	zones := make([]*models.Zone, 0, len(s.zones))
	for _, zone := range s.zones {
		zones = append(zones, zone)
	}
	return zones, nil
}

func (s *ExampleServices) GetZone(ctx context.Context, id string) (*models.Zone, error) {
	zone, exists := s.zones[id]
	if !exists {
		return nil, fmt.Errorf("zone not found")
	}
	return zone, nil
}

func (s *ExampleServices) CreateZone(ctx context.Context, zone *models.Zone) error {
	if zone.ID == "" {
		zone.ID = fmt.Sprintf("zone-%d", time.Now().Unix())
	}
	zone.CreatedAt = time.Now()
	zone.UpdatedAt = time.Now()
	s.zones[zone.ID] = zone
	return nil
}

func (s *ExampleServices) UpdateZone(ctx context.Context, id string, zone *models.Zone) error {
	if _, exists := s.zones[id]; !exists {
		return fmt.Errorf("zone not found")
	}
	zone.ID = id
	zone.UpdatedAt = time.Now()
	s.zones[id] = zone
	return nil
}

func (s *ExampleServices) DeleteZone(ctx context.Context, id string) error {
	if _, exists := s.zones[id]; !exists {
		return fmt.Errorf("zone not found")
	}
	delete(s.zones, id)
	return nil
}

// Cluster management methods
func (s *ExampleServices) GetClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error) {
	clusters := make([]*models.Cluster, 0)
	for _, cluster := range s.clusters {
		if zoneID == "" || cluster.ZoneID == zoneID {
			clusters = append(clusters, cluster)
		}
	}
	return clusters, nil
}

func (s *ExampleServices) GetCluster(ctx context.Context, id string) (*models.Cluster, error) {
	cluster, exists := s.clusters[id]
	if !exists {
		return nil, fmt.Errorf("cluster not found")
	}
	return cluster, nil
}

func (s *ExampleServices) CreateCluster(ctx context.Context, cluster *models.Cluster) error {
	if cluster.ID == "" {
		cluster.ID = fmt.Sprintf("cluster-%d", time.Now().Unix())
	}
	cluster.CreatedAt = time.Now()
	cluster.UpdatedAt = time.Now()
	s.clusters[cluster.ID] = cluster
	return nil
}

func (s *ExampleServices) UpdateCluster(ctx context.Context, id string, cluster *models.Cluster) error {
	if _, exists := s.clusters[id]; !exists {
		return fmt.Errorf("cluster not found")
	}
	cluster.ID = id
	cluster.UpdatedAt = time.Now()
	s.clusters[id] = cluster
	return nil
}

func (s *ExampleServices) DeleteCluster(ctx context.Context, id string) error {
	if _, exists := s.clusters[id]; !exists {
		return fmt.Errorf("cluster not found")
	}
	delete(s.clusters, id)
	return nil
}

// Content management methods
func (s *ExampleServices) PinContent(ctx context.Context, request *PinRequest) (*models.PinPlan, error) {
	plan := &models.PinPlan{
		ID:  fmt.Sprintf("plan-%d", time.Now().Unix()),
		CID: request.CID,
		Assignments: []*models.NodeAssignment{
			{
				NodeID:    "node-1",
				ClusterID: "cluster-1",
				ZoneID:    "zone-1",
				Priority:  1,
			},
		},
		Cost: &models.CostEstimate{
			StorageCost: 0.01,
			EgressCost:  0.005,
			ComputeCost: 0.002,
			TotalCost:   0.017,
			Currency:    "USD",
		},
		CreatedAt: time.Now(),
	}
	return plan, nil
}

func (s *ExampleServices) UnpinContent(ctx context.Context, cid string) error {
	if _, exists := s.content[cid]; !exists {
		return fmt.Errorf("content not found")
	}
	delete(s.content, cid)
	return nil
}

func (s *ExampleServices) GetContentStatus(ctx context.Context, cid string) (*models.PinStatus, error) {
	status := &models.PinStatus{
		CID:       cid,
		Status:    models.ContentStatusPinned,
		Progress:  1.0,
		UpdatedAt: time.Now(),
	}
	return status, nil
}

func (s *ExampleServices) ListContent(ctx context.Context, filter *ContentFilter) ([]*models.Content, error) {
	content := make([]*models.Content, 0, len(s.content))
	for _, c := range s.content {
		content = append(content, c)
	}
	return content, nil
}

// Policy management methods
func (s *ExampleServices) GetPolicies(ctx context.Context) ([]*models.Policy, error) {
	policies := make([]*models.Policy, 0, len(s.policies))
	for _, policy := range s.policies {
		policies = append(policies, policy)
	}
	return policies, nil
}

func (s *ExampleServices) GetPolicy(ctx context.Context, id string) (*models.Policy, error) {
	policy, exists := s.policies[id]
	if !exists {
		return nil, fmt.Errorf("policy not found")
	}
	return policy, nil
}

func (s *ExampleServices) CreatePolicy(ctx context.Context, policy *models.Policy) error {
	if policy.ID == "" {
		policy.ID = fmt.Sprintf("policy-%d", time.Now().Unix())
	}
	policy.CreatedAt = time.Now()
	s.policies[policy.ID] = policy
	return nil
}

func (s *ExampleServices) UpdatePolicy(ctx context.Context, id string, policy *models.Policy) error {
	if _, exists := s.policies[id]; !exists {
		return fmt.Errorf("policy not found")
	}
	policy.ID = id
	s.policies[id] = policy
	return nil
}

func (s *ExampleServices) DeletePolicy(ctx context.Context, id string) error {
	if _, exists := s.policies[id]; !exists {
		return fmt.Errorf("policy not found")
	}
	delete(s.policies, id)
	return nil
}

// Topology method
func (s *ExampleServices) GetTopology(ctx context.Context) (*models.Topology, error) {
	topology := &models.Topology{
		Zones:     s.zones,
		Clusters:  s.clusters,
		UpdatedAt: time.Now(),
	}
	return topology, nil
}

// ExampleUsage demonstrates how to use the API Gateway
func ExampleUsage() {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal("Failed to create logger:", err)
	}

	// Create example services
	services := NewExampleServices()

	// Add some sample data
	sampleZone := &models.Zone{
		ID:     "zone-msk-1",
		Name:   "Moscow Primary",
		Region: "RU-MSK",
		Status: models.ZoneStatusActive,
		Capabilities: map[string]string{
			"storage_type": "ssd",
			"network_tier": "premium",
		},
		Coordinates: &models.GeoCoordinates{
			Latitude:  55.7558,
			Longitude: 37.6176,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	services.CreateZone(context.Background(), sampleZone)

	sampleCluster := &models.Cluster{
		ID:       "cluster-msk-1",
		ZoneID:   "zone-msk-1",
		Name:     "Moscow Cluster 1",
		Endpoint: "https://cluster-msk-1.example.com:9094",
		Status:   models.ClusterStatusHealthy,
		Version:  "1.1.4",
		Nodes: []*models.Node{
			{
				ID:        "node-1",
				ClusterID: "cluster-msk-1",
				Address:   "10.0.1.10:4001",
				Status:    models.NodeStatusOnline,
				Resources: &models.NodeResources{
					CPUCores:     8,
					MemoryBytes:  32 * 1024 * 1024 * 1024, // 32GB
					StorageBytes: 2 * 1024 * 1024 * 1024 * 1024, // 2TB
					NetworkMbps:  1000,
				},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
		},
		Capabilities: map[string]string{
			"pin_tracker": "stateless",
			"consensus":   "crdt",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	services.CreateCluster(context.Background(), sampleCluster)

	// Create HTTP Gateway
	httpGateway := NewHTTPGateway(services, logger)

	// Create gRPC Server
	grpcServer := NewGRPCServer(services, logger)

	// Start servers
	ctx := context.Background()

	// Start HTTP Gateway
	if err := httpGateway.Start(ctx, ":8080"); err != nil {
		logger.Fatal("Failed to start HTTP gateway", zap.Error(err))
	}

	// Start gRPC Server
	if err := grpcServer.Start(ctx, ":9090"); err != nil {
		logger.Fatal("Failed to start gRPC server", zap.Error(err))
	}

	logger.Info("API Gateway started successfully",
		zap.String("http_addr", ":8080"),
		zap.String("grpc_addr", ":9090"),
	)

	// The servers are now running and ready to accept requests
	// Example endpoints:
	// GET  http://localhost:8080/health
	// GET  http://localhost:8080/ready
	// GET  http://localhost:8080/api/v1/zones
	// POST http://localhost:8080/api/v1/zones
	// GET  http://localhost:8080/api/v1/clusters
	// POST http://localhost:8080/api/v1/content/pin
	// GET  http://localhost:8080/api/v1/openapi.json

	// To stop the servers gracefully:
	// httpGateway.Stop(ctx)
	// grpcServer.Stop(ctx)
}