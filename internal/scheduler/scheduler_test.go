package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/global-data-controller/gdc/internal/policy"
	"github.com/global-data-controller/gdc/internal/topology"
)

// Mock implementations for testing

type mockPolicyEngine struct {
	policies map[string]*policy.Policy
	results  map[string]*policy.Result
}

func newMockPolicyEngine() *mockPolicyEngine {
	return &mockPolicyEngine{
		policies: make(map[string]*policy.Policy),
		results:  make(map[string]*policy.Result),
	}
}

func (m *mockPolicyEngine) CreatePolicy(ctx context.Context, policy *policy.Policy) error {
	m.policies[policy.ID] = policy
	return nil
}

func (m *mockPolicyEngine) UpdatePolicy(ctx context.Context, id string, policy *policy.Policy) error {
	m.policies[id] = policy
	return nil
}

func (m *mockPolicyEngine) DeletePolicy(ctx context.Context, id string) error {
	delete(m.policies, id)
	return nil
}

func (m *mockPolicyEngine) GetPolicy(ctx context.Context, id string) (*policy.Policy, error) {
	if p, exists := m.policies[id]; exists {
		return p, nil
	}
	return nil, nil
}

func (m *mockPolicyEngine) ListPolicies(ctx context.Context, filter *policy.Filter) ([]*policy.Policy, error) {
	var result []*policy.Policy
	for _, p := range m.policies {
		result = append(result, p)
	}
	return result, nil
}

func (m *mockPolicyEngine) EvaluatePolicies(ctx context.Context, request *policy.PlacementRequest) (*policy.Result, error) {
	// Return a default result or a pre-configured one
	if result, exists := m.results[request.CID]; exists {
		return result, nil
	}
	
	return &policy.Result{
		ReplicationFactor: 2,
		PlacementRules: []policy.PlacementRule{
			{
				Type: "zone_constraint",
				Constraints: map[string]string{
					"min_zones": "2",
				},
			},
		},
		Constraints: make(map[string]string),
	}, nil
}

func (m *mockPolicyEngine) ValidatePolicy(ctx context.Context, policy *policy.Policy) error {
	return nil
}

func (m *mockPolicyEngine) setResult(cid string, result *policy.Result) {
	m.results[cid] = result
}

type mockTopologyService struct {
	topology *models.Topology
}

func newMockTopologyService() *mockTopologyService {
	// Create a test topology with 2 zones and multiple nodes
	topology := &models.Topology{
		Zones: map[string]*models.Zone{
			"MSK": {
				ID:     "MSK",
				Name:   "Moscow",
				Region: "RU-MOW",
				Status: models.ZoneStatusActive,
			},
			"NN": {
				ID:     "NN",
				Name:   "Nizhny Novgorod",
				Region: "RU-NIZ",
				Status: models.ZoneStatusActive,
			},
		},
		Clusters: map[string]*models.Cluster{
			"cluster-msk-1": {
				ID:     "cluster-msk-1",
				ZoneID: "MSK",
				Name:   "Moscow Cluster 1",
				Status: models.ClusterStatusHealthy,
				Nodes: []*models.Node{
					{
						ID:        "node-msk-1",
						ClusterID: "cluster-msk-1",
						Status:    models.NodeStatusOnline,
						Resources: &models.NodeResources{
							StorageBytes: 1000 * 1024 * 1024 * 1024, // 1TB
						},
						Metrics: &models.NodeMetrics{
							StorageUsage: 0.3, // 30% used
						},
					},
					{
						ID:        "node-msk-2",
						ClusterID: "cluster-msk-1",
						Status:    models.NodeStatusOnline,
						Resources: &models.NodeResources{
							StorageBytes: 1000 * 1024 * 1024 * 1024, // 1TB
						},
						Metrics: &models.NodeMetrics{
							StorageUsage: 0.5, // 50% used
						},
					},
				},
			},
			"cluster-nn-1": {
				ID:     "cluster-nn-1",
				ZoneID: "NN",
				Name:   "Nizhny Novgorod Cluster 1",
				Status: models.ClusterStatusHealthy,
				Nodes: []*models.Node{
					{
						ID:        "node-nn-1",
						ClusterID: "cluster-nn-1",
						Status:    models.NodeStatusOnline,
						Resources: &models.NodeResources{
							StorageBytes: 1000 * 1024 * 1024 * 1024, // 1TB
						},
						Metrics: &models.NodeMetrics{
							StorageUsage: 0.2, // 20% used
						},
					},
					{
						ID:        "node-nn-2",
						ClusterID: "cluster-nn-1",
						Status:    models.NodeStatusOnline,
						Resources: &models.NodeResources{
							StorageBytes: 1000 * 1024 * 1024 * 1024, // 1TB
						},
						Metrics: &models.NodeMetrics{
							StorageUsage: 0.4, // 40% used
						},
					},
				},
			},
		},
		UpdatedAt: time.Now(),
	}
	
	return &mockTopologyService{topology: topology}
}

func (m *mockTopologyService) GetTopology(ctx context.Context) (*models.Topology, error) {
	return m.topology, nil
}

// Implement other required methods (stubs for testing)
func (m *mockTopologyService) RegisterZone(ctx context.Context, zone *models.Zone) error { return nil }
func (m *mockTopologyService) GetZone(ctx context.Context, zoneID string) (*models.Zone, error) { return nil, nil }
func (m *mockTopologyService) ListZones(ctx context.Context) ([]*models.Zone, error) { return nil, nil }
func (m *mockTopologyService) UpdateZoneStatus(ctx context.Context, zoneID string, status models.ZoneStatus) error { return nil }
func (m *mockTopologyService) DeleteZone(ctx context.Context, zoneID string) error { return nil }
func (m *mockTopologyService) RegisterCluster(ctx context.Context, cluster *models.Cluster) error { return nil }
func (m *mockTopologyService) GetCluster(ctx context.Context, clusterID string) (*models.Cluster, error) { return nil, nil }
func (m *mockTopologyService) ListClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error) { return nil, nil }
func (m *mockTopologyService) UpdateClusterStatus(ctx context.Context, clusterID string, status models.ClusterStatus) error { return nil }
func (m *mockTopologyService) DeleteCluster(ctx context.Context, clusterID string) error { return nil }
func (m *mockTopologyService) AddNode(ctx context.Context, node *models.Node) error { return nil }
func (m *mockTopologyService) RemoveNode(ctx context.Context, nodeID string) error { return nil }
func (m *mockTopologyService) UpdateNodeMetrics(ctx context.Context, nodeID string, metrics *models.NodeMetrics) error { return nil }

// Add missing methods from topology.Service interface
func (m *mockTopologyService) GetZoneHealth(ctx context.Context, zoneID string) (*topology.ZoneHealth, error) {
	return &topology.ZoneHealth{
		ZoneID: zoneID,
		Status: models.ZoneStatusActive,
		TotalClusters: 1,
		HealthyClusters: 1,
		TotalNodes: 2,
		OnlineNodes: 2,
	}, nil
}

func (m *mockTopologyService) GetClusterHealth(ctx context.Context, clusterID string) (*topology.ClusterHealth, error) {
	return &topology.ClusterHealth{
		ClusterID: clusterID,
		Status: models.ClusterStatusHealthy,
		TotalNodes: 2,
		OnlineNodes: 2,
	}, nil
}

// Test functions

func TestScheduler_ComputePlacement_BasicReplication(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	ctx := context.Background()
	request := &PlacementRequest{
		CID:      "QmTest123",
		Size:     1024 * 1024, // 1MB
		Policies: []string{"default-rf-2"},
		Priority: 5,
	}
	
	// Execute
	plan, err := scheduler.ComputePlacement(ctx, request)
	
	// Verify
	if err != nil {
		t.Fatalf("ComputePlacement failed: %v", err)
	}
	
	if plan == nil {
		t.Fatal("Plan is nil")
	}
	
	if len(plan.Assignments) != 2 {
		t.Errorf("Expected 2 assignments, got %d", len(plan.Assignments))
	}
	
	// Verify assignments are in different zones
	zones := make(map[string]bool)
	for _, assignment := range plan.Assignments {
		zones[assignment.ZoneID] = true
	}
	
	if len(zones) != 2 {
		t.Errorf("Expected assignments in 2 zones, got %d", len(zones))
	}
	
	// Verify cost calculation
	if plan.Cost == nil {
		t.Error("Cost estimate is nil")
	} else if plan.Cost.TotalCost <= 0 {
		t.Error("Total cost should be positive")
	}
}

func TestScheduler_ComputePlacement_ErasureCoding(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	// Configure policy engine to return erasure coding result
	policyEngine.setResult("QmTestEC", &policy.Result{
		ReplicationFactor: 0, // Not used with EC
		ErasureCoding: &policy.ErasureCoding{
			DataShards:   2,
			ParityShards: 1,
		},
		PlacementRules: []policy.PlacementRule{},
		Constraints:    make(map[string]string),
	})
	
	ctx := context.Background()
	request := &PlacementRequest{
		CID:      "QmTestEC",
		Size:     10 * 1024 * 1024, // 10MB
		Policies: []string{"erasure-coding"},
		Priority: 8,
	}
	
	// Execute
	plan, err := scheduler.ComputePlacement(ctx, request)
	
	// Verify
	if err != nil {
		t.Fatalf("ComputePlacement failed: %v", err)
	}
	
	if len(plan.Assignments) != 3 {
		t.Errorf("Expected 3 assignments (2 data + 1 parity), got %d", len(plan.Assignments))
	}
	
	// Count data and parity shards
	dataShards := 0
	parityShards := 0
	for _, assignment := range plan.Assignments {
		switch assignment.Role {
		case "data":
			dataShards++
		case "parity":
			parityShards++
		}
	}
	
	if dataShards != 2 {
		t.Errorf("Expected 2 data shards, got %d", dataShards)
	}
	
	if parityShards != 1 {
		t.Errorf("Expected 1 parity shard, got %d", parityShards)
	}
}

func TestScheduler_ComputePlacement_ZoneConstraints(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	// Configure policy engine to return zone constraints
	policyEngine.setResult("QmTestZone", &policy.Result{
		ReplicationFactor: 3,
		PlacementRules: []policy.PlacementRule{
			{
				Type: "zone_constraint",
				Constraints: map[string]string{
					"min_zones":        "2",
					"preferred_zones":  "MSK,NN",
				},
			},
		},
		Constraints: make(map[string]string),
	})
	
	ctx := context.Background()
	request := &PlacementRequest{
		CID:      "QmTestZone",
		Size:     5 * 1024 * 1024, // 5MB
		Policies: []string{"zone-constraints"},
		Priority: 7,
	}
	
	// Execute
	plan, err := scheduler.ComputePlacement(ctx, request)
	
	// Verify
	if err != nil {
		t.Fatalf("ComputePlacement failed: %v", err)
	}
	
	if len(plan.Assignments) != 3 {
		t.Errorf("Expected 3 assignments, got %d", len(plan.Assignments))
	}
	
	// Verify zone distribution
	zones := make(map[string]int)
	for _, assignment := range plan.Assignments {
		zones[assignment.ZoneID]++
	}
	
	if len(zones) < 2 {
		t.Errorf("Expected at least 2 zones, got %d", len(zones))
	}
	
	// Verify preferred zones are used
	for zoneID := range zones {
		if zoneID != "MSK" && zoneID != "NN" {
			t.Errorf("Unexpected zone %s, should prefer MSK or NN", zoneID)
		}
	}
}

func TestScheduler_ComputePlacement_InsufficientNodes(t *testing.T) {
	// Setup with limited topology
	policyEngine := newMockPolicyEngine()
	topologyService := &mockTopologyService{
		topology: &models.Topology{
			Zones: map[string]*models.Zone{
				"MSK": {
					ID:     "MSK",
					Name:   "Moscow",
					Status: models.ZoneStatusActive,
				},
			},
			Clusters: map[string]*models.Cluster{
				"cluster-msk-1": {
					ID:     "cluster-msk-1",
					ZoneID: "MSK",
					Status: models.ClusterStatusHealthy,
					Nodes: []*models.Node{
						{
							ID:        "node-msk-1",
							ClusterID: "cluster-msk-1",
							Status:    models.NodeStatusOnline,
						},
					},
				},
			},
		},
	}
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	// Configure policy engine to require more replicas than available nodes
	policyEngine.setResult("QmTestInsufficient", &policy.Result{
		ReplicationFactor: 5, // More than available nodes
		PlacementRules:    []policy.PlacementRule{},
		Constraints:       make(map[string]string),
	})
	
	ctx := context.Background()
	request := &PlacementRequest{
		CID:      "QmTestInsufficient",
		Size:     1024 * 1024,
		Policies: []string{"high-rf"},
		Priority: 5,
	}
	
	// Execute
	_, err := scheduler.ComputePlacement(ctx, request)
	
	// Verify error is returned
	if err == nil {
		t.Error("Expected error for insufficient nodes, got nil")
	}
}

func TestScheduler_SimulatePlacement(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	ctx := context.Background()
	request := &PlacementRequest{
		CID:      "QmTestSim",
		Size:     2 * 1024 * 1024, // 2MB
		Policies: []string{"default"},
		Priority: 5,
	}
	
	// Execute
	result, err := scheduler.SimulatePlacement(ctx, request)
	
	// Verify
	if err != nil {
		t.Fatalf("SimulatePlacement failed: %v", err)
	}
	
	if result == nil {
		t.Fatal("Simulation result is nil")
	}
	
	if !result.Feasible {
		t.Error("Simulation should be feasible")
	}
	
	if result.Plan == nil {
		t.Error("Simulation plan is nil")
	}
	
	if len(result.Recommendations) == 0 {
		t.Error("Expected recommendations in simulation result")
	}
}

func TestScheduler_ValidatePlan(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel).(*schedulerService)
	
	// Test valid plan
	validPlan := &PlacementPlan{
		ID: "test-plan",
		Request: &PlacementRequest{
			CID:  "test-cid",
			Size: 1024,
		},
		Assignments: []*NodeAssignment{
			{NodeID: "node1", ClusterID: "cluster1", ZoneID: "MSK", Role: "replica"},
			{NodeID: "node2", ClusterID: "cluster2", ZoneID: "NN", Role: "replica"},
		},
		Cost: &CostEstimate{TotalCost: 0.05},
	}
	
	policyResult := &policy.Result{
		ReplicationFactor: 2,
		PlacementRules: []policy.PlacementRule{
			{
				Type: "zone_constraint",
				Constraints: map[string]string{"min_zones": "2"},
			},
		},
	}
	
	err := scheduler.validatePlan(validPlan, policyResult)
	if err != nil {
		t.Errorf("Valid plan should pass validation: %v", err)
	}
	
	// Test invalid plan - insufficient replicas
	invalidPlan := &PlacementPlan{
		ID: "test-plan-invalid",
		Assignments: []*NodeAssignment{
			{NodeID: "node1", ZoneID: "MSK"},
		},
		Cost: &CostEstimate{TotalCost: 0.025},
	}
	
	err = scheduler.validatePlan(invalidPlan, policyResult)
	if err == nil {
		t.Error("Invalid plan should fail validation")
	}
}

func TestCostModel_CalculateStorageCost(t *testing.T) {
	costModel := NewSimpleCostModel()
	
	// Test storage cost calculation
	sizeBytes := int64(1024 * 1024 * 1024) // 1GB
	cost := costModel.CalculateStorageCost(sizeBytes, "MSK")
	
	expectedCost := 0.023 // $0.023 per GB for MSK
	if cost != expectedCost {
		t.Errorf("Expected storage cost %f, got %f", expectedCost, cost)
	}
	
	// Test unknown zone (should use default)
	cost = costModel.CalculateStorageCost(sizeBytes, "UNKNOWN")
	expectedCost = 0.025 // Default cost
	if cost != expectedCost {
		t.Errorf("Expected default storage cost %f, got %f", expectedCost, cost)
	}
}

func TestCostModel_CalculateEgressCost(t *testing.T) {
	costModel := NewSimpleCostModel()
	
	// Test egress cost calculation
	sizeBytes := int64(1024 * 1024 * 1024) // 1GB
	cost := costModel.CalculateEgressCost(sizeBytes, "MSK", "NN")
	
	expectedCost := 0.05 // $0.05 per GB MSK -> NN
	if cost != expectedCost {
		t.Errorf("Expected egress cost %f, got %f", expectedCost, cost)
	}
	
	// Test same zone (should be 0)
	cost = costModel.CalculateEgressCost(sizeBytes, "MSK", "MSK")
	if cost != 0.0 {
		t.Errorf("Expected 0 cost for same zone, got %f", cost)
	}
}

func TestCostModel_GetZoneLatency(t *testing.T) {
	costModel := NewSimpleCostModel()
	
	// Test known latency
	latency := costModel.GetZoneLatency("MSK", "NN")
	expectedLatency := 15 * time.Millisecond
	if latency != expectedLatency {
		t.Errorf("Expected latency %v, got %v", expectedLatency, latency)
	}
	
	// Test same zone
	latency = costModel.GetZoneLatency("MSK", "MSK")
	expectedLatency = 1 * time.Millisecond
	if latency != expectedLatency {
		t.Errorf("Expected intra-zone latency %v, got %v", expectedLatency, latency)
	}
	
	// Test unknown zones
	latency = costModel.GetZoneLatency("UNKNOWN1", "UNKNOWN2")
	expectedLatency = 50 * time.Millisecond // Default
	if latency != expectedLatency {
		t.Errorf("Expected default latency %v, got %v", expectedLatency, latency)
	}
}

func TestScheduler_NodeScoring(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel).(*schedulerService)
	
	// Create test nodes with different metrics
	node1 := &models.Node{
		ID: "node1",
		Metrics: &models.NodeMetrics{
			StorageUsage: 0.2, // 20% used - better
			CPUUsage:     0.3,
			MemoryUsage:  0.4,
		},
	}
	
	node2 := &models.Node{
		ID: "node2",
		Metrics: &models.NodeMetrics{
			StorageUsage: 0.8, // 80% used - worse
			CPUUsage:     0.1,
			MemoryUsage:  0.2,
		},
	}
	
	request := &PlacementRequest{
		CID:  "test",
		Size: 1024,
	}
	
	score1 := scheduler.calculateNodeScore(request, node1)
	score2 := scheduler.calculateNodeScore(request, node2)
	
	// Node1 should have higher score due to lower storage usage
	if score1 <= score2 {
		t.Errorf("Node1 should have higher score than Node2: %f vs %f", score1, score2)
	}
}

func TestScheduler_ZoneScoring(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel).(*schedulerService)
	
	// Create test nodes for zone scoring
	nodes := []*models.Node{
		{
			ID: "node1",
			Status: models.NodeStatusOnline,
			Resources: &models.NodeResources{
				StorageBytes: 1000 * 1024 * 1024 * 1024, // 1TB
			},
			Metrics: &models.NodeMetrics{
				StorageUsage: 0.3,
				CPUUsage:     0.2,
				MemoryUsage:  0.4,
			},
		},
		{
			ID: "node2",
			Status: models.NodeStatusOnline,
			Resources: &models.NodeResources{
				StorageBytes: 1000 * 1024 * 1024 * 1024, // 1TB
			},
			Metrics: &models.NodeMetrics{
				StorageUsage: 0.5,
				CPUUsage:     0.1,
				MemoryUsage:  0.3,
			},
		},
	}
	
	request := &PlacementRequest{
		CID:  "test",
		Size: 1024 * 1024, // 1MB
	}
	
	score := scheduler.calculateZoneScore(request, "MSK", nodes)
	
	// Score should be positive and reasonable
	if score <= 0 {
		t.Errorf("Zone score should be positive, got %f", score)
	}
	
	if score > 100 {
		t.Errorf("Zone score should be reasonable (<=100), got %f", score)
	}
}

func TestScheduler_OptimizePlacement(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	ctx := context.Background()
	constraints := &OptimizationConstraints{
		MaxCost:        1.0,
		PreferredZones: []string{"MSK", "NN"},
		Objectives:     []string{"cost", "availability"},
	}
	
	// Execute
	plan, err := scheduler.OptimizePlacement(ctx, constraints)
	
	// Verify
	if err != nil {
		t.Fatalf("OptimizePlacement failed: %v", err)
	}
	
	if plan == nil {
		t.Fatal("Optimized plan is nil")
	}
	
	if plan.Cost.TotalCost > constraints.MaxCost {
		t.Errorf("Plan cost %f exceeds max cost %f", plan.Cost.TotalCost, constraints.MaxCost)
	}
	
	// Verify preferred zones are used
	zones := make(map[string]bool)
	for _, assignment := range plan.Assignments {
		zones[assignment.ZoneID] = true
	}
	
	for zoneID := range zones {
		found := false
		for _, preferred := range constraints.PreferredZones {
			if zoneID == preferred {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Plan uses non-preferred zone: %s", zoneID)
		}
	}
}

func TestScheduler_ComputeRebalance(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	ctx := context.Background()
	target := &RebalanceTarget{
		ZoneID:     "MSK",
		TargetLoad: 0.5, // 50% target load
	}
	
	// Execute
	plan, err := scheduler.ComputeRebalance(ctx, target)
	
	// Verify
	if err != nil {
		t.Fatalf("ComputeRebalance failed: %v", err)
	}
	
	if plan == nil {
		t.Fatal("Rebalance plan is nil")
	}
	
	if plan.Target.ZoneID != target.ZoneID {
		t.Errorf("Plan target zone %s != expected %s", plan.Target.ZoneID, target.ZoneID)
	}
	
	// Verify moves are reasonable
	for _, move := range plan.Moves {
		if move.FromNodeID == "" {
			t.Error("Move has empty FromNodeID")
		}
		if move.ToNodeID == "" {
			t.Error("Move has empty ToNodeID")
		}
		if move.Size <= 0 {
			t.Error("Move size should be positive")
		}
		if move.FromNodeID == move.ToNodeID {
			t.Error("Move should not be from node to itself")
		}
	}
}

func TestScheduler_ValidationEdgeCases(t *testing.T) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel).(*schedulerService)
	
	// Test nil plan validation
	err := scheduler.validatePlan(nil, &policy.Result{})
	if err == nil {
		t.Error("Should fail validation for nil plan")
	}
	
	// Test empty assignments
	plan := &PlacementPlan{
		ID:          "test",
		Assignments: []*NodeAssignment{},
		Cost:        &CostEstimate{},
	}
	err = scheduler.validatePlan(plan, &policy.Result{ReplicationFactor: 2})
	if err == nil {
		t.Error("Should fail validation for empty assignments")
	}
	
	// Test duplicate node assignments
	plan = &PlacementPlan{
		ID: "test",
		Request: &PlacementRequest{
			CID:  "test",
			Size: 1024,
		},
		Assignments: []*NodeAssignment{
			{NodeID: "node1", ClusterID: "cluster1", ZoneID: "zone1", Role: "replica"},
			{NodeID: "node1", ClusterID: "cluster1", ZoneID: "zone1", Role: "replica"}, // Duplicate
		},
		Cost: &CostEstimate{TotalCost: 0.1},
	}
	err = scheduler.validatePlan(plan, &policy.Result{ReplicationFactor: 2})
	if err == nil {
		t.Error("Should fail validation for duplicate node assignments")
	}
	
	// Test negative cost
	plan = &PlacementPlan{
		ID: "test",
		Request: &PlacementRequest{
			CID:  "test",
			Size: 1024,
		},
		Assignments: []*NodeAssignment{
			{NodeID: "node1", ClusterID: "cluster1", ZoneID: "zone1", Role: "replica"},
		},
		Cost: &CostEstimate{TotalCost: -1.0}, // Negative cost
	}
	err = scheduler.validatePlan(plan, &policy.Result{ReplicationFactor: 1})
	if err == nil {
		t.Error("Should fail validation for negative cost")
	}
}

// Benchmark tests

func BenchmarkScheduler_ComputePlacement(b *testing.B) {
	// Setup
	policyEngine := newMockPolicyEngine()
	topologyService := newMockTopologyService()
	costModel := NewSimpleCostModel()
	scheduler := NewScheduler(policyEngine, topologyService, costModel)
	
	ctx := context.Background()
	request := &PlacementRequest{
		CID:      "QmBenchmark",
		Size:     1024 * 1024,
		Policies: []string{"default"},
		Priority: 5,
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_, err := scheduler.ComputePlacement(ctx, request)
		if err != nil {
			b.Fatalf("ComputePlacement failed: %v", err)
		}
	}
}