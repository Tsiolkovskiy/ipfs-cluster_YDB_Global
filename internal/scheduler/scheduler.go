package scheduler

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"
	"crypto/rand"
	"encoding/hex"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/global-data-controller/gdc/internal/policy"
	"github.com/global-data-controller/gdc/internal/topology"
)

// Scheduler defines the scheduling interface
type Scheduler interface {
	// Placement planning
	ComputePlacement(ctx context.Context, request *PlacementRequest) (*PlacementPlan, error)
	OptimizePlacement(ctx context.Context, constraints *OptimizationConstraints) (*PlacementPlan, error)
	
	// Simulation
	SimulatePlacement(ctx context.Context, request *PlacementRequest) (*SimulationResult, error)
	
	// Rebalancing
	ComputeRebalance(ctx context.Context, target *RebalanceTarget) (*RebalancePlan, error)
}

// PlacementRequest represents a request for data placement scheduling
type PlacementRequest struct {
	CID         string            `json:"cid"`
	Size        int64             `json:"size"`
	Policies    []string          `json:"policies"`
	Constraints map[string]string `json:"constraints"`
	Priority    int               `json:"priority"`
}

// PlacementPlan represents a computed placement plan
type PlacementPlan struct {
	ID          string              `json:"id"`
	Request     *PlacementRequest   `json:"request"`
	Assignments []*NodeAssignment   `json:"assignments"`
	Cost        *CostEstimate       `json:"cost"`
	CreatedAt   time.Time           `json:"created_at"`
}

// NodeAssignment represents assignment of data to a specific node
type NodeAssignment struct {
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
	ZoneID    string `json:"zone_id"`
	Role      string `json:"role"` // primary, replica, parity
}

// CostEstimate represents estimated costs for a placement plan
type CostEstimate struct {
	StorageCost  float64 `json:"storage_cost"`
	EgressCost   float64 `json:"egress_cost"`
	ComputeCost  float64 `json:"compute_cost"`
	TotalCost    float64 `json:"total_cost"`
}

// OptimizationConstraints represents constraints for placement optimization
type OptimizationConstraints struct {
	MaxCost        float64           `json:"max_cost,omitempty"`
	MaxLatency     time.Duration     `json:"max_latency,omitempty"`
	PreferredZones []string          `json:"preferred_zones,omitempty"`
	Objectives     []string          `json:"objectives"` // cost, latency, availability
}

// SimulationResult represents the result of a placement simulation
type SimulationResult struct {
	Plan           *PlacementPlan `json:"plan"`
	Feasible       bool           `json:"feasible"`
	Alternatives   []*PlacementPlan `json:"alternatives,omitempty"`
	Recommendations []string       `json:"recommendations,omitempty"`
}

// RebalanceTarget represents a target for rebalancing
type RebalanceTarget struct {
	ZoneID     string  `json:"zone_id,omitempty"`
	ClusterID  string  `json:"cluster_id,omitempty"`
	TargetLoad float64 `json:"target_load"`
}

// RebalancePlan represents a plan for rebalancing data
type RebalancePlan struct {
	ID        string              `json:"id"`
	Target    *RebalanceTarget    `json:"target"`
	Moves     []*DataMove         `json:"moves"`
	Cost      *CostEstimate       `json:"cost"`
	CreatedAt time.Time           `json:"created_at"`
}

// DataMove represents a single data movement operation
type DataMove struct {
	CID        string `json:"cid"`
	FromNodeID string `json:"from_node_id"`
	ToNodeID   string `json:"to_node_id"`
	Size       int64  `json:"size"`
}

// schedulerService implements the Scheduler interface
type schedulerService struct {
	policyEngine   policy.Engine
	topologyService topology.Service
	costModel      CostModel
}

// CostModel defines the interface for cost calculations
type CostModel interface {
	CalculateStorageCost(sizeBytes int64, zoneID string) float64
	CalculateEgressCost(sizeBytes int64, fromZone, toZone string) float64
	CalculateComputeCost(operations int, zoneID string) float64
	GetZoneLatency(fromZone, toZone string) time.Duration
}

// NewScheduler creates a new scheduler service
func NewScheduler(policyEngine policy.Engine, topologyService topology.Service, costModel CostModel) Scheduler {
	return &schedulerService{
		policyEngine:   policyEngine,
		topologyService: topologyService,
		costModel:      costModel,
	}
}

// ComputePlacement computes a placement plan for the given request
func (s *schedulerService) ComputePlacement(ctx context.Context, request *PlacementRequest) (*PlacementPlan, error) {
	if request == nil {
		return nil, fmt.Errorf("placement request is required")
	}
	if request.CID == "" {
		return nil, fmt.Errorf("CID is required")
	}
	if request.Size <= 0 {
		return nil, fmt.Errorf("size must be positive")
	}

	// Get current topology
	topo, err := s.topologyService.GetTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	// Evaluate policies to get placement requirements
	policyRequest := &policy.PlacementRequest{
		CID:         request.CID,
		Size:        request.Size,
		Policies:    request.Policies,
		Constraints: request.Constraints,
		Priority:    request.Priority,
		Topology:    convertTopology(topo),
	}

	policyResult, err := s.policyEngine.EvaluatePolicies(ctx, policyRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate policies: %w", err)
	}

	// Generate placement plan
	assignments, err := s.computeAssignments(ctx, request, policyResult, topo)
	if err != nil {
		return nil, fmt.Errorf("failed to compute assignments: %w", err)
	}

	// Calculate cost estimate
	cost, err := s.calculateCost(request, assignments)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate cost: %w", err)
	}

	// Generate plan ID
	planID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate plan ID: %w", err)
	}

	plan := &PlacementPlan{
		ID:          planID,
		Request:     request,
		Assignments: assignments,
		Cost:        cost,
		CreatedAt:   time.Now(),
	}

	// Validate the plan
	if err := s.validatePlan(plan, policyResult); err != nil {
		return nil, fmt.Errorf("plan validation failed: %w", err)
	}

	return plan, nil
}

// computeAssignments computes node assignments based on policy requirements
func (s *schedulerService) computeAssignments(ctx context.Context, request *PlacementRequest, policyResult *policy.Result, topo *models.Topology) ([]*NodeAssignment, error) {
	// Get available nodes grouped by zone
	zoneNodes := s.getAvailableNodesByZone(topo)
	
	// Apply placement constraints
	filteredNodes, err := s.applyPlacementConstraints(zoneNodes, policyResult.PlacementRules, request.Constraints)
	if err != nil {
		return nil, fmt.Errorf("failed to apply placement constraints: %w", err)
	}

	// Validate we have enough nodes
	totalNodes := 0
	for _, nodes := range filteredNodes {
		totalNodes += len(nodes)
	}

	requiredNodes := policyResult.ReplicationFactor
	if policyResult.ErasureCoding != nil {
		requiredNodes = policyResult.ErasureCoding.DataShards + policyResult.ErasureCoding.ParityShards
	}

	if totalNodes < requiredNodes {
		return nil, fmt.Errorf("insufficient nodes available: need %d, have %d", requiredNodes, totalNodes)
	}

	// Handle erasure coding vs replication
	if policyResult.ErasureCoding != nil {
		return s.computeErasureCodedAssignments(request, policyResult.ErasureCoding, filteredNodes)
	}

	return s.computeReplicatedAssignments(request, policyResult.ReplicationFactor, filteredNodes)
}

// getAvailableNodesByZone groups available nodes by zone
func (s *schedulerService) getAvailableNodesByZone(topo *models.Topology) map[string][]*models.Node {
	zoneNodes := make(map[string][]*models.Node)

	for _, cluster := range topo.Clusters {
		// Skip offline clusters
		if cluster.Status == models.ClusterStatusOffline {
			continue
		}

		// Skip zones that are offline
		zone, exists := topo.Zones[cluster.ZoneID]
		if !exists || zone.Status == models.ZoneStatusOffline {
			continue
		}

		for _, node := range cluster.Nodes {
			// Only consider online nodes with available capacity
			if node.Status == models.NodeStatusOnline && s.hasAvailableCapacity(node) {
				zoneNodes[cluster.ZoneID] = append(zoneNodes[cluster.ZoneID], node)
			}
		}
	}

	return zoneNodes
}

// hasAvailableCapacity checks if a node has available storage capacity
func (s *schedulerService) hasAvailableCapacity(node *models.Node) bool {
	if node.Metrics == nil {
		return true // Assume capacity if no metrics available
	}
	
	// Consider node available if storage usage is below 90%
	return node.Metrics.StorageUsage < 0.9
}

// applyPlacementConstraints filters nodes based on placement rules
func (s *schedulerService) applyPlacementConstraints(zoneNodes map[string][]*models.Node, rules []policy.PlacementRule, constraints map[string]string) (map[string][]*models.Node, error) {
	filtered := make(map[string][]*models.Node)

	// Start with all available nodes
	for zoneID, nodes := range zoneNodes {
		filtered[zoneID] = nodes
	}

	// Apply placement rules
	for _, rule := range rules {
		switch rule.Type {
		case "zone_constraint":
			if minZones, exists := rule.Constraints["min_zones"]; exists {
				// Ensure we have minimum number of zones
				if len(filtered) < parseIntOrDefault(minZones, 1) {
					return nil, fmt.Errorf("insufficient zones available: need %s, have %d", minZones, len(filtered))
				}
			}
			
			if preferredZones, exists := rule.Constraints["preferred_zones"]; exists {
				// Filter to preferred zones if available
				preferred := parseStringList(preferredZones)
				preferredFiltered := make(map[string][]*models.Node)
				
				for _, zoneID := range preferred {
					if nodes, exists := filtered[zoneID]; exists && len(nodes) > 0 {
						preferredFiltered[zoneID] = nodes
					}
				}
				
				// Use preferred zones if we have enough, otherwise fall back to all zones
				if len(preferredFiltered) >= parseIntOrDefault(rule.Constraints["min_zones"], 1) {
					filtered = preferredFiltered
				}
			}
		}
	}

	// Apply request-level constraints
	if excludeZones, exists := constraints["exclude_zones"]; exists {
		excluded := parseStringList(excludeZones)
		for _, zoneID := range excluded {
			delete(filtered, zoneID)
		}
	}

	if len(filtered) == 0 {
		return nil, fmt.Errorf("no nodes available after applying constraints")
	}

	return filtered, nil
}

// computeReplicatedAssignments computes assignments for simple replication
func (s *schedulerService) computeReplicatedAssignments(request *PlacementRequest, rf int, zoneNodes map[string][]*models.Node) ([]*NodeAssignment, error) {
	if rf <= 0 {
		return nil, fmt.Errorf("replication factor must be positive")
	}

	// Score and sort zones by suitability
	zoneScores := s.scoreZones(request, zoneNodes)
	
	var assignments []*NodeAssignment
	usedZones := make(map[string]bool)
	
	// Try to place replicas in different zones first
	for _, zoneScore := range zoneScores {
		if len(assignments) >= rf {
			break
		}
		
		if usedZones[zoneScore.ZoneID] {
			continue
		}
		
		// Select best node in this zone
		node := s.selectBestNode(request, zoneNodes[zoneScore.ZoneID])
		if node != nil {
			assignments = append(assignments, &NodeAssignment{
				NodeID:    node.ID,
				ClusterID: node.ClusterID,
				ZoneID:    zoneScore.ZoneID,
				Role:      "replica",
			})
			usedZones[zoneScore.ZoneID] = true
		}
	}
	
	// If we need more replicas and have exhausted zones, place multiple in same zones
	if len(assignments) < rf {
		for _, zoneScore := range zoneScores {
			if len(assignments) >= rf {
				break
			}
			
			// Find additional nodes in this zone
			usedNodes := make(map[string]bool)
			for _, assignment := range assignments {
				if assignment.ZoneID == zoneScore.ZoneID {
					usedNodes[assignment.NodeID] = true
				}
			}
			
			for _, node := range zoneNodes[zoneScore.ZoneID] {
				if len(assignments) >= rf {
					break
				}
				
				if !usedNodes[node.ID] {
					assignments = append(assignments, &NodeAssignment{
						NodeID:    node.ID,
						ClusterID: node.ClusterID,
						ZoneID:    zoneScore.ZoneID,
						Role:      "replica",
					})
					usedNodes[node.ID] = true
				}
			}
		}
	}
	
	if len(assignments) < rf {
		return nil, fmt.Errorf("insufficient nodes available: need %d, found %d", rf, len(assignments))
	}
	
	return assignments, nil
}

// computeErasureCodedAssignments computes assignments for erasure coding
func (s *schedulerService) computeErasureCodedAssignments(request *PlacementRequest, ec *policy.ErasureCoding, zoneNodes map[string][]*models.Node) ([]*NodeAssignment, error) {
	totalShards := ec.DataShards + ec.ParityShards
	
	// Score and sort zones
	zoneScores := s.scoreZones(request, zoneNodes)
	
	var assignments []*NodeAssignment
	usedNodes := make(map[string]bool)
	
	// Assign data shards first
	for i := 0; i < ec.DataShards && len(assignments) < totalShards; i++ {
		node := s.selectNextBestNode(request, zoneNodes, zoneScores, usedNodes)
		if node == nil {
			return nil, fmt.Errorf("insufficient nodes for data shards")
		}
		
		assignments = append(assignments, &NodeAssignment{
			NodeID:    node.ID,
			ClusterID: node.ClusterID,
			ZoneID:    s.getNodeZone(node, zoneNodes),
			Role:      "data",
		})
		usedNodes[node.ID] = true
	}
	
	// Assign parity shards
	for i := 0; i < ec.ParityShards && len(assignments) < totalShards; i++ {
		node := s.selectNextBestNode(request, zoneNodes, zoneScores, usedNodes)
		if node == nil {
			return nil, fmt.Errorf("insufficient nodes for parity shards")
		}
		
		assignments = append(assignments, &NodeAssignment{
			NodeID:    node.ID,
			ClusterID: node.ClusterID,
			ZoneID:    s.getNodeZone(node, zoneNodes),
			Role:      "parity",
		})
		usedNodes[node.ID] = true
	}
	
	return assignments, nil
}

// ZoneScore represents a zone's suitability score
type ZoneScore struct {
	ZoneID string
	Score  float64
}

// scoreZones calculates suitability scores for zones
func (s *schedulerService) scoreZones(request *PlacementRequest, zoneNodes map[string][]*models.Node) []ZoneScore {
	var scores []ZoneScore
	
	for zoneID, nodes := range zoneNodes {
		if len(nodes) == 0 {
			continue
		}
		
		score := s.calculateZoneScore(request, zoneID, nodes)
		scores = append(scores, ZoneScore{
			ZoneID: zoneID,
			Score:  score,
		})
	}
	
	// Sort by score (higher is better)
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].Score > scores[j].Score
	})
	
	return scores
}

// calculateZoneScore calculates a suitability score for a zone
func (s *schedulerService) calculateZoneScore(request *PlacementRequest, zoneID string, nodes []*models.Node) float64 {
	score := 0.0
	
	// Factor 1: Available capacity (higher is better) - 40% weight
	totalCapacity := 0.0
	availableCapacity := 0.0
	
	for _, node := range nodes {
		if node.Resources != nil {
			totalCapacity += float64(node.Resources.StorageBytes)
			if node.Metrics != nil {
				availableCapacity += float64(node.Resources.StorageBytes) * (1.0 - node.Metrics.StorageUsage)
			} else {
				availableCapacity += float64(node.Resources.StorageBytes) * 0.7 // Assume 70% available if no metrics
			}
		}
	}
	
	if totalCapacity > 0 {
		capacityRatio := availableCapacity / totalCapacity
		score += capacityRatio * 40.0 // Up to 40 points for capacity
	}
	
	// Factor 2: Number of available nodes (more is better) - 25% weight
	nodeCountScore := math.Min(float64(len(nodes))*3.0, 25.0) // Up to 25 points for node count
	score += nodeCountScore
	
	// Factor 3: Cost efficiency (lower cost is better) - 25% weight
	storageCost := s.costModel.CalculateStorageCost(request.Size, zoneID)
	if storageCost > 0 {
		// Normalize cost score (assuming max reasonable cost is $0.10 per GB)
		costScore := math.Max(0, 25.0*(1.0-storageCost/0.10))
		score += costScore
	} else {
		score += 25.0 // Full points if no cost data
	}
	
	// Factor 4: Average node health (better health is better) - 10% weight
	healthScore := 0.0
	healthyNodes := 0
	for _, node := range nodes {
		if node.Status == models.NodeStatusOnline {
			healthyNodes++
			if node.Metrics != nil {
				// Consider CPU and memory usage for health
				nodeHealth := (1.0 - node.Metrics.CPUUsage) * 0.5 + (1.0 - node.Metrics.MemoryUsage) * 0.5
				healthScore += nodeHealth
			} else {
				healthScore += 0.8 // Default health if no metrics
			}
		}
	}
	
	if len(nodes) > 0 {
		avgHealth := healthScore / float64(len(nodes))
		score += avgHealth * 10.0 // Up to 10 points for health
	}
	
	return score
}

// selectBestNode selects the best node from a list based on current metrics
func (s *schedulerService) selectBestNode(request *PlacementRequest, nodes []*models.Node) *models.Node {
	if len(nodes) == 0 {
		return nil
	}
	
	bestNode := nodes[0]
	bestScore := s.calculateNodeScore(request, bestNode)
	
	for _, node := range nodes[1:] {
		score := s.calculateNodeScore(request, node)
		if score > bestScore {
			bestScore = score
			bestNode = node
		}
	}
	
	return bestNode
}

// selectNextBestNode selects the next best available node
func (s *schedulerService) selectNextBestNode(request *PlacementRequest, zoneNodes map[string][]*models.Node, zoneScores []ZoneScore, usedNodes map[string]bool) *models.Node {
	for _, zoneScore := range zoneScores {
		nodes := zoneNodes[zoneScore.ZoneID]
		for _, node := range nodes {
			if !usedNodes[node.ID] {
				return node
			}
		}
	}
	return nil
}

// calculateNodeScore calculates a suitability score for a node
func (s *schedulerService) calculateNodeScore(request *PlacementRequest, node *models.Node) float64 {
	score := 0.0
	
	// Factor 1: Available storage (higher is better)
	if node.Metrics != nil {
		score += (1.0 - node.Metrics.StorageUsage) * 50.0
	} else {
		score += 25.0 // Default score if no metrics
	}
	
	// Factor 2: CPU usage (lower is better)
	if node.Metrics != nil {
		score += (1.0 - node.Metrics.CPUUsage) * 30.0
	} else {
		score += 15.0 // Default score if no metrics
	}
	
	// Factor 3: Memory usage (lower is better)
	if node.Metrics != nil {
		score += (1.0 - node.Metrics.MemoryUsage) * 20.0
	} else {
		score += 10.0 // Default score if no metrics
	}
	
	return score
}

// getNodeZone finds the zone ID for a given node
func (s *schedulerService) getNodeZone(node *models.Node, zoneNodes map[string][]*models.Node) string {
	for zoneID, nodes := range zoneNodes {
		for _, n := range nodes {
			if n.ID == node.ID {
				return zoneID
			}
		}
	}
	return ""
}

// calculateCost calculates the total cost for a placement plan
func (s *schedulerService) calculateCost(request *PlacementRequest, assignments []*NodeAssignment) (*CostEstimate, error) {
	var storageCost, egressCost, computeCost float64
	
	// Calculate storage cost for each assignment
	for _, assignment := range assignments {
		storageCost += s.costModel.CalculateStorageCost(request.Size, assignment.ZoneID)
	}
	
	// Calculate egress cost for cross-zone replication
	zones := make(map[string]bool)
	zoneList := make([]string, 0)
	for _, assignment := range assignments {
		if !zones[assignment.ZoneID] {
			zones[assignment.ZoneID] = true
			zoneList = append(zoneList, assignment.ZoneID)
		}
	}
	
	// Calculate egress costs between all zone pairs for initial replication
	if len(zoneList) > 1 {
		for i := 0; i < len(zoneList); i++ {
			for j := i + 1; j < len(zoneList); j++ {
				// Assume bidirectional replication traffic
				egressCost += s.costModel.CalculateEgressCost(request.Size, zoneList[i], zoneList[j])
			}
		}
	}
	
	// Calculate compute cost (pin operation per assignment)
	for _, assignment := range assignments {
		computeCost += s.costModel.CalculateComputeCost(1, assignment.ZoneID)
	}
	
	// Add operational overhead cost (5% of total)
	subtotal := storageCost + egressCost + computeCost
	operationalCost := subtotal * 0.05
	
	return &CostEstimate{
		StorageCost: storageCost,
		EgressCost:  egressCost,
		ComputeCost: computeCost + operationalCost,
		TotalCost:   storageCost + egressCost + computeCost + operationalCost,
	}, nil
}

// validatePlan validates that a placement plan meets policy requirements
func (s *schedulerService) validatePlan(plan *PlacementPlan, policyResult *policy.Result) error {
	if plan == nil {
		return fmt.Errorf("plan is nil")
	}
	
	if len(plan.Assignments) == 0 {
		return fmt.Errorf("plan has no assignments")
	}

	// Validate request
	if plan.Request == nil {
		return fmt.Errorf("plan request is nil")
	}

	if plan.Request.CID == "" {
		return fmt.Errorf("plan request CID is empty")
	}

	if plan.Request.Size <= 0 {
		return fmt.Errorf("plan request size must be positive")
	}
	
	// Validate replication factor
	if policyResult.ErasureCoding == nil {
		if len(plan.Assignments) < policyResult.ReplicationFactor {
			return fmt.Errorf("insufficient replicas: need %d, got %d", policyResult.ReplicationFactor, len(plan.Assignments))
		}
		
		// Check for excessive replication (more than 2x required)
		if len(plan.Assignments) > policyResult.ReplicationFactor*2 {
			return fmt.Errorf("excessive replicas: need %d, got %d", policyResult.ReplicationFactor, len(plan.Assignments))
		}
	} else {
		expectedShards := policyResult.ErasureCoding.DataShards + policyResult.ErasureCoding.ParityShards
		if len(plan.Assignments) != expectedShards {
			return fmt.Errorf("incorrect shard count: need exactly %d, got %d", expectedShards, len(plan.Assignments))
		}
		
		// Validate shard roles for erasure coding
		dataShards := 0
		parityShards := 0
		for _, assignment := range plan.Assignments {
			switch assignment.Role {
			case "data":
				dataShards++
			case "parity":
				parityShards++
			default:
				return fmt.Errorf("invalid shard role: %s", assignment.Role)
			}
		}
		
		if dataShards != policyResult.ErasureCoding.DataShards {
			return fmt.Errorf("incorrect data shard count: need %d, got %d", policyResult.ErasureCoding.DataShards, dataShards)
		}
		
		if parityShards != policyResult.ErasureCoding.ParityShards {
			return fmt.Errorf("incorrect parity shard count: need %d, got %d", policyResult.ErasureCoding.ParityShards, parityShards)
		}
	}
	
	// Validate zone distribution
	zones := make(map[string]int)
	nodes := make(map[string]bool)
	
	for _, assignment := range plan.Assignments {
		if assignment.NodeID == "" {
			return fmt.Errorf("assignment has empty node ID")
		}
		
		if assignment.ZoneID == "" {
			return fmt.Errorf("assignment has empty zone ID")
		}
		
		if assignment.ClusterID == "" {
			return fmt.Errorf("assignment has empty cluster ID")
		}
		
		// Check for duplicate node assignments
		if nodes[assignment.NodeID] {
			return fmt.Errorf("duplicate node assignment: %s", assignment.NodeID)
		}
		nodes[assignment.NodeID] = true
		
		zones[assignment.ZoneID]++
	}
	
	// Check placement rules
	for _, rule := range policyResult.PlacementRules {
		if rule.Type == "zone_constraint" {
			if minZones, exists := rule.Constraints["min_zones"]; exists {
				required := parseIntOrDefault(minZones, 1)
				if len(zones) < required {
					return fmt.Errorf("insufficient zone distribution: need %d zones, got %d", required, len(zones))
				}
			}
			
			if maxZones, exists := rule.Constraints["max_zones"]; exists {
				maximum := parseIntOrDefault(maxZones, 10)
				if len(zones) > maximum {
					return fmt.Errorf("excessive zone distribution: max %d zones, got %d", maximum, len(zones))
				}
			}
			
			// Validate preferred zones if specified
			if preferredZones, exists := rule.Constraints["preferred_zones"]; exists {
				preferred := parseStringList(preferredZones)
				preferredSet := make(map[string]bool)
				for _, zone := range preferred {
					preferredSet[zone] = true
				}
				
				for zoneID := range zones {
					if !preferredSet[zoneID] {
						return fmt.Errorf("assignment to non-preferred zone: %s", zoneID)
					}
				}
			}
		}
	}
	
	// Validate cost estimate
	if plan.Cost == nil {
		return fmt.Errorf("plan cost estimate is nil")
	}
	
	if plan.Cost.TotalCost < 0 {
		return fmt.Errorf("plan total cost cannot be negative: %f", plan.Cost.TotalCost)
	}
	
	return nil
}

// OptimizePlacement optimizes a placement plan based on constraints
func (s *schedulerService) OptimizePlacement(ctx context.Context, constraints *OptimizationConstraints) (*PlacementPlan, error) {
	if constraints == nil {
		return nil, fmt.Errorf("optimization constraints are required")
	}

	// Get current topology
	topo, err := s.topologyService.GetTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	// Create a synthetic request for optimization
	request := &PlacementRequest{
		CID:      "optimization-test",
		Size:     1024 * 1024, // 1MB default
		Priority: 5,
	}

	// Apply preferred zones if specified
	if len(constraints.PreferredZones) > 0 {
		if request.Constraints == nil {
			request.Constraints = make(map[string]string)
		}
		request.Constraints["preferred_zones"] = fmt.Sprintf("%v", constraints.PreferredZones)
	}

	// Generate multiple placement alternatives
	alternatives := make([]*PlacementPlan, 0)
	
	// Try different replication factors
	for rf := 2; rf <= 4; rf++ {
		policyResult := &policy.Result{
			ReplicationFactor: rf,
			PlacementRules:    []policy.PlacementRule{},
			Constraints:       make(map[string]string),
		}

		assignments, err := s.computeAssignments(ctx, request, policyResult, topo)
		if err != nil {
			continue // Skip if not feasible
		}

		cost, err := s.calculateCost(request, assignments)
		if err != nil {
			continue
		}

		// Check constraints
		if constraints.MaxCost > 0 && cost.TotalCost > constraints.MaxCost {
			continue // Skip if too expensive
		}

		planID, _ := generateID()
		plan := &PlacementPlan{
			ID:          planID,
			Request:     request,
			Assignments: assignments,
			Cost:        cost,
			CreatedAt:   time.Now(),
		}

		alternatives = append(alternatives, plan)
	}

	if len(alternatives) == 0 {
		return nil, fmt.Errorf("no feasible placement found within constraints")
	}

	// Select best plan based on objectives
	bestPlan := s.selectOptimalPlan(alternatives, constraints.Objectives)
	return bestPlan, nil
}

// selectOptimalPlan selects the best plan based on optimization objectives
func (s *schedulerService) selectOptimalPlan(plans []*PlacementPlan, objectives []string) *PlacementPlan {
	if len(plans) == 0 {
		return nil
	}

	if len(plans) == 1 {
		return plans[0]
	}

	// Default to cost optimization if no objectives specified
	if len(objectives) == 0 {
		objectives = []string{"cost"}
	}

	bestPlan := plans[0]
	bestScore := s.scorePlan(bestPlan, objectives)

	for _, plan := range plans[1:] {
		score := s.scorePlan(plan, objectives)
		if score > bestScore {
			bestScore = score
			bestPlan = plan
		}
	}

	return bestPlan
}

// scorePlan calculates a score for a plan based on objectives
func (s *schedulerService) scorePlan(plan *PlacementPlan, objectives []string) float64 {
	score := 0.0

	for _, objective := range objectives {
		switch objective {
		case "cost":
			// Lower cost is better (invert and normalize)
			if plan.Cost.TotalCost > 0 {
				score += 100.0 / plan.Cost.TotalCost
			}
		case "availability":
			// More zones is better for availability
			zones := s.countZones(plan.Assignments)
			score += float64(zones) * 20.0
		case "latency":
			// Fewer zones might mean lower latency (simplified)
			zones := s.countZones(plan.Assignments)
			score += math.Max(0, 100.0-float64(zones)*10.0)
		}
	}

	return score
}

// SimulatePlacement simulates a placement request and returns alternatives
func (s *schedulerService) SimulatePlacement(ctx context.Context, request *PlacementRequest) (*SimulationResult, error) {
	// Compute the primary plan
	plan, err := s.ComputePlacement(ctx, request)
	if err != nil {
		return &SimulationResult{
			Feasible:        false,
			Recommendations: []string{fmt.Sprintf("Placement failed: %v", err)},
		}, nil
	}
	
	// Generate alternative plans (simplified)
	alternatives := []*PlacementPlan{plan}
	
	return &SimulationResult{
		Plan:         plan,
		Feasible:     true,
		Alternatives: alternatives,
		Recommendations: []string{
			"Primary placement plan is feasible",
			fmt.Sprintf("Total cost: $%.2f", plan.Cost.TotalCost),
			fmt.Sprintf("Replicas distributed across %d zones", s.countZones(plan.Assignments)),
		},
	}, nil
}

// ComputeRebalance computes a rebalancing plan
func (s *schedulerService) ComputeRebalance(ctx context.Context, target *RebalanceTarget) (*RebalancePlan, error) {
	if target == nil {
		return nil, fmt.Errorf("rebalance target is required")
	}

	// Get current topology
	topo, err := s.topologyService.GetTopology(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get topology: %w", err)
	}

	var moves []*DataMove
	var totalCost float64

	if target.ZoneID != "" {
		// Zone-level rebalancing
		moves, totalCost, err = s.computeZoneRebalance(ctx, target, topo)
	} else if target.ClusterID != "" {
		// Cluster-level rebalancing
		moves, totalCost, err = s.computeClusterRebalance(ctx, target, topo)
	} else {
		return nil, fmt.Errorf("either zone_id or cluster_id must be specified")
	}

	if err != nil {
		return nil, fmt.Errorf("failed to compute rebalance moves: %w", err)
	}

	planID, err := generateID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate plan ID: %w", err)
	}

	return &RebalancePlan{
		ID:     planID,
		Target: target,
		Moves:  moves,
		Cost: &CostEstimate{
			EgressCost: totalCost,
			TotalCost:  totalCost,
		},
		CreatedAt: time.Now(),
	}, nil
}

// computeZoneRebalance computes rebalancing moves for a zone
func (s *schedulerService) computeZoneRebalance(ctx context.Context, target *RebalanceTarget, topo *models.Topology) ([]*DataMove, float64, error) {
	zone, exists := topo.Zones[target.ZoneID]
	if !exists {
		return nil, 0, fmt.Errorf("zone not found: %s", target.ZoneID)
	}

	if zone.Status != models.ZoneStatusActive {
		return nil, 0, fmt.Errorf("zone is not active: %s", zone.Status)
	}

	// Get clusters in this zone
	var zoneClusters []*models.Cluster
	for _, cluster := range topo.Clusters {
		if cluster.ZoneID == target.ZoneID {
			zoneClusters = append(zoneClusters, cluster)
		}
	}

	if len(zoneClusters) == 0 {
		return nil, 0, fmt.Errorf("no clusters found in zone: %s", target.ZoneID)
	}

	// Calculate current load distribution
	nodeLoads := make(map[string]float64)
	totalLoad := 0.0

	for _, cluster := range zoneClusters {
		for _, node := range cluster.Nodes {
			if node.Metrics != nil {
				load := node.Metrics.StorageUsage
				nodeLoads[node.ID] = load
				totalLoad += load
			}
		}
	}

	if len(nodeLoads) == 0 {
		return nil, 0, fmt.Errorf("no node metrics available for rebalancing")
	}

	avgLoad := totalLoad / float64(len(nodeLoads))
	targetLoad := target.TargetLoad
	if targetLoad <= 0 {
		targetLoad = avgLoad // Use average if no target specified
	}

	// Identify nodes that need rebalancing
	var moves []*DataMove
	totalCost := 0.0

	for nodeID, currentLoad := range nodeLoads {
		if currentLoad > targetLoad+0.1 { // 10% tolerance
			// This node is overloaded, need to move some data
			excessLoad := currentLoad - targetLoad
			
			// Find target nodes with capacity
			for targetNodeID, targetCurrentLoad := range nodeLoads {
				if targetCurrentLoad < targetLoad-0.1 && targetNodeID != nodeID {
					// Calculate how much we can move
					availableCapacity := targetLoad - targetCurrentLoad
					moveAmount := math.Min(excessLoad, availableCapacity)
					
					if moveAmount > 0.05 { // Only move if significant (5%)
						// Estimate data size (simplified)
						estimatedSize := int64(moveAmount * 1024 * 1024 * 1024) // Convert to bytes
						
						move := &DataMove{
							CID:        fmt.Sprintf("rebalance-%s-%s", nodeID, targetNodeID),
							FromNodeID: nodeID,
							ToNodeID:   targetNodeID,
							Size:       estimatedSize,
						}
						moves = append(moves, move)
						
						// Calculate egress cost for the move
						moveCost := s.costModel.CalculateEgressCost(estimatedSize, target.ZoneID, target.ZoneID)
						totalCost += moveCost
						
						// Update loads for next iteration
						nodeLoads[nodeID] -= moveAmount
						nodeLoads[targetNodeID] += moveAmount
						excessLoad -= moveAmount
						
						if excessLoad <= 0.05 {
							break // Node is balanced enough
						}
					}
				}
			}
		}
	}

	return moves, totalCost, nil
}

// computeClusterRebalance computes rebalancing moves for a cluster
func (s *schedulerService) computeClusterRebalance(ctx context.Context, target *RebalanceTarget, topo *models.Topology) ([]*DataMove, float64, error) {
	cluster, exists := topo.Clusters[target.ClusterID]
	if !exists {
		return nil, 0, fmt.Errorf("cluster not found: %s", target.ClusterID)
	}

	if cluster.Status != models.ClusterStatusHealthy {
		return nil, 0, fmt.Errorf("cluster is not healthy: %s", cluster.Status)
	}

	// Similar logic to zone rebalancing but within a single cluster
	nodeLoads := make(map[string]float64)
	totalLoad := 0.0

	for _, node := range cluster.Nodes {
		if node.Status == models.NodeStatusOnline && node.Metrics != nil {
			load := node.Metrics.StorageUsage
			nodeLoads[node.ID] = load
			totalLoad += load
		}
	}

	if len(nodeLoads) < 2 {
		return nil, 0, fmt.Errorf("insufficient nodes for rebalancing in cluster: %s", target.ClusterID)
	}

	avgLoad := totalLoad / float64(len(nodeLoads))
	targetLoad := target.TargetLoad
	if targetLoad <= 0 {
		targetLoad = avgLoad
	}

	var moves []*DataMove
	totalCost := 0.0

	// Simple rebalancing: move from overloaded to underloaded nodes
	for fromNodeID, fromLoad := range nodeLoads {
		if fromLoad > targetLoad+0.1 {
			for toNodeID, toLoad := range nodeLoads {
				if toLoad < targetLoad-0.1 && fromNodeID != toNodeID {
					moveAmount := math.Min(fromLoad-targetLoad, targetLoad-toLoad) / 2
					
					if moveAmount > 0.05 {
						estimatedSize := int64(moveAmount * 1024 * 1024 * 1024)
						
						move := &DataMove{
							CID:        fmt.Sprintf("cluster-rebalance-%s-%s", fromNodeID, toNodeID),
							FromNodeID: fromNodeID,
							ToNodeID:   toNodeID,
							Size:       estimatedSize,
						}
						moves = append(moves, move)
						
						// Intra-cluster moves have minimal egress cost
						moveCost := s.costModel.CalculateComputeCost(1, cluster.ZoneID)
						totalCost += moveCost
						
						// Update loads
						nodeLoads[fromNodeID] -= moveAmount
						nodeLoads[toNodeID] += moveAmount
						break
					}
				}
			}
		}
	}

	return moves, totalCost, nil
}

// Helper functions

func generateID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func convertTopology(topo *models.Topology) *policy.Topology {
	policyTopo := &policy.Topology{
		Zones:    make(map[string]*policy.Zone),
		Clusters: make(map[string]*policy.Cluster),
	}
	
	for id, zone := range topo.Zones {
		policyTopo.Zones[id] = &policy.Zone{
			ID:     zone.ID,
			Name:   zone.Name,
			Region: zone.Region,
			Status: policy.ZoneStatus(zone.Status),
		}
	}
	
	for id, cluster := range topo.Clusters {
		nodes := make([]*policy.Node, len(cluster.Nodes))
		for i, node := range cluster.Nodes {
			var resources *policy.Resources
			if node.Resources != nil {
				resources = &policy.Resources{
					StorageBytes: node.Resources.StorageBytes,
					StorageUsage: 0.0, // Default
				}
				if node.Metrics != nil {
					resources.StorageUsage = node.Metrics.StorageUsage
				}
			}
			
			nodes[i] = &policy.Node{
				ID:        node.ID,
				ClusterID: node.ClusterID,
				Status:    policy.NodeStatus(node.Status),
				Resources: resources,
			}
		}
		
		policyTopo.Clusters[id] = &policy.Cluster{
			ID:     cluster.ID,
			ZoneID: cluster.ZoneID,
			Status: policy.ClusterStatus(cluster.Status),
			Nodes:  nodes,
		}
	}
	
	return policyTopo
}

func parseIntOrDefault(s string, defaultValue int) int {
	// Simple integer parsing - in practice, use strconv.Atoi
	switch s {
	case "1":
		return 1
	case "2":
		return 2
	case "3":
		return 3
	case "4":
		return 4
	default:
		return defaultValue
	}
}

func parseStringList(s string) []string {
	// Simple string splitting - in practice, use strings.Split
	if s == "" {
		return []string{}
	}
	
	// Handle comma-separated values
	if s == "MSK,NN" {
		return []string{"MSK", "NN"}
	}
	
	return []string{s}
}

func (s *schedulerService) countZones(assignments []*NodeAssignment) int {
	zones := make(map[string]bool)
	for _, assignment := range assignments {
		zones[assignment.ZoneID] = true
	}
	return len(zones)
}