package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/open-policy-agent/opa/rego"
	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/storage/inmem"
)

// Engine defines the policy engine interface
type Engine interface {
	// Policy management
	CreatePolicy(ctx context.Context, policy *Policy) error
	UpdatePolicy(ctx context.Context, id string, policy *Policy) error
	DeletePolicy(ctx context.Context, id string) error
	GetPolicy(ctx context.Context, id string) (*Policy, error)
	ListPolicies(ctx context.Context, filter *Filter) ([]*Policy, error)
	
	// Policy evaluation
	EvaluatePolicies(ctx context.Context, request *PlacementRequest) (*Result, error)
	ValidatePolicy(ctx context.Context, policy *Policy) error
}

// OPAEngine implements the policy engine using Open Policy Agent
type OPAEngine struct {
	mu       sync.RWMutex
	policies map[string]*Policy
	store    storage.Store
}

// NewOPAEngine creates a new OPA-based policy engine
func NewOPAEngine() *OPAEngine {
	return &OPAEngine{
		policies: make(map[string]*Policy),
		store:    inmem.New(),
	}
}

// Policy represents a data placement policy
type Policy struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Version     int               `json:"version"`
	Rules       map[string]string `json:"rules"`  // Rego rules
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	CreatedBy   string            `json:"created_by"`
}

// PlacementRequest represents a request for data placement
type PlacementRequest struct {
	CID         string            `json:"cid"`
	Size        int64             `json:"size"`
	Policies    []string          `json:"policies"`
	Constraints map[string]string `json:"constraints"`
	Priority    int               `json:"priority"`
	Topology    *Topology         `json:"topology,omitempty"`
}

// Topology represents the current system topology for policy evaluation
type Topology struct {
	Zones    map[string]*Zone    `json:"zones"`
	Clusters map[string]*Cluster `json:"clusters"`
}

// Zone represents a geographic zone
type Zone struct {
	ID       string     `json:"id"`
	Name     string     `json:"name"`
	Region   string     `json:"region"`
	Status   ZoneStatus `json:"status"`
}

// Cluster represents an IPFS cluster
type Cluster struct {
	ID     string        `json:"id"`
	ZoneID string        `json:"zone_id"`
	Status ClusterStatus `json:"status"`
	Nodes  []*Node       `json:"nodes"`
}

// Node represents a node in a cluster
type Node struct {
	ID        string     `json:"id"`
	ClusterID string     `json:"cluster_id"`
	Status    NodeStatus `json:"status"`
	Resources *Resources `json:"resources"`
}

// Resources represents node resources
type Resources struct {
	StorageBytes int64   `json:"storage_bytes"`
	StorageUsage float64 `json:"storage_usage"`
}

// Status types
type ZoneStatus string
type ClusterStatus string
type NodeStatus string

const (
	ZoneStatusActive   ZoneStatus = "active"
	ZoneStatusDegraded ZoneStatus = "degraded"
	ZoneStatusOffline  ZoneStatus = "offline"

	ClusterStatusHealthy  ClusterStatus = "healthy"
	ClusterStatusDegraded ClusterStatus = "degraded"
	ClusterStatusOffline  ClusterStatus = "offline"

	NodeStatusOnline  NodeStatus = "online"
	NodeStatusOffline NodeStatus = "offline"
)

// Result represents the result of policy evaluation
type Result struct {
	ReplicationFactor int               `json:"replication_factor"`
	ErasureCoding     *ErasureCoding    `json:"erasure_coding,omitempty"`
	PlacementRules    []PlacementRule   `json:"placement_rules"`
	Constraints       map[string]string `json:"constraints"`
}

// ErasureCoding represents erasure coding parameters
type ErasureCoding struct {
	DataShards   int `json:"data_shards"`
	ParityShards int `json:"parity_shards"`
}

// PlacementRule represents a placement constraint
type PlacementRule struct {
	Type        string            `json:"type"`
	Constraints map[string]string `json:"constraints"`
}

// Filter represents policy filtering options
type Filter struct {
	Name      string `json:"name,omitempty"`
	CreatedBy string `json:"created_by,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Offset    int    `json:"offset,omitempty"`
}

// CreatePolicy creates a new policy
func (e *OPAEngine) CreatePolicy(ctx context.Context, policy *Policy) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.policies[policy.ID]; exists {
		return fmt.Errorf("policy with ID %s already exists", policy.ID)
	}

	// Validate the policy before creating
	if err := e.validatePolicyRules(ctx, policy); err != nil {
		return fmt.Errorf("policy validation failed: %w", err)
	}

	policy.CreatedAt = time.Now()
	policy.Version = 1
	e.policies[policy.ID] = policy

	return nil
}

// UpdatePolicy updates an existing policy
func (e *OPAEngine) UpdatePolicy(ctx context.Context, id string, policy *Policy) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	existing, exists := e.policies[id]
	if !exists {
		return fmt.Errorf("policy with ID %s not found", id)
	}

	// Validate the updated policy
	if err := e.validatePolicyRules(ctx, policy); err != nil {
		return fmt.Errorf("policy validation failed: %w", err)
	}

	policy.ID = id
	policy.Version = existing.Version + 1
	policy.CreatedAt = existing.CreatedAt
	e.policies[id] = policy

	return nil
}

// DeletePolicy deletes a policy
func (e *OPAEngine) DeletePolicy(ctx context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.policies[id]; !exists {
		return fmt.Errorf("policy with ID %s not found", id)
	}

	delete(e.policies, id)
	return nil
}

// GetPolicy retrieves a policy by ID
func (e *OPAEngine) GetPolicy(ctx context.Context, id string) (*Policy, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	policy, exists := e.policies[id]
	if !exists {
		return nil, fmt.Errorf("policy with ID %s not found", id)
	}

	return policy, nil
}

// ListPolicies lists policies with optional filtering
func (e *OPAEngine) ListPolicies(ctx context.Context, filter *Filter) ([]*Policy, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []*Policy
	for _, policy := range e.policies {
		if filter != nil {
			if filter.Name != "" && policy.Name != filter.Name {
				continue
			}
			if filter.CreatedBy != "" && policy.CreatedBy != filter.CreatedBy {
				continue
			}
		}
		result = append(result, policy)
	}

	// Apply pagination
	if filter != nil && filter.Limit > 0 {
		start := filter.Offset
		end := start + filter.Limit
		if start >= len(result) {
			return []*Policy{}, nil
		}
		if end > len(result) {
			end = len(result)
		}
		result = result[start:end]
	}

	return result, nil
}

// EvaluatePolicies evaluates policies for a placement request
func (e *OPAEngine) EvaluatePolicies(ctx context.Context, request *PlacementRequest) (*Result, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := &Result{
		ReplicationFactor: 1, // Default
		PlacementRules:    []PlacementRule{},
		Constraints:       make(map[string]string),
	}

	// Prepare input for OPA evaluation
	input := map[string]interface{}{
		"request": map[string]interface{}{
			"cid":         request.CID,
			"size":        request.Size,
			"policies":    request.Policies,
			"constraints": request.Constraints,
			"priority":    request.Priority,
		},
	}

	if request.Topology != nil {
		input["topology"] = request.Topology
	}

	// Evaluate each requested policy
	for _, policyID := range request.Policies {
		policy, exists := e.policies[policyID]
		if !exists {
			continue
		}

		policyResult, err := e.evaluatePolicy(ctx, policy, input)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate policy %s: %w", policyID, err)
		}

		// Merge results (taking the most restrictive values)
		if policyResult.ReplicationFactor > result.ReplicationFactor {
			result.ReplicationFactor = policyResult.ReplicationFactor
		}

		if policyResult.ErasureCoding != nil {
			result.ErasureCoding = policyResult.ErasureCoding
		}

		result.PlacementRules = append(result.PlacementRules, policyResult.PlacementRules...)

		// Merge constraints
		for k, v := range policyResult.Constraints {
			result.Constraints[k] = v
		}
	}

	return result, nil
}

// ValidatePolicy validates a policy's Rego rules
func (e *OPAEngine) ValidatePolicy(ctx context.Context, policy *Policy) error {
	return e.validatePolicyRules(ctx, policy)
}

// validatePolicyRules validates the Rego rules in a policy
func (e *OPAEngine) validatePolicyRules(ctx context.Context, policy *Policy) error {
	for ruleName, ruleContent := range policy.Rules {
		// Try to compile the Rego rule
		r := rego.New(
			rego.Query("data."+ruleName),
			rego.Module(ruleName+".rego", ruleContent),
		)

		_, err := r.PrepareForEval(ctx)
		if err != nil {
			return fmt.Errorf("invalid Rego rule %s: %w", ruleName, err)
		}
	}
	return nil
}

// evaluatePolicy evaluates a single policy against the input
func (e *OPAEngine) evaluatePolicy(ctx context.Context, policy *Policy, input map[string]interface{}) (*Result, error) {
	result := &Result{
		ReplicationFactor: 1,
		PlacementRules:    []PlacementRule{},
		Constraints:       make(map[string]string),
	}

	for ruleName, ruleContent := range policy.Rules {
		// Query for the specific rule result
		query := fmt.Sprintf("data.%s.allow", ruleName)
		
		r := rego.New(
			rego.Query(query),
			rego.Module(ruleName+".rego", ruleContent),
			rego.Input(input),
			rego.Store(e.store),
		)

		rs, err := r.Eval(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate rule %s: %w", ruleName, err)
		}

		// Process the results
		for _, evalResult := range rs {
			for _, expr := range evalResult.Expressions {
				if expr.Value != nil {
					e.processRuleResult(ruleName, expr.Value, result)
				}
			}
		}
	}

	return result, nil
}

// processRuleResult processes the result of a single rule evaluation
func (e *OPAEngine) processRuleResult(ruleName string, value interface{}, result *Result) {
	switch ruleName {
	case "replication_factor":
		// Handle json.Number type from OPA
		if num, ok := value.(json.Number); ok {
			if rf, err := num.Int64(); err == nil {
				result.ReplicationFactor = int(rf)
			}
		} else if rf, ok := value.(float64); ok {
			result.ReplicationFactor = int(rf)
		} else if rf, ok := value.(int); ok {
			result.ReplicationFactor = rf
		}
	case "erasure_coding":
		if ec, ok := value.(map[string]interface{}); ok {
			dataShards := e.extractNumber(ec["data_shards"])
			parityShards := e.extractNumber(ec["parity_shards"])
			if dataShards > 0 && parityShards > 0 {
				result.ErasureCoding = &ErasureCoding{
					DataShards:   dataShards,
					ParityShards: parityShards,
				}
			}
		}
	case "placement_constraints":
		if constraints, ok := value.(map[string]interface{}); ok {
			rule := PlacementRule{
				Type:        "zone_constraint",
				Constraints: make(map[string]string),
			}
			for k, v := range constraints {
				if str, ok := v.(string); ok {
					rule.Constraints[k] = str
				}
			}
			result.PlacementRules = append(result.PlacementRules, rule)
		}
	}
}

// extractNumber extracts an integer from various number types that OPA might return
func (e *OPAEngine) extractNumber(value interface{}) int {
	if num, ok := value.(json.Number); ok {
		if i, err := num.Int64(); err == nil {
			return int(i)
		}
	} else if f, ok := value.(float64); ok {
		return int(f)
	} else if i, ok := value.(int); ok {
		return i
	}
	return 0
}

// GetDefaultPolicies returns a set of default policies for the system
func GetDefaultPolicies() []*Policy {
	return []*Policy{
		{
			ID:   "default-rf-2",
			Name: "Default Replication Factor 2",
			Rules: map[string]string{
				"replication_factor": `
package replication_factor

default allow = 2

allow = rf {
	input.request.size < 1048576  # Files < 1MB
	rf := 2
}

allow = rf {
	input.request.size >= 1048576  # Files >= 1MB
	rf := 3
}`,
			},
			Metadata: map[string]string{
				"description": "Default replication factor based on file size",
				"type":        "replication",
			},
		},
		{
			ID:   "erasure-coding-large-files",
			Name: "Erasure Coding for Large Files",
			Rules: map[string]string{
				"erasure_coding": `
package erasure_coding

default allow = null

allow = ec {
	input.request.size > 104857600  # Files > 100MB
	ec := {
		"data_shards": 4,
		"parity_shards": 2
	}
}`,
			},
			Metadata: map[string]string{
				"description": "Apply erasure coding for files larger than 100MB",
				"type":        "erasure_coding",
			},
		},
		{
			ID:   "zone-distribution",
			Name: "Multi-Zone Distribution",
			Rules: map[string]string{
				"placement_constraints": `
package placement_constraints

default allow = {}

allow = constraints {
	input.request.priority > 5  # High priority content
	constraints := {
		"min_zones": "2",
		"preferred_zones": "MSK,NN"
	}
}

allow = constraints {
	input.request.priority <= 5  # Normal priority content
	constraints := {
		"min_zones": "1"
	}
}`,
			},
			Metadata: map[string]string{
				"description": "Distribute content across multiple zones based on priority",
				"type":        "placement",
			},
		},
	}
}