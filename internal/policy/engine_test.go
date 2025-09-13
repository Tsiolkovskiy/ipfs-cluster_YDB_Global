package policy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOPAEngine_CreatePolicy(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	policy := &Policy{
		ID:   "test-policy",
		Name: "Test Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
`,
		},
		Metadata: map[string]string{
			"description": "Test policy",
		},
		CreatedBy: "test-user",
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Verify policy was created
	retrieved, err := engine.GetPolicy(ctx, "test-policy")
	require.NoError(t, err)
	assert.Equal(t, "test-policy", retrieved.ID)
	assert.Equal(t, "Test Policy", retrieved.Name)
	assert.Equal(t, 1, retrieved.Version)
	assert.Equal(t, "test-user", retrieved.CreatedBy)
	assert.False(t, retrieved.CreatedAt.IsZero())
}

func TestOPAEngine_CreatePolicy_Duplicate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	policy := &Policy{
		ID:   "test-policy",
		Name: "Test Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
`,
		},
	}

	// Create first policy
	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Try to create duplicate
	err = engine.CreatePolicy(ctx, policy)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestOPAEngine_CreatePolicy_InvalidRego(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	policy := &Policy{
		ID:   "invalid-policy",
		Name: "Invalid Policy",
		Rules: map[string]string{
			"invalid_rule": "this is not valid rego syntax",
		},
	}

	err := engine.CreatePolicy(ctx, policy)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "policy validation failed")
}

func TestOPAEngine_UpdatePolicy(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create initial policy
	policy := &Policy{
		ID:   "test-policy",
		Name: "Test Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
`,
		},
		CreatedBy: "test-user",
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Update policy
	updatedPolicy := &Policy{
		Name: "Updated Test Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 3
`,
		},
		CreatedBy: "test-user",
	}

	err = engine.UpdatePolicy(ctx, "test-policy", updatedPolicy)
	require.NoError(t, err)

	// Verify update
	retrieved, err := engine.GetPolicy(ctx, "test-policy")
	require.NoError(t, err)
	assert.Equal(t, "Updated Test Policy", retrieved.Name)
	assert.Equal(t, 2, retrieved.Version)
	assert.Contains(t, retrieved.Rules["replication_factor"], "allow = 3")
}

func TestOPAEngine_UpdatePolicy_NotFound(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	policy := &Policy{
		Name: "Non-existent Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
`,
		},
	}

	err := engine.UpdatePolicy(ctx, "non-existent", policy)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestOPAEngine_DeletePolicy(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create policy
	policy := &Policy{
		ID:   "test-policy",
		Name: "Test Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
`,
		},
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Delete policy
	err = engine.DeletePolicy(ctx, "test-policy")
	require.NoError(t, err)

	// Verify deletion
	_, err = engine.GetPolicy(ctx, "test-policy")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestOPAEngine_ListPolicies(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create multiple policies
	policies := []*Policy{
		{
			ID:        "policy-1",
			Name:      "Policy 1",
			CreatedBy: "user1",
			Rules: map[string]string{
				"replication_factor": `package replication_factor
default allow = 2`,
			},
		},
		{
			ID:        "policy-2",
			Name:      "Policy 2",
			CreatedBy: "user2",
			Rules: map[string]string{
				"replication_factor": `package replication_factor
default allow = 3`,
			},
		},
		{
			ID:        "policy-3",
			Name:      "Policy 3",
			CreatedBy: "user1",
			Rules: map[string]string{
				"replication_factor": `package replication_factor
default allow = 4`,
			},
		},
	}

	for _, policy := range policies {
		err := engine.CreatePolicy(ctx, policy)
		require.NoError(t, err)
	}

	// List all policies
	result, err := engine.ListPolicies(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, result, 3)

	// Filter by creator
	filter := &Filter{CreatedBy: "user1"}
	result, err = engine.ListPolicies(ctx, filter)
	require.NoError(t, err)
	assert.Len(t, result, 2)

	// Filter by name
	filter = &Filter{Name: "Policy 2"}
	result, err = engine.ListPolicies(ctx, filter)
	require.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, "policy-2", result[0].ID)

	// Test pagination
	filter = &Filter{Limit: 2, Offset: 1}
	result, err = engine.ListPolicies(ctx, filter)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestOPAEngine_EvaluatePolicies_ReplicationFactor(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create replication factor policy
	policy := &Policy{
		ID:   "rf-policy",
		Name: "Replication Factor Policy",
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
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Test small file
	request := &PlacementRequest{
		CID:      "QmSmallFile",
		Size:     500000, // 500KB
		Policies: []string{"rf-policy"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReplicationFactor)

	// Test large file
	request.Size = 2000000 // 2MB
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 3, result.ReplicationFactor)
}

func TestOPAEngine_EvaluatePolicies_ErasureCoding(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create erasure coding policy
	policy := &Policy{
		ID:   "ec-policy",
		Name: "Erasure Coding Policy",
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
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Test small file (no erasure coding)
	request := &PlacementRequest{
		CID:      "QmSmallFile",
		Size:     50000000, // 50MB
		Policies: []string{"ec-policy"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Nil(t, result.ErasureCoding)

	// Test large file (with erasure coding)
	request.Size = 200000000 // 200MB
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, result.ErasureCoding)
	assert.Equal(t, 4, result.ErasureCoding.DataShards)
	assert.Equal(t, 2, result.ErasureCoding.ParityShards)
}

func TestOPAEngine_EvaluatePolicies_PlacementConstraints(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create placement policy
	policy := &Policy{
		ID:   "placement-policy",
		Name: "Placement Policy",
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
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Test high priority content
	request := &PlacementRequest{
		CID:      "QmHighPriority",
		Size:     1000000,
		Priority: 8,
		Policies: []string{"placement-policy"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "zone_constraint", result.PlacementRules[0].Type)
	assert.Equal(t, "2", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "MSK,NN", result.PlacementRules[0].Constraints["preferred_zones"])

	// Test normal priority content
	request.Priority = 3
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "1", result.PlacementRules[0].Constraints["min_zones"])
}

func TestOPAEngine_EvaluatePolicies_MultiplePolicies(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create multiple policies
	rfPolicy := &Policy{
		ID:   "rf-policy",
		Name: "RF Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
`,
		},
	}

	ecPolicy := &Policy{
		ID:   "ec-policy",
		Name: "EC Policy",
		Rules: map[string]string{
			"erasure_coding": `
package erasure_coding
default allow = {
	"data_shards": 4,
	"parity_shards": 2
}
`,
		},
	}

	err := engine.CreatePolicy(ctx, rfPolicy)
	require.NoError(t, err)
	err = engine.CreatePolicy(ctx, ecPolicy)
	require.NoError(t, err)

	// Evaluate both policies
	request := &PlacementRequest{
		CID:      "QmTestFile",
		Size:     1000000,
		Policies: []string{"rf-policy", "ec-policy"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReplicationFactor)
	require.NotNil(t, result.ErasureCoding)
	assert.Equal(t, 4, result.ErasureCoding.DataShards)
	assert.Equal(t, 2, result.ErasureCoding.ParityShards)
}

func TestOPAEngine_ValidatePolicy(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Valid policy
	validPolicy := &Policy{
		ID:   "valid-policy",
		Name: "Valid Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor
default allow = 2
allow = 3 { input.request.size > 1000000 }
`,
		},
	}

	err := engine.ValidatePolicy(ctx, validPolicy)
	assert.NoError(t, err)

	// Invalid policy
	invalidPolicy := &Policy{
		ID:   "invalid-policy",
		Name: "Invalid Policy",
		Rules: map[string]string{
			"invalid_rule": "this is not valid rego",
		},
	}

	err = engine.ValidatePolicy(ctx, invalidPolicy)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid Rego rule")
}

func TestGetDefaultPolicies(t *testing.T) {
	policies := GetDefaultPolicies()
	
	assert.Len(t, policies, 3)
	
	// Check that all default policies have required fields
	for _, policy := range policies {
		assert.NotEmpty(t, policy.ID)
		assert.NotEmpty(t, policy.Name)
		assert.NotEmpty(t, policy.Rules)
		assert.NotEmpty(t, policy.Metadata)
	}
	
	// Check specific policies
	var rfPolicy, ecPolicy, placementPolicy *Policy
	for _, policy := range policies {
		switch policy.ID {
		case "default-rf-2":
			rfPolicy = policy
		case "erasure-coding-large-files":
			ecPolicy = policy
		case "zone-distribution":
			placementPolicy = policy
		}
	}
	
	assert.NotNil(t, rfPolicy)
	assert.NotNil(t, ecPolicy)
	assert.NotNil(t, placementPolicy)
	
	assert.Contains(t, rfPolicy.Rules, "replication_factor")
	assert.Contains(t, ecPolicy.Rules, "erasure_coding")
	assert.Contains(t, placementPolicy.Rules, "placement_constraints")
}

func TestOPAEngine_EvaluatePolicies_WithTopology(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create a policy that uses topology information
	policy := &Policy{
		ID:   "topology-aware-policy",
		Name: "Topology Aware Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor

default allow = 1

allow = 3 {
	count(input.topology.zones) >= 3
}

allow = 2 {
	count(input.topology.zones) >= 2
	count(input.topology.zones) < 3
}`,
		},
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(t, err)

	// Test with topology having 2 zones
	topology := &Topology{
		Zones: map[string]*Zone{
			"zone1": {ID: "zone1", Name: "Zone 1", Status: ZoneStatusActive},
			"zone2": {ID: "zone2", Name: "Zone 2", Status: ZoneStatusActive},
		},
	}

	request := &PlacementRequest{
		CID:      "QmTestFile",
		Size:     1000000,
		Policies: []string{"topology-aware-policy"},
		Topology: topology,
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReplicationFactor)

	// Test with topology having 3 zones
	topology.Zones["zone3"] = &Zone{ID: "zone3", Name: "Zone 3", Status: ZoneStatusActive}
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 3, result.ReplicationFactor)
}

func TestOPAEngine_ConcurrentAccess(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Test concurrent policy creation and reading
	done := make(chan bool, 10)

	// Create policies concurrently
	for i := 0; i < 5; i++ {
		go func(id int) {
			policy := &Policy{
				ID:   fmt.Sprintf("policy-%d", id),
				Name: fmt.Sprintf("Policy %d", id),
				Rules: map[string]string{
					"replication_factor": `package replication_factor
default allow = 2`,
				},
			}
			err := engine.CreatePolicy(ctx, policy)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Read policies concurrently
	for i := 0; i < 5; i++ {
		go func() {
			_, err := engine.ListPolicies(ctx, nil)
			assert.NoError(t, err)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all policies were created
	policies, err := engine.ListPolicies(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, policies, 5)
}

// Benchmark tests
func BenchmarkOPAEngine_EvaluatePolicies(b *testing.B) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Create a complex policy
	policy := &Policy{
		ID:   "benchmark-policy",
		Name: "Benchmark Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor

default allow = 1

allow = rf {
	input.request.size < 1048576
	rf := 2
}

allow = rf {
	input.request.size >= 1048576
	input.request.size < 104857600
	rf := 3
}

allow = rf {
	input.request.size >= 104857600
	rf := 4
}`,
		},
	}

	err := engine.CreatePolicy(ctx, policy)
	require.NoError(b, err)

	request := &PlacementRequest{
		CID:      "QmBenchmarkFile",
		Size:     5000000,
		Policies: []string{"benchmark-policy"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.EvaluatePolicies(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
	}
}