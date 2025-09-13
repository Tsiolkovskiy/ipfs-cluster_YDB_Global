package policy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPolicyTemplate(t *testing.T) {
	// Test existing template
	template, exists := GetPolicyTemplate("basic-replication")
	assert.True(t, exists)
	assert.NotNil(t, template)
	assert.Equal(t, "basic-replication", template.ID)
	assert.Equal(t, "Basic Replication Policy", template.Name)
	assert.Contains(t, template.Rules, "replication_factor")

	// Test non-existing template
	template, exists = GetPolicyTemplate("non-existent")
	assert.False(t, exists)
	assert.Nil(t, template)
}

func TestListPolicyTemplates(t *testing.T) {
	templates := ListPolicyTemplates()
	assert.NotEmpty(t, templates)
	
	expectedTemplates := []string{
		"basic-replication",
		"priority-based-placement",
		"erasure-coding-efficiency",
		"cost-optimization",
		"compliance-policy",
		"disaster-recovery",
	}
	
	for _, expected := range expectedTemplates {
		assert.Contains(t, templates, expected)
	}
}

func TestGetPolicyTemplatesByCategory(t *testing.T) {
	// Test replication category
	replicationPolicies := GetPolicyTemplatesByCategory(CategoryReplication)
	assert.NotEmpty(t, replicationPolicies)
	
	for _, policy := range replicationPolicies {
		assert.Equal(t, "replication", policy.Metadata["category"])
	}

	// Test placement category
	placementPolicies := GetPolicyTemplatesByCategory(CategoryPlacement)
	assert.NotEmpty(t, placementPolicies)
	
	for _, policy := range placementPolicies {
		assert.Equal(t, "placement", policy.Metadata["category"])
	}

	// Test non-existent category
	nonExistentPolicies := GetPolicyTemplatesByCategory("non-existent")
	assert.Empty(t, nonExistentPolicies)
}

func TestBasicReplicationPolicyTemplate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	template, exists := GetPolicyTemplate("basic-replication")
	require.True(t, exists)

	err := engine.CreatePolicy(ctx, template)
	require.NoError(t, err)

	// Test small file (< 1MB)
	request := &PlacementRequest{
		CID:      "QmSmallFile",
		Size:     500000, // 500KB
		Policies: []string{"basic-replication"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReplicationFactor)

	// Test medium file (1MB - 100MB)
	request.Size = 50000000 // 50MB
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 3, result.ReplicationFactor)

	// Test large file (>= 100MB)
	request.Size = 200000000 // 200MB
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 4, result.ReplicationFactor)
}

func TestPriorityBasedPlacementPolicyTemplate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	template, exists := GetPolicyTemplate("priority-based-placement")
	require.True(t, exists)

	err := engine.CreatePolicy(ctx, template)
	require.NoError(t, err)

	// Test critical priority (9-10)
	request := &PlacementRequest{
		CID:      "QmCriticalFile",
		Size:     1000000,
		Priority: 9,
		Policies: []string{"priority-based-placement"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "3", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "MSK,NN,SPB", result.PlacementRules[0].Constraints["required_zones"])

	// Test high priority (7-8)
	request.Priority = 7
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "2", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "MSK,NN", result.PlacementRules[0].Constraints["preferred_zones"])

	// Test medium priority (4-6)
	request.Priority = 5
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "1", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "2", result.PlacementRules[0].Constraints["max_zones"])
	assert.Equal(t, "true", result.PlacementRules[0].Constraints["cost_optimize"])

	// Test low priority (< 4) - should use default
	request.Priority = 2
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "1", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "1", result.PlacementRules[0].Constraints["max_zones"])
}

func TestErasureCodingEfficiencyPolicyTemplate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	template, exists := GetPolicyTemplate("erasure-coding-efficiency")
	require.True(t, exists)

	err := engine.CreatePolicy(ctx, template)
	require.NoError(t, err)

	// Test small file (no EC)
	request := &PlacementRequest{
		CID:      "QmSmallFile",
		Size:     50000000, // 50MB
		Policies: []string{"erasure-coding-efficiency"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Nil(t, result.ErasureCoding)

	// Test large file (100MB - 1GB)
	request.Size = 500000000 // 500MB
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, result.ErasureCoding)
	assert.Equal(t, 4, result.ErasureCoding.DataShards)
	assert.Equal(t, 2, result.ErasureCoding.ParityShards)

	// Test very large file (>= 1GB)
	request.Size = 2000000000 // 2GB
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.NotNil(t, result.ErasureCoding)
	assert.Equal(t, 6, result.ErasureCoding.DataShards)
	assert.Equal(t, 3, result.ErasureCoding.ParityShards)
}

func TestCostOptimizationPolicyTemplate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	template, exists := GetPolicyTemplate("cost-optimization")
	require.True(t, exists)

	err := engine.CreatePolicy(ctx, template)
	require.NoError(t, err)

	// Test archive content
	request := &PlacementRequest{
		CID:      "QmArchiveFile",
		Size:     1000000,
		Policies: []string{"cost-optimization"},
		Constraints: map[string]string{
			"archive": "true",
		},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 1, result.ReplicationFactor) // Minimal replication for archive
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "cold", result.PlacementRules[0].Constraints["storage_class"])
	assert.Equal(t, "aggressive", result.PlacementRules[0].Constraints["cost_optimize"])

	// Test hot content
	request.Constraints = map[string]string{
		"access_pattern": "hot",
	}

	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 3, result.ReplicationFactor) // Higher replication for hot content
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "hot", result.PlacementRules[0].Constraints["storage_class"])
	assert.Equal(t, "true", result.PlacementRules[0].Constraints["latency_optimize"])
}

func TestCompliancePolicyTemplate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	template, exists := GetPolicyTemplate("compliance-policy")
	require.True(t, exists)

	err := engine.CreatePolicy(ctx, template)
	require.NoError(t, err)

	// Test GDPR compliance
	request := &PlacementRequest{
		CID:      "QmGDPRFile",
		Size:     1000000,
		Policies: []string{"compliance-policy"},
		Constraints: map[string]string{
			"compliance": "gdpr",
		},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "EU", result.PlacementRules[0].Constraints["required_regions"])
	assert.Equal(t, "US,APAC", result.PlacementRules[0].Constraints["forbidden_regions"])
	assert.Equal(t, "2", result.PlacementRules[0].Constraints["min_zones"])

	// Test financial data
	request.Constraints = map[string]string{
		"data_classification": "financial",
	}

	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "MSK,SPB", result.PlacementRules[0].Constraints["required_zones"])
	assert.Equal(t, "required", result.PlacementRules[0].Constraints["encryption"])
	assert.Equal(t, "enabled", result.PlacementRules[0].Constraints["audit_logging"])

	// Test personal data
	request.Constraints = map[string]string{
		"data_classification": "personal",
	}

	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "required", result.PlacementRules[0].Constraints["encryption"])
	assert.Equal(t, "enabled", result.PlacementRules[0].Constraints["access_logging"])
	assert.Equal(t, "strict", result.PlacementRules[0].Constraints["retention_policy"])
}

func TestDisasterRecoveryPolicyTemplate(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	template, exists := GetPolicyTemplate("disaster-recovery")
	require.True(t, exists)

	err := engine.CreatePolicy(ctx, template)
	require.NoError(t, err)

	// Test critical data (priority >= 8)
	request := &PlacementRequest{
		CID:      "QmCriticalData",
		Size:     1000000,
		Priority: 9,
		Policies: []string{"disaster-recovery"},
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 4, result.ReplicationFactor)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "3", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "2", result.PlacementRules[0].Constraints["min_regions"])
	assert.Equal(t, "sync", result.PlacementRules[0].Constraints["cross_region_replication"])

	// Test important data (priority 5-7)
	request.Priority = 6
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 3, result.ReplicationFactor)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "2", result.PlacementRules[0].Constraints["min_zones"])
	assert.Equal(t, "async", result.PlacementRules[0].Constraints["cross_zone_replication"])
	assert.Equal(t, "daily", result.PlacementRules[0].Constraints["backup_schedule"])

	// Test normal data (priority < 5)
	request.Priority = 3
	result, err = engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, 2, result.ReplicationFactor)
	require.Len(t, result.PlacementRules, 1)
	assert.Equal(t, "1", result.PlacementRules[0].Constraints["min_zones"])
}

func TestPolicyTemplateIntegration(t *testing.T) {
	engine := NewOPAEngine()
	ctx := context.Background()

	// Load multiple policy templates
	templates := []string{
		"basic-replication",
		"priority-based-placement",
		"erasure-coding-efficiency",
	}

	for _, templateName := range templates {
		template, exists := GetPolicyTemplate(templateName)
		require.True(t, exists)
		err := engine.CreatePolicy(ctx, template)
		require.NoError(t, err)
	}

	// Test evaluation with multiple policies
	request := &PlacementRequest{
		CID:      "QmIntegrationTest",
		Size:     200000000, // 200MB
		Priority: 8,
		Policies: templates,
	}

	result, err := engine.EvaluatePolicies(ctx, request)
	require.NoError(t, err)

	// Should get RF=4 from basic-replication (large file)
	assert.Equal(t, 4, result.ReplicationFactor)

	// Should get EC from erasure-coding-efficiency (200MB file gets 4+2 EC)
	require.NotNil(t, result.ErasureCoding)
	assert.Equal(t, 4, result.ErasureCoding.DataShards)
	assert.Equal(t, 2, result.ErasureCoding.ParityShards)

	// Should get placement constraints from priority-based-placement
	require.NotEmpty(t, result.PlacementRules)
	found := false
	for _, rule := range result.PlacementRules {
		if rule.Constraints["min_zones"] == "2" {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected placement rule with min_zones=2")
}