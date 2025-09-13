package auth

import (
	"context"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestOPAAuthorizationService_Initialize(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)
}

func TestOPAAuthorizationService_LoadPolicies(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Test loading custom policies
	customPolicies := map[string]string{
		"test_policy": `
package authz

import rego.v1

default allow := {"allow": false, "reason": "Default deny"}

allow := {"allow": true, "reason": "Test policy allows all"} if {
	input.resource == "test"
}`,
	}

	err = service.LoadPolicies(ctx, customPolicies)
	assert.NoError(t, err)
}

func TestOPAAuthorizationService_Authorize_AdminUser(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Create admin identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/admin")
	identity := &Identity{
		ID:       "admin",
		Subject:  "admin",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Test admin access to policies
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "policies",
		Action:   "create",
	}

	result, err := service.Authorize(ctx, request)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Contains(t, result.Reason, "Admin user")
}

func TestOPAAuthorizationService_Authorize_OperatorUser(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Create operator identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/operator")
	identity := &Identity{
		ID:       "operator",
		Subject:  "operator",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Test operator access to content operations (should be allowed)
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "content",
		Action:   "pin",
	}

	result, err := service.Authorize(ctx, request)
	require.NoError(t, err)
	assert.True(t, result.Allowed)

	// Test operator access to policy creation (should be denied)
	request = &AuthorizationRequest{
		Identity: identity,
		Resource: "policies",
		Action:   "create",
	}

	result, err = service.Authorize(ctx, request)
	require.NoError(t, err)
	assert.False(t, result.Allowed)
}

func TestOPAAuthorizationService_Authorize_ServiceToService(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Create service identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/scheduler")
	identity := &Identity{
		ID:       "scheduler",
		Subject:  "spiffe://gdc.local/scheduler",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Test service access to health endpoint (should be allowed)
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "health",
		Action:   "read",
	}

	result, err := service.Authorize(ctx, request)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Contains(t, result.Reason, "Service-to-service communication")
}

func TestOPAAuthorizationService_Authorize_UserOwnResources(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Create user identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/user123")
	identity := &Identity{
		ID:       "user123",
		Subject:  "user123",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Test user access to their own resources (should be allowed)
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "users/user123/profile",
		Action:   "read",
	}

	result, err := service.Authorize(ctx, request)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Contains(t, result.Reason, "User accessing own resources")

	// Test user access to other user's resources (should be denied)
	request = &AuthorizationRequest{
		Identity: identity,
		Resource: "users/other_user/profile",
		Action:   "read",
	}

	result, err = service.Authorize(ctx, request)
	require.NoError(t, err)
	assert.False(t, result.Allowed)
}

func TestOPAAuthorizationService_Authorize_TimeConstraints(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Create operator identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/operator")
	identity := &Identity{
		ID:       "operator",
		Subject:  "operator",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Create a time during business hours (10 AM)
	businessHourTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "content",
		Action:   "pin",
		Context: map[string]interface{}{
			"time": businessHourTime.Unix(),
		},
	}

	// Override the input time for testing
	result, err := service.Authorize(ctx, request)
	require.NoError(t, err)
	// Should be allowed due to role-based permission, time constraint is additional
	assert.True(t, result.Allowed)
}

func TestOPAAuthorizationService_Authorize_MaintenanceWindow(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	// Set a maintenance window
	start := time.Now().Add(-1 * time.Hour)
	end := time.Now().Add(1 * time.Hour)
	err = service.SetMaintenanceWindow(ctx, start, end, "Test maintenance")
	require.NoError(t, err)

	// Create admin identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/admin")
	identity := &Identity{
		ID:       "admin",
		Subject:  "admin",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Test sensitive operation during maintenance (should be denied by ABAC policy)
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "policies",
		Action:   "delete",
	}

	result, err := service.Authorize(ctx, request)
	require.NoError(t, err)
	// Admin should still be allowed due to RBAC taking precedence
	assert.True(t, result.Allowed)
}

func TestOPAAuthorizationService_AssignRevokeRole(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	userID := "test_user"
	role := "monitor"

	// Assign role
	err = service.AssignRole(ctx, userID, role)
	assert.NoError(t, err)

	// Verify role assignment
	rolesData, err := service.GetData(ctx, "roles")
	require.NoError(t, err)
	
	roles, ok := rolesData.(map[string]interface{})
	require.True(t, ok)
	
	userRoles, exists := roles[userID]
	require.True(t, exists)
	
	rolesList, ok := userRoles.([]string)
	require.True(t, ok)
	assert.Contains(t, rolesList, role)

	// Revoke role
	err = service.RevokeRole(ctx, userID, role)
	assert.NoError(t, err)

	// Verify role revocation
	rolesData, err = service.GetData(ctx, "roles")
	require.NoError(t, err)
	
	roles, ok = rolesData.(map[string]interface{})
	require.True(t, ok)
	
	// User should be removed from roles map if no roles left
	_, exists = roles[userID]
	assert.False(t, exists)
}

func TestOPAAuthorizationService_SetGetData(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()

	testData := map[string]interface{}{
		"key1": "value1",
		"key2": 42,
		"key3": []string{"a", "b", "c"},
	}

	// Set data
	err := service.SetData(ctx, "test/data", testData)
	assert.NoError(t, err)

	// Get data
	retrievedData, err := service.GetData(ctx, "test/data")
	assert.NoError(t, err)
	assert.Equal(t, testData, retrievedData)
}

func TestOPAAuthorizationService_UpdateDeletePolicy(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(t, err)

	policyName := "test_policy"
	policy := `
package authz

import rego.v1

default allow := {"allow": true, "reason": "Test policy"}
`

	// Update (create) policy
	err = service.UpdatePolicy(ctx, policyName, policy)
	assert.NoError(t, err)

	// Delete policy
	err = service.DeletePolicy(ctx, policyName)
	assert.NoError(t, err)
}

func TestOPAAuthorizationService_PolicyValidation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()

	// Test invalid policy (syntax error)
	invalidPolicy := `
package authz

invalid rego syntax here
`

	err := service.UpdatePolicy(ctx, "invalid_policy", invalidPolicy)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare policy")
}

// Benchmark tests
func BenchmarkOPAAuthorizationService_Authorize(b *testing.B) {
	logger := zaptest.NewLogger(b)
	service := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	err := service.Initialize(ctx)
	require.NoError(b, err)

	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/operator")
	identity := &Identity{
		ID:       "operator",
		Subject:  "operator",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "content",
		Action:   "pin",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := service.Authorize(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
	}
}