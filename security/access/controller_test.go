package access

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// MockAuditLogger implements AuditLogger for testing
type MockAuditLogger struct {
	logs []map[string]interface{}
}

func (m *MockAuditLogger) LogAccess(operation string, userID, sourceIP string, details map[string]interface{}) {
	log := map[string]interface{}{
		"type":      "access",
		"operation": operation,
		"user_id":   userID,
		"source_ip": sourceIP,
		"details":   details,
	}
	m.logs = append(m.logs, log)
}

func (m *MockAuditLogger) LogSecurityViolation(operation string, userID, sourceIP string, details map[string]interface{}) {
	log := map[string]interface{}{
		"type":      "security_violation",
		"operation": operation,
		"user_id":   userID,
		"source_ip": sourceIP,
		"details":   details,
	}
	m.logs = append(m.logs, log)
}

func TestAccessController(t *testing.T) {
	config := DefaultConfig()
	config.ZFSIntegration.Enabled = false // Disable ZFS for testing
	
	auditLogger := &MockAuditLogger{}
	
	ac, err := NewAccessController(config, auditLogger)
	if err != nil {
		t.Fatalf("Failed to create access controller: %v", err)
	}
	defer ac.Shutdown(context.Background())

	t.Run("GrantAndCheckPermission", func(t *testing.T) {
		permission := &Permission{
			ID:        "test-permission-1",
			Subject:   "test-user",
			Resource:  "test-resource",
			Action:    "read",
			GrantedAt: time.Now(),
			GrantedBy: "admin",
		}

		// Grant permission
		err := ac.GrantPermission(permission)
		if err != nil {
			t.Fatalf("Failed to grant permission: %v", err)
		}

		// Check access
		request := &AccessRequest{
			Subject:   "test-user",
			Resource:  "test-resource",
			Action:    "read",
			Timestamp: time.Now(),
		}

		result, err := ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if !result.Allowed {
			t.Errorf("Access should be allowed, but was denied: %s", result.Reason)
		}
	})

	t.Run("DenyUnauthorizedAccess", func(t *testing.T) {
		request := &AccessRequest{
			Subject:   "unauthorized-user",
			Resource:  "protected-resource",
			Action:    "write",
			Timestamp: time.Now(),
		}

		result, err := ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if result.Allowed {
			t.Errorf("Access should be denied for unauthorized user")
		}
	})

	t.Run("WildcardPermissions", func(t *testing.T) {
		permission := &Permission{
			ID:        "wildcard-permission",
			Subject:   "*",
			Resource:  "public-*",
			Action:    "*",
			GrantedAt: time.Now(),
			GrantedBy: "admin",
		}

		err := ac.GrantPermission(permission)
		if err != nil {
			t.Fatalf("Failed to grant wildcard permission: %v", err)
		}

		request := &AccessRequest{
			Subject:   "any-user",
			Resource:  "public-data",
			Action:    "read",
			Timestamp: time.Now(),
		}

		result, err := ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if !result.Allowed {
			t.Errorf("Wildcard permission should allow access")
		}
	})

	t.Run("TimeBasedConditions", func(t *testing.T) {
		futureTime := time.Now().Add(1 * time.Hour)
		
		permission := &Permission{
			ID:        "time-based-permission",
			Subject:   "time-user",
			Resource:  "time-resource",
			Action:    "read",
			Conditions: []Condition{
				{
					Type:     ConditionTypeTime,
					Field:    "timestamp",
					Operator: "after",
					Value:    futureTime.Format(time.RFC3339),
				},
			},
			GrantedAt: time.Now(),
			GrantedBy: "admin",
		}

		err := ac.GrantPermission(permission)
		if err != nil {
			t.Fatalf("Failed to grant time-based permission: %v", err)
		}

		request := &AccessRequest{
			Subject:   "time-user",
			Resource:  "time-resource",
			Action:    "read",
			Timestamp: time.Now(), // Current time, should be denied
		}

		result, err := ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if result.Allowed {
			t.Errorf("Access should be denied due to time condition")
		}
	})

	t.Run("RevokePermission", func(t *testing.T) {
		permission := &Permission{
			ID:        "revoke-test-permission",
			Subject:   "revoke-user",
			Resource:  "revoke-resource",
			Action:    "write",
			GrantedAt: time.Now(),
			GrantedBy: "admin",
		}

		// Grant permission
		err := ac.GrantPermission(permission)
		if err != nil {
			t.Fatalf("Failed to grant permission: %v", err)
		}

		// Verify access is allowed
		request := &AccessRequest{
			Subject:   "revoke-user",
			Resource:  "revoke-resource",
			Action:    "write",
			Timestamp: time.Now(),
		}

		result, err := ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if !result.Allowed {
			t.Errorf("Access should be allowed before revocation")
		}

		// Revoke permission
		err = ac.RevokePermission(permission.ID, "admin")
		if err != nil {
			t.Fatalf("Failed to revoke permission: %v", err)
		}

		// Verify access is now denied
		result, err = ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access after revocation: %v", err)
		}

		if result.Allowed {
			t.Errorf("Access should be denied after revocation")
		}
	})

	t.Run("ListPermissions", func(t *testing.T) {
		subject := "list-test-user"
		
		// Grant multiple permissions
		permissions := []*Permission{
			{
				ID:        "list-perm-1",
				Subject:   subject,
				Resource:  "resource-1",
				Action:    "read",
				GrantedAt: time.Now(),
				GrantedBy: "admin",
			},
			{
				ID:        "list-perm-2",
				Subject:   subject,
				Resource:  "resource-2",
				Action:    "write",
				GrantedAt: time.Now(),
				GrantedBy: "admin",
			},
		}

		for _, perm := range permissions {
			err := ac.GrantPermission(perm)
			if err != nil {
				t.Fatalf("Failed to grant permission %s: %v", perm.ID, err)
			}
		}

		// List permissions
		listedPerms, err := ac.ListPermissions(subject)
		if err != nil {
			t.Fatalf("Failed to list permissions: %v", err)
		}

		if len(listedPerms) < 2 {
			t.Errorf("Expected at least 2 permissions, got %d", len(listedPerms))
		}
	})
}

func TestRPCPolicyIntegration(t *testing.T) {
	config := DefaultConfig()
	auditLogger := &MockAuditLogger{}
	
	ac, err := NewAccessController(config, auditLogger)
	if err != nil {
		t.Fatalf("Failed to create access controller: %v", err)
	}
	defer ac.Shutdown(context.Background())

	rpcIntegration := NewRPCPolicyIntegration(ac, &config.RPCIntegration, auditLogger)

	// Create a test peer ID
	peerID, err := peer.Decode("12D3KooWBhSAXVqzjVrXbmjbVMQiLUqAEJhuQAMxQDWWqKFyJbJW")
	if err != nil {
		t.Fatalf("Failed to create test peer ID: %v", err)
	}

	t.Run("DefaultRPCPolicy", func(t *testing.T) {
		// Disable RPC integration to test default policy
		rpcIntegration.config.Enabled = false
		
		// Test default policy (should allow Cluster.ID)
		allowed := rpcIntegration.AuthorizeRPC(peerID, "Cluster", "ID", nil)
		if !allowed {
			t.Errorf("Default policy should allow Cluster.ID")
		}

		// Test default policy (should deny PeerAdd)
		allowed = rpcIntegration.AuthorizeRPC(peerID, "Cluster", "PeerAdd", nil)
		if allowed {
			t.Errorf("Default policy should deny Cluster.PeerAdd")
		}
		
		// Re-enable for other tests
		rpcIntegration.config.Enabled = true
	})

	t.Run("GrantRPCPermission", func(t *testing.T) {
		// Grant permission for PeerAdd
		err := rpcIntegration.GrantRPCPermission(peerID, "Cluster", "PeerAdd", "admin", nil)
		if err != nil {
			t.Fatalf("Failed to grant RPC permission: %v", err)
		}

		// Test that access is now allowed
		allowed := rpcIntegration.AuthorizeRPC(peerID, "Cluster", "PeerAdd", nil)
		if !allowed {
			t.Errorf("Access should be allowed after granting permission")
		}
	})

	t.Run("RevokeRPCPermission", func(t *testing.T) {
		// First grant permission
		err := rpcIntegration.GrantRPCPermission(peerID, "Cluster", "PeerRemove", "admin", nil)
		if err != nil {
			t.Fatalf("Failed to grant RPC permission: %v", err)
		}

		// Verify access is allowed
		allowed := rpcIntegration.AuthorizeRPC(peerID, "Cluster", "PeerRemove", nil)
		if !allowed {
			t.Errorf("Access should be allowed after granting permission")
		}

		// Revoke permission
		err = rpcIntegration.RevokeRPCPermission(peerID, "Cluster", "PeerRemove", "admin")
		if err != nil {
			t.Fatalf("Failed to revoke RPC permission: %v", err)
		}

		// Verify access is now denied
		allowed = rpcIntegration.AuthorizeRPC(peerID, "Cluster", "PeerRemove", nil)
		if allowed {
			t.Errorf("Access should be denied after revoking permission")
		}
	})

	t.Run("ListRPCPermissions", func(t *testing.T) {
		// Grant some RPC permissions
		methods := []string{"Pin", "Unpin", "Status"}
		for _, method := range methods {
			err := rpcIntegration.GrantRPCPermission(peerID, "Cluster", method, "admin", nil)
			if err != nil {
				t.Fatalf("Failed to grant RPC permission for %s: %v", method, err)
			}
		}

		// List permissions
		permissions, err := rpcIntegration.ListRPCPermissions(peerID)
		if err != nil {
			t.Fatalf("Failed to list RPC permissions: %v", err)
		}

		if len(permissions) < len(methods) {
			t.Errorf("Expected at least %d RPC permissions, got %d", len(methods), len(permissions))
		}
	})

	t.Run("GetRPCPolicyStatus", func(t *testing.T) {
		status, err := rpcIntegration.GetRPCPolicyStatus(peerID)
		if err != nil {
			t.Fatalf("Failed to get RPC policy status: %v", err)
		}

		if status.PeerID != peerID.String() {
			t.Errorf("Expected peer ID %s, got %s", peerID.String(), status.PeerID)
		}

		if len(status.Permissions) == 0 {
			t.Errorf("Expected some permissions in status")
		}
	})
}

func TestConditionEvaluation(t *testing.T) {
	config := DefaultConfig()
	auditLogger := &MockAuditLogger{}
	
	ac, err := NewAccessController(config, auditLogger)
	if err != nil {
		t.Fatalf("Failed to create access controller: %v", err)
	}
	defer ac.Shutdown(context.Background())

	t.Run("IPCondition", func(t *testing.T) {
		permission := &Permission{
			ID:      "ip-condition-test",
			Subject: "ip-user",
			Resource: "ip-resource",
			Action:  "read",
			Conditions: []Condition{
				{
					Type:     ConditionTypeIP,
					Field:    "source_ip",
					Operator: "equals",
					Value:    "192.168.1.100",
				},
			},
			GrantedAt: time.Now(),
			GrantedBy: "admin",
		}

		err := ac.GrantPermission(permission)
		if err != nil {
			t.Fatalf("Failed to grant permission: %v", err)
		}

		// Test with matching IP
		request := &AccessRequest{
			Subject:   "ip-user",
			Resource:  "ip-resource",
			Action:    "read",
			SourceIP:  "192.168.1.100",
			Timestamp: time.Now(),
		}

		result, err := ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if !result.Allowed {
			t.Errorf("Access should be allowed for matching IP")
		}

		// Test with non-matching IP
		request.SourceIP = "192.168.1.101"
		result, err = ac.CheckAccess(request)
		if err != nil {
			t.Fatalf("Failed to check access: %v", err)
		}

		if result.Allowed {
			t.Errorf("Access should be denied for non-matching IP")
		}
	})
}