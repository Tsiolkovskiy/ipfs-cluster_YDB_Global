package access

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// RPCPolicyIntegration integrates access control with IPFS Cluster RPC policy
type RPCPolicyIntegration struct {
	accessController *AccessController
	config           *RPCIntegrationConfig
	policyCache      map[string]*RPCPolicyEntry
	cacheMutex       sync.RWMutex
	auditLogger      AuditLogger
}

// RPCPolicyEntry represents a cached RPC policy entry
type RPCPolicyEntry struct {
	PeerID      string                 `json:"peer_id"`
	Service     string                 `json:"service"`
	Method      string                 `json:"method"`
	Allowed     bool                   `json:"allowed"`
	CachedAt    time.Time              `json:"cached_at"`
	ExpiresAt   time.Time              `json:"expires_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// NewRPCPolicyIntegration creates a new RPC policy integration
func NewRPCPolicyIntegration(accessController *AccessController, config *RPCIntegrationConfig, auditLogger AuditLogger) *RPCPolicyIntegration {
	return &RPCPolicyIntegration{
		accessController: accessController,
		config:           config,
		policyCache:      make(map[string]*RPCPolicyEntry),
		auditLogger:      auditLogger,
	}
}

// AuthorizeRPC authorizes an RPC call using the access control system
func (rpi *RPCPolicyIntegration) AuthorizeRPC(peerID peer.ID, service, method string, context map[string]interface{}) bool {
	if !rpi.config.Enabled {
		// Fall back to default policy if integration is disabled
		return rpi.checkDefaultPolicy(service, method)
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%s:%s.%s", peerID.String(), service, method)
	if entry := rpi.getCachedPolicy(cacheKey); entry != nil {
		rpi.auditLogger.LogAccess("rpc_cache_hit", peerID.String(), "", map[string]interface{}{
			"service": service,
			"method":  method,
			"allowed": entry.Allowed,
		})
		return entry.Allowed
	}

	// Create access request
	request := &AccessRequest{
		Subject:   peerID.String(),
		Resource:  fmt.Sprintf("rpc:%s.%s", service, method),
		Action:    "call",
		Context:   context,
		Timestamp: time.Now(),
	}

	// Check access
	result, err := rpi.accessController.CheckAccess(request)
	if err != nil {
		logger.Errorf("Error checking RPC access for %s: %s", peerID.String(), err)
		rpi.auditLogger.LogSecurityViolation("rpc_access_error", peerID.String(), "", map[string]interface{}{
			"service": service,
			"method":  method,
			"error":   err.Error(),
		})
		return false
	}

	// Cache the result
	rpi.cachePolicy(cacheKey, &RPCPolicyEntry{
		PeerID:    peerID.String(),
		Service:   service,
		Method:    method,
		Allowed:   result.Allowed,
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(5 * time.Minute), // 5 minute cache
		Metadata:  result.Metadata,
	})

	return result.Allowed
}

// GrantRPCPermission grants RPC permission to a peer
func (rpi *RPCPolicyIntegration) GrantRPCPermission(peerID peer.ID, service, method string, grantedBy string, conditions []Condition) error {
	permission := &Permission{
		ID:         fmt.Sprintf("rpc-%s-%s-%s-%d", peerID.String(), service, method, time.Now().Unix()),
		Subject:    peerID.String(),
		Resource:   fmt.Sprintf("rpc:%s.%s", service, method),
		Action:     "call",
		Conditions: conditions,
		GrantedAt:  time.Now(),
		GrantedBy:  grantedBy,
	}

	if err := rpi.accessController.GrantPermission(permission); err != nil {
		return fmt.Errorf("failed to grant RPC permission: %w", err)
	}

	// Invalidate cache for this peer/service/method
	rpi.invalidateCache(peerID.String(), service, method)

	rpi.auditLogger.LogAccess("rpc_permission_granted", grantedBy, "", map[string]interface{}{
		"peer_id":       peerID.String(),
		"service":       service,
		"method":        method,
		"permission_id": permission.ID,
	})

	return nil
}

// RevokeRPCPermission revokes RPC permission from a peer
func (rpi *RPCPolicyIntegration) RevokeRPCPermission(peerID peer.ID, service, method string, revokedBy string) error {
	// Find and revoke matching permissions
	permissions, err := rpi.accessController.ListPermissions(peerID.String())
	if err != nil {
		return fmt.Errorf("failed to list permissions: %w", err)
	}

	resource := fmt.Sprintf("rpc:%s.%s", service, method)
	var revokedCount int

	for _, permission := range permissions {
		if permission.Resource == resource && permission.Action == "call" {
			if err := rpi.accessController.RevokePermission(permission.ID, revokedBy); err != nil {
				logger.Errorf("Failed to revoke permission %s: %s", permission.ID, err)
				continue
			}
			revokedCount++
		}
	}

	if revokedCount == 0 {
		return fmt.Errorf("no matching RPC permissions found to revoke")
	}

	// Invalidate cache
	rpi.invalidateCache(peerID.String(), service, method)

	rpi.auditLogger.LogAccess("rpc_permission_revoked", revokedBy, "", map[string]interface{}{
		"peer_id":       peerID.String(),
		"service":       service,
		"method":        method,
		"revoked_count": revokedCount,
	})

	return nil
}

// ListRPCPermissions lists RPC permissions for a peer
func (rpi *RPCPolicyIntegration) ListRPCPermissions(peerID peer.ID) ([]*Permission, error) {
	permissions, err := rpi.accessController.ListPermissions(peerID.String())
	if err != nil {
		return nil, err
	}

	var rpcPermissions []*Permission
	for _, permission := range permissions {
		if strings.HasPrefix(permission.Resource, "rpc:") && permission.Action == "call" {
			rpcPermissions = append(rpcPermissions, permission)
		}
	}

	return rpcPermissions, nil
}

// SetDefaultRPCPolicy sets the default RPC policy for new peers
func (rpi *RPCPolicyIntegration) SetDefaultRPCPolicy(policy map[string]string) error {
	rpi.config.DefaultPolicy = policy

	// Clear cache to force re-evaluation
	rpi.cacheMutex.Lock()
	rpi.policyCache = make(map[string]*RPCPolicyEntry)
	rpi.cacheMutex.Unlock()

	rpi.auditLogger.LogAccess("default_rpc_policy_updated", "system", "", map[string]interface{}{
		"policy": policy,
	})

	return nil
}

// GetRPCPolicyStatus returns the current RPC policy status for a peer
func (rpi *RPCPolicyIntegration) GetRPCPolicyStatus(peerID peer.ID) (*RPCPolicyStatus, error) {
	_, err := rpi.ListRPCPermissions(peerID)
	if err != nil {
		return nil, err
	}

	status := &RPCPolicyStatus{
		PeerID:      peerID.String(),
		Permissions: make(map[string]string),
		LastUpdated: time.Now(),
	}

	// Check permissions for common RPC methods
	commonMethods := []string{
		"Cluster.ID",
		"Cluster.Peers",
		"Cluster.PeerAdd",
		"Cluster.PeerRemove",
		"Cluster.Pin",
		"Cluster.Unpin",
		"PinTracker.StatusAll",
		"PinTracker.Status",
	}

	for _, method := range commonMethods {
		parts := strings.Split(method, ".")
		if len(parts) == 2 {
			allowed := rpi.AuthorizeRPC(peerID, parts[0], parts[1], nil)
			if allowed {
				status.Permissions[method] = "allow"
			} else {
				status.Permissions[method] = "deny"
			}
		}
	}

	return status, nil
}

// RPCPolicyStatus represents the RPC policy status for a peer
type RPCPolicyStatus struct {
	PeerID      string            `json:"peer_id"`
	Permissions map[string]string `json:"permissions"`
	LastUpdated time.Time         `json:"last_updated"`
}

// getCachedPolicy retrieves a cached policy entry
func (rpi *RPCPolicyIntegration) getCachedPolicy(cacheKey string) *RPCPolicyEntry {
	rpi.cacheMutex.RLock()
	defer rpi.cacheMutex.RUnlock()

	entry, exists := rpi.policyCache[cacheKey]
	if !exists {
		return nil
	}

	// Check if entry has expired
	if time.Now().After(entry.ExpiresAt) {
		// Remove expired entry
		delete(rpi.policyCache, cacheKey)
		return nil
	}

	return entry
}

// cachePolicy caches a policy entry
func (rpi *RPCPolicyIntegration) cachePolicy(cacheKey string, entry *RPCPolicyEntry) {
	rpi.cacheMutex.Lock()
	defer rpi.cacheMutex.Unlock()

	rpi.policyCache[cacheKey] = entry
}

// invalidateCache invalidates cache entries for a specific peer/service/method
func (rpi *RPCPolicyIntegration) invalidateCache(peerID, service, method string) {
	rpi.cacheMutex.Lock()
	defer rpi.cacheMutex.Unlock()

	// Remove specific entry
	cacheKey := fmt.Sprintf("%s:%s.%s", peerID, service, method)
	delete(rpi.policyCache, cacheKey)

	// Also remove wildcard entries that might match
	for key := range rpi.policyCache {
		if strings.HasPrefix(key, peerID+":") {
			delete(rpi.policyCache, key)
		}
	}
}

// checkDefaultPolicy checks the default policy for a service/method
func (rpi *RPCPolicyIntegration) checkDefaultPolicy(service, method string) bool {
	if rpi.config.DefaultPolicy == nil {
		return false
	}

	methodKey := fmt.Sprintf("%s.%s", service, method)
	
	// Check exact match
	if policy, exists := rpi.config.DefaultPolicy[methodKey]; exists {
		return policy == "allow"
	}

	// Check service wildcard
	serviceKey := service + ".*"
	if policy, exists := rpi.config.DefaultPolicy[serviceKey]; exists {
		return policy == "allow"
	}

	// Check global wildcard
	if policy, exists := rpi.config.DefaultPolicy["*"]; exists {
		return policy == "allow"
	}

	// Default deny
	return false
}

// cleanupExpiredCache removes expired entries from the cache
func (rpi *RPCPolicyIntegration) cleanupExpiredCache() {
	rpi.cacheMutex.Lock()
	defer rpi.cacheMutex.Unlock()

	now := time.Now()
	for key, entry := range rpi.policyCache {
		if now.After(entry.ExpiresAt) {
			delete(rpi.policyCache, key)
		}
	}
}

// StartCacheCleanup starts a background goroutine to clean up expired cache entries
func (rpi *RPCPolicyIntegration) StartCacheCleanup(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rpi.cleanupExpiredCache()
		case <-ctx.Done():
			return
		}
	}
}