// Package access provides access control integration between ZFS delegation and IPFS Cluster RPC policy
package access

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("access")

// AccessController manages access control for ZFS datasets and IPFS Cluster operations
type AccessController struct {
	config          *Config
	permissions     map[string]*Permission
	delegations     map[string]*ZFSDelegation
	auditLogger     AuditLogger
	
	// Synchronization
	permissionsMutex sync.RWMutex
	delegationsMutex sync.RWMutex
	
	// Context management
	ctx    context.Context
	cancel context.CancelFunc
}

// Permission represents an access permission with conditions
type Permission struct {
	ID          string                 `json:"id"`
	Subject     string                 `json:"subject"`     // user, peer ID, or group
	Resource    string                 `json:"resource"`    // dataset, RPC method, etc.
	Action      string                 `json:"action"`      // read, write, admin, etc.
	Conditions  []Condition            `json:"conditions"`
	GrantedAt   time.Time              `json:"granted_at"`
	ExpiresAt   *time.Time             `json:"expires_at,omitempty"`
	GrantedBy   string                 `json:"granted_by"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// Condition represents a condition that must be met for permission to be granted
type Condition struct {
	Type      ConditionType          `json:"type"`
	Field     string                 `json:"field"`
	Operator  string                 `json:"operator"`
	Value     interface{}            `json:"value"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// ConditionType represents the type of condition
type ConditionType string

const (
	ConditionTypeTime      ConditionType = "time"
	ConditionTypeIP        ConditionType = "ip"
	ConditionTypeDataset   ConditionType = "dataset"
	ConditionTypeRPCMethod ConditionType = "rpc_method"
	ConditionTypeCustom    ConditionType = "custom"
)

// ZFSDelegation represents a ZFS delegation permission
type ZFSDelegation struct {
	ID          string    `json:"id"`
	Dataset     string    `json:"dataset"`
	User        string    `json:"user"`
	Permissions []string  `json:"permissions"` // create, destroy, mount, etc.
	Recursive   bool      `json:"recursive"`
	CreatedAt   time.Time `json:"created_at"`
	CreatedBy   string    `json:"created_by"`
}

// AccessRequest represents an access request to be evaluated
type AccessRequest struct {
	Subject    string                 `json:"subject"`
	Resource   string                 `json:"resource"`
	Action     string                 `json:"action"`
	Context    map[string]interface{} `json:"context"`
	Timestamp  time.Time              `json:"timestamp"`
	SourceIP   string                 `json:"source_ip,omitempty"`
	UserAgent  string                 `json:"user_agent,omitempty"`
}

// AccessResult represents the result of an access control evaluation
type AccessResult struct {
	Allowed     bool                   `json:"allowed"`
	Reason      string                 `json:"reason"`
	Permission  *Permission            `json:"permission,omitempty"`
	Conditions  []string               `json:"conditions,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	EvaluatedAt time.Time              `json:"evaluated_at"`
}

// AuditLogger interface for access control audit logging
type AuditLogger interface {
	LogAccess(operation string, userID, sourceIP string, details map[string]interface{})
	LogSecurityViolation(operation string, userID, sourceIP string, details map[string]interface{})
}

// NewAccessController creates a new access controller
func NewAccessController(config *Config, auditLogger AuditLogger) (*AccessController, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	ac := &AccessController{
		config:      config,
		permissions: make(map[string]*Permission),
		delegations: make(map[string]*ZFSDelegation),
		auditLogger: auditLogger,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Load existing permissions and delegations
	if err := ac.loadExistingPermissions(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load existing permissions: %w", err)
	}

	if err := ac.loadExistingDelegations(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load existing delegations: %w", err)
	}

	logger.Info("Access controller initialized successfully")
	return ac, nil
}

// CheckAccess evaluates an access request and returns the result
func (ac *AccessController) CheckAccess(request *AccessRequest) (*AccessResult, error) {
	start := time.Now()
	defer func() {
		logger.Debugf("Access check for %s on %s took %v", 
			request.Subject, request.Resource, time.Since(start))
	}()

	result := &AccessResult{
		Allowed:     false,
		EvaluatedAt: time.Now(),
		Metadata:    make(map[string]interface{}),
	}

	// Log access attempt
	ac.auditLogger.LogAccess("access_check", request.Subject, request.SourceIP, map[string]interface{}{
		"resource": request.Resource,
		"action":   request.Action,
		"context":  request.Context,
	})

	// Find applicable permissions
	permissions := ac.findApplicablePermissions(request)
	if len(permissions) == 0 {
		result.Reason = "no applicable permissions found"
		ac.auditLogger.LogSecurityViolation("access_denied", request.Subject, request.SourceIP, map[string]interface{}{
			"reason":   result.Reason,
			"resource": request.Resource,
			"action":   request.Action,
		})
		return result, nil
	}

	// Evaluate each permission
	for _, permission := range permissions {
		if ac.evaluatePermission(permission, request) {
			result.Allowed = true
			result.Permission = permission
			result.Reason = "permission granted"
			
			// Check if this is a ZFS resource and verify delegation (only if ZFS integration is enabled)
			if strings.HasPrefix(request.Resource, "zfs:") && ac.config.ZFSIntegration.Enabled {
				if err := ac.verifyZFSDelegation(request, permission); err != nil {
					result.Allowed = false
					result.Reason = fmt.Sprintf("ZFS delegation check failed: %s", err.Error())
					ac.auditLogger.LogSecurityViolation("zfs_delegation_failed", request.Subject, request.SourceIP, map[string]interface{}{
						"error":    err.Error(),
						"resource": request.Resource,
					})
					continue
				}
			}
			
			break
		}
	}

	if !result.Allowed && result.Reason == "" {
		result.Reason = "permission conditions not met"
	}

	// Log result
	if result.Allowed {
		ac.auditLogger.LogAccess("access_granted", request.Subject, request.SourceIP, map[string]interface{}{
			"resource":    request.Resource,
			"action":      request.Action,
			"permission":  result.Permission.ID,
		})
	} else {
		ac.auditLogger.LogSecurityViolation("access_denied", request.Subject, request.SourceIP, map[string]interface{}{
			"reason":   result.Reason,
			"resource": request.Resource,
			"action":   request.Action,
		})
	}

	return result, nil
}

// CheckRPCAccess checks access for IPFS Cluster RPC calls
func (ac *AccessController) CheckRPCAccess(peerID peer.ID, service, method string, context map[string]interface{}) bool {
	request := &AccessRequest{
		Subject:   peerID.String(),
		Resource:  fmt.Sprintf("rpc:%s.%s", service, method),
		Action:    "call",
		Context:   context,
		Timestamp: time.Now(),
	}

	result, err := ac.CheckAccess(request)
	if err != nil {
		logger.Errorf("Error checking RPC access: %s", err)
		return false
	}

	return result.Allowed
}

// GrantPermission grants a new permission
func (ac *AccessController) GrantPermission(permission *Permission) error {
	ac.permissionsMutex.Lock()
	defer ac.permissionsMutex.Unlock()

	// Validate permission
	if err := ac.validatePermission(permission); err != nil {
		return fmt.Errorf("invalid permission: %w", err)
	}

	// Store permission
	ac.permissions[permission.ID] = permission

	// If this is a ZFS resource, create corresponding delegation
	if strings.HasPrefix(permission.Resource, "zfs:") {
		if err := ac.createZFSDelegation(permission); err != nil {
			logger.Warnf("Failed to create ZFS delegation for permission %s: %s", permission.ID, err)
		}
	}

	// Persist permission
	if err := ac.persistPermission(permission); err != nil {
		return fmt.Errorf("failed to persist permission: %w", err)
	}

	ac.auditLogger.LogAccess("permission_granted", permission.GrantedBy, "", map[string]interface{}{
		"permission_id": permission.ID,
		"subject":       permission.Subject,
		"resource":      permission.Resource,
		"action":        permission.Action,
	})

	logger.Infof("Granted permission %s to %s for %s:%s", 
		permission.ID, permission.Subject, permission.Resource, permission.Action)
	return nil
}

// RevokePermission revokes an existing permission
func (ac *AccessController) RevokePermission(permissionID, revokedBy string) error {
	ac.permissionsMutex.Lock()
	defer ac.permissionsMutex.Unlock()

	permission, exists := ac.permissions[permissionID]
	if !exists {
		return fmt.Errorf("permission not found: %s", permissionID)
	}

	// Remove ZFS delegation if applicable
	if strings.HasPrefix(permission.Resource, "zfs:") {
		if err := ac.removeZFSDelegation(permission); err != nil {
			logger.Warnf("Failed to remove ZFS delegation for permission %s: %s", permissionID, err)
		}
	}

	// Remove permission
	delete(ac.permissions, permissionID)

	// Persist removal
	if err := ac.removePersistedPermission(permissionID); err != nil {
		return fmt.Errorf("failed to remove persisted permission: %w", err)
	}

	ac.auditLogger.LogAccess("permission_revoked", revokedBy, "", map[string]interface{}{
		"permission_id": permissionID,
		"subject":       permission.Subject,
		"resource":      permission.Resource,
	})

	logger.Infof("Revoked permission %s from %s", permissionID, permission.Subject)
	return nil
}

// ListPermissions returns all permissions for a subject
func (ac *AccessController) ListPermissions(subject string) ([]*Permission, error) {
	ac.permissionsMutex.RLock()
	defer ac.permissionsMutex.RUnlock()

	var permissions []*Permission
	for _, permission := range ac.permissions {
		if permission.Subject == subject {
			permissions = append(permissions, permission)
		}
	}

	return permissions, nil
}

// CreateZFSDelegation creates a ZFS delegation
func (ac *AccessController) CreateZFSDelegation(delegation *ZFSDelegation) error {
	ac.delegationsMutex.Lock()
	defer ac.delegationsMutex.Unlock()

	// Execute ZFS allow command
	cmd := ac.buildZFSAllowCommand(delegation)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create ZFS delegation: %w, output: %s", err, output)
	}

	// Store delegation
	ac.delegations[delegation.ID] = delegation

	ac.auditLogger.LogAccess("zfs_delegation_created", delegation.CreatedBy, "", map[string]interface{}{
		"delegation_id": delegation.ID,
		"dataset":       delegation.Dataset,
		"user":          delegation.User,
		"permissions":   delegation.Permissions,
	})

	logger.Infof("Created ZFS delegation %s for user %s on dataset %s", 
		delegation.ID, delegation.User, delegation.Dataset)
	return nil
}

// RemoveZFSDelegation removes a ZFS delegation
func (ac *AccessController) RemoveZFSDelegation(delegationID string) error {
	ac.delegationsMutex.Lock()
	defer ac.delegationsMutex.Unlock()

	delegation, exists := ac.delegations[delegationID]
	if !exists {
		return fmt.Errorf("delegation not found: %s", delegationID)
	}

	// Execute ZFS unallow command
	cmd := ac.buildZFSUnallowCommand(delegation)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to remove ZFS delegation: %w, output: %s", err, output)
	}

	// Remove delegation
	delete(ac.delegations, delegationID)

	ac.auditLogger.LogAccess("zfs_delegation_removed", "", "", map[string]interface{}{
		"delegation_id": delegationID,
		"dataset":       delegation.Dataset,
		"user":          delegation.User,
	})

	logger.Infof("Removed ZFS delegation %s for user %s", delegationID, delegation.User)
	return nil
}

// Shutdown gracefully shuts down the access controller
func (ac *AccessController) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down access controller")
	ac.cancel()
	return nil
}

// findApplicablePermissions finds permissions that apply to the request
func (ac *AccessController) findApplicablePermissions(request *AccessRequest) []*Permission {
	ac.permissionsMutex.RLock()
	defer ac.permissionsMutex.RUnlock()

	var applicable []*Permission
	for _, permission := range ac.permissions {
		if ac.permissionMatches(permission, request) {
			applicable = append(applicable, permission)
		}
	}

	return applicable
}

// permissionMatches checks if a permission matches a request
func (ac *AccessController) permissionMatches(permission *Permission, request *AccessRequest) bool {
	// Check subject match
	if permission.Subject != "*" && permission.Subject != request.Subject {
		return false
	}

	// Check resource match (support wildcards)
	if !ac.resourceMatches(permission.Resource, request.Resource) {
		return false
	}

	// Check action match
	if permission.Action != "*" && permission.Action != request.Action {
		return false
	}

	// Check expiration
	if permission.ExpiresAt != nil && time.Now().After(*permission.ExpiresAt) {
		return false
	}

	return true
}

// resourceMatches checks if a resource pattern matches a resource
func (ac *AccessController) resourceMatches(pattern, resource string) bool {
	if pattern == "*" {
		return true
	}

	if pattern == resource {
		return true
	}

	// Support wildcard matching
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(resource, prefix)
	}

	return false
}

// evaluatePermission evaluates if a permission allows the request
func (ac *AccessController) evaluatePermission(permission *Permission, request *AccessRequest) bool {
	// Evaluate all conditions
	for _, condition := range permission.Conditions {
		if !ac.evaluateCondition(condition, request) {
			return false
		}
	}

	return true
}

// evaluateCondition evaluates a single condition
func (ac *AccessController) evaluateCondition(condition Condition, request *AccessRequest) bool {
	switch condition.Type {
	case ConditionTypeTime:
		return ac.evaluateTimeCondition(condition, request)
	case ConditionTypeIP:
		return ac.evaluateIPCondition(condition, request)
	case ConditionTypeDataset:
		return ac.evaluateDatasetCondition(condition, request)
	case ConditionTypeRPCMethod:
		return ac.evaluateRPCMethodCondition(condition, request)
	default:
		logger.Warnf("Unknown condition type: %s", condition.Type)
		return false
	}
}

// evaluateTimeCondition evaluates time-based conditions
func (ac *AccessController) evaluateTimeCondition(condition Condition, request *AccessRequest) bool {
	now := time.Now()
	
	switch condition.Operator {
	case "after":
		if timeStr, ok := condition.Value.(string); ok {
			if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
				return now.After(t)
			}
		}
	case "before":
		if timeStr, ok := condition.Value.(string); ok {
			if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
				return now.Before(t)
			}
		}
	case "between":
		// Implementation for time range conditions
	}
	
	return false
}

// evaluateIPCondition evaluates IP-based conditions
func (ac *AccessController) evaluateIPCondition(condition Condition, request *AccessRequest) bool {
	if request.SourceIP == "" {
		return false
	}

	switch condition.Operator {
	case "equals":
		return request.SourceIP == condition.Value
	case "in_range":
		// Implementation for IP range checking
	}

	return false
}

// evaluateDatasetCondition evaluates dataset-specific conditions
func (ac *AccessController) evaluateDatasetCondition(condition Condition, request *AccessRequest) bool {
	// Extract dataset from resource
	if !strings.HasPrefix(request.Resource, "zfs:") {
		return false
	}

	dataset := strings.TrimPrefix(request.Resource, "zfs:")
	
	switch condition.Operator {
	case "equals":
		return dataset == condition.Value
	case "starts_with":
		if prefix, ok := condition.Value.(string); ok {
			return strings.HasPrefix(dataset, prefix)
		}
	}

	return false
}

// evaluateRPCMethodCondition evaluates RPC method conditions
func (ac *AccessController) evaluateRPCMethodCondition(condition Condition, request *AccessRequest) bool {
	if !strings.HasPrefix(request.Resource, "rpc:") {
		return false
	}

	method := strings.TrimPrefix(request.Resource, "rpc:")
	
	switch condition.Operator {
	case "equals":
		return method == condition.Value
	case "matches":
		// Implementation for pattern matching
	}

	return false
}

// verifyZFSDelegation verifies ZFS delegation for a request
func (ac *AccessController) verifyZFSDelegation(request *AccessRequest, permission *Permission) error {
	dataset := strings.TrimPrefix(request.Resource, "zfs:")
	
	// Check if user has ZFS delegation for this dataset
	cmd := exec.Command("zfs", "allow", dataset)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to check ZFS delegations: %w", err)
	}

	// Parse output to verify delegation exists
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, permission.Subject) {
			return nil
		}
	}

	return fmt.Errorf("no ZFS delegation found for user %s on dataset %s", permission.Subject, dataset)
}

// createZFSDelegation creates a ZFS delegation for a permission
func (ac *AccessController) createZFSDelegation(permission *Permission) error {
	dataset := strings.TrimPrefix(permission.Resource, "zfs:")
	
	delegation := &ZFSDelegation{
		ID:          fmt.Sprintf("perm-%s", permission.ID),
		Dataset:     dataset,
		User:        permission.Subject,
		Permissions: ac.mapActionToZFSPermissions(permission.Action),
		Recursive:   true,
		CreatedAt:   time.Now(),
		CreatedBy:   permission.GrantedBy,
	}

	return ac.CreateZFSDelegation(delegation)
}

// removeZFSDelegation removes a ZFS delegation for a permission
func (ac *AccessController) removeZFSDelegation(permission *Permission) error {
	delegationID := fmt.Sprintf("perm-%s", permission.ID)
	return ac.RemoveZFSDelegation(delegationID)
}

// mapActionToZFSPermissions maps permission actions to ZFS permissions
func (ac *AccessController) mapActionToZFSPermissions(action string) []string {
	switch action {
	case "read":
		return []string{"send", "hold"}
	case "write":
		return []string{"create", "destroy", "mount", "receive"}
	case "admin":
		return []string{"create", "destroy", "mount", "receive", "send", "hold", "allow"}
	default:
		return []string{}
	}
}

// buildZFSAllowCommand builds a ZFS allow command
func (ac *AccessController) buildZFSAllowCommand(delegation *ZFSDelegation) *exec.Cmd {
	args := []string{"allow"}
	
	if delegation.Recursive {
		args = append(args, "-d")
	}
	
	args = append(args, delegation.User)
	args = append(args, strings.Join(delegation.Permissions, ","))
	args = append(args, delegation.Dataset)
	
	return exec.Command("zfs", args...)
}

// buildZFSUnallowCommand builds a ZFS unallow command
func (ac *AccessController) buildZFSUnallowCommand(delegation *ZFSDelegation) *exec.Cmd {
	args := []string{"unallow"}
	
	if delegation.Recursive {
		args = append(args, "-d")
	}
	
	args = append(args, delegation.User)
	args = append(args, strings.Join(delegation.Permissions, ","))
	args = append(args, delegation.Dataset)
	
	return exec.Command("zfs", args...)
}

// validatePermission validates a permission before granting
func (ac *AccessController) validatePermission(permission *Permission) error {
	if permission.ID == "" {
		return fmt.Errorf("permission ID cannot be empty")
	}
	
	if permission.Subject == "" {
		return fmt.Errorf("permission subject cannot be empty")
	}
	
	if permission.Resource == "" {
		return fmt.Errorf("permission resource cannot be empty")
	}
	
	if permission.Action == "" {
		return fmt.Errorf("permission action cannot be empty")
	}
	
	return nil
}

// loadExistingPermissions loads existing permissions from storage
func (ac *AccessController) loadExistingPermissions() error {
	// Implementation would load from persistent storage
	logger.Info("Loading existing permissions")
	return nil
}

// loadExistingDelegations loads existing ZFS delegations
func (ac *AccessController) loadExistingDelegations() error {
	// Implementation would query ZFS for existing delegations
	logger.Info("Loading existing ZFS delegations")
	return nil
}

// persistPermission persists a permission to storage
func (ac *AccessController) persistPermission(permission *Permission) error {
	// Implementation would save to persistent storage
	return nil
}

// removePersistedPermission removes a permission from storage
func (ac *AccessController) removePersistedPermission(permissionID string) error {
	// Implementation would remove from persistent storage
	return nil
}