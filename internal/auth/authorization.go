package auth

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/open-policy-agent/opa/rego"
	"github.com/open-policy-agent/opa/storage"
	"github.com/open-policy-agent/opa/storage/inmem"
	"go.uber.org/zap"
)

// AuthorizationService handles authorization decisions using OPA/Rego
type AuthorizationService interface {
	// Initialization
	Initialize(ctx context.Context) error
	
	// Policy management
	LoadPolicies(ctx context.Context, policies map[string]string) error
	UpdatePolicy(ctx context.Context, name string, policy string) error
	DeletePolicy(ctx context.Context, name string) error
	
	// Authorization decisions
	Authorize(ctx context.Context, request *AuthorizationRequest) (*AuthorizationResult, error)
	
	// Data management for policies
	SetData(ctx context.Context, path string, data interface{}) error
	GetData(ctx context.Context, path string) (interface{}, error)
}

// AuthorizationRequest represents an authorization request
type AuthorizationRequest struct {
	Identity *Identity         `json:"identity"`
	Resource string            `json:"resource"`
	Action   string            `json:"action"`
	Context  map[string]interface{} `json:"context,omitempty"`
}

// AuthorizationResult represents the result of an authorization decision
type AuthorizationResult struct {
	Allowed bool                   `json:"allowed"`
	Reason  string                 `json:"reason,omitempty"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// Role represents a user role with permissions
type Role struct {
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
	Description string   `json:"description"`
}

// Permission represents a specific permission
type Permission struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}

// OPAAuthorizationService implements authorization using Open Policy Agent
type OPAAuthorizationService struct {
	mu             sync.RWMutex
	store          storage.Store
	policies       map[string]*rego.PreparedEvalQuery
	policyContents map[string]string           // Store policy content for re-evaluation
	data           map[string]interface{}      // Simple in-memory data store
	logger         *zap.Logger
}

// NewOPAAuthorizationService creates a new OPA-based authorization service
func NewOPAAuthorizationService(logger *zap.Logger) *OPAAuthorizationService {
	return &OPAAuthorizationService{
		store:          inmem.New(),
		policies:       make(map[string]*rego.PreparedEvalQuery),
		policyContents: make(map[string]string),
		data:           make(map[string]interface{}),
		logger:         logger,
	}
}

// Initialize initializes the authorization service with default policies and data
func (s *OPAAuthorizationService) Initialize(ctx context.Context) error {
	s.logger.Info("Initializing OPA authorization service")

	// Load default policies
	defaultPolicies := s.getDefaultPolicies()
	if err := s.LoadPolicies(ctx, defaultPolicies); err != nil {
		return fmt.Errorf("failed to load default policies: %w", err)
	}

	// Load default roles and permissions
	if err := s.loadDefaultRolesAndPermissions(ctx); err != nil {
		return fmt.Errorf("failed to load default roles and permissions: %w", err)
	}

	s.logger.Info("OPA authorization service initialized successfully")
	return nil
}

// LoadPolicies loads multiple Rego policies
func (s *OPAAuthorizationService) LoadPolicies(ctx context.Context, policies map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, policy := range policies {
		if err := s.loadPolicy(ctx, name, policy); err != nil {
			return fmt.Errorf("failed to load policy %s: %w", name, err)
		}
	}

	return nil
}

// UpdatePolicy updates or creates a single policy
func (s *OPAAuthorizationService) UpdatePolicy(ctx context.Context, name string, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.loadPolicy(ctx, name, policy)
}

// DeletePolicy removes a policy
func (s *OPAAuthorizationService) DeletePolicy(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.policies, name)
	s.logger.Info("Policy deleted", zap.String("name", name))
	return nil
}

// loadPolicy loads a single Rego policy
func (s *OPAAuthorizationService) loadPolicy(ctx context.Context, name string, policy string) error {
	// Store policy content
	s.policyContents[name] = policy

	// Prepare the Rego query
	r := rego.New(
		rego.Query("data.authz.allow"),
		rego.Module(name+".rego", policy),
	)

	prepared, err := r.PrepareForEval(ctx)
	if err != nil {
		return fmt.Errorf("failed to prepare policy %s: %w", name, err)
	}

	s.policies[name] = &prepared
	s.logger.Info("Policy loaded", zap.String("name", name))
	return nil
}

// getPolicyContent retrieves the content of a policy
func (s *OPAAuthorizationService) getPolicyContent(name string) string {
	if content, exists := s.policyContents[name]; exists {
		return content
	}
	return ""
}

// Authorize makes an authorization decision
func (s *OPAAuthorizationService) Authorize(ctx context.Context, request *AuthorizationRequest) (*AuthorizationResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Prepare input for OPA evaluation
	input := map[string]interface{}{
		"identity": map[string]interface{}{
			"id":        request.Identity.ID,
			"subject":   request.Identity.Subject,
			"spiffe_id": request.Identity.SPIFFEID.String(),
			"claims":    request.Identity.Claims,
		},
		"resource": request.Resource,
		"action":   request.Action,
		"context":  request.Context,
		"time":     time.Now().Unix(),
	}

	// Evaluate all policies and combine results
	allowed := false
	var reasons []string
	details := make(map[string]interface{})

	for policyName, prepared := range s.policies {
		// Use prepared query for better performance
		rs, err := prepared.Eval(ctx, rego.EvalInput(input))
		if err != nil {
			s.logger.Error("Policy evaluation failed", 
				zap.String("policy", policyName),
				zap.Error(err))
			continue
		}

		// Process results
		for _, result := range rs {
			for _, expr := range result.Expressions {
				if decision, ok := expr.Value.(map[string]interface{}); ok {
					if allow, exists := decision["allow"]; exists {
						if allowBool, ok := allow.(bool); ok && allowBool {
							allowed = true
						}
					}
					if reason, exists := decision["reason"]; exists {
						if reasonStr, ok := reason.(string); ok {
							reasons = append(reasons, reasonStr)
						}
					}
					// Collect additional details
					for k, v := range decision {
						if k != "allow" && k != "reason" {
							details[k] = v
						}
					}
				} else if allowBool, ok := expr.Value.(bool); ok {
					if allowBool {
						allowed = true
					}
				}
			}
		}
	}

	result := &AuthorizationResult{
		Allowed: allowed,
		Details: details,
	}

	if len(reasons) > 0 {
		result.Reason = strings.Join(reasons, "; ")
	} else if !allowed {
		result.Reason = "Access denied by policy"
	}

	s.logger.Debug("Authorization decision", 
		zap.String("spiffe_id", request.Identity.SPIFFEID.String()),
		zap.String("resource", request.Resource),
		zap.String("action", request.Action),
		zap.Bool("allowed", allowed),
		zap.String("reason", result.Reason))

	return result, nil
}

// SetData sets data in the OPA store for use by policies
func (s *OPAAuthorizationService) SetData(ctx context.Context, path string, data interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// For simplicity, we'll store data in memory and recreate prepared queries
	// In a production system, you might want to use OPA's bundle API or server mode
	
	// Store the data in a simple map for now
	if s.data == nil {
		s.data = make(map[string]interface{})
	}
	s.data[path] = data

	s.logger.Debug("Data set in OPA store", zap.String("path", path))
	return nil
}

// GetData retrieves data from the OPA store
func (s *OPAAuthorizationService) GetData(ctx context.Context, path string) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.data == nil {
		return nil, fmt.Errorf("no data found at path: %s", path)
	}

	data, exists := s.data[path]
	if !exists {
		return nil, fmt.Errorf("no data found at path: %s", path)
	}

	return data, nil
}

// getDefaultPolicies returns the default authorization policies
func (s *OPAAuthorizationService) getDefaultPolicies() map[string]string {
	return map[string]string{
		"rbac": `
package authz

import rego.v1

# Default deny
default allow := {"allow": false, "reason": "No matching policy"}

# Define roles and permissions inline for now
roles := {
	"admin": ["admin"],
	"operator": ["operator", "monitor"],
	"policy_admin": ["policy_admin", "monitor"],
	"cluster_admin": ["cluster_admin", "monitor"],
	"zone_admin": ["zone_admin", "monitor"],
	"monitor": ["monitor"],
	"spiffe://gdc.local/scheduler": ["service", "monitor"],
	"spiffe://gdc.local/orchestrator": ["service", "monitor"],
	"spiffe://gdc.local/policy-engine": ["service", "monitor"],
}

role_permissions := {
	"admin": ["*:*"],
	"operator": ["content:pin", "content:unpin", "content:replicate", "clusters:read", "zones:read", "policies:read", "metrics:read", "health:read", "status:read", "topology:read"],
	"policy_admin": ["policies:create", "policies:update", "policies:delete", "policies:read", "metrics:read", "health:read", "status:read"],
	"cluster_admin": ["clusters:create", "clusters:update", "clusters:delete", "clusters:read", "clusters:drain", "clusters:evacuate", "metrics:read", "health:read", "status:read", "topology:read"],
	"zone_admin": ["zones:create", "zones:update", "zones:delete", "zones:read", "zones:maintenance", "metrics:read", "health:read", "status:read", "topology:read"],
	"monitor": ["metrics:read", "health:read", "status:read", "topology:read"],
	"service": ["health:read", "metrics:read", "status:read"],
}

# Allow if user has required permission
allow := {"allow": true, "reason": "User has required permission"} if {
	user_roles := roles[input.identity.subject]
	some role in user_roles
	permissions := role_permissions[role]
	required_permission := sprintf("%s:%s", [input.resource, input.action])
	required_permission in permissions
}

# Allow admin users for all operations
allow := {"allow": true, "reason": "Admin user"} if {
	user_roles := roles[input.identity.subject]
	"admin" in user_roles
}

# Allow service-to-service communication for specific SPIFFE IDs
allow := {"allow": true, "reason": "Service-to-service communication"} if {
	startswith(input.identity.spiffe_id, "spiffe://gdc.local/")
	input.resource in ["health", "metrics", "status"]
	input.action == "read"
}

# Allow users to read their own resources
allow := {"allow": true, "reason": "User accessing own resources"} if {
	input.action == "read"
	startswith(input.resource, sprintf("users/%s", [input.identity.subject]))
}`,

		"abac": `
package authz

import rego.v1

# Attribute-based access control rules

# Allow based on time constraints (business hours 9-17)
allow := {"allow": true, "reason": "Within business hours"} if {
	input.identity.subject in ["operator", "policy_admin"]
	current_hour := time.clock(input.time)[0]
	current_hour >= 9
	current_hour <= 17
}

# Allow based on IP/location constraints (internal networks)
allow := {"allow": true, "reason": "Request from allowed location"} if {
	input.context.client_ip
	net.cidr_contains("10.0.0.0/8", input.context.client_ip)
}

allow := {"allow": true, "reason": "Request from allowed location"} if {
	input.context.client_ip
	net.cidr_contains("192.168.0.0/16", input.context.client_ip)
}

# Allow emergency access (simplified - in production use proper token validation)
allow := {"allow": true, "reason": "Emergency access granted"} if {
	input.identity.subject == "admin"
	input.context.emergency_token == "emergency-token-admin-123"
}`,

		"resource_specific": `
package authz

import rego.v1

# Resource-specific authorization rules

# Policy management permissions
allow := {"allow": true, "reason": "Policy management permission"} if {
	input.resource == "policies"
	input.action in ["create", "update", "delete"]
	user_roles := data.roles[input.identity.subject]
	"policy_admin" in user_roles
}

# Cluster management permissions
allow := {"allow": true, "reason": "Cluster management permission"} if {
	startswith(input.resource, "clusters/")
	input.action in ["create", "update", "delete", "drain", "evacuate"]
	user_roles := data.roles[input.identity.subject]
	"cluster_admin" in user_roles
}

# Zone management permissions
allow := {"allow": true, "reason": "Zone management permission"} if {
	startswith(input.resource, "zones/")
	input.action in ["create", "update", "delete", "maintenance"]
	user_roles := data.roles[input.identity.subject]
	"zone_admin" in user_roles
}

# Content operations permissions
allow := {"allow": true, "reason": "Content operation permission"} if {
	input.resource == "content"
	input.action in ["pin", "unpin", "replicate"]
	user_roles := data.roles[input.identity.subject]
	some role in user_roles
	role in ["content_admin", "operator"]
}

# Read-only access for monitoring
allow := {"allow": true, "reason": "Monitoring access"} if {
	input.action == "read"
	input.resource in ["metrics", "health", "status", "topology"]
	user_roles := data.roles[input.identity.subject]
	"monitor" in user_roles
}`,
	}
}

// loadDefaultRolesAndPermissions loads default roles and permissions into the OPA store
func (s *OPAAuthorizationService) loadDefaultRolesAndPermissions(ctx context.Context) error {
	// For now, roles and permissions are defined inline in the policies
	// This method is kept for future extensibility when we need runtime role management
	
	s.logger.Info("Default roles and permissions loaded (inline in policies)")
	return nil
}

// Helper functions for managing roles and permissions at runtime

// AssignRole assigns a role to a user
func (s *OPAAuthorizationService) AssignRole(ctx context.Context, userID string, role string) error {
	// Get current roles
	rolesData, err := s.GetData(ctx, "roles")
	if err != nil {
		return fmt.Errorf("failed to get roles data: %w", err)
	}

	roles, ok := rolesData.(map[string]interface{})
	if !ok {
		roles = make(map[string]interface{})
	}

	// Get user's current roles
	var userRoles []string
	if existingRoles, exists := roles[userID]; exists {
		if rolesList, ok := existingRoles.([]interface{}); ok {
			for _, r := range rolesList {
				if roleStr, ok := r.(string); ok {
					userRoles = append(userRoles, roleStr)
				}
			}
		}
	}

	// Add new role if not already present
	for _, existingRole := range userRoles {
		if existingRole == role {
			return nil // Role already assigned
		}
	}
	userRoles = append(userRoles, role)

	// Update roles
	roles[userID] = userRoles
	return s.SetData(ctx, "roles", roles)
}

// RevokeRole removes a role from a user
func (s *OPAAuthorizationService) RevokeRole(ctx context.Context, userID string, role string) error {
	// Get current roles
	rolesData, err := s.GetData(ctx, "roles")
	if err != nil {
		return fmt.Errorf("failed to get roles data: %w", err)
	}

	roles, ok := rolesData.(map[string]interface{})
	if !ok {
		return nil // No roles to revoke
	}

	// Get user's current roles
	var userRoles []string
	if existingRoles, exists := roles[userID]; exists {
		if rolesList, ok := existingRoles.([]interface{}); ok {
			for _, r := range rolesList {
				if roleStr, ok := r.(string); ok && roleStr != role {
					userRoles = append(userRoles, roleStr)
				}
			}
		}
	}

	// Update roles
	if len(userRoles) == 0 {
		delete(roles, userID)
	} else {
		roles[userID] = userRoles
	}
	
	return s.SetData(ctx, "roles", roles)
}

// SetMaintenanceWindow sets a maintenance window during which certain operations are restricted
func (s *OPAAuthorizationService) SetMaintenanceWindow(ctx context.Context, start, end time.Time, description string) error {
	// Get current maintenance windows
	windowsData, err := s.GetData(ctx, "maintenance_windows")
	if err != nil {
		return fmt.Errorf("failed to get maintenance windows data: %w", err)
	}

	var windows []interface{}
	if windowsList, ok := windowsData.([]interface{}); ok {
		windows = windowsList
	}

	// Add new maintenance window
	newWindow := map[string]interface{}{
		"start":       start.Unix(),
		"end":         end.Unix(),
		"description": description,
	}
	windows = append(windows, newWindow)

	return s.SetData(ctx, "maintenance_windows", windows)
}