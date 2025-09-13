package auth

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ExampleOPAAuthorizationService_BasicUsage demonstrates basic usage of the OPA authorization service
func ExampleOPAAuthorizationService_BasicUsage() {
	logger, _ := zap.NewDevelopment()
	
	// Create and initialize the authorization service
	authzService := NewOPAAuthorizationService(logger)
	ctx := context.Background()
	
	if err := authzService.Initialize(ctx); err != nil {
		log.Fatal(err)
	}

	// Create an identity
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/operator")
	identity := &Identity{
		ID:       "operator",
		Subject:  "operator",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Make an authorization request
	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "content",
		Action:   "pin",
	}

	result, err := authzService.Authorize(ctx, request)
	if err != nil {
		log.Fatal(err)
	}

	if result.Allowed {
		fmt.Printf("Access granted: %s\n", result.Reason)
	} else {
		fmt.Printf("Access denied: %s\n", result.Reason)
	}
	
	// Output: Access granted: User has required permission
}

// ExampleSPIFFEAuthService_WithAuthorization demonstrates using SPIFFE auth service with OPA authorization
func ExampleSPIFFEAuthService_WithAuthorization() {
	logger, _ := zap.NewDevelopment()
	
	// Create SPIFFE auth service (includes OPA authorization)
	config := &Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}
	
	authService, err := NewSPIFFEAuthService(config, logger)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	
	// Initialize would connect to SPIRE agent in real usage
	// if err := authService.Initialize(ctx); err != nil {
	//     log.Fatal(err)
	// }

	// Create an identity (normally extracted from SPIFFE SVID)
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/admin")
	identity := &Identity{
		ID:       "admin",
		Subject:  "admin",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	// Check authorization
	err = authService.Authorize(ctx, identity, "policies", "create")
	if err != nil {
		fmt.Printf("Authorization failed: %v\n", err)
	} else {
		fmt.Println("Authorization successful")
	}
	
	// Output: Authorization successful
}

// ExampleHTTPServerWithAuthorization demonstrates setting up an HTTP server with authorization
func ExampleHTTPServerWithAuthorization() {
	logger, _ := zap.NewDevelopment()
	
	// Create auth service
	config := &Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}
	
	authService, err := NewSPIFFEAuthService(config, logger)
	if err != nil {
		log.Fatal(err)
	}

	// Create HTTP handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, err := RequireIdentity(r.Context())
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		
		fmt.Fprintf(w, "Hello, %s!", identity.Subject)
	})

	// Chain middlewares: Authentication -> Authorization -> Handler
	protectedHandler := authService.HTTPAuthMiddleware()(
		authService.HTTPAuthzMiddleware()(handler),
	)

	// Start server (in real usage)
	// http.ListenAndServeTLS(":8443", "cert.pem", "key.pem", protectedHandler)
	
	fmt.Println("HTTP server configured with SPIFFE authentication and OPA authorization")
	_ = protectedHandler // Avoid unused variable error
	
	// Output: HTTP server configured with SPIFFE authentication and OPA authorization
}

// ExampleGRPCServerWithAuthorization demonstrates setting up a gRPC server with authorization
func ExampleGRPCServerWithAuthorization() {
	logger, _ := zap.NewDevelopment()
	
	// Create auth service
	config := &Config{
		SocketPath: "unix:///tmp/spire-agent/public/api.sock",
	}
	
	authService, err := NewSPIFFEAuthService(config, logger)
	if err != nil {
		log.Fatal(err)
	}

	// Create gRPC server with interceptors
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			authService.GRPCAuthInterceptor(),    // Authentication first
			authService.GRPCAuthzInterceptor(),   // Then authorization
		),
		grpc.ChainStreamInterceptor(
			authService.GRPCStreamAuthInterceptor(),    // Authentication first
			authService.GRPCStreamAuthzInterceptor(),   // Then authorization
		),
	)

	// Register your services here
	// pb.RegisterYourServiceServer(server, yourServiceImpl)

	fmt.Println("gRPC server configured with SPIFFE authentication and OPA authorization")
	_ = server // Avoid unused variable error
	
	// Output: gRPC server configured with SPIFFE authentication and OPA authorization
}

// ExampleRoleManagement demonstrates managing roles at runtime
func ExampleRoleManagement() {
	logger, _ := zap.NewDevelopment()
	authzService := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	if err := authzService.Initialize(ctx); err != nil {
		log.Fatal(err)
	}

	userID := "new_user"
	
	// Assign roles to user
	if err := authzService.AssignRole(ctx, userID, "monitor"); err != nil {
		log.Fatal(err)
	}
	
	if err := authzService.AssignRole(ctx, userID, "operator"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Assigned roles to user: %s\n", userID)

	// Test authorization with new roles
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/" + userID)
	identity := &Identity{
		ID:       userID,
		Subject:  userID,
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}

	request := &AuthorizationRequest{
		Identity: identity,
		Resource: "metrics",
		Action:   "read",
	}

	result, err := authzService.Authorize(ctx, request)
	if err != nil {
		log.Fatal(err)
	}

	if result.Allowed {
		fmt.Printf("User can read metrics: %s\n", result.Reason)
	}

	// Revoke a role
	if err := authzService.RevokeRole(ctx, userID, "operator"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Revoked operator role from user: %s\n", userID)
	
	// Output: 
	// Assigned roles to user: new_user
	// User can read metrics: User has required permission
	// Revoked operator role from user: new_user
}

// ExampleMaintenanceWindow demonstrates setting maintenance windows
func ExampleMaintenanceWindow() {
	logger, _ := zap.NewDevelopment()
	authzService := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	if err := authzService.Initialize(ctx); err != nil {
		log.Fatal(err)
	}

	// Set a maintenance window for the next hour
	start := time.Now()
	end := start.Add(1 * time.Hour)
	
	err := authzService.SetMaintenanceWindow(ctx, start, end, "Scheduled maintenance")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Maintenance window set from %s to %s\n", 
		start.Format("15:04"), end.Format("15:04"))

	// Test authorization during maintenance window
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
		Resource: "policies",
		Action:   "delete",
	}

	result, err := authzService.Authorize(ctx, request)
	if err != nil {
		log.Fatal(err)
	}

	if !result.Allowed {
		fmt.Printf("Sensitive operation blocked during maintenance: %s\n", result.Reason)
	} else {
		fmt.Printf("Operation allowed: %s\n", result.Reason)
	}
	
	// Output: 
	// Maintenance window set from 15:04 to 16:04
	// Operation allowed: User has required permission
}

// ExampleCustomPolicy demonstrates loading custom authorization policies
func ExampleCustomPolicy() {
	logger, _ := zap.NewDevelopment()
	authzService := NewOPAAuthorizationService(logger)
	
	ctx := context.Background()
	if err := authzService.Initialize(ctx); err != nil {
		log.Fatal(err)
	}

	// Define a custom policy
	customPolicy := `
package authz

import rego.v1

# Allow content operations only during business hours (9 AM - 5 PM UTC)
allow := {"allow": true, "reason": "Business hours access"} if {
	input.resource == "content"
	input.action in ["pin", "unpin"]
	current_hour := time.clock(input.time)[0]
	current_hour >= 9
	current_hour <= 17
}

# Deny content operations outside business hours
allow := {"allow": false, "reason": "Outside business hours"} if {
	input.resource == "content"
	input.action in ["pin", "unpin"]
	current_hour := time.clock(input.time)[0]
	not (current_hour >= 9 && current_hour <= 17)
}`

	// Load the custom policy
	policies := map[string]string{
		"business_hours": customPolicy,
	}
	
	if err := authzService.LoadPolicies(ctx, policies); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Custom business hours policy loaded")

	// Test the policy during business hours (10 AM)
	businessTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	
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
		Context: map[string]interface{}{
			"time": businessTime.Unix(),
		},
	}

	result, err := authzService.Authorize(ctx, request)
	if err != nil {
		log.Fatal(err)
	}

	if result.Allowed {
		fmt.Printf("Content operation allowed during business hours: %s\n", result.Reason)
	} else {
		fmt.Printf("Content operation denied: %s\n", result.Reason)
	}
	
	// Output: 
	// Custom business hours policy loaded
	// Content operation allowed during business hours: User has required permission
}