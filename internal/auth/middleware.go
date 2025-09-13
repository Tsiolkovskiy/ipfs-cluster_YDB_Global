package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ContextKey is used for storing values in context
type ContextKey string

const (
	// IdentityContextKey is the key for storing identity in context
	IdentityContextKey ContextKey = "identity"
)

// GRPCAuthInterceptor creates a gRPC unary interceptor for SPIFFE authentication
func (s *SPIFFEAuthService) GRPCAuthInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip authentication for health checks and other public endpoints
		if isPublicEndpoint(info.FullMethod) {
			return handler(ctx, req)
		}

		identity, err := s.authenticateGRPCRequest(ctx)
		if err != nil {
			s.logger.Warn("gRPC authentication failed", 
				zap.String("method", info.FullMethod),
				zap.Error(err))
			return nil, status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}

		// Add identity to context
		ctx = context.WithValue(ctx, IdentityContextKey, identity)

		s.logger.Debug("gRPC request authenticated", 
			zap.String("method", info.FullMethod),
			zap.String("spiffe_id", identity.SPIFFEID.String()))

		return handler(ctx, req)
	}
}

// GRPCAuthzInterceptor creates a gRPC unary interceptor for authorization
func (s *SPIFFEAuthService) GRPCAuthzInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip authorization for public endpoints
		if isPublicEndpoint(info.FullMethod) {
			return handler(ctx, req)
		}

		identity, ok := GetIdentityFromContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "no identity found in context")
		}

		// Extract resource and action from gRPC method
		resource, action := extractResourceActionFromGRPCMethod(info.FullMethod)
		
		// Perform authorization check
		if err := s.Authorize(ctx, identity, resource, action); err != nil {
			s.logger.Warn("gRPC authorization failed", 
				zap.String("method", info.FullMethod),
				zap.String("spiffe_id", identity.SPIFFEID.String()),
				zap.String("resource", resource),
				zap.String("action", action),
				zap.Error(err))
			return nil, status.Errorf(codes.PermissionDenied, "authorization failed: %v", err)
		}

		s.logger.Debug("gRPC request authorized", 
			zap.String("method", info.FullMethod),
			zap.String("spiffe_id", identity.SPIFFEID.String()),
			zap.String("resource", resource),
			zap.String("action", action))

		return handler(ctx, req)
	}
}

// GRPCStreamAuthInterceptor creates a gRPC stream interceptor for SPIFFE authentication
func (s *SPIFFEAuthService) GRPCStreamAuthInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Skip authentication for health checks and other public endpoints
		if isPublicEndpoint(info.FullMethod) {
			return handler(srv, ss)
		}

		identity, err := s.authenticateGRPCRequest(ss.Context())
		if err != nil {
			s.logger.Warn("gRPC stream authentication failed", 
				zap.String("method", info.FullMethod),
				zap.Error(err))
			return status.Errorf(codes.Unauthenticated, "authentication failed: %v", err)
		}

		// Create new context with identity
		ctx := context.WithValue(ss.Context(), IdentityContextKey, identity)
		
		// Wrap the stream with new context
		wrappedStream := &contextServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		s.logger.Debug("gRPC stream authenticated", 
			zap.String("method", info.FullMethod),
			zap.String("spiffe_id", identity.SPIFFEID.String()))

		return handler(srv, wrappedStream)
	}
}

// GRPCStreamAuthzInterceptor creates a gRPC stream interceptor for authorization
func (s *SPIFFEAuthService) GRPCStreamAuthzInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Skip authorization for public endpoints
		if isPublicEndpoint(info.FullMethod) {
			return handler(srv, ss)
		}

		identity, ok := GetIdentityFromContext(ss.Context())
		if !ok {
			return status.Errorf(codes.Unauthenticated, "no identity found in context")
		}

		// Extract resource and action from gRPC method
		resource, action := extractResourceActionFromGRPCMethod(info.FullMethod)
		
		// Perform authorization check
		if err := s.Authorize(ss.Context(), identity, resource, action); err != nil {
			s.logger.Warn("gRPC stream authorization failed", 
				zap.String("method", info.FullMethod),
				zap.String("spiffe_id", identity.SPIFFEID.String()),
				zap.String("resource", resource),
				zap.String("action", action),
				zap.Error(err))
			return status.Errorf(codes.PermissionDenied, "authorization failed: %v", err)
		}

		s.logger.Debug("gRPC stream authorized", 
			zap.String("method", info.FullMethod),
			zap.String("spiffe_id", identity.SPIFFEID.String()),
			zap.String("resource", resource),
			zap.String("action", action))

		return handler(srv, ss)
	}
}

// HTTPAuthMiddleware creates an HTTP middleware for SPIFFE authentication
func (s *SPIFFEAuthService) HTTPAuthMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip authentication for health checks and other public endpoints
			if isPublicHTTPEndpoint(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			identity, err := s.authenticateHTTPRequest(r)
			if err != nil {
				s.logger.Warn("HTTP authentication failed", 
					zap.String("path", r.URL.Path),
					zap.String("method", r.Method),
					zap.Error(err))
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}

			// Add identity to request context
			ctx := context.WithValue(r.Context(), IdentityContextKey, identity)
			r = r.WithContext(ctx)

			s.logger.Debug("HTTP request authenticated", 
				zap.String("path", r.URL.Path),
				zap.String("method", r.Method),
				zap.String("spiffe_id", identity.SPIFFEID.String()))

			next.ServeHTTP(w, r)
		})
	}
}

// HTTPAuthzMiddleware creates an HTTP middleware for authorization
func (s *SPIFFEAuthService) HTTPAuthzMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip authorization for public endpoints
			if isPublicHTTPEndpoint(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			identity, ok := GetIdentityFromContext(r.Context())
			if !ok {
				http.Error(w, "No identity found in context", http.StatusUnauthorized)
				return
			}

			// Extract resource and action from HTTP request
			resource, action := extractResourceActionFromHTTPRequest(r)
			
			// Create authorization context with request details
			authzContext := map[string]interface{}{
				"client_ip": getClientIP(r),
				"user_agent": r.UserAgent(),
				"path": r.URL.Path,
				"query": r.URL.RawQuery,
			}

			// Perform authorization check
			if err := s.AuthorizeWithContext(r.Context(), identity, resource, action, authzContext); err != nil {
				s.logger.Warn("HTTP authorization failed", 
					zap.String("path", r.URL.Path),
					zap.String("method", r.Method),
					zap.String("spiffe_id", identity.SPIFFEID.String()),
					zap.String("resource", resource),
					zap.String("action", action),
					zap.Error(err))
				http.Error(w, "Authorization failed", http.StatusForbidden)
				return
			}

			s.logger.Debug("HTTP request authorized", 
				zap.String("path", r.URL.Path),
				zap.String("method", r.Method),
				zap.String("spiffe_id", identity.SPIFFEID.String()),
				zap.String("resource", resource),
				zap.String("action", action))

			next.ServeHTTP(w, r)
		})
	}
}

// authenticateGRPCRequest extracts and validates the client certificate from gRPC context
func (s *SPIFFEAuthService) authenticateGRPCRequest(ctx context.Context) (*Identity, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no peer information found in context")
	}

	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, fmt.Errorf("no TLS information found in peer")
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, fmt.Errorf("no client certificates found")
	}

	// Validate the client certificate
	clientCert := tlsInfo.State.PeerCertificates[0]
	identity, err := s.ValidateSVID(ctx, clientCert)
	if err != nil {
		return nil, fmt.Errorf("SVID validation failed: %w", err)
	}

	return identity, nil
}

// authenticateHTTPRequest extracts and validates the client certificate from HTTP request
func (s *SPIFFEAuthService) authenticateHTTPRequest(r *http.Request) (*Identity, error) {
	if r.TLS == nil {
		return nil, fmt.Errorf("no TLS connection found")
	}

	if len(r.TLS.PeerCertificates) == 0 {
		return nil, fmt.Errorf("no client certificates found")
	}

	// Validate the client certificate
	clientCert := r.TLS.PeerCertificates[0]
	identity, err := s.ValidateSVID(r.Context(), clientCert)
	if err != nil {
		return nil, fmt.Errorf("SVID validation failed: %w", err)
	}

	return identity, nil
}

// isPublicEndpoint checks if a gRPC method should skip authentication
func isPublicEndpoint(method string) bool {
	publicEndpoints := []string{
		"/grpc.health.v1.Health/Check",
		"/grpc.health.v1.Health/Watch",
		"/gdc.v1.HealthService/Check",
	}

	for _, endpoint := range publicEndpoints {
		if strings.HasSuffix(method, endpoint) {
			return true
		}
	}
	return false
}

// isPublicHTTPEndpoint checks if an HTTP path should skip authentication
func isPublicHTTPEndpoint(path string) bool {
	publicPaths := []string{
		"/health",
		"/healthz",
		"/ready",
		"/metrics",
		"/version",
	}

	for _, publicPath := range publicPaths {
		if strings.HasPrefix(path, publicPath) {
			return true
		}
	}
	return false
}

// contextServerStream wraps grpc.ServerStream to provide a custom context
type contextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the custom context
func (css *contextServerStream) Context() context.Context {
	return css.ctx
}

// GetIdentityFromContext extracts the identity from the context
func GetIdentityFromContext(ctx context.Context) (*Identity, bool) {
	identity, ok := ctx.Value(IdentityContextKey).(*Identity)
	return identity, ok
}

// RequireIdentity is a helper function that extracts identity from context and returns an error if not found
func RequireIdentity(ctx context.Context) (*Identity, error) {
	identity, ok := GetIdentityFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no identity found in context")
	}
	return identity, nil
}

// extractResourceActionFromGRPCMethod extracts resource and action from gRPC method name
func extractResourceActionFromGRPCMethod(method string) (resource, action string) {
	// Example: /gdc.v1.PolicyService/CreatePolicy -> resource: policies, action: create
	parts := strings.Split(method, "/")
	if len(parts) < 3 {
		return "unknown", "unknown"
	}

	serviceParts := strings.Split(parts[1], ".")
	if len(serviceParts) < 3 {
		return "unknown", "unknown"
	}

	serviceName := serviceParts[2] // PolicyService
	methodName := parts[2]         // CreatePolicy

	// Map service names to resources
	resource = mapServiceToResource(serviceName)
	action = mapMethodToAction(methodName)

	return resource, action
}

// extractResourceActionFromHTTPRequest extracts resource and action from HTTP request
func extractResourceActionFromHTTPRequest(r *http.Request) (resource, action string) {
	// Example: GET /api/v1/policies -> resource: policies, action: read
	// Example: POST /api/v1/policies -> resource: policies, action: create
	// Example: PUT /api/v1/policies/123 -> resource: policies, action: update
	// Example: DELETE /api/v1/policies/123 -> resource: policies, action: delete

	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")

	// Skip api/v1 prefix if present
	if len(parts) >= 2 && parts[0] == "api" && strings.HasPrefix(parts[1], "v") {
		parts = parts[2:]
	}

	if len(parts) == 0 {
		return "unknown", "unknown"
	}

	resource = parts[0]
	action = mapHTTPMethodToAction(r.Method, len(parts) > 1)

	return resource, action
}

// mapServiceToResource maps gRPC service names to resource names
func mapServiceToResource(serviceName string) string {
	serviceToResource := map[string]string{
		"PolicyService":      "policies",
		"SchedulerService":   "scheduler",
		"OrchestratorService": "orchestrator",
		"ClusterService":     "clusters",
		"ZoneService":        "zones",
		"TopologyService":    "topology",
		"HealthService":      "health",
		"MetricsService":     "metrics",
		"ContentService":     "content",
	}

	if resource, exists := serviceToResource[serviceName]; exists {
		return resource
	}

	// Default: convert CamelCase to lowercase
	return strings.ToLower(strings.TrimSuffix(serviceName, "Service"))
}

// mapMethodToAction maps gRPC method names to actions
func mapMethodToAction(methodName string) string {
	methodToAction := map[string]string{
		// CRUD operations
		"Create":   "create",
		"Get":      "read",
		"List":     "read",
		"Update":   "update",
		"Delete":   "delete",
		
		// Policy operations
		"CreatePolicy":   "create",
		"GetPolicy":      "read",
		"ListPolicies":   "read",
		"UpdatePolicy":   "update",
		"DeletePolicy":   "delete",
		"EvaluatePolicy": "evaluate",
		
		// Scheduler operations
		"ComputePlacement":   "compute",
		"OptimizePlacement":  "optimize",
		"SimulatePlacement":  "simulate",
		
		// Orchestrator operations
		"StartWorkflow":      "start",
		"GetWorkflowStatus":  "read",
		"CancelWorkflow":     "cancel",
		
		// Cluster operations
		"RegisterCluster":    "register",
		"UnregisterCluster":  "unregister",
		"DrainCluster":       "drain",
		"EvacuateCluster":    "evacuate",
		
		// Content operations
		"PinContent":         "pin",
		"UnpinContent":       "unpin",
		"ReplicateContent":   "replicate",
		
		// Health and monitoring
		"Check":              "read",
		"GetMetrics":         "read",
		"GetStatus":          "read",
	}

	if action, exists := methodToAction[methodName]; exists {
		return action
	}

	// Default: extract action from method name
	for prefix, action := range map[string]string{
		"Create": "create",
		"Get":    "read",
		"List":   "read",
		"Update": "update",
		"Delete": "delete",
		"Start":  "start",
		"Stop":   "stop",
		"Cancel": "cancel",
	} {
		if strings.HasPrefix(methodName, prefix) {
			return action
		}
	}

	return "unknown"
}

// mapHTTPMethodToAction maps HTTP methods to actions
func mapHTTPMethodToAction(method string, hasID bool) string {
	switch method {
	case "GET":
		return "read"
	case "POST":
		return "create"
	case "PUT", "PATCH":
		return "update"
	case "DELETE":
		return "delete"
	case "HEAD", "OPTIONS":
		return "read"
	default:
		return "unknown"
	}
}

// getClientIP extracts the client IP address from the HTTP request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ips := strings.Split(xff, ",")
		return strings.TrimSpace(ips[0])
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	ip := r.RemoteAddr
	if colon := strings.LastIndex(ip, ":"); colon != -1 {
		ip = ip[:colon]
	}
	return ip
}