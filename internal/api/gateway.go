package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Gateway represents the API Gateway component
type Gateway interface {
	// HTTP/REST endpoints
	Start(ctx context.Context, addr string) error
	Stop(ctx context.Context) error
	
	// Health check
	Health(ctx context.Context) error
}

// Services interface defines the backend services
type Services interface {
	// Zone management
	GetZones(ctx context.Context) ([]*models.Zone, error)
	GetZone(ctx context.Context, id string) (*models.Zone, error)
	CreateZone(ctx context.Context, zone *models.Zone) error
	UpdateZone(ctx context.Context, id string, zone *models.Zone) error
	DeleteZone(ctx context.Context, id string) error

	// Cluster management
	GetClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error)
	GetCluster(ctx context.Context, id string) (*models.Cluster, error)
	CreateCluster(ctx context.Context, cluster *models.Cluster) error
	UpdateCluster(ctx context.Context, id string, cluster *models.Cluster) error
	DeleteCluster(ctx context.Context, id string) error

	// Content management
	PinContent(ctx context.Context, request *PinRequest) (*models.PinPlan, error)
	UnpinContent(ctx context.Context, cid string) error
	GetContentStatus(ctx context.Context, cid string) (*models.PinStatus, error)
	ListContent(ctx context.Context, filter *ContentFilter) ([]*models.Content, error)

	// Policy management
	GetPolicies(ctx context.Context) ([]*models.Policy, error)
	GetPolicy(ctx context.Context, id string) (*models.Policy, error)
	CreatePolicy(ctx context.Context, policy *models.Policy) error
	UpdatePolicy(ctx context.Context, id string, policy *models.Policy) error
	DeletePolicy(ctx context.Context, id string) error

	// Topology
	GetTopology(ctx context.Context) (*models.Topology, error)
}

// HTTPGateway implements the Gateway interface
type HTTPGateway struct {
	server   *http.Server
	services Services
	logger   *zap.Logger
	limiter  *RateLimiter
	mux      *http.ServeMux
}

// RateLimiter implements per-IP rate limiting
type RateLimiter struct {
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	rate     rate.Limit
	burst    int
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(r rate.Limit, b int) *RateLimiter {
	return &RateLimiter{
		limiters: make(map[string]*rate.Limiter),
		rate:     r,
		burst:    b,
	}
}

// Allow checks if the request from the given IP is allowed
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, exists := rl.limiters[ip]
	if !exists {
		limiter = rate.NewLimiter(rl.rate, rl.burst)
		rl.limiters[ip] = limiter
	}

	return limiter.Allow()
}

// NewHTTPGateway creates a new HTTP gateway instance
func NewHTTPGateway(services Services, logger *zap.Logger) *HTTPGateway {
	// Rate limit: 100 requests per minute per IP
	limiter := NewRateLimiter(rate.Every(time.Minute/100), 10)
	
	gateway := &HTTPGateway{
		services: services,
		logger:   logger,
		limiter:  limiter,
		mux:      http.NewServeMux(),
	}
	
	gateway.setupRoutes()
	return gateway
}

// setupRoutes configures all HTTP routes
func (g *HTTPGateway) setupRoutes() {
	// Health and readiness
	g.mux.HandleFunc("/health", g.HealthHandler)
	g.mux.HandleFunc("/ready", g.ReadyHandler)
	
	// API v1 routes
	g.mux.HandleFunc("/api/v1/zones", g.rateLimitMiddleware(g.corsMiddleware(g.zonesHandler)))
	g.mux.HandleFunc("/api/v1/zones/", g.rateLimitMiddleware(g.corsMiddleware(g.zoneHandler)))
	g.mux.HandleFunc("/api/v1/clusters", g.rateLimitMiddleware(g.corsMiddleware(g.clustersHandler)))
	g.mux.HandleFunc("/api/v1/clusters/", g.rateLimitMiddleware(g.corsMiddleware(g.clusterHandler)))
	g.mux.HandleFunc("/api/v1/content/pin", g.rateLimitMiddleware(g.corsMiddleware(g.pinContentHandler)))
	g.mux.HandleFunc("/api/v1/content/unpin/", g.rateLimitMiddleware(g.corsMiddleware(g.unpinContentHandler)))
	g.mux.HandleFunc("/api/v1/content/status/", g.rateLimitMiddleware(g.corsMiddleware(g.contentStatusHandler)))
	g.mux.HandleFunc("/api/v1/content", g.rateLimitMiddleware(g.corsMiddleware(g.listContentHandler)))
	g.mux.HandleFunc("/api/v1/policies", g.rateLimitMiddleware(g.corsMiddleware(g.policiesHandler)))
	g.mux.HandleFunc("/api/v1/policies/", g.rateLimitMiddleware(g.corsMiddleware(g.policyHandler)))
	g.mux.HandleFunc("/api/v1/topology", g.rateLimitMiddleware(g.corsMiddleware(g.topologyHandler)))
	
	// OpenAPI spec
	g.mux.HandleFunc("/api/v1/openapi.json", g.openAPISpecHandler)
}

// Start starts the HTTP server
func (g *HTTPGateway) Start(ctx context.Context, addr string) error {
	g.server = &http.Server{
		Addr:         addr,
		Handler:      g.mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	g.logger.Info("Starting API Gateway", zap.String("addr", addr))
	
	go func() {
		if err := g.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			g.logger.Error("API Gateway server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully stops the HTTP server
func (g *HTTPGateway) Stop(ctx context.Context) error {
	if g.server == nil {
		return nil
	}

	g.logger.Info("Stopping API Gateway")
	return g.server.Shutdown(ctx)
}

// Health performs a health check
func (g *HTTPGateway) Health(ctx context.Context) error {
	// Check if services are available
	if g.services == nil {
		return fmt.Errorf("services not available")
	}
	return nil
}

// Middleware functions

// rateLimitMiddleware applies rate limiting
func (g *HTTPGateway) rateLimitMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := getClientIP(r)
		if !g.limiter.Allow(ip) {
			g.writeErrorResponse(w, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", "Rate limit exceeded", nil)
			return
		}
		next(w, r)
	}
}

// corsMiddleware adds CORS headers
func (g *HTTPGateway) corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		next(w, r)
	}
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header
	xff := r.Header.Get("X-Forwarded-For")
	if xff != "" {
		ips := strings.Split(xff, ",")
		return strings.TrimSpace(ips[0])
	}
	
	// Check X-Real-IP header
	xri := r.Header.Get("X-Real-IP")
	if xri != "" {
		return xri
	}
	
	// Fall back to RemoteAddr
	ip := r.RemoteAddr
	if colon := strings.LastIndex(ip, ":"); colon != -1 {
		ip = ip[:colon]
	}
	return ip
}

// HealthHandler handles health check requests
func (g *HTTPGateway) HealthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	response := map[string]interface{}{
		"status":    "healthy",
		"service":   "global-data-controller",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	g.writeJSONResponse(w, http.StatusOK, response)
}

// ReadyHandler handles readiness check requests
func (g *HTTPGateway) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	// Check if services are ready
	if err := g.Health(r.Context()); err != nil {
		g.writeErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "Service not ready", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	response := map[string]interface{}{
		"status":    "ready",
		"service":   "global-data-controller",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}
	
	g.writeJSONResponse(w, http.StatusOK, response)
}

// Request and Response types

// PinRequest represents a request to pin content
type PinRequest struct {
	CID         string            `json:"cid" validate:"required"`
	Size        int64             `json:"size,omitempty"`
	Policies    []string          `json:"policies,omitempty"`
	Constraints map[string]string `json:"constraints,omitempty"`
	Priority    int               `json:"priority,omitempty"`
}

// ContentFilter represents filters for listing content
type ContentFilter struct {
	Status   string `json:"status,omitempty"`
	ZoneID   string `json:"zone_id,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Offset   int    `json:"offset,omitempty"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

// API Handlers

// zonesHandler handles /api/v1/zones
func (g *HTTPGateway) zonesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.listZones(w, r)
	case http.MethodPost:
		g.createZone(w, r)
	default:
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
	}
}

// zoneHandler handles /api/v1/zones/{id}
func (g *HTTPGateway) zoneHandler(w http.ResponseWriter, r *http.Request) {
	// Extract zone ID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/zones/")
	zoneID := strings.Split(path, "/")[0]
	
	if zoneID == "" {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_ZONE_ID", "Zone ID is required", nil)
		return
	}

	switch r.Method {
	case http.MethodGet:
		g.getZone(w, r, zoneID)
	case http.MethodPut:
		g.updateZone(w, r, zoneID)
	case http.MethodDelete:
		g.deleteZone(w, r, zoneID)
	default:
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
	}
}

// listZones handles GET /api/v1/zones
func (g *HTTPGateway) listZones(w http.ResponseWriter, r *http.Request) {
	zones, err := g.services.GetZones(r.Context())
	if err != nil {
		g.logger.Error("Failed to get zones", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get zones", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"zones": zones,
		"count": len(zones),
	})
}

// createZone handles POST /api/v1/zones
func (g *HTTPGateway) createZone(w http.ResponseWriter, r *http.Request) {
	var zone models.Zone
	if err := g.parseJSONBody(r, &zone); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validateZone(&zone); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.services.CreateZone(r.Context(), &zone); err != nil {
		g.logger.Error("Failed to create zone", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create zone", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusCreated, zone)
}

// getZone handles GET /api/v1/zones/{id}
func (g *HTTPGateway) getZone(w http.ResponseWriter, r *http.Request, zoneID string) {
	zone, err := g.services.GetZone(r.Context(), zoneID)
	if err != nil {
		g.logger.Error("Failed to get zone", zap.String("zone_id", zoneID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusNotFound, "ZONE_NOT_FOUND", "Zone not found", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, zone)
}

// updateZone handles PUT /api/v1/zones/{id}
func (g *HTTPGateway) updateZone(w http.ResponseWriter, r *http.Request, zoneID string) {
	var zone models.Zone
	if err := g.parseJSONBody(r, &zone); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validateZone(&zone); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.services.UpdateZone(r.Context(), zoneID, &zone); err != nil {
		g.logger.Error("Failed to update zone", zap.String("zone_id", zoneID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update zone", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, zone)
}

// deleteZone handles DELETE /api/v1/zones/{id}
func (g *HTTPGateway) deleteZone(w http.ResponseWriter, r *http.Request, zoneID string) {
	if err := g.services.DeleteZone(r.Context(), zoneID); err != nil {
		g.logger.Error("Failed to delete zone", zap.String("zone_id", zoneID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete zone", nil)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// clustersHandler handles /api/v1/clusters
func (g *HTTPGateway) clustersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.listClusters(w, r)
	case http.MethodPost:
		g.createCluster(w, r)
	default:
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
	}
}

// clusterHandler handles /api/v1/clusters/{id}
func (g *HTTPGateway) clusterHandler(w http.ResponseWriter, r *http.Request) {
	// Extract cluster ID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/clusters/")
	clusterID := strings.Split(path, "/")[0]
	
	if clusterID == "" {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_CLUSTER_ID", "Cluster ID is required", nil)
		return
	}

	switch r.Method {
	case http.MethodGet:
		g.getCluster(w, r, clusterID)
	case http.MethodPut:
		g.updateCluster(w, r, clusterID)
	case http.MethodDelete:
		g.deleteCluster(w, r, clusterID)
	default:
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
	}
}

// listClusters handles GET /api/v1/clusters
func (g *HTTPGateway) listClusters(w http.ResponseWriter, r *http.Request) {
	zoneID := r.URL.Query().Get("zone_id")
	
	clusters, err := g.services.GetClusters(r.Context(), zoneID)
	if err != nil {
		g.logger.Error("Failed to get clusters", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get clusters", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"clusters": clusters,
		"count":    len(clusters),
	})
}

// createCluster handles POST /api/v1/clusters
func (g *HTTPGateway) createCluster(w http.ResponseWriter, r *http.Request) {
	var cluster models.Cluster
	if err := g.parseJSONBody(r, &cluster); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validateCluster(&cluster); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.services.CreateCluster(r.Context(), &cluster); err != nil {
		g.logger.Error("Failed to create cluster", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create cluster", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusCreated, cluster)
}

// getCluster handles GET /api/v1/clusters/{id}
func (g *HTTPGateway) getCluster(w http.ResponseWriter, r *http.Request, clusterID string) {
	cluster, err := g.services.GetCluster(r.Context(), clusterID)
	if err != nil {
		g.logger.Error("Failed to get cluster", zap.String("cluster_id", clusterID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusNotFound, "CLUSTER_NOT_FOUND", "Cluster not found", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, cluster)
}

// updateCluster handles PUT /api/v1/clusters/{id}
func (g *HTTPGateway) updateCluster(w http.ResponseWriter, r *http.Request, clusterID string) {
	var cluster models.Cluster
	if err := g.parseJSONBody(r, &cluster); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validateCluster(&cluster); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.services.UpdateCluster(r.Context(), clusterID, &cluster); err != nil {
		g.logger.Error("Failed to update cluster", zap.String("cluster_id", clusterID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update cluster", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, cluster)
}

// deleteCluster handles DELETE /api/v1/clusters/{id}
func (g *HTTPGateway) deleteCluster(w http.ResponseWriter, r *http.Request, clusterID string) {
	if err := g.services.DeleteCluster(r.Context(), clusterID); err != nil {
		g.logger.Error("Failed to delete cluster", zap.String("cluster_id", clusterID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete cluster", nil)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// pinContentHandler handles POST /api/v1/content/pin
func (g *HTTPGateway) pinContentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	var request PinRequest
	if err := g.parseJSONBody(r, &request); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validatePinRequest(&request); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	plan, err := g.services.PinContent(r.Context(), &request)
	if err != nil {
		g.logger.Error("Failed to pin content", zap.String("cid", request.CID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to pin content", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusAccepted, plan)
}

// unpinContentHandler handles DELETE /api/v1/content/unpin/{cid}
func (g *HTTPGateway) unpinContentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	// Extract CID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/content/unpin/")
	cid := strings.Split(path, "/")[0]
	
	if cid == "" {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_CID", "CID is required", nil)
		return
	}

	if err := g.services.UnpinContent(r.Context(), cid); err != nil {
		g.logger.Error("Failed to unpin content", zap.String("cid", cid), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to unpin content", nil)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// contentStatusHandler handles GET /api/v1/content/status/{cid}
func (g *HTTPGateway) contentStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	// Extract CID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/content/status/")
	cid := strings.Split(path, "/")[0]
	
	if cid == "" {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_CID", "CID is required", nil)
		return
	}

	status, err := g.services.GetContentStatus(r.Context(), cid)
	if err != nil {
		g.logger.Error("Failed to get content status", zap.String("cid", cid), zap.Error(err))
		g.writeErrorResponse(w, http.StatusNotFound, "CONTENT_NOT_FOUND", "Content not found", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, status)
}

// listContentHandler handles GET /api/v1/content
func (g *HTTPGateway) listContentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	filter := &ContentFilter{
		Status: r.URL.Query().Get("status"),
		ZoneID: r.URL.Query().Get("zone_id"),
	}

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			filter.Limit = limit
		}
	}

	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil {
			filter.Offset = offset
		}
	}

	content, err := g.services.ListContent(r.Context(), filter)
	if err != nil {
		g.logger.Error("Failed to list content", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to list content", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"content": content,
		"count":   len(content),
	})
}

// policiesHandler handles /api/v1/policies
func (g *HTTPGateway) policiesHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		g.listPolicies(w, r)
	case http.MethodPost:
		g.createPolicy(w, r)
	default:
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
	}
}

// policyHandler handles /api/v1/policies/{id}
func (g *HTTPGateway) policyHandler(w http.ResponseWriter, r *http.Request) {
	// Extract policy ID from path
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/policies/")
	policyID := strings.Split(path, "/")[0]
	
	if policyID == "" {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_POLICY_ID", "Policy ID is required", nil)
		return
	}

	switch r.Method {
	case http.MethodGet:
		g.getPolicy(w, r, policyID)
	case http.MethodPut:
		g.updatePolicy(w, r, policyID)
	case http.MethodDelete:
		g.deletePolicy(w, r, policyID)
	default:
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
	}
}

// listPolicies handles GET /api/v1/policies
func (g *HTTPGateway) listPolicies(w http.ResponseWriter, r *http.Request) {
	policies, err := g.services.GetPolicies(r.Context())
	if err != nil {
		g.logger.Error("Failed to get policies", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get policies", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, map[string]interface{}{
		"policies": policies,
		"count":    len(policies),
	})
}

// createPolicy handles POST /api/v1/policies
func (g *HTTPGateway) createPolicy(w http.ResponseWriter, r *http.Request) {
	var policy models.Policy
	if err := g.parseJSONBody(r, &policy); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validatePolicy(&policy); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.services.CreatePolicy(r.Context(), &policy); err != nil {
		g.logger.Error("Failed to create policy", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create policy", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusCreated, policy)
}

// getPolicy handles GET /api/v1/policies/{id}
func (g *HTTPGateway) getPolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	policy, err := g.services.GetPolicy(r.Context(), policyID)
	if err != nil {
		g.logger.Error("Failed to get policy", zap.String("policy_id", policyID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusNotFound, "POLICY_NOT_FOUND", "Policy not found", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, policy)
}

// updatePolicy handles PUT /api/v1/policies/{id}
func (g *HTTPGateway) updatePolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	var policy models.Policy
	if err := g.parseJSONBody(r, &policy); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON body", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.validatePolicy(&policy); err != nil {
		g.writeErrorResponse(w, http.StatusBadRequest, "VALIDATION_ERROR", "Validation failed", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	if err := g.services.UpdatePolicy(r.Context(), policyID, &policy); err != nil {
		g.logger.Error("Failed to update policy", zap.String("policy_id", policyID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to update policy", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, policy)
}

// deletePolicy handles DELETE /api/v1/policies/{id}
func (g *HTTPGateway) deletePolicy(w http.ResponseWriter, r *http.Request, policyID string) {
	if err := g.services.DeletePolicy(r.Context(), policyID); err != nil {
		g.logger.Error("Failed to delete policy", zap.String("policy_id", policyID), zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to delete policy", nil)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// topologyHandler handles GET /api/v1/topology
func (g *HTTPGateway) topologyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	topology, err := g.services.GetTopology(r.Context())
	if err != nil {
		g.logger.Error("Failed to get topology", zap.Error(err))
		g.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to get topology", nil)
		return
	}

	g.writeJSONResponse(w, http.StatusOK, topology)
}

// Utility methods

// parseJSONBody parses JSON request body
func (g *HTTPGateway) parseJSONBody(r *http.Request, v interface{}) error {
	if r.Header.Get("Content-Type") != "application/json" {
		return fmt.Errorf("content-type must be application/json")
	}

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}

// writeJSONResponse writes a JSON response
func (g *HTTPGateway) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	if err := json.NewEncoder(w).Encode(data); err != nil {
		g.logger.Error("Failed to encode JSON response", zap.Error(err))
	}
}

// writeErrorResponse writes an error response
func (g *HTTPGateway) writeErrorResponse(w http.ResponseWriter, statusCode int, code, message string, details map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	errorResp := ErrorResponse{
		Code:    code,
		Message: message,
		Details: details,
	}
	
	if err := json.NewEncoder(w).Encode(errorResp); err != nil {
		g.logger.Error("Failed to encode error response", zap.Error(err))
	}
}

// Validation methods

// validateZone validates a zone object
func (g *HTTPGateway) validateZone(zone *models.Zone) error {
	if zone.Name == "" {
		return fmt.Errorf("name is required")
	}
	if zone.Region == "" {
		return fmt.Errorf("region is required")
	}
	if zone.Status != "" {
		switch zone.Status {
		case models.ZoneStatusActive, models.ZoneStatusDegraded, models.ZoneStatusMaintenance, models.ZoneStatusOffline:
			// Valid status
		default:
			return fmt.Errorf("invalid status: %s", zone.Status)
		}
	}
	return nil
}

// validateCluster validates a cluster object
func (g *HTTPGateway) validateCluster(cluster *models.Cluster) error {
	if cluster.Name == "" {
		return fmt.Errorf("name is required")
	}
	if cluster.ZoneID == "" {
		return fmt.Errorf("zone_id is required")
	}
	if cluster.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if cluster.Status != "" {
		switch cluster.Status {
		case models.ClusterStatusHealthy, models.ClusterStatusDegraded, models.ClusterStatusOffline:
			// Valid status
		default:
			return fmt.Errorf("invalid status: %s", cluster.Status)
		}
	}
	return nil
}

// validatePolicy validates a policy object
func (g *HTTPGateway) validatePolicy(policy *models.Policy) error {
	if policy.Name == "" {
		return fmt.Errorf("name is required")
	}
	if policy.Rules == nil || len(policy.Rules) == 0 {
		return fmt.Errorf("rules are required")
	}
	return nil
}

// validatePinRequest validates a pin request
func (g *HTTPGateway) validatePinRequest(request *PinRequest) error {
	if request.CID == "" {
		return fmt.Errorf("cid is required")
	}
	if request.Size < 0 {
		return fmt.Errorf("size cannot be negative")
	}
	if request.Priority < 0 {
		return fmt.Errorf("priority cannot be negative")
	}
	return nil
}

// openAPISpecHandler serves the OpenAPI specification
func (g *HTTPGateway) openAPISpecHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		g.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Method not allowed", nil)
		return
	}

	spec := g.generateOpenAPISpec()
	g.writeJSONResponse(w, http.StatusOK, spec)
}

// generateOpenAPISpec generates the OpenAPI specification
func (g *HTTPGateway) generateOpenAPISpec() map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.3",
		"info": map[string]interface{}{
			"title":       "Global Data Controller API",
			"description": "API for managing distributed IPFS clusters and content replication",
			"version":     "1.0.0",
			"contact": map[string]interface{}{
				"name": "Global Data Controller Team",
			},
		},
		"servers": []map[string]interface{}{
			{
				"url":         "/api/v1",
				"description": "API v1",
			},
		},
		"paths": map[string]interface{}{
			"/health": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "Health check",
					"description": "Check if the service is healthy",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Service is healthy",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"type": "object",
										"properties": map[string]interface{}{
											"status": map[string]interface{}{
												"type": "string",
												"example": "healthy",
											},
											"service": map[string]interface{}{
												"type": "string",
												"example": "global-data-controller",
											},
											"timestamp": map[string]interface{}{
												"type": "string",
												"format": "date-time",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			"/ready": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "Readiness check",
					"description": "Check if the service is ready to serve requests",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Service is ready",
						},
						"503": map[string]interface{}{
							"description": "Service is not ready",
						},
					},
				},
			},
			"/zones": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "List zones",
					"description": "Get a list of all zones",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "List of zones",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"type": "object",
										"properties": map[string]interface{}{
											"zones": map[string]interface{}{
												"type": "array",
												"items": map[string]interface{}{
													"$ref": "#/components/schemas/Zone",
												},
											},
											"count": map[string]interface{}{
												"type": "integer",
											},
										},
									},
								},
							},
						},
					},
				},
				"post": map[string]interface{}{
					"summary":     "Create zone",
					"description": "Create a new zone",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"schema": map[string]interface{}{
									"$ref": "#/components/schemas/Zone",
								},
							},
						},
					},
					"responses": map[string]interface{}{
						"201": map[string]interface{}{
							"description": "Zone created successfully",
						},
						"400": map[string]interface{}{
							"description": "Invalid request",
						},
					},
				},
			},
			"/zones/{id}": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "Get zone",
					"description": "Get a specific zone by ID",
					"parameters": []map[string]interface{}{
						{
							"name":        "id",
							"in":          "path",
							"required":    true,
							"description": "Zone ID",
							"schema": map[string]interface{}{
								"type": "string",
							},
						},
					},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Zone details",
						},
						"404": map[string]interface{}{
							"description": "Zone not found",
						},
					},
				},
				"put": map[string]interface{}{
					"summary":     "Update zone",
					"description": "Update a specific zone",
					"parameters": []map[string]interface{}{
						{
							"name":        "id",
							"in":          "path",
							"required":    true,
							"description": "Zone ID",
							"schema": map[string]interface{}{
								"type": "string",
							},
						},
					},
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"schema": map[string]interface{}{
									"$ref": "#/components/schemas/Zone",
								},
							},
						},
					},
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Zone updated successfully",
						},
						"404": map[string]interface{}{
							"description": "Zone not found",
						},
					},
				},
				"delete": map[string]interface{}{
					"summary":     "Delete zone",
					"description": "Delete a specific zone",
					"parameters": []map[string]interface{}{
						{
							"name":        "id",
							"in":          "path",
							"required":    true,
							"description": "Zone ID",
							"schema": map[string]interface{}{
								"type": "string",
							},
						},
					},
					"responses": map[string]interface{}{
						"204": map[string]interface{}{
							"description": "Zone deleted successfully",
						},
						"404": map[string]interface{}{
							"description": "Zone not found",
						},
					},
				},
			},
			"/content/pin": map[string]interface{}{
				"post": map[string]interface{}{
					"summary":     "Pin content",
					"description": "Pin content to the distributed storage",
					"requestBody": map[string]interface{}{
						"required": true,
						"content": map[string]interface{}{
							"application/json": map[string]interface{}{
								"schema": map[string]interface{}{
									"$ref": "#/components/schemas/PinRequest",
								},
							},
						},
					},
					"responses": map[string]interface{}{
						"202": map[string]interface{}{
							"description": "Pin request accepted",
						},
						"400": map[string]interface{}{
							"description": "Invalid request",
						},
					},
				},
			},
		},
		"components": map[string]interface{}{
			"schemas": map[string]interface{}{
				"Zone": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"id": map[string]interface{}{
							"type": "string",
						},
						"name": map[string]interface{}{
							"type": "string",
						},
						"region": map[string]interface{}{
							"type": "string",
						},
						"status": map[string]interface{}{
							"type": "string",
							"enum": []string{"active", "degraded", "maintenance", "offline"},
						},
						"capabilities": map[string]interface{}{
							"type": "object",
							"additionalProperties": map[string]interface{}{
								"type": "string",
							},
						},
						"coordinates": map[string]interface{}{
							"$ref": "#/components/schemas/GeoCoordinates",
						},
						"created_at": map[string]interface{}{
							"type":   "string",
							"format": "date-time",
						},
						"updated_at": map[string]interface{}{
							"type":   "string",
							"format": "date-time",
						},
					},
					"required": []string{"name", "region"},
				},
				"GeoCoordinates": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"latitude": map[string]interface{}{
							"type": "number",
						},
						"longitude": map[string]interface{}{
							"type": "number",
						},
					},
				},
				"PinRequest": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"cid": map[string]interface{}{
							"type": "string",
						},
						"size": map[string]interface{}{
							"type": "integer",
						},
						"policies": map[string]interface{}{
							"type": "array",
							"items": map[string]interface{}{
								"type": "string",
							},
						},
						"constraints": map[string]interface{}{
							"type": "object",
							"additionalProperties": map[string]interface{}{
								"type": "string",
							},
						},
						"priority": map[string]interface{}{
							"type": "integer",
						},
					},
					"required": []string{"cid"},
				},
				"ErrorResponse": map[string]interface{}{
					"type": "object",
					"properties": map[string]interface{}{
						"code": map[string]interface{}{
							"type": "string",
						},
						"message": map[string]interface{}{
							"type": "string",
						},
						"details": map[string]interface{}{
							"type": "object",
						},
					},
				},
			},
		},
	}
}