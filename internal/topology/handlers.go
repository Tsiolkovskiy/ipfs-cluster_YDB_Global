package topology

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/global-data-controller/gdc/internal/models"
)

// Handler provides HTTP handlers for topology management
type Handler struct {
	service Service
}

// NewHandler creates a new topology handler
func NewHandler(service Service) *Handler {
	return &Handler{
		service: service,
	}
}

// RegisterRoutes registers topology routes with the router
func (h *Handler) RegisterRoutes(router *mux.Router) {
	// Zone routes
	router.HandleFunc("/zones", h.ListZones).Methods("GET")
	router.HandleFunc("/zones", h.RegisterZone).Methods("POST")
	router.HandleFunc("/zones/{zoneId}", h.GetZone).Methods("GET")
	router.HandleFunc("/zones/{zoneId}", h.DeleteZone).Methods("DELETE")
	router.HandleFunc("/zones/{zoneId}/status", h.UpdateZoneStatus).Methods("PUT")
	router.HandleFunc("/zones/{zoneId}/health", h.GetZoneHealth).Methods("GET")

	// Cluster routes
	router.HandleFunc("/clusters", h.ListClusters).Methods("GET")
	router.HandleFunc("/clusters", h.RegisterCluster).Methods("POST")
	router.HandleFunc("/clusters/{clusterId}", h.GetCluster).Methods("GET")
	router.HandleFunc("/clusters/{clusterId}", h.DeleteCluster).Methods("DELETE")
	router.HandleFunc("/clusters/{clusterId}/status", h.UpdateClusterStatus).Methods("PUT")
	router.HandleFunc("/clusters/{clusterId}/health", h.GetClusterHealth).Methods("GET")

	// Node routes
	router.HandleFunc("/nodes", h.AddNode).Methods("POST")
	router.HandleFunc("/nodes/{nodeId}", h.RemoveNode).Methods("DELETE")
	router.HandleFunc("/nodes/{nodeId}/metrics", h.UpdateNodeMetrics).Methods("PUT")

	// Topology routes
	router.HandleFunc("/topology", h.GetTopology).Methods("GET")
}

// ListZones handles GET /zones
func (h *Handler) ListZones(w http.ResponseWriter, r *http.Request) {
	zones, err := h.service.ListZones(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list zones: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"zones": zones,
		"count": len(zones),
	})
}

// RegisterZone handles POST /zones
func (h *Handler) RegisterZone(w http.ResponseWriter, r *http.Request) {
	var zone models.Zone
	if err := json.NewDecoder(r.Body).Decode(&zone); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.service.RegisterZone(r.Context(), &zone); err != nil {
		http.Error(w, fmt.Sprintf("Failed to register zone: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(zone)
}

// GetZone handles GET /zones/{zoneId}
func (h *Handler) GetZone(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	zoneID := vars["zoneId"]

	zone, err := h.service.GetZone(r.Context(), zoneID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get zone: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(zone)
}

// DeleteZone handles DELETE /zones/{zoneId}
func (h *Handler) DeleteZone(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	zoneID := vars["zoneId"]

	if err := h.service.DeleteZone(r.Context(), zoneID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete zone: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateZoneStatus handles PUT /zones/{zoneId}/status
func (h *Handler) UpdateZoneStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	zoneID := vars["zoneId"]

	var request struct {
		Status models.ZoneStatus `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.service.UpdateZoneStatus(r.Context(), zoneID, request.Status); err != nil {
		http.Error(w, fmt.Sprintf("Failed to update zone status: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetZoneHealth handles GET /zones/{zoneId}/health
func (h *Handler) GetZoneHealth(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	zoneID := vars["zoneId"]

	health, err := h.service.GetZoneHealth(r.Context(), zoneID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get zone health: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// ListClusters handles GET /clusters
func (h *Handler) ListClusters(w http.ResponseWriter, r *http.Request) {
	zoneID := r.URL.Query().Get("zone_id")

	clusters, err := h.service.ListClusters(r.Context(), zoneID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list clusters: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"clusters": clusters,
		"count":    len(clusters),
		"zone_id":  zoneID,
	})
}

// RegisterCluster handles POST /clusters
func (h *Handler) RegisterCluster(w http.ResponseWriter, r *http.Request) {
	var cluster models.Cluster
	if err := json.NewDecoder(r.Body).Decode(&cluster); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.service.RegisterCluster(r.Context(), &cluster); err != nil {
		http.Error(w, fmt.Sprintf("Failed to register cluster: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(cluster)
}

// GetCluster handles GET /clusters/{clusterId}
func (h *Handler) GetCluster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clusterID := vars["clusterId"]

	cluster, err := h.service.GetCluster(r.Context(), clusterID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get cluster: %v", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cluster)
}

// DeleteCluster handles DELETE /clusters/{clusterId}
func (h *Handler) DeleteCluster(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clusterID := vars["clusterId"]

	if err := h.service.DeleteCluster(r.Context(), clusterID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete cluster: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateClusterStatus handles PUT /clusters/{clusterId}/status
func (h *Handler) UpdateClusterStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clusterID := vars["clusterId"]

	var request struct {
		Status models.ClusterStatus `json:"status"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.service.UpdateClusterStatus(r.Context(), clusterID, request.Status); err != nil {
		http.Error(w, fmt.Sprintf("Failed to update cluster status: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetClusterHealth handles GET /clusters/{clusterId}/health
func (h *Handler) GetClusterHealth(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	clusterID := vars["clusterId"]

	health, err := h.service.GetClusterHealth(r.Context(), clusterID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get cluster health: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

// AddNode handles POST /nodes
func (h *Handler) AddNode(w http.ResponseWriter, r *http.Request) {
	var node models.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.service.AddNode(r.Context(), &node); err != nil {
		http.Error(w, fmt.Sprintf("Failed to add node: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(node)
}

// RemoveNode handles DELETE /nodes/{nodeId}
func (h *Handler) RemoveNode(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	if err := h.service.RemoveNode(r.Context(), nodeID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to remove node: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// UpdateNodeMetrics handles PUT /nodes/{nodeId}/metrics
func (h *Handler) UpdateNodeMetrics(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	nodeID := vars["nodeId"]

	var metrics models.NodeMetrics
	if err := json.NewDecoder(r.Body).Decode(&metrics); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if err := h.service.UpdateNodeMetrics(r.Context(), nodeID, &metrics); err != nil {
		http.Error(w, fmt.Sprintf("Failed to update node metrics: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// GetTopology handles GET /topology
func (h *Handler) GetTopology(w http.ResponseWriter, r *http.Request) {
	topology, err := h.service.GetTopology(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get topology: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topology)
}