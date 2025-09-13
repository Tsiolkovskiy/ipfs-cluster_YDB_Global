package cluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var tracer = otel.Tracer("cluster-adapter")

// ControlAdapter handles communication with IPFS clusters
type ControlAdapter interface {
	// Pin operations
	SubmitPinPlan(ctx context.Context, clusterID string, plan *models.PinPlan) error
	GetPinStatus(ctx context.Context, clusterID string, cid string) (*models.PinStatus, error)
	UnpinContent(ctx context.Context, clusterID string, cid string) error
	
	// Cluster monitoring
	GetClusterStatus(ctx context.Context, clusterID string) (*models.ClusterStatus, error)
	GetNodeMetrics(ctx context.Context, clusterID string) ([]*models.NodeMetrics, error)
	
	// Node management
	AddNode(ctx context.Context, clusterID string, nodeConfig *models.NodeConfig) error
	RemoveNode(ctx context.Context, clusterID string, nodeID string) error
	
	// Health check
	HealthCheck(ctx context.Context, clusterID string) error
	
	// Close connections
	Close() error
}

// RetryConfig defines retry behavior for operations
type RetryConfig struct {
	MaxAttempts   int           `json:"max_attempts"`
	InitialDelay  time.Duration `json:"initial_delay"`
	MaxDelay      time.Duration `json:"max_delay"`
	BackoffFactor float64       `json:"backoff_factor"`
	Jitter        bool          `json:"jitter"`
}

// DefaultRetryConfig returns a sensible default retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        true,
	}
}

// NextDelay calculates the next delay for exponential backoff with jitter
func (r *RetryConfig) NextDelay(attempt int) time.Duration {
	delay := time.Duration(float64(r.InitialDelay) * math.Pow(r.BackoffFactor, float64(attempt)))
	if delay > r.MaxDelay {
		delay = r.MaxDelay
	}
	if r.Jitter {
		jitter := time.Duration(rand.Float64() * float64(delay) * 0.1)
		delay = delay + jitter
	}
	return delay
}

// ClusterConnection represents a connection to an IPFS cluster
type ClusterConnection struct {
	ID       string
	Endpoint string
	Conn     *grpc.ClientConn
	Client   IPFSClusterServiceClient
	LastUsed time.Time
}

// IPFSClusterAdapter implements the ControlAdapter for IPFS Cluster
type IPFSClusterAdapter struct {
	connections map[string]*ClusterConnection
	mutex       sync.RWMutex
	timeout     time.Duration
	retryConfig *RetryConfig
	logger      *zap.Logger
	tlsConfig   *tls.Config
}

// AdapterConfig holds configuration for the cluster adapter
type AdapterConfig struct {
	Timeout     time.Duration
	RetryConfig *RetryConfig
	Logger      *zap.Logger
	TLSConfig   *tls.Config
}

// NewIPFSClusterAdapter creates a new IPFS cluster adapter
func NewIPFSClusterAdapter(config *AdapterConfig) *IPFSClusterAdapter {
	if config.RetryConfig == nil {
		config.RetryConfig = DefaultRetryConfig()
	}
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	return &IPFSClusterAdapter{
		connections: make(map[string]*ClusterConnection),
		timeout:     config.Timeout,
		retryConfig: config.RetryConfig,
		logger:      config.Logger,
		tlsConfig:   config.TLSConfig,
	}
}

// getConnection gets or creates a connection to the specified cluster
func (a *IPFSClusterAdapter) getConnection(ctx context.Context, clusterID string) (*ClusterConnection, error) {
	a.mutex.RLock()
	conn, exists := a.connections[clusterID]
	a.mutex.RUnlock()

	if exists && (conn.Conn == nil || conn.Conn.GetState() == connectivity.Ready) {
		conn.LastUsed = time.Now()
		return conn, nil
	}

	// Need to create or recreate connection
	a.mutex.Lock()
	defer a.mutex.Unlock()

	// Double-check after acquiring write lock
	if conn, exists := a.connections[clusterID]; exists && (conn.Conn == nil || conn.Conn.GetState() == connectivity.Ready) {
		conn.LastUsed = time.Now()
		return conn, nil
	}

	// Get cluster endpoint from topology or configuration
	endpoint, err := a.getClusterEndpoint(ctx, clusterID)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster endpoint: %w", err)
	}

	// Create gRPC connection with mTLS
	var opts []grpc.DialOption
	if a.tlsConfig != nil {
		creds := credentials.NewTLS(a.tlsConfig)
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	opts = append(opts,
		grpc.WithTimeout(a.timeout),
		grpc.WithBlock(),
	)

	grpcConn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cluster %s at %s: %w", clusterID, endpoint, err)
	}

	client := NewIPFSClusterServiceClient(grpcConn)
	
	connection := &ClusterConnection{
		ID:       clusterID,
		Endpoint: endpoint,
		Conn:     grpcConn,
		Client:   client,
		LastUsed: time.Now(),
	}

	a.connections[clusterID] = connection
	
	a.logger.Info("Created new cluster connection",
		zap.String("cluster_id", clusterID),
		zap.String("endpoint", endpoint))

	return connection, nil
}

// getClusterEndpoint retrieves the endpoint for a cluster
// This is a placeholder - in real implementation, this would query the topology service
func (a *IPFSClusterAdapter) getClusterEndpoint(ctx context.Context, clusterID string) (string, error) {
	// TODO: Integrate with topology service to get actual cluster endpoints
	// For now, return a mock endpoint based on cluster ID
	return fmt.Sprintf("cluster-%s.local:9094", clusterID), nil
}

// withRetry executes a function with retry logic
func (a *IPFSClusterAdapter) withRetry(ctx context.Context, operation string, fn func() error) error {
	ctx, span := tracer.Start(ctx, "cluster_adapter_retry",
		trace.WithAttributes(attribute.String("operation", operation)))
	defer span.End()

	var lastErr error
	for attempt := 0; attempt < a.retryConfig.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := a.retryConfig.NextDelay(attempt - 1)
			a.logger.Debug("Retrying operation",
				zap.String("operation", operation),
				zap.Int("attempt", attempt+1),
				zap.Duration("delay", delay))
			
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := fn()
		if err == nil {
			if attempt > 0 {
				a.logger.Info("Operation succeeded after retry",
					zap.String("operation", operation),
					zap.Int("attempts", attempt+1))
			}
			return nil
		}

		lastErr = err
		
		// Check if error is retryable
		if !a.isRetryableError(err) {
			a.logger.Debug("Non-retryable error, stopping retry",
				zap.String("operation", operation),
				zap.Error(err))
			break
		}

		a.logger.Debug("Retryable error occurred",
			zap.String("operation", operation),
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}

	span.RecordError(lastErr)
	return fmt.Errorf("operation %s failed after %d attempts: %w", operation, a.retryConfig.MaxAttempts, lastErr)
}

// isRetryableError determines if an error should trigger a retry
func (a *IPFSClusterAdapter) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check gRPC status codes
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied:
			return false
		default:
			return true
		}
	}

	// For non-gRPC errors, retry by default
	return true
}

// SubmitPinPlan submits a pin plan to the specified cluster with idempotent retry logic
func (a *IPFSClusterAdapter) SubmitPinPlan(ctx context.Context, clusterID string, plan *models.PinPlan) error {
	ctx, span := tracer.Start(ctx, "submit_pin_plan",
		trace.WithAttributes(
			attribute.String("cluster_id", clusterID),
			attribute.String("cid", plan.CID),
			attribute.String("plan_id", plan.ID)))
	defer span.End()

	return a.withRetry(ctx, "SubmitPinPlan", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		// Convert plan to gRPC request
		req := &PinRequest{
			Cid:         plan.CID,
			PlanId:      plan.ID,
			Assignments: make([]*NodeAssignment, len(plan.Assignments)),
		}

		for i, assignment := range plan.Assignments {
			req.Assignments[i] = &NodeAssignment{
				NodeId:    assignment.NodeID,
				ClusterId: assignment.ClusterID,
				ZoneId:    assignment.ZoneID,
				Priority:  int32(assignment.Priority),
			}
		}

		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.SubmitPin(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to submit pin plan: %w", err)
		}

		if !resp.Success {
			return fmt.Errorf("pin plan rejected: %s", resp.Message)
		}

		a.logger.Info("Pin plan submitted successfully",
			zap.String("cluster_id", clusterID),
			zap.String("cid", plan.CID),
			zap.String("plan_id", plan.ID))

		return nil
	})
}

// GetPinStatus retrieves the status of a pin operation
func (a *IPFSClusterAdapter) GetPinStatus(ctx context.Context, clusterID string, cid string) (*models.PinStatus, error) {
	ctx, span := tracer.Start(ctx, "get_pin_status",
		trace.WithAttributes(
			attribute.String("cluster_id", clusterID),
			attribute.String("cid", cid)))
	defer span.End()

	var result *models.PinStatus
	err := a.withRetry(ctx, "GetPinStatus", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &PinStatusRequest{Cid: cid}
		
		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.GetPinStatus(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to get pin status: %w", err)
		}

		result = &models.PinStatus{
			CID:       resp.Cid,
			Status:    models.ContentStatus(resp.Status),
			Progress:  float64(resp.Progress),
			Error:     resp.Error,
			UpdatedAt: time.Unix(resp.UpdatedAt, 0),
		}

		return nil
	})

	return result, err
}

// UnpinContent removes content from the cluster
func (a *IPFSClusterAdapter) UnpinContent(ctx context.Context, clusterID string, cid string) error {
	ctx, span := tracer.Start(ctx, "unpin_content",
		trace.WithAttributes(
			attribute.String("cluster_id", clusterID),
			attribute.String("cid", cid)))
	defer span.End()

	return a.withRetry(ctx, "UnpinContent", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &UnpinRequest{Cid: cid}
		
		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.Unpin(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to unpin content: %w", err)
		}

		if !resp.Success {
			return fmt.Errorf("unpin operation failed: %s", resp.Message)
		}

		a.logger.Info("Content unpinned successfully",
			zap.String("cluster_id", clusterID),
			zap.String("cid", cid))

		return nil
	})
}

// GetClusterStatus retrieves the overall status of a cluster
func (a *IPFSClusterAdapter) GetClusterStatus(ctx context.Context, clusterID string) (*models.ClusterStatus, error) {
	ctx, span := tracer.Start(ctx, "get_cluster_status",
		trace.WithAttributes(attribute.String("cluster_id", clusterID)))
	defer span.End()

	var result *models.ClusterStatus
	err := a.withRetry(ctx, "GetClusterStatus", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &ClusterStatusRequest{ClusterId: clusterID}
		
		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.GetClusterStatus(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to get cluster status: %w", err)
		}

		status := models.ClusterStatusHealthy
		switch resp.Status {
		case "healthy":
			status = models.ClusterStatusHealthy
		case "degraded":
			status = models.ClusterStatusDegraded
		case "offline":
			status = models.ClusterStatusOffline
		}

		result = &status
		return nil
	})

	return result, err
}

// GetNodeMetrics retrieves metrics from all nodes in a cluster
func (a *IPFSClusterAdapter) GetNodeMetrics(ctx context.Context, clusterID string) ([]*models.NodeMetrics, error) {
	ctx, span := tracer.Start(ctx, "get_node_metrics",
		trace.WithAttributes(attribute.String("cluster_id", clusterID)))
	defer span.End()

	var result []*models.NodeMetrics
	err := a.withRetry(ctx, "GetNodeMetrics", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &NodeMetricsRequest{ClusterId: clusterID}
		
		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.GetNodeMetrics(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to get node metrics: %w", err)
		}

		result = make([]*models.NodeMetrics, len(resp.Metrics))
		for i, metric := range resp.Metrics {
			result[i] = &models.NodeMetrics{
				CPUUsage:       float64(metric.CpuUsage),
				MemoryUsage:    float64(metric.MemoryUsage),
				StorageUsage:   float64(metric.StorageUsage),
				NetworkInMbps:  float64(metric.NetworkInMbps),
				NetworkOutMbps: float64(metric.NetworkOutMbps),
				PinnedObjects:  metric.PinnedObjects,
				LastUpdated:    time.Unix(metric.LastUpdated, 0),
			}
		}

		return nil
	})

	return result, err
}

// AddNode adds a new node to the cluster
func (a *IPFSClusterAdapter) AddNode(ctx context.Context, clusterID string, nodeConfig *models.NodeConfig) error {
	ctx, span := tracer.Start(ctx, "add_node",
		trace.WithAttributes(
			attribute.String("cluster_id", clusterID),
			attribute.String("node_address", nodeConfig.Address)))
	defer span.End()

	return a.withRetry(ctx, "AddNode", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &AddNodeRequest{
			ClusterId: clusterID,
			Address:   nodeConfig.Address,
			Resources: &NodeResources{
				CpuCores:     int32(nodeConfig.Resources.CPUCores),
				MemoryBytes:  nodeConfig.Resources.MemoryBytes,
				StorageBytes: nodeConfig.Resources.StorageBytes,
				NetworkMbps:  int32(nodeConfig.Resources.NetworkMbps),
			},
		}

		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.AddNode(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to add node: %w", err)
		}

		if !resp.Success {
			return fmt.Errorf("add node operation failed: %s", resp.Message)
		}

		a.logger.Info("Node added successfully",
			zap.String("cluster_id", clusterID),
			zap.String("node_address", nodeConfig.Address),
			zap.String("node_id", resp.NodeId))

		return nil
	})
}

// RemoveNode removes a node from the cluster
func (a *IPFSClusterAdapter) RemoveNode(ctx context.Context, clusterID string, nodeID string) error {
	ctx, span := tracer.Start(ctx, "remove_node",
		trace.WithAttributes(
			attribute.String("cluster_id", clusterID),
			attribute.String("node_id", nodeID)))
	defer span.End()

	return a.withRetry(ctx, "RemoveNode", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &RemoveNodeRequest{
			ClusterId: clusterID,
			NodeId:    nodeID,
		}

		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.RemoveNode(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to remove node: %w", err)
		}

		if !resp.Success {
			return fmt.Errorf("remove node operation failed: %s", resp.Message)
		}

		a.logger.Info("Node removed successfully",
			zap.String("cluster_id", clusterID),
			zap.String("node_id", nodeID))

		return nil
	})
}

// HealthCheck performs a health check on the cluster
func (a *IPFSClusterAdapter) HealthCheck(ctx context.Context, clusterID string) error {
	ctx, span := tracer.Start(ctx, "health_check",
		trace.WithAttributes(attribute.String("cluster_id", clusterID)))
	defer span.End()

	return a.withRetry(ctx, "HealthCheck", func() error {
		conn, err := a.getConnection(ctx, clusterID)
		if err != nil {
			return err
		}

		req := &HealthCheckRequest{ClusterId: clusterID}
		
		ctx, cancel := context.WithTimeout(ctx, a.timeout)
		defer cancel()

		resp, err := conn.Client.HealthCheck(ctx, req)
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}

		if !resp.Healthy {
			return fmt.Errorf("cluster is unhealthy: %s", resp.Message)
		}

		return nil
	})
}

// Close closes all connections
func (a *IPFSClusterAdapter) Close() error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	var errors []error
	for clusterID, conn := range a.connections {
		if conn.Conn != nil {
			if err := conn.Conn.Close(); err != nil {
				errors = append(errors, fmt.Errorf("failed to close connection to cluster %s: %w", clusterID, err))
			}
		}
	}

	a.connections = make(map[string]*ClusterConnection)

	if len(errors) > 0 {
		return fmt.Errorf("errors closing connections: %v", errors)
	}

	return nil
}