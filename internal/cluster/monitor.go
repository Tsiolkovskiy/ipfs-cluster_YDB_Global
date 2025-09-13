package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// OperationMonitor tracks the status of cluster operations
type OperationMonitor struct {
	adapter         ControlAdapter
	logger          *zap.Logger
	operations      map[string]*OperationStatus
	mutex           sync.RWMutex
	stopCh          chan struct{}
	checkInterval   time.Duration
	
	// Metrics
	operationCounter    metric.Int64Counter
	operationDuration   metric.Float64Histogram
	clusterHealthGauge  metric.Float64ObservableGauge
}

// OperationStatus tracks the status of a single operation
type OperationStatus struct {
	ID          string                `json:"id"`
	Type        OperationType         `json:"type"`
	ClusterID   string                `json:"cluster_id"`
	CID         string                `json:"cid,omitempty"`
	Status      models.ContentStatus  `json:"status"`
	Progress    float64               `json:"progress"`
	Error       string                `json:"error,omitempty"`
	StartedAt   time.Time             `json:"started_at"`
	UpdatedAt   time.Time             `json:"updated_at"`
	CompletedAt *time.Time            `json:"completed_at,omitempty"`
}

// OperationType represents the type of operation being monitored
type OperationType string

const (
	OperationTypePin    OperationType = "pin"
	OperationTypeUnpin  OperationType = "unpin"
	OperationTypeHealth OperationType = "health_check"
)

// MonitorConfig holds configuration for the operation monitor
type MonitorConfig struct {
	Adapter       ControlAdapter
	Logger        *zap.Logger
	CheckInterval time.Duration
	MeterProvider metric.MeterProvider
}

// NewOperationMonitor creates a new operation monitor
func NewOperationMonitor(config *MonitorConfig) (*OperationMonitor, error) {
	if config.CheckInterval == 0 {
		config.CheckInterval = 30 * time.Second
	}
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}

	monitor := &OperationMonitor{
		adapter:       config.Adapter,
		logger:        config.Logger,
		operations:    make(map[string]*OperationStatus),
		stopCh:        make(chan struct{}),
		checkInterval: config.CheckInterval,
	}

	// Initialize metrics if meter provider is available
	if config.MeterProvider != nil {
		meter := config.MeterProvider.Meter("cluster_monitor")
		
		var err error
		monitor.operationCounter, err = meter.Int64Counter(
			"cluster_operations_total",
			metric.WithDescription("Total number of cluster operations"),
		)
		if err != nil {
			return nil, err
		}

		monitor.operationDuration, err = meter.Float64Histogram(
			"cluster_operation_duration_seconds",
			metric.WithDescription("Duration of cluster operations in seconds"),
		)
		if err != nil {
			return nil, err
		}

		monitor.clusterHealthGauge, err = meter.Float64ObservableGauge(
			"cluster_health_status",
			metric.WithDescription("Health status of clusters (1=healthy, 0=unhealthy)"),
		)
		if err != nil {
			return nil, err
		}
	}

	return monitor, nil
}

// Start begins monitoring operations
func (m *OperationMonitor) Start(ctx context.Context) {
	go m.monitorLoop(ctx)
}

// Stop stops the monitoring
func (m *OperationMonitor) Stop() {
	close(m.stopCh)
}

// TrackOperation starts tracking a new operation
func (m *OperationMonitor) TrackOperation(operationType OperationType, clusterID, cid string) string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	operationID := generateOperationID()
	operation := &OperationStatus{
		ID:        operationID,
		Type:      operationType,
		ClusterID: clusterID,
		CID:       cid,
		Status:    models.ContentStatusPending,
		Progress:  0.0,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	m.operations[operationID] = operation

	// Record metric
	if m.operationCounter != nil {
		m.operationCounter.Add(context.Background(), 1,
			metric.WithAttributes(
				attribute.String("operation_type", string(operationType)),
				attribute.String("cluster_id", clusterID),
				attribute.String("status", "started"),
			))
	}

	m.logger.Info("Started tracking operation",
		zap.String("operation_id", operationID),
		zap.String("type", string(operationType)),
		zap.String("cluster_id", clusterID),
		zap.String("cid", cid))

	return operationID
}

// GetOperationStatus returns the status of an operation
func (m *OperationMonitor) GetOperationStatus(operationID string) (*OperationStatus, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	operation, exists := m.operations[operationID]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	operationCopy := *operation
	return &operationCopy, true
}

// ListOperations returns all tracked operations
func (m *OperationMonitor) ListOperations() []*OperationStatus {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	operations := make([]*OperationStatus, 0, len(m.operations))
	for _, operation := range m.operations {
		operationCopy := *operation
		operations = append(operations, &operationCopy)
	}

	return operations
}

// CompleteOperation marks an operation as completed
func (m *OperationMonitor) CompleteOperation(operationID string, status models.ContentStatus, err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	operation, exists := m.operations[operationID]
	if !exists {
		return
	}

	now := time.Now()
	operation.Status = status
	operation.Progress = 1.0
	operation.UpdatedAt = now
	operation.CompletedAt = &now

	if err != nil {
		operation.Error = err.Error()
		operation.Status = models.ContentStatusFailed
	}

	// Record metrics
	if m.operationCounter != nil {
		m.operationCounter.Add(context.Background(), 1,
			metric.WithAttributes(
				attribute.String("operation_type", string(operation.Type)),
				attribute.String("cluster_id", operation.ClusterID),
				attribute.String("status", "completed"),
			))
	}

	if m.operationDuration != nil {
		duration := now.Sub(operation.StartedAt).Seconds()
		m.operationDuration.Record(context.Background(), duration,
			metric.WithAttributes(
				attribute.String("operation_type", string(operation.Type)),
				attribute.String("cluster_id", operation.ClusterID),
				attribute.String("status", string(status)),
			))
	}

	m.logger.Info("Operation completed",
		zap.String("operation_id", operationID),
		zap.String("type", string(operation.Type)),
		zap.String("cluster_id", operation.ClusterID),
		zap.String("status", string(status)),
		zap.Duration("duration", now.Sub(operation.StartedAt)),
		zap.Error(err))
}

// monitorLoop runs the monitoring loop
func (m *OperationMonitor) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkOperations(ctx)
		}
	}
}

// checkOperations checks the status of all tracked operations
func (m *OperationMonitor) checkOperations(ctx context.Context) {
	m.mutex.RLock()
	operations := make([]*OperationStatus, 0, len(m.operations))
	for _, operation := range m.operations {
		if operation.CompletedAt == nil {
			operationCopy := *operation
			operations = append(operations, &operationCopy)
		}
	}
	m.mutex.RUnlock()

	for _, operation := range operations {
		m.checkSingleOperation(ctx, operation)
	}

	// Clean up old completed operations
	m.cleanupOldOperations()
}

// checkSingleOperation checks the status of a single operation
func (m *OperationMonitor) checkSingleOperation(ctx context.Context, operation *OperationStatus) {
	switch operation.Type {
	case OperationTypePin:
		m.checkPinOperation(ctx, operation)
	case OperationTypeHealth:
		m.checkHealthOperation(ctx, operation)
	}
}

// checkPinOperation checks the status of a pin operation
func (m *OperationMonitor) checkPinOperation(ctx context.Context, operation *OperationStatus) {
	status, err := m.adapter.GetPinStatus(ctx, operation.ClusterID, operation.CID)
	if err != nil {
		m.logger.Warn("Failed to check pin status",
			zap.String("operation_id", operation.ID),
			zap.String("cluster_id", operation.ClusterID),
			zap.String("cid", operation.CID),
			zap.Error(err))
		return
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if op, exists := m.operations[operation.ID]; exists && op.CompletedAt == nil {
		op.Status = status.Status
		op.Progress = status.Progress
		op.UpdatedAt = time.Now()

		if status.Error != "" {
			op.Error = status.Error
		}

		// Mark as completed if status indicates completion
		if status.Status == models.ContentStatusPinned || status.Status == models.ContentStatusFailed {
			now := time.Now()
			op.CompletedAt = &now
		}
	}
}

// checkHealthOperation checks the status of a health check operation
func (m *OperationMonitor) checkHealthOperation(ctx context.Context, operation *OperationStatus) {
	err := m.adapter.HealthCheck(ctx, operation.ClusterID)
	
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if op, exists := m.operations[operation.ID]; exists && op.CompletedAt == nil {
		now := time.Now()
		op.UpdatedAt = now
		op.CompletedAt = &now
		op.Progress = 1.0

		if err != nil {
			op.Status = models.ContentStatusFailed
			op.Error = err.Error()
		} else {
			op.Status = models.ContentStatusPinned // Use as "healthy" status
		}
	}
}

// cleanupOldOperations removes old completed operations
func (m *OperationMonitor) cleanupOldOperations() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	cutoff := time.Now().Add(-24 * time.Hour) // Keep operations for 24 hours
	
	for id, operation := range m.operations {
		if operation.CompletedAt != nil && operation.CompletedAt.Before(cutoff) {
			delete(m.operations, id)
		}
	}
}

// generateOperationID generates a unique operation ID
func generateOperationID() string {
	// Simple implementation - in production, use UUID or similar
	// Add some randomness to avoid collisions in tests
	return fmt.Sprintf("op_%d_%d", time.Now().UnixNano(), rand.Int63n(1000000))
}