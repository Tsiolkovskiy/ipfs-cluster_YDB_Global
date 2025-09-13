package cluster

import (
	"context"
	"time"

	"github.com/global-data-controller/gdc/internal/models"
	"go.uber.org/zap"
)

// ExampleUsage demonstrates how to use the Cluster Control Adapter
func ExampleUsage() {
	// Create adapter configuration
	config := &AdapterConfig{
		Timeout: 30 * time.Second,
		RetryConfig: &RetryConfig{
			MaxAttempts:   3,
			InitialDelay:  100 * time.Millisecond,
			MaxDelay:      5 * time.Second,
			BackoffFactor: 2.0,
			Jitter:        true,
		},
		Logger: zap.NewExample(),
	}

	// Create the adapter
	adapter := NewIPFSClusterAdapter(config)
	defer adapter.Close()

	// Create operation monitor
	monitorConfig := &MonitorConfig{
		Adapter:       adapter,
		Logger:        zap.NewExample(),
		CheckInterval: 30 * time.Second,
	}
	monitor, _ := NewOperationMonitor(monitorConfig)

	ctx := context.Background()
	monitor.Start(ctx)
	defer monitor.Stop()

	// Example 1: Submit a pin plan
	plan := &models.PinPlan{
		ID:  "example-plan-1",
		CID: "QmExampleContent123",
		Assignments: []*models.NodeAssignment{
			{
				NodeID:    "node-1",
				ClusterID: "cluster-msk",
				ZoneID:    "zone-msk",
				Priority:  1,
			},
			{
				NodeID:    "node-2",
				ClusterID: "cluster-nn",
				ZoneID:    "zone-nn",
				Priority:  2,
			},
		},
	}

	// Track the operation
	operationID := monitor.TrackOperation(OperationTypePin, "cluster-msk", plan.CID)

	// Submit the pin plan (with automatic retry on failure)
	err := adapter.SubmitPinPlan(ctx, "cluster-msk", plan)
	if err != nil {
		monitor.CompleteOperation(operationID, models.ContentStatusFailed, err)
		return
	}

	// Example 2: Check pin status
	status, err := adapter.GetPinStatus(ctx, "cluster-msk", plan.CID)
	if err != nil {
		monitor.CompleteOperation(operationID, models.ContentStatusFailed, err)
		return
	}

	// Complete the operation based on status
	monitor.CompleteOperation(operationID, status.Status, nil)

	// Example 3: Health check
	healthOpID := monitor.TrackOperation(OperationTypeHealth, "cluster-msk", "")
	err = adapter.HealthCheck(ctx, "cluster-msk")
	if err != nil {
		monitor.CompleteOperation(healthOpID, models.ContentStatusFailed, err)
	} else {
		monitor.CompleteOperation(healthOpID, models.ContentStatusPinned, nil) // Using as "healthy"
	}

	// Example 4: Get cluster metrics
	metrics, err := adapter.GetNodeMetrics(ctx, "cluster-msk")
	if err == nil {
		// Process metrics...
		_ = metrics
	}

	// Example 5: Node management
	nodeConfig := &models.NodeConfig{
		Address: "192.168.1.100:4001",
		Resources: &models.NodeResources{
			CPUCores:     8,
			MemoryBytes:  16 * 1024 * 1024 * 1024, // 16GB
			StorageBytes: 1024 * 1024 * 1024 * 1024, // 1TB
			NetworkMbps:  1000,
		},
	}

	err = adapter.AddNode(ctx, "cluster-msk", nodeConfig)
	if err != nil {
		// Handle error...
	}

	// Example 6: List all tracked operations
	operations := monitor.ListOperations()
	for _, op := range operations {
		// Process operation status...
		_ = op
	}
}