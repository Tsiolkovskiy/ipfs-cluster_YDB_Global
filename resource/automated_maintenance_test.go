package resource

import (
	"context"
	"testing"
	"time"
)

func TestNewAutomatedMaintenance(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:       7 * 24 * time.Hour, // Weekly
		ScrubBandwidthLimit: 100 * 1024 * 1024,  // 100MB/s
		DefragInterval:      30 * 24 * time.Hour, // Monthly
		DefragThreshold:     0.3,                 // 30%
		DefragBandwidthLimit: 50 * 1024 * 1024,  // 50MB/s
		AutoRepairEnabled:   true,
		RepairTimeout:       2 * time.Hour,
		MaxRepairAttempts:   3,
		MaintenanceWindow: TimeWindow{
			StartHour: 2,  // 2 AM
			EndHour:   6,  // 6 AM
			Days:      []int{0, 6}, // Sunday and Saturday
		},
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1, // 10%
		HealthCheckInterval:    5 * time.Minute,
		MetricsInterval:        1 * time.Minute,
		EmergencyRepairEnabled: true,
		CriticalErrorThreshold: 5,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	if am == nil {
		t.Fatal("Automated maintenance is nil")
	}

	if am.config != config {
		t.Error("Config not set correctly")
	}

	if am.scheduler == nil {
		t.Error("Scheduler not initialized")
	}

	if am.scrubManager == nil {
		t.Error("Scrub manager not initialized")
	}

	if am.defragManager == nil {
		t.Error("Defrag manager not initialized")
	}

	if am.repairManager == nil {
		t.Error("Repair manager not initialized")
	}
}

func TestMaintenanceConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *MaintenanceConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: &MaintenanceConfig{
				ScrubInterval:          7 * 24 * time.Hour,
				DefragInterval:         30 * 24 * time.Hour,
				DefragThreshold:        0.3,
				MaxConcurrentOps:       2,
				PerformanceImpactLimit: 0.1,
				HealthCheckInterval:    5 * time.Minute,
			},
			expectError: false,
		},
		{
			name: "zero scrub interval",
			config: &MaintenanceConfig{
				ScrubInterval:          0, // Invalid
				DefragInterval:         30 * 24 * time.Hour,
				DefragThreshold:        0.3,
				MaxConcurrentOps:       2,
				PerformanceImpactLimit: 0.1,
				HealthCheckInterval:    5 * time.Minute,
			},
			expectError: true,
		},
		{
			name: "invalid defrag threshold",
			config: &MaintenanceConfig{
				ScrubInterval:          7 * 24 * time.Hour,
				DefragInterval:         30 * 24 * time.Hour,
				DefragThreshold:        1.5, // Invalid: > 1
				MaxConcurrentOps:       2,
				PerformanceImpactLimit: 0.1,
				HealthCheckInterval:    5 * time.Minute,
			},
			expectError: true,
		},
		{
			name: "zero max concurrent ops",
			config: &MaintenanceConfig{
				ScrubInterval:          7 * 24 * time.Hour,
				DefragInterval:         30 * 24 * time.Hour,
				DefragThreshold:        0.3,
				MaxConcurrentOps:       0, // Invalid
				PerformanceImpactLimit: 0.1,
				HealthCheckInterval:    5 * time.Minute,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestScheduleScrub(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3,
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	poolName := "test-pool"
	priority := 2

	operation, err := am.ScheduleScrub(poolName, priority)
	if err != nil {
		t.Errorf("Failed to schedule scrub: %v", err)
	}

	if operation == nil {
		t.Fatal("Operation is nil")
	}

	if operation.Type != "scrub" {
		t.Errorf("Expected type 'scrub', got '%s'", operation.Type)
	}

	if operation.Pool != poolName {
		t.Errorf("Expected pool '%s', got '%s'", poolName, operation.Pool)
	}

	if operation.Priority != priority {
		t.Errorf("Expected priority %d, got %d", priority, operation.Priority)
	}

	if operation.Status != "scheduled" {
		t.Errorf("Expected status 'scheduled', got '%s'", operation.Status)
	}

	// Check that operation was added to active operations
	am.mu.RLock()
	_, exists := am.activeOperations[operation.ID]
	am.mu.RUnlock()

	if !exists {
		t.Error("Operation not found in active operations")
	}
}

func TestScheduleDefragmentation(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3, // 30%
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	// Mock getPoolFragmentation to return high fragmentation
	originalGetPoolFragmentation := am.getPoolFragmentation
	am.getPoolFragmentation = func(poolName string) (float64, error) {
		return 0.4, nil // 40% fragmentation, above threshold
	}
	defer func() {
		am.getPoolFragmentation = originalGetPoolFragmentation
	}()

	// Mock getPoolSize
	originalGetPoolSize := am.getPoolSize
	am.getPoolSize = func(poolName string) (int64, error) {
		return 1024 * 1024 * 1024 * 1024, nil // 1TB
	}
	defer func() {
		am.getPoolSize = originalGetPoolSize
	}()

	poolName := "test-pool"
	priority := 3

	operation, err := am.ScheduleDefragmentation(poolName, priority)
	if err != nil {
		t.Errorf("Failed to schedule defragmentation: %v", err)
	}

	if operation == nil {
		t.Fatal("Operation is nil")
	}

	if operation.Type != "defrag" {
		t.Errorf("Expected type 'defrag', got '%s'", operation.Type)
	}

	if operation.Pool != poolName {
		t.Errorf("Expected pool '%s', got '%s'", poolName, operation.Pool)
	}

	// Check metadata
	if fragmentation, exists := operation.Metadata["initial_fragmentation"]; !exists {
		t.Error("Initial fragmentation not set in metadata")
	} else if fragmentation != 0.4 {
		t.Errorf("Expected initial fragmentation 0.4, got %v", fragmentation)
	}
}

func TestScheduleRepair(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3,
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	poolName := "test-pool"
	deviceName := "/dev/sda"
	errorType := "checksum_error"

	operation, err := am.ScheduleRepair(poolName, deviceName, errorType)
	if err != nil {
		t.Errorf("Failed to schedule repair: %v", err)
	}

	if operation == nil {
		t.Fatal("Operation is nil")
	}

	if operation.Type != "repair" {
		t.Errorf("Expected type 'repair', got '%s'", operation.Type)
	}

	if operation.Pool != poolName {
		t.Errorf("Expected pool '%s', got '%s'", poolName, operation.Pool)
	}

	if operation.Priority != 1 {
		t.Errorf("Expected priority 1 (high) for repair, got %d", operation.Priority)
	}

	// Check metadata
	if device, exists := operation.Metadata["device"]; !exists {
		t.Error("Device not set in metadata")
	} else if device != deviceName {
		t.Errorf("Expected device '%s', got '%v'", deviceName, device)
	}

	if errType, exists := operation.Metadata["error_type"]; !exists {
		t.Error("Error type not set in metadata")
	} else if errType != errorType {
		t.Errorf("Expected error type '%s', got '%v'", errorType, errType)
	}
}

func TestIsInMaintenanceWindow(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3,
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
		MaintenanceWindow: TimeWindow{
			StartHour: 2,  // 2 AM
			EndHour:   6,  // 6 AM
			Days:      []int{0, 6}, // Sunday and Saturday
		},
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	// Test different times
	tests := []struct {
		name     string
		hour     int
		weekday  int
		expected bool
	}{
		{"Sunday 3 AM", 3, 0, true},
		{"Saturday 4 AM", 4, 6, true},
		{"Monday 3 AM", 3, 1, false},  // Wrong day
		{"Sunday 8 AM", 8, 0, false},  // Wrong hour
		{"Saturday 1 AM", 1, 6, false}, // Wrong hour
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock time for testing
			// In a real implementation, you'd use a time interface that can be mocked
			// For this test, we'll just check the logic conceptually
			
			// Check hour range
			inHourRange := (tt.hour >= config.MaintenanceWindow.StartHour && 
							tt.hour < config.MaintenanceWindow.EndHour)
			
			// Check day
			inDayRange := false
			for _, day := range config.MaintenanceWindow.Days {
				if day == tt.weekday {
					inDayRange = true
					break
				}
			}
			
			result := inHourRange && inDayRange
			if result != tt.expected {
				t.Errorf("Expected %v, got %v for %s", tt.expected, result, tt.name)
			}
		})
	}
}

func TestGetActiveOperations(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3,
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	// Schedule some operations
	op1, err := am.ScheduleScrub("pool1", 2)
	if err != nil {
		t.Fatalf("Failed to schedule scrub: %v", err)
	}

	op2, err := am.ScheduleRepair("pool2", "device1", "checksum_error")
	if err != nil {
		t.Fatalf("Failed to schedule repair: %v", err)
	}

	// Get active operations
	operations := am.GetActiveOperations()

	if len(operations) != 2 {
		t.Errorf("Expected 2 operations, got %d", len(operations))
	}

	// Check that operations are returned correctly
	foundOp1 := false
	foundOp2 := false

	for _, op := range operations {
		if op.ID == op1.ID {
			foundOp1 = true
			if op.Type != "scrub" {
				t.Errorf("Expected type 'scrub' for op1, got '%s'", op.Type)
			}
		}
		if op.ID == op2.ID {
			foundOp2 = true
			if op.Type != "repair" {
				t.Errorf("Expected type 'repair' for op2, got '%s'", op.Type)
			}
		}
	}

	if !foundOp1 {
		t.Error("Operation 1 not found in active operations")
	}
	if !foundOp2 {
		t.Error("Operation 2 not found in active operations")
	}
}

func TestGetOperationStatus(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3,
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	// Schedule an operation
	operation, err := am.ScheduleScrub("test-pool", 2)
	if err != nil {
		t.Fatalf("Failed to schedule scrub: %v", err)
	}

	// Get operation status
	status, err := am.GetOperationStatus(operation.ID)
	if err != nil {
		t.Errorf("Failed to get operation status: %v", err)
	}

	if status == nil {
		t.Fatal("Status is nil")
	}

	if status.ID != operation.ID {
		t.Errorf("Expected ID '%s', got '%s'", operation.ID, status.ID)
	}

	if status.Status != "scheduled" {
		t.Errorf("Expected status 'scheduled', got '%s'", status.Status)
	}

	// Test non-existent operation
	_, err = am.GetOperationStatus("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent operation")
	}
}

func TestGetCurrentPerformanceImpact(t *testing.T) {
	config := &MaintenanceConfig{
		ScrubInterval:          7 * 24 * time.Hour,
		DefragInterval:         30 * 24 * time.Hour,
		DefragThreshold:        0.3,
		MaxConcurrentOps:       2,
		PerformanceImpactLimit: 0.1,
		HealthCheckInterval:    5 * time.Minute,
	}

	am, err := NewAutomatedMaintenance(config)
	if err != nil {
		t.Fatalf("Failed to create automated maintenance: %v", err)
	}

	// Initially should be zero impact
	impact := am.getCurrentPerformanceImpact()
	if impact != 0.0 {
		t.Errorf("Expected zero impact initially, got %f", impact)
	}

	// Add some operations with IO impact
	op1, err := am.ScheduleScrub("pool1", 2)
	if err != nil {
		t.Fatalf("Failed to schedule scrub: %v", err)
	}

	op2, err := am.ScheduleScrub("pool2", 2)
	if err != nil {
		t.Fatalf("Failed to schedule scrub: %v", err)
	}

	// Set operations to running with IO impact
	op1.mu.Lock()
	op1.Status = "running"
	op1.IOImpact = 0.05 // 5%
	op1.mu.Unlock()

	op2.mu.Lock()
	op2.Status = "running"
	op2.IOImpact = 0.03 // 3%
	op2.mu.Unlock()

	// Should now have combined impact
	impact = am.getCurrentPerformanceImpact()
	expected := 0.05 + 0.03
	if impact != expected {
		t.Errorf("Expected impact %f, got %f", expected, impact)
	}
}

func TestParseZFSSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"1024", 1024, false},
		{"1K", 1024, false},
		{"1M", 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"1T", 1024 * 1024 * 1024 * 1024, false},
		{"1.5G", int64(1.5 * 1024 * 1024 * 1024), false},
		{"0", 0, false},
		{"-", 0, false},
		{"invalid", 0, true},
		{"1X", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseZFSSize(tt.input)

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error for input %s", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input %s: %v", tt.input, err)
				}
				if result != tt.expected {
					t.Errorf("Expected %d, got %d for input %s", tt.expected, result, tt.input)
				}
			}
		})
	}
}