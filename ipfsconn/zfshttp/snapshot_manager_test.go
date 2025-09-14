package zfshttp

import (
	"testing"
	"time"
)

func TestNewZFSSnapshotManager(t *testing.T) {
	config := &SnapshotConfig{
		AutoSnapshotInterval:    time.Hour,
		MaxConcurrentOperations: 4,
		SnapshotPrefix:          "ipfs-",
		CompressionEnabled:      true,
		VerifyChecksums:         true,
		RetryAttempts:           3,
		RetryDelay:              time.Second,
		EnableAutoCleanup:       true,
		CleanupInterval:         24 * time.Hour,
	}
	
	retentionPolicy := &RetentionPolicy{
		HourlySnapshots:  24,
		DailySnapshots:   7,
		WeeklySnapshots:  4,
		MonthlySnapshots: 12,
		YearlySnapshots:  5,
	}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	if manager == nil {
		t.Fatal("Expected non-nil snapshot manager")
	}
	
	if manager.config.SnapshotPrefix != "ipfs-" {
		t.Errorf("Expected snapshot prefix 'ipfs-', got %s", manager.config.SnapshotPrefix)
	}
	
	if manager.retentionPolicy.HourlySnapshots != 24 {
		t.Errorf("Expected 24 hourly snapshots, got %d", manager.retentionPolicy.HourlySnapshots)
	}
	
	if len(manager.snapshots) != 0 {
		t.Errorf("Expected empty snapshots map, got %d entries", len(manager.snapshots))
	}
	
	if len(manager.scheduledTasks) != 0 {
		t.Errorf("Expected empty scheduled tasks map, got %d entries", len(manager.scheduledTasks))
	}
}

func TestZFSSnapshotManager_GetSnapshotsByDataset(t *testing.T) {
	config := &SnapshotConfig{
		SnapshotPrefix: "test-",
	}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	// Test empty dataset
	snapshots := manager.GetSnapshotsByDataset("nonexistent")
	if len(snapshots) != 0 {
		t.Errorf("Expected 0 snapshots for nonexistent dataset, got %d", len(snapshots))
	}
	
	// Add test snapshots
	testDataset := "tank/test"
	testSnapshots := []*Snapshot{
		{
			Name:      "snap1",
			Dataset:   testDataset,
			FullName:  "tank/test@snap1",
			CreatedAt: time.Now().Add(-2 * time.Hour),
			Status:    SnapshotStatusActive,
		},
		{
			Name:      "snap2",
			Dataset:   testDataset,
			FullName:  "tank/test@snap2",
			CreatedAt: time.Now().Add(-1 * time.Hour),
			Status:    SnapshotStatusActive,
		},
	}
	
	manager.snapshots[testDataset] = testSnapshots
	
	// Test with existing dataset
	snapshots = manager.GetSnapshotsByDataset(testDataset)
	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots for test dataset, got %d", len(snapshots))
	}
	
	if snapshots[0].Name != "snap1" {
		t.Errorf("Expected first snapshot name 'snap1', got %s", snapshots[0].Name)
	}
}

func TestZFSSnapshotManager_GetMetrics(t *testing.T) {
	config := &SnapshotConfig{}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	// Set test metrics
	manager.metrics.TotalSnapshots = 100
	manager.metrics.SnapshotsCreated = 80
	manager.metrics.SnapshotsDeleted = 20
	manager.metrics.FailedOperations = 5
	
	metrics := manager.GetMetrics()
	
	if metrics.TotalSnapshots != 100 {
		t.Errorf("Expected 100 total snapshots, got %d", metrics.TotalSnapshots)
	}
	
	if metrics.SnapshotsCreated != 80 {
		t.Errorf("Expected 80 created snapshots, got %d", metrics.SnapshotsCreated)
	}
	
	if metrics.SnapshotsDeleted != 20 {
		t.Errorf("Expected 20 deleted snapshots, got %d", metrics.SnapshotsDeleted)
	}
	
	if metrics.FailedOperations != 5 {
		t.Errorf("Expected 5 failed operations, got %d", metrics.FailedOperations)
	}
}

func TestZFSSnapshotManager_GroupSnapshotsByAge(t *testing.T) {
	config := &SnapshotConfig{}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	now := time.Now()
	testSnapshots := []*Snapshot{
		{Name: "hourly1", CreatedAt: now.Add(-30 * time.Minute)},   // hourly
		{Name: "daily1", CreatedAt: now.Add(-2 * 24 * time.Hour)},  // daily
		{Name: "weekly1", CreatedAt: now.Add(-10 * 24 * time.Hour)}, // weekly
		{Name: "monthly1", CreatedAt: now.Add(-40 * 24 * time.Hour)}, // monthly
		{Name: "yearly1", CreatedAt: now.Add(-400 * 24 * time.Hour)}, // yearly
	}
	
	groups := manager.groupSnapshotsByAge(testSnapshots)
	
	if len(groups["hourly"]) != 1 {
		t.Errorf("Expected 1 hourly snapshot, got %d", len(groups["hourly"]))
	}
	
	if len(groups["daily"]) != 1 {
		t.Errorf("Expected 1 daily snapshot, got %d", len(groups["daily"]))
	}
	
	if len(groups["weekly"]) != 1 {
		t.Errorf("Expected 1 weekly snapshot, got %d", len(groups["weekly"]))
	}
	
	if len(groups["monthly"]) != 1 {
		t.Errorf("Expected 1 monthly snapshot, got %d", len(groups["monthly"]))
	}
	
	if len(groups["yearly"]) != 1 {
		t.Errorf("Expected 1 yearly snapshot, got %d", len(groups["yearly"]))
	}
}

func TestZFSSnapshotManager_UpdateMetrics(t *testing.T) {
	config := &SnapshotConfig{}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	// Test successful creation
	manager.updateMetrics(true, 1000, 0)
	
	if manager.metrics.SnapshotsCreated != 1 {
		t.Errorf("Expected 1 created snapshot, got %d", manager.metrics.SnapshotsCreated)
	}
	
	if manager.metrics.AverageCreationTime != 1000.0 {
		t.Errorf("Expected average creation time 1000.0, got %f", manager.metrics.AverageCreationTime)
	}
	
	// Test successful deletion
	manager.updateMetrics(true, 0, 500)
	
	if manager.metrics.SnapshotsDeleted != 1 {
		t.Errorf("Expected 1 deleted snapshot, got %d", manager.metrics.SnapshotsDeleted)
	}
	
	if manager.metrics.AverageDeletionTime != 500.0 {
		t.Errorf("Expected average deletion time 500.0, got %f", manager.metrics.AverageDeletionTime)
	}
	
	// Test failed operation
	manager.updateMetrics(false, 0, 0)
	
	if manager.metrics.FailedOperations != 1 {
		t.Errorf("Expected 1 failed operation, got %d", manager.metrics.FailedOperations)
	}
}

func TestZFSSnapshotManager_GetScheduledTasks(t *testing.T) {
	config := &SnapshotConfig{}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	// Test empty scheduled tasks
	tasks := manager.GetScheduledTasks()
	if len(tasks) != 0 {
		t.Errorf("Expected 0 scheduled tasks, got %d", len(tasks))
	}
	
	// Add test scheduled task
	testTask := &ScheduledTask{
		ID:       "test-task-1",
		Dataset:  "tank/test",
		Interval: time.Hour,
		NextRun:  time.Now().Add(time.Hour),
		Enabled:  true,
	}
	
	manager.scheduledTasks["tank/test"] = testTask
	
	// Test with existing tasks
	tasks = manager.GetScheduledTasks()
	if len(tasks) != 1 {
		t.Errorf("Expected 1 scheduled task, got %d", len(tasks))
	}
	
	task, exists := tasks["tank/test"]
	if !exists {
		t.Fatal("Expected task for 'tank/test' dataset")
	}
	
	if task.ID != "test-task-1" {
		t.Errorf("Expected task ID 'test-task-1', got %s", task.ID)
	}
	
	if task.Dataset != "tank/test" {
		t.Errorf("Expected dataset 'tank/test', got %s", task.Dataset)
	}
	
	if !task.Enabled {
		t.Error("Expected task to be enabled")
	}
}

func TestScheduledTask_StopScheduledTask(t *testing.T) {
	config := &SnapshotConfig{}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	// Create a test task
	task := &ScheduledTask{
		ID:       "test-task",
		Dataset:  "tank/test",
		Interval: time.Hour,
		Enabled:  true,
		ticker:   time.NewTicker(time.Hour),
		stopChan: make(chan struct{}),
	}
	
	// Stop the task
	manager.stopScheduledTask(task)
	
	if task.Enabled {
		t.Error("Expected task to be disabled after stopping")
	}
	
	// Verify ticker is stopped (ticker.Stop() doesn't change the ticker state,
	// but we can verify the task is marked as disabled)
	if task.Enabled {
		t.Error("Task should be disabled after stopping")
	}
}

func TestZFSSnapshotManager_CollectMetrics(t *testing.T) {
	config := &SnapshotConfig{}
	retentionPolicy := &RetentionPolicy{}
	
	manager := NewZFSSnapshotManager(config, retentionPolicy)
	
	// Add test snapshots
	testSnapshots := []*Snapshot{
		{Name: "snap1", Used: 1000},
		{Name: "snap2", Used: 2000},
	}
	
	manager.snapshots["tank/test1"] = testSnapshots[:1]
	manager.snapshots["tank/test2"] = testSnapshots[1:]
	
	// Collect metrics
	manager.collectMetrics()
	
	if manager.metrics.TotalSnapshots != 2 {
		t.Errorf("Expected 2 total snapshots, got %d", manager.metrics.TotalSnapshots)
	}
	
	if manager.metrics.TotalStorageUsed != 3000 {
		t.Errorf("Expected total storage 3000, got %d", manager.metrics.TotalStorageUsed)
	}
	
	if manager.metrics.LastUpdate.IsZero() {
		t.Error("Expected LastUpdate to be set")
	}
}