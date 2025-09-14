package sharding

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func createTestShardingManager(t *testing.T) *ShardingManager {
	config := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}
	
	sm, err := NewShardingManager(config)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	return sm
}

func TestMigrationEngine_Creation(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs:   2,
		ChunkSize:          1024,
		VerificationEnabled: true,
		CompressionEnabled:  true,
		ProgressInterval:   time.Second,
		RetryAttempts:      3,
		RetryDelay:         time.Second,
		ZFSSnapshotPrefix:  "test",
	}

	// Create sharding manager
	smConfig := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(smConfig)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	// Create mock ZFS manager
	zfsManager := NewMockZFSManager()

	// Create migration engine
	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	if len(me.workerPool) != config.MaxConcurrentJobs {
		t.Errorf("Expected %d workers, got %d", config.MaxConcurrentJobs, len(me.workerPool))
	}

	if me.config.VerificationEnabled != true {
		t.Error("Expected verification to be enabled")
	}
}

func TestMigrationEngine_ScheduleMigration(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs:   1,
		ChunkSize:          1024,
		VerificationEnabled: false, // Disable for faster tests
		RetryAttempts:      1,
		RetryDelay:         100 * time.Millisecond,
	}

	// Create sharding manager with test shards
	smConfig := &Config{
		MaxPinsPerShard:   1000000,
		VirtualNodes:      150,
		ReplicationFactor: 3,
		LoadThreshold:     0.8,
	}

	sm, err := NewShardingManager(smConfig)
	if err != nil {
		t.Fatalf("Failed to create sharding manager: %v", err)
	}
	defer sm.Close()

	// Create test shards
	ctx := context.Background()
	sourceShard, _ := sm.CreateShard(ctx)
	targetShard, _ := sm.CreateShard(ctx)

	// Create migration engine
	zfsManager := NewMockZFSManager()
	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	// Create test migration job
	job := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: sourceShard.ID,
		TargetShardID: targetShard.ID,
		CIDs:          []string{"QmTest1", "QmTest2", "QmTest3"},
		TotalSize:     3072, // 3KB
		Priority:      1,
	}

	// Schedule migration
	err = me.ScheduleMigration(ctx, job)
	if err != nil {
		t.Fatalf("Failed to schedule migration: %v", err)
	}

	if job.ID == "" {
		t.Error("Expected job ID to be generated")
	}

	if job.Status != MigrationStatusPending {
		t.Errorf("Expected status %v, got %v", MigrationStatusPending, job.Status)
	}

	// Wait for job to complete
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Migration job did not complete within timeout")
		case <-ticker.C:
			status, err := me.GetMigrationStatus(job.ID)
			if err != nil {
				continue
			}
			if status.Status == MigrationStatusCompleted {
				t.Logf("Migration completed successfully")
				return
			}
			if status.Status == MigrationStatusFailed {
				t.Fatalf("Migration failed: %s", status.Error)
			}
		}
	}
}

func TestMigrationEngine_JobValidation(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs: 1,
		ChunkSize:        1024,
	}

	sm := createTestShardingManager(t)
	defer sm.Close()

	zfsManager := NewMockZFSManager()
	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	ctx := context.Background()

	// Test missing source shard
	job1 := &MigrationJob{
		Type:          MigrationTypeRebalance,
		TargetShardID: "target",
		CIDs:          []string{"QmTest1"},
	}
	err = me.ScheduleMigration(ctx, job1)
	if err == nil {
		t.Error("Expected error for missing source shard")
	}

	// Test missing target shard
	job2 := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: "source",
		CIDs:          []string{"QmTest1"},
	}
	err = me.ScheduleMigration(ctx, job2)
	if err == nil {
		t.Error("Expected error for missing target shard")
	}

	// Test same source and target
	job3 := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: "same",
		TargetShardID: "same",
		CIDs:          []string{"QmTest1"},
	}
	err = me.ScheduleMigration(ctx, job3)
	if err == nil {
		t.Error("Expected error for same source and target")
	}

	// Test empty CIDs
	job4 := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: "source",
		TargetShardID: "target",
		CIDs:          []string{},
	}
	err = me.ScheduleMigration(ctx, job4)
	if err == nil {
		t.Error("Expected error for empty CIDs")
	}
}

func TestMigrationEngine_JobControl(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs:   1,
		ChunkSize:          1024,
		VerificationEnabled: false,
		RetryAttempts:      0, // No retries for this test
	}

	sm := createTestShardingManager(t)
	defer sm.Close()

	// Create test shards
	ctx := context.Background()
	sourceShard, _ := sm.CreateShard(ctx)
	targetShard, _ := sm.CreateShard(ctx)

	zfsManager := NewMockZFSManager()
	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	// Create a long-running job
	job := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: sourceShard.ID,
		TargetShardID: targetShard.ID,
		CIDs:          make([]string, 100), // Many CIDs for longer execution
		TotalSize:     102400,
		Priority:      1,
	}

	// Fill CIDs
	for i := 0; i < 100; i++ {
		job.CIDs[i] = fmt.Sprintf("QmTest%d", i)
	}

	// Schedule migration
	err = me.ScheduleMigration(ctx, job)
	if err != nil {
		t.Fatalf("Failed to schedule migration: %v", err)
	}

	// Wait for job to start
	time.Sleep(100 * time.Millisecond)

	// Test pause
	err = me.PauseMigration(job.ID)
	if err != nil {
		t.Fatalf("Failed to pause migration: %v", err)
	}

	status, _ := me.GetMigrationStatus(job.ID)
	if status.Status != MigrationStatusPaused {
		t.Errorf("Expected status %v, got %v", MigrationStatusPaused, status.Status)
	}

	// Test resume
	err = me.ResumeMigration(job.ID)
	if err != nil {
		t.Fatalf("Failed to resume migration: %v", err)
	}

	// Test cancel
	err = me.CancelMigration(job.ID)
	if err != nil {
		t.Fatalf("Failed to cancel migration: %v", err)
	}

	status, _ = me.GetMigrationStatus(job.ID)
	if status.Status != MigrationStatusCancelled {
		t.Errorf("Expected status %v, got %v", MigrationStatusCancelled, status.Status)
	}
}

func TestMigrationEngine_ProgressTracking(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs:   1,
		ChunkSize:          1024,
		VerificationEnabled: false,
	}

	sm := createTestShardingManager(t)
	defer sm.Close()

	ctx := context.Background()
	sourceShard, _ := sm.CreateShard(ctx)
	targetShard, _ := sm.CreateShard(ctx)

	zfsManager := NewMockZFSManager()
	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	// Create test job
	job := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: sourceShard.ID,
		TargetShardID: targetShard.ID,
		CIDs:          []string{"QmTest1", "QmTest2", "QmTest3", "QmTest4", "QmTest5"},
		TotalSize:     5120,
		Priority:      1,
	}

	// Schedule migration
	err = me.ScheduleMigration(ctx, job)
	if err != nil {
		t.Fatalf("Failed to schedule migration: %v", err)
	}

	// Monitor progress
	timeout := time.After(3 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var lastProgress float64
	progressIncreased := false

	for {
		select {
		case <-timeout:
			if !progressIncreased {
				t.Error("Progress did not increase during migration")
			}
			return
		case <-ticker.C:
			status, err := me.GetMigrationStatus(job.ID)
			if err != nil {
				continue
			}

			if status.Progress.PercentComplete > lastProgress {
				progressIncreased = true
				lastProgress = status.Progress.PercentComplete
				t.Logf("Progress: %.1f%% (%d/%d CIDs)", 
					status.Progress.PercentComplete,
					status.Progress.MigratedCIDs,
					status.Progress.TotalCIDs)
			}

			if status.Status == MigrationStatusCompleted {
				if status.Progress.PercentComplete != 100.0 {
					t.Errorf("Expected 100%% completion, got %.1f%%", status.Progress.PercentComplete)
				}
				return
			}

			if status.Status == MigrationStatusFailed {
				t.Fatalf("Migration failed: %s", status.Error)
			}
		}
	}
}

func TestMigrationEngine_ZFSFailure(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs:   1,
		ChunkSize:          1024,
		VerificationEnabled: false,
		RetryAttempts:      1,
		RetryDelay:         100 * time.Millisecond,
	}

	sm := createTestShardingManager(t)
	defer sm.Close()

	ctx := context.Background()
	sourceShard, _ := sm.CreateShard(ctx)
	targetShard, _ := sm.CreateShard(ctx)

	// Create mock ZFS manager that fails snapshot creation
	zfsManager := NewMockZFSManager()
	zfsManager.SetFailOperation("CreateSnapshot", true)

	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	// Create test job
	job := &MigrationJob{
		Type:          MigrationTypeRebalance,
		SourceShardID: sourceShard.ID,
		TargetShardID: targetShard.ID,
		CIDs:          []string{"QmTest1"},
		TotalSize:     1024,
		Priority:      1,
	}

	// Schedule migration
	err = me.ScheduleMigration(ctx, job)
	if err != nil {
		t.Fatalf("Failed to schedule migration: %v", err)
	}

	// Wait for job to fail
	timeout := time.After(3 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Migration job did not fail within timeout")
		case <-ticker.C:
			status, err := me.GetMigrationStatus(job.ID)
			if err != nil {
				continue
			}
			if status.Status == MigrationStatusFailed {
				if status.Error == "" {
					t.Error("Expected error message for failed migration")
				}
				t.Logf("Migration failed as expected: %s", status.Error)
				return
			}
		}
	}
}

func TestMigrationEngine_OverallProgress(t *testing.T) {
	config := &MigrationConfig{
		MaxConcurrentJobs:   2,
		ChunkSize:          1024,
		VerificationEnabled: false,
	}

	sm := createTestShardingManager(t)
	defer sm.Close()

	ctx := context.Background()
	sourceShard, _ := sm.CreateShard(ctx)
	targetShard, _ := sm.CreateShard(ctx)

	zfsManager := NewMockZFSManager()
	me, err := NewMigrationEngine(config, sm, zfsManager)
	if err != nil {
		t.Fatalf("Failed to create migration engine: %v", err)
	}
	defer me.Close()

	// Schedule multiple jobs
	for i := 0; i < 3; i++ {
		job := &MigrationJob{
			Type:          MigrationTypeRebalance,
			SourceShardID: sourceShard.ID,
			TargetShardID: targetShard.ID,
			CIDs:          []string{fmt.Sprintf("QmTest%d", i)},
			TotalSize:     1024,
			Priority:      1,
		}

		err = me.ScheduleMigration(ctx, job)
		if err != nil {
			t.Fatalf("Failed to schedule migration %d: %v", i, err)
		}
	}

	// Check overall progress
	progress := me.GetOverallProgress()
	if progress.TotalJobs != 3 {
		t.Errorf("Expected 3 total jobs, got %d", progress.TotalJobs)
	}

	// Wait for jobs to complete
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Jobs did not complete within timeout")
		case <-ticker.C:
			progress := me.GetOverallProgress()
			t.Logf("Overall progress: %d/%d jobs completed", progress.CompletedJobs, progress.TotalJobs)
			
			if progress.CompletedJobs == progress.TotalJobs {
				if progress.ActiveJobs != 0 {
					t.Errorf("Expected 0 active jobs, got %d", progress.ActiveJobs)
				}
				return
			}
		}
	}
}