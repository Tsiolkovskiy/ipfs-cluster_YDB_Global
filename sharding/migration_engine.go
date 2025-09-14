// Package sharding - Migration Engine for zero-downtime data migration between ZFS shards
package sharding

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var migrationLogger = logging.Logger("migration-engine")

// MigrationEngine handles data migration between shards using ZFS send/receive
type MigrationEngine struct {
	config           *MigrationConfig
	shardingManager  *ShardingManager
	activeJobs       map[string]*MigrationJob
	jobQueue         []*MigrationJob
	progressTracker  *ProgressTracker
	zfsManager       ZFSManager
	
	// Background processing
	workerPool       []*MigrationWorker
	jobChan          chan *MigrationJob
	stopChan         chan struct{}
	
	mutex            sync.RWMutex
}

// MigrationConfig holds configuration for the migration engine
type MigrationConfig struct {
	MaxConcurrentJobs    int           `json:"max_concurrent_jobs"`
	ChunkSize           int64         `json:"chunk_size"`           // Size of data chunks to migrate
	VerificationEnabled bool          `json:"verification_enabled"`
	CompressionEnabled  bool          `json:"compression_enabled"`
	ProgressInterval    time.Duration `json:"progress_interval"`
	RetryAttempts       int           `json:"retry_attempts"`
	RetryDelay          time.Duration `json:"retry_delay"`
	
	// ZFS specific settings
	ZFSSnapshotPrefix   string        `json:"zfs_snapshot_prefix"`
	ZFSCompressionLevel string        `json:"zfs_compression_level"`
	ZFSTransferTimeout  time.Duration `json:"zfs_transfer_timeout"`
}

// MigrationJob represents a data migration operation
type MigrationJob struct {
	ID              string                 `json:"id"`
	Type            MigrationType          `json:"type"`
	SourceShardID   string                 `json:"source_shard_id"`
	TargetShardID   string                 `json:"target_shard_id"`
	CIDs            []string               `json:"cids"`
	TotalSize       int64                  `json:"total_size"`
	Status          MigrationStatus        `json:"status"`
	Progress        *MigrationProgress     `json:"progress"`
	CreatedAt       time.Time              `json:"created_at"`
	StartedAt       time.Time              `json:"started_at"`
	CompletedAt     time.Time              `json:"completed_at"`
	Error           string                 `json:"error,omitempty"`
	RetryCount      int                    `json:"retry_count"`
	Priority        int                    `json:"priority"`
	Metadata        map[string]interface{} `json:"metadata"`
}

// MigrationType defines the type of migration
type MigrationType int

const (
	MigrationTypeRebalance MigrationType = iota
	MigrationTypeTiering
	MigrationTypeEvacuation
	MigrationTypeExpansion
)

// MigrationStatus defines the status of a migration job
type MigrationStatus int

const (
	MigrationStatusPending MigrationStatus = iota
	MigrationStatusRunning
	MigrationStatusCompleted
	MigrationStatusFailed
	MigrationStatusCancelled
	MigrationStatusPaused
)

// MigrationProgress tracks the progress of a migration job
type MigrationProgress struct {
	TotalCIDs       int64     `json:"total_cids"`
	MigratedCIDs    int64     `json:"migrated_cids"`
	TotalBytes      int64     `json:"total_bytes"`
	MigratedBytes   int64     `json:"migrated_bytes"`
	PercentComplete float64   `json:"percent_complete"`
	EstimatedTime   time.Duration `json:"estimated_time"`
	TransferRate    float64   `json:"transfer_rate"` // MB/s
	LastUpdate      time.Time `json:"last_update"`
}

// MigrationWorker handles individual migration jobs
type MigrationWorker struct {
	id              int
	engine          *MigrationEngine
	currentJob      *MigrationJob
	stopChan        chan struct{}
	mutex           sync.RWMutex
}

// ProgressTracker tracks overall migration progress and statistics
type ProgressTracker struct {
	totalJobs       int64
	completedJobs   int64
	failedJobs      int64
	totalDataMoved  int64
	avgTransferRate float64
	mutex           sync.RWMutex
}

// ZFSManager interface for ZFS operations
type ZFSManager interface {
	CreateSnapshot(dataset, name string) error
	SendSnapshot(dataset, snapshot, target string) error
	ReceiveSnapshot(source string) error
	DestroySnapshot(dataset, snapshot string) error
	VerifyDataIntegrity(dataset string) error
	GetDatasetSize(dataset string) (int64, error)
}

// NewMigrationEngine creates a new migration engine
func NewMigrationEngine(config *MigrationConfig, shardingManager *ShardingManager, zfsManager ZFSManager) (*MigrationEngine, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid migration config: %w", err)
	}

	me := &MigrationEngine{
		config:          config,
		shardingManager: shardingManager,
		activeJobs:      make(map[string]*MigrationJob),
		jobQueue:        make([]*MigrationJob, 0),
		progressTracker: &ProgressTracker{},
		zfsManager:      zfsManager,
		jobChan:         make(chan *MigrationJob, config.MaxConcurrentJobs*2),
		stopChan:        make(chan struct{}),
	}

	// Initialize worker pool
	me.workerPool = make([]*MigrationWorker, config.MaxConcurrentJobs)
	for i := 0; i < config.MaxConcurrentJobs; i++ {
		me.workerPool[i] = &MigrationWorker{
			id:       i,
			engine:   me,
			stopChan: make(chan struct{}),
		}
	}

	// Start workers
	me.startWorkers()

	migrationLogger.Infof("Migration engine initialized with %d workers", config.MaxConcurrentJobs)
	return me, nil
}

// ScheduleMigration schedules a new migration job
func (me *MigrationEngine) ScheduleMigration(ctx context.Context, job *MigrationJob) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	// Validate job
	if err := me.validateMigrationJob(job); err != nil {
		return fmt.Errorf("invalid migration job: %w", err)
	}

	// Generate job ID if not provided
	if job.ID == "" {
		job.ID = me.generateJobID()
	}

	// Initialize job
	job.Status = MigrationStatusPending
	job.CreatedAt = time.Now()
	job.Progress = &MigrationProgress{
		TotalCIDs:   int64(len(job.CIDs)),
		TotalBytes:  job.TotalSize,
		LastUpdate:  time.Now(),
	}

	// Add to queue
	me.jobQueue = append(me.jobQueue, job)
	me.progressTracker.totalJobs++

	// Try to schedule immediately if workers are available
	select {
	case me.jobChan <- job:
		migrationLogger.Infof("Migration job %s scheduled immediately", job.ID)
	default:
		migrationLogger.Infof("Migration job %s queued (workers busy)", job.ID)
	}

	return nil
}

// GetMigrationStatus returns the status of a migration job
func (me *MigrationEngine) GetMigrationStatus(jobID string) (*MigrationJob, error) {
	me.mutex.RLock()
	defer me.mutex.RUnlock()

	job, exists := me.activeJobs[jobID]
	if !exists {
		// Check queue
		for _, queuedJob := range me.jobQueue {
			if queuedJob.ID == jobID {
				jobCopy := *queuedJob
				return &jobCopy, nil
			}
		}
		return nil, fmt.Errorf("migration job %s not found", jobID)
	}

	// Return a copy to avoid race conditions
	jobCopy := *job
	return &jobCopy, nil
}

// CancelMigration cancels a migration job
func (me *MigrationEngine) CancelMigration(jobID string) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	job, exists := me.activeJobs[jobID]
	if !exists {
		return fmt.Errorf("migration job %s not found or not active", jobID)
	}

	job.Status = MigrationStatusCancelled
	migrationLogger.Infof("Migration job %s cancelled", jobID)
	return nil
}

// PauseMigration pauses a migration job
func (me *MigrationEngine) PauseMigration(jobID string) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	job, exists := me.activeJobs[jobID]
	if !exists {
		return fmt.Errorf("migration job %s not found or not active", jobID)
	}

	if job.Status != MigrationStatusRunning {
		return fmt.Errorf("migration job %s is not running", jobID)
	}

	job.Status = MigrationStatusPaused
	migrationLogger.Infof("Migration job %s paused", jobID)
	return nil
}

// ResumeMigration resumes a paused migration job
func (me *MigrationEngine) ResumeMigration(jobID string) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	job, exists := me.activeJobs[jobID]
	if !exists {
		return fmt.Errorf("migration job %s not found or not active", jobID)
	}

	if job.Status != MigrationStatusPaused {
		return fmt.Errorf("migration job %s is not paused", jobID)
	}

	job.Status = MigrationStatusRunning
	migrationLogger.Infof("Migration job %s resumed", jobID)
	return nil
}

// GetOverallProgress returns overall migration statistics
func (me *MigrationEngine) GetOverallProgress() *OverallProgress {
	me.progressTracker.mutex.RLock()
	defer me.progressTracker.mutex.RUnlock()

	me.mutex.RLock()
	activeJobCount := len(me.activeJobs)
	queuedJobCount := len(me.jobQueue)
	me.mutex.RUnlock()

	return &OverallProgress{
		TotalJobs:       me.progressTracker.totalJobs,
		CompletedJobs:   me.progressTracker.completedJobs,
		FailedJobs:      me.progressTracker.failedJobs,
		ActiveJobs:      int64(activeJobCount),
		QueuedJobs:      int64(queuedJobCount),
		TotalDataMoved:  me.progressTracker.totalDataMoved,
		AvgTransferRate: me.progressTracker.avgTransferRate,
	}
}

// OverallProgress contains overall migration statistics
type OverallProgress struct {
	TotalJobs       int64   `json:"total_jobs"`
	CompletedJobs   int64   `json:"completed_jobs"`
	FailedJobs      int64   `json:"failed_jobs"`
	ActiveJobs      int64   `json:"active_jobs"`
	QueuedJobs      int64   `json:"queued_jobs"`
	TotalDataMoved  int64   `json:"total_data_moved"`
	AvgTransferRate float64 `json:"avg_transfer_rate"`
}

// Close shuts down the migration engine
func (me *MigrationEngine) Close() error {
	migrationLogger.Info("Shutting down migration engine")

	// Stop workers
	close(me.stopChan)
	for _, worker := range me.workerPool {
		close(worker.stopChan)
	}

	// Wait for active jobs to complete or timeout
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			migrationLogger.Warn("Migration engine shutdown timeout - forcing close")
			return nil
		case <-ticker.C:
			me.mutex.RLock()
			activeCount := len(me.activeJobs)
			me.mutex.RUnlock()
			
			if activeCount == 0 {
				migrationLogger.Info("Migration engine shutdown complete")
				return nil
			}
			migrationLogger.Infof("Waiting for %d active migrations to complete", activeCount)
		}
	}
}

// Worker implementation and helper methods

func (me *MigrationEngine) startWorkers() {
	for _, worker := range me.workerPool {
		go worker.run()
	}
}

func (worker *MigrationWorker) run() {
	migrationLogger.Infof("Migration worker %d started", worker.id)
	
	for {
		select {
		case job := <-worker.engine.jobChan:
			worker.processJob(job)
		case <-worker.stopChan:
			migrationLogger.Infof("Migration worker %d stopped", worker.id)
			return
		}
	}
}

func (worker *MigrationWorker) processJob(job *MigrationJob) {
	worker.mutex.Lock()
	worker.currentJob = job
	worker.mutex.Unlock()

	// Add to active jobs
	worker.engine.mutex.Lock()
	worker.engine.activeJobs[job.ID] = job
	worker.engine.mutex.Unlock()

	// Remove from queue
	worker.engine.removeFromQueue(job.ID)

	migrationLogger.Infof("Worker %d processing migration job %s", worker.id, job.ID)

	// Execute migration
	err := worker.executeMigration(job)
	
	// Update job status
	worker.engine.mutex.Lock()
	if err != nil {
		job.Status = MigrationStatusFailed
		job.Error = err.Error()
		worker.engine.progressTracker.failedJobs++
		migrationLogger.Errorf("Migration job %s failed: %v", job.ID, err)
		
		// Retry if configured
		if job.RetryCount < worker.engine.config.RetryAttempts {
			job.RetryCount++
			job.Status = MigrationStatusPending
			
			// Schedule retry after delay
			go func() {
				time.Sleep(worker.engine.config.RetryDelay)
				select {
				case worker.engine.jobChan <- job:
					migrationLogger.Infof("Migration job %s scheduled for retry %d", job.ID, job.RetryCount)
				default:
					migrationLogger.Warnf("Failed to schedule retry for job %s", job.ID)
				}
			}()
		}
	} else {
		job.Status = MigrationStatusCompleted
		job.CompletedAt = time.Now()
		worker.engine.progressTracker.completedJobs++
		worker.engine.progressTracker.totalDataMoved += job.TotalSize
		migrationLogger.Infof("Migration job %s completed successfully", job.ID)
	}
	
	// Remove from active jobs if not retrying
	if job.Status != MigrationStatusPending {
		delete(worker.engine.activeJobs, job.ID)
	}
	worker.engine.mutex.Unlock()

	worker.mutex.Lock()
	worker.currentJob = nil
	worker.mutex.Unlock()
}

func (worker *MigrationWorker) executeMigration(job *MigrationJob) error {
	ctx := context.Background()
	
	// Update job status
	job.Status = MigrationStatusRunning
	job.StartedAt = time.Now()

	// Get source and target shards
	sourceShards := worker.engine.shardingManager.ListShards()
	var sourceShard, targetShard *Shard
	
	for _, shard := range sourceShards {
		if shard.ID == job.SourceShardID {
			sourceShard = shard
		}
		if shard.ID == job.TargetShardID {
			targetShard = shard
		}
	}

	if sourceShard == nil {
		return fmt.Errorf("source shard %s not found", job.SourceShardID)
	}
	if targetShard == nil {
		return fmt.Errorf("target shard %s not found", job.TargetShardID)
	}

	// Execute migration based on type
	switch job.Type {
	case MigrationTypeRebalance:
		return worker.executeRebalanceMigration(ctx, job, sourceShard, targetShard)
	case MigrationTypeTiering:
		return worker.executeTieringMigration(ctx, job, sourceShard, targetShard)
	case MigrationTypeEvacuation:
		return worker.executeEvacuationMigration(ctx, job, sourceShard, targetShard)
	case MigrationTypeExpansion:
		return worker.executeExpansionMigration(ctx, job, sourceShard, targetShard)
	default:
		return fmt.Errorf("unknown migration type: %v", job.Type)
	}
}

func (worker *MigrationWorker) executeRebalanceMigration(ctx context.Context, job *MigrationJob, source, target *Shard) error {
	migrationLogger.Infof("Executing rebalance migration from %s to %s", source.ID, target.ID)

	// Create snapshot for consistent migration
	snapshotName := fmt.Sprintf("%s-%s-%d", 
		worker.engine.config.ZFSSnapshotPrefix, job.ID, time.Now().Unix())
	
	if err := worker.engine.zfsManager.CreateSnapshot(source.DatasetPath, snapshotName); err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Ensure cleanup
	defer func() {
		if err := worker.engine.zfsManager.DestroySnapshot(source.DatasetPath, snapshotName); err != nil {
			migrationLogger.Errorf("Failed to cleanup snapshot %s: %v", snapshotName, err)
		}
	}()

	// Migrate data in chunks
	return worker.migrateDataChunks(ctx, job, source, target, snapshotName)
}

func (worker *MigrationWorker) executeTieringMigration(ctx context.Context, job *MigrationJob, source, target *Shard) error {
	migrationLogger.Infof("Executing tiering migration from %s to %s", source.ID, target.ID)
	
	// Similar to rebalance but with tier-specific optimizations
	return worker.executeRebalanceMigration(ctx, job, source, target)
}

func (worker *MigrationWorker) executeEvacuationMigration(ctx context.Context, job *MigrationJob, source, target *Shard) error {
	migrationLogger.Infof("Executing evacuation migration from %s to %s", source.ID, target.ID)
	
	// Evacuation requires moving ALL data from source
	return worker.executeRebalanceMigration(ctx, job, source, target)
}

func (worker *MigrationWorker) executeExpansionMigration(ctx context.Context, job *MigrationJob, source, target *Shard) error {
	migrationLogger.Infof("Executing expansion migration from %s to %s", source.ID, target.ID)
	
	// Expansion migration for scaling out
	return worker.executeRebalanceMigration(ctx, job, source, target)
}

func (worker *MigrationWorker) migrateDataChunks(ctx context.Context, job *MigrationJob, source, target *Shard, snapshotName string) error {
	totalCIDs := int64(len(job.CIDs))
	
	for i, cid := range job.CIDs {
		// Check for cancellation or pause
		if job.Status == MigrationStatusCancelled {
			return fmt.Errorf("migration cancelled")
		}
		if job.Status == MigrationStatusPaused {
			// Wait for resume
			for job.Status == MigrationStatusPaused {
				time.Sleep(1 * time.Second)
			}
		}

		// Migrate individual CID
		if err := worker.migrateCID(ctx, cid, source, target, snapshotName); err != nil {
			return fmt.Errorf("failed to migrate CID %s: %w", cid, err)
		}

		// Update progress
		job.Progress.MigratedCIDs = int64(i + 1)
		job.Progress.PercentComplete = float64(job.Progress.MigratedCIDs) / float64(totalCIDs) * 100
		job.Progress.LastUpdate = time.Now()

		// Calculate transfer rate and ETA
		elapsed := time.Since(job.StartedAt)
		if elapsed > 0 {
			job.Progress.TransferRate = float64(job.Progress.MigratedBytes) / elapsed.Seconds() / 1024 / 1024 // MB/s
			remaining := totalCIDs - job.Progress.MigratedCIDs
			if job.Progress.TransferRate > 0 {
				job.Progress.EstimatedTime = time.Duration(float64(remaining)/job.Progress.TransferRate) * time.Second
			}
		}

		// Log progress periodically
		if i%100 == 0 || i == len(job.CIDs)-1 {
			migrationLogger.Infof("Migration job %s progress: %d/%d CIDs (%.1f%%)", 
				job.ID, job.Progress.MigratedCIDs, totalCIDs, job.Progress.PercentComplete)
		}
	}

	// Verify migration if enabled
	if worker.engine.config.VerificationEnabled {
		if err := worker.verifyMigration(ctx, job, source, target); err != nil {
			return fmt.Errorf("migration verification failed: %w", err)
		}
	}

	return nil
}

func (worker *MigrationWorker) migrateCID(ctx context.Context, cid string, source, target *Shard, snapshotName string) error {
	// In a real implementation, this would:
	// 1. Use ZFS send/receive to transfer the specific CID data
	// 2. Update metadata mappings
	// 3. Verify data integrity
	// 4. Update shard statistics

	// Simulate migration time
	time.Sleep(10 * time.Millisecond)
	
	// Update migrated bytes (estimated)
	estimatedSize := int64(1024) // 1KB per CID metadata
	worker.currentJob.Progress.MigratedBytes += estimatedSize

	return nil
}

func (worker *MigrationWorker) verifyMigration(ctx context.Context, job *MigrationJob, source, target *Shard) error {
	migrationLogger.Infof("Verifying migration for job %s", job.ID)

	// Verify data integrity on target
	if err := worker.engine.zfsManager.VerifyDataIntegrity(target.DatasetPath); err != nil {
		return fmt.Errorf("target data integrity check failed: %w", err)
	}

	// Additional verification logic would go here
	// - Compare checksums
	// - Verify all CIDs are accessible
	// - Check metadata consistency

	migrationLogger.Infof("Migration verification completed for job %s", job.ID)
	return nil
}

// Helper methods

func (me *MigrationEngine) validateMigrationJob(job *MigrationJob) error {
	if job.SourceShardID == "" {
		return fmt.Errorf("source shard ID is required")
	}
	if job.TargetShardID == "" {
		return fmt.Errorf("target shard ID is required")
	}
	if job.SourceShardID == job.TargetShardID {
		return fmt.Errorf("source and target shards cannot be the same")
	}
	if len(job.CIDs) == 0 {
		return fmt.Errorf("no CIDs specified for migration")
	}
	return nil
}

func (me *MigrationEngine) generateJobID() string {
	return fmt.Sprintf("migration-%d", time.Now().UnixNano())
}

func (me *MigrationEngine) removeFromQueue(jobID string) {
	for i, job := range me.jobQueue {
		if job.ID == jobID {
			me.jobQueue = append(me.jobQueue[:i], me.jobQueue[i+1:]...)
			break
		}
	}
}

// Validate validates the migration configuration
func (c *MigrationConfig) Validate() error {
	if c.MaxConcurrentJobs <= 0 {
		return fmt.Errorf("max_concurrent_jobs must be positive")
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("chunk_size must be positive")
	}
	if c.RetryAttempts < 0 {
		return fmt.Errorf("retry_attempts cannot be negative")
	}
	if c.ZFSSnapshotPrefix == "" {
		c.ZFSSnapshotPrefix = "migration"
	}
	if c.ZFSCompressionLevel == "" {
		c.ZFSCompressionLevel = "lz4"
	}
	if c.ZFSTransferTimeout == 0 {
		c.ZFSTransferTimeout = 30 * time.Minute
	}
	return nil
}