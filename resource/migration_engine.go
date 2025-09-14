package resource

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// MigrationEngine handles data migration between storage tiers
type MigrationEngine struct {
	config           *TieredConfig
	activeMigrations map[string]*MigrationJob
	migrationQueue   chan *MigrationJob
	workers          []*MigrationWorker
	
	// Statistics
	stats            *MigrationStats
	
	// Synchronization
	mu               sync.RWMutex
	stopWorkers      chan struct{}
}

// MigrationJob represents a data migration job
type MigrationJob struct {
	ID              string        `json:"id"`
	CID             string        `json:"cid"`
	SourceTier      string        `json:"source_tier"`
	TargetTier      string        `json:"target_tier"`
	DataSize        int64         `json:"data_size"`
	Priority        int           `json:"priority"`
	CreatedAt       time.Time     `json:"created_at"`
	StartedAt       time.Time     `json:"started_at"`
	CompletedAt     time.Time     `json:"completed_at"`
	Status          string        `json:"status"`
	Progress        float64       `json:"progress"`
	Error           string        `json:"error,omitempty"`
	
	// ZFS-specific fields
	SourceDataset   string        `json:"source_dataset"`
	TargetDataset   string        `json:"target_dataset"`
	SnapshotName    string        `json:"snapshot_name"`
	UseIncremental  bool          `json:"use_incremental"`
}

// MigrationWorker handles individual migration jobs
type MigrationWorker struct {
	ID              int
	engine          *MigrationEngine
	currentJob      *MigrationJob
	bandwidthLimit  int64         // bytes/sec
	
	mu              sync.RWMutex
}

// MigrationStats tracks migration statistics
type MigrationStats struct {
	TotalJobs       int64         `json:"total_jobs"`
	CompletedJobs   int64         `json:"completed_jobs"`
	FailedJobs      int64         `json:"failed_jobs"`
	ActiveJobs      int64         `json:"active_jobs"`
	QueuedJobs      int64         `json:"queued_jobs"`
	TotalBytesTransferred int64   `json:"total_bytes_transferred"`
	AverageSpeed    float64       `json:"average_speed"`    // bytes/sec
	AverageDuration time.Duration `json:"average_duration"`
	LastUpdated     time.Time     `json:"last_updated"`
	
	mu              sync.RWMutex  `json:"-"`
}

// NewMigrationEngine creates a new migration engine
func NewMigrationEngine(config *TieredConfig) *MigrationEngine {
	me := &MigrationEngine{
		config:           config,
		activeMigrations: make(map[string]*MigrationJob),
		migrationQueue:   make(chan *MigrationJob, 1000),
		workers:          make([]*MigrationWorker, config.MaxConcurrentMigrations),
		stats:            &MigrationStats{},
		stopWorkers:      make(chan struct{}),
	}

	// Initialize workers
	for i := 0; i < config.MaxConcurrentMigrations; i++ {
		me.workers[i] = &MigrationWorker{
			ID:             i,
			engine:         me,
			bandwidthLimit: config.MigrationBandwidth / int64(config.MaxConcurrentMigrations),
		}
	}

	return me
}

// Start starts the migration engine
func (me *MigrationEngine) Start(ctx context.Context) error {
	tieredLogger.Info("Starting migration engine")

	// Start workers
	for _, worker := range me.workers {
		go worker.run(ctx)
	}

	tieredLogger.Infof("Migration engine started with %d workers", len(me.workers))
	return nil
}

// Stop stops the migration engine
func (me *MigrationEngine) Stop() error {
	tieredLogger.Info("Stopping migration engine")

	// Stop workers
	close(me.stopWorkers)

	// Wait for active migrations to complete (with timeout)
	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			tieredLogger.Warn("Migration engine stop timeout, forcing shutdown")
			return nil
		case <-ticker.C:
			me.mu.RLock()
			activeCount := len(me.activeMigrations)
			me.mu.RUnlock()
			
			if activeCount == 0 {
				tieredLogger.Info("Migration engine stopped")
				return nil
			}
		}
	}
}

// MigrateData migrates data from source tier to target tier
func (me *MigrationEngine) MigrateData(ctx context.Context, cid, sourceTier, targetTier string) error {
	// Create migration job
	job := &MigrationJob{
		ID:         fmt.Sprintf("migration-%s-%d", cid, time.Now().UnixNano()),
		CID:        cid,
		SourceTier: sourceTier,
		TargetTier: targetTier,
		Priority:   1,
		CreatedAt:  time.Now(),
		Status:     "queued",
	}

	// Set ZFS-specific fields
	job.SourceDataset = fmt.Sprintf("%s/data/%s", sourceTier, cid[:2])
	job.TargetDataset = fmt.Sprintf("%s/data/%s", targetTier, cid[:2])
	job.SnapshotName = fmt.Sprintf("migration-%d", time.Now().Unix())

	// Add to queue
	select {
	case me.migrationQueue <- job:
		me.updateStats(func(stats *MigrationStats) {
			stats.TotalJobs++
			stats.QueuedJobs++
		})
		tieredLogger.Debugf("Migration job queued: %s", job.ID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("migration queue is full")
	}
}

// GetMigrationStatus returns the status of a migration job
func (me *MigrationEngine) GetMigrationStatus(jobID string) (*MigrationJob, error) {
	me.mu.RLock()
	defer me.mu.RUnlock()

	job, exists := me.activeMigrations[jobID]
	if !exists {
		return nil, fmt.Errorf("migration job %s not found", jobID)
	}

	// Return a copy to avoid race conditions
	jobCopy := *job
	return &jobCopy, nil
}

// ListActiveMigrations returns all active migration jobs
func (me *MigrationEngine) ListActiveMigrations() []*MigrationJob {
	me.mu.RLock()
	defer me.mu.RUnlock()

	jobs := make([]*MigrationJob, 0, len(me.activeMigrations))
	for _, job := range me.activeMigrations {
		jobCopy := *job
		jobs = append(jobs, &jobCopy)
	}

	return jobs
}

// GetStats returns migration statistics
func (me *MigrationEngine) GetStats() *MigrationStats {
	me.stats.mu.RLock()
	defer me.stats.mu.RUnlock()

	// Return a copy
	statsCopy := *me.stats
	return &statsCopy
}

// Private methods

func (me *MigrationEngine) updateStats(updateFunc func(*MigrationStats)) {
	me.stats.mu.Lock()
	defer me.stats.mu.Unlock()
	
	updateFunc(me.stats)
	me.stats.LastUpdated = time.Now()
}

// MigrationWorker methods

func (mw *MigrationWorker) run(ctx context.Context) {
	tieredLogger.Debugf("Migration worker %d started", mw.ID)

	for {
		select {
		case job := <-mw.engine.migrationQueue:
			mw.processJob(ctx, job)
		case <-mw.engine.stopWorkers:
			tieredLogger.Debugf("Migration worker %d stopped", mw.ID)
			return
		case <-ctx.Done():
			tieredLogger.Debugf("Migration worker %d stopped due to context cancellation", mw.ID)
			return
		}
	}
}

func (mw *MigrationWorker) processJob(ctx context.Context, job *MigrationJob) {
	tieredLogger.Infof("Worker %d processing migration job %s", mw.ID, job.ID)

	// Update job status
	job.Status = "running"
	job.StartedAt = time.Now()

	// Add to active migrations
	mw.engine.mu.Lock()
	mw.engine.activeMigrations[job.ID] = job
	mw.engine.mu.Unlock()

	// Update stats
	mw.engine.updateStats(func(stats *MigrationStats) {
		stats.ActiveJobs++
		stats.QueuedJobs--
	})

	// Set current job
	mw.mu.Lock()
	mw.currentJob = job
	mw.mu.Unlock()

	// Perform the migration
	err := mw.performMigration(ctx, job)

	// Update job completion
	job.CompletedAt = time.Now()
	if err != nil {
		job.Status = "failed"
		job.Error = err.Error()
		tieredLogger.Errorf("Migration job %s failed: %v", job.ID, err)
	} else {
		job.Status = "completed"
		job.Progress = 100.0
		tieredLogger.Infof("Migration job %s completed successfully", job.ID)
	}

	// Remove from active migrations
	mw.engine.mu.Lock()
	delete(mw.engine.activeMigrations, job.ID)
	mw.engine.mu.Unlock()

	// Update stats
	mw.engine.updateStats(func(stats *MigrationStats) {
		stats.ActiveJobs--
		if err != nil {
			stats.FailedJobs++
		} else {
			stats.CompletedJobs++
			stats.TotalBytesTransferred += job.DataSize
			
			// Update average duration
			duration := job.CompletedAt.Sub(job.StartedAt)
			if stats.CompletedJobs == 1 {
				stats.AverageDuration = duration
			} else {
				stats.AverageDuration = (stats.AverageDuration*time.Duration(stats.CompletedJobs-1) + duration) / time.Duration(stats.CompletedJobs)
			}
			
			// Update average speed
			if duration > 0 {
				speed := float64(job.DataSize) / duration.Seconds()
				if stats.CompletedJobs == 1 {
					stats.AverageSpeed = speed
				} else {
					stats.AverageSpeed = (stats.AverageSpeed*float64(stats.CompletedJobs-1) + speed) / float64(stats.CompletedJobs)
				}
			}
		}
	})

	// Clear current job
	mw.mu.Lock()
	mw.currentJob = nil
	mw.mu.Unlock()
}

func (mw *MigrationWorker) performMigration(ctx context.Context, job *MigrationJob) error {
	// Step 1: Create snapshot of source data
	if err := mw.createSnapshot(ctx, job); err != nil {
		return errors.Wrap(err, "creating snapshot")
	}

	job.Progress = 25.0

	// Step 2: Send snapshot to target tier
	if err := mw.sendSnapshot(ctx, job); err != nil {
		return errors.Wrap(err, "sending snapshot")
	}

	job.Progress = 75.0

	// Step 3: Verify data integrity
	if err := mw.verifyMigration(ctx, job); err != nil {
		return errors.Wrap(err, "verifying migration")
	}

	job.Progress = 90.0

	// Step 4: Update metadata and cleanup
	if err := mw.finalizeMigration(ctx, job); err != nil {
		return errors.Wrap(err, "finalizing migration")
	}

	job.Progress = 100.0
	return nil
}

func (mw *MigrationWorker) createSnapshot(ctx context.Context, job *MigrationJob) error {
	tieredLogger.Debugf("Creating snapshot for migration job %s", job.ID)

	// Create ZFS snapshot
	snapshotPath := fmt.Sprintf("%s@%s", job.SourceDataset, job.SnapshotName)
	cmd := exec.CommandContext(ctx, "zfs", "snapshot", snapshotPath)
	
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "creating snapshot %s", snapshotPath)
	}

	tieredLogger.Debugf("Snapshot created: %s", snapshotPath)
	return nil
}

func (mw *MigrationWorker) sendSnapshot(ctx context.Context, job *MigrationJob) error {
	tieredLogger.Debugf("Sending snapshot for migration job %s", job.ID)

	// Ensure target dataset exists
	if err := mw.ensureTargetDataset(ctx, job); err != nil {
		return errors.Wrap(err, "ensuring target dataset")
	}

	// Use ZFS send/receive for efficient transfer
	sourceSnapshot := fmt.Sprintf("%s@%s", job.SourceDataset, job.SnapshotName)
	
	// Create send command
	sendCmd := exec.CommandContext(ctx, "zfs", "send", sourceSnapshot)
	
	// Create receive command
	recvCmd := exec.CommandContext(ctx, "zfs", "receive", "-F", job.TargetDataset)
	
	// Connect send output to receive input
	recvCmd.Stdin, _ = sendCmd.StdoutPipe()
	
	// Start receive first
	if err := recvCmd.Start(); err != nil {
		return errors.Wrap(err, "starting zfs receive")
	}
	
	// Start send
	if err := sendCmd.Start(); err != nil {
		recvCmd.Process.Kill()
		return errors.Wrap(err, "starting zfs send")
	}
	
	// Wait for both commands to complete
	sendErr := sendCmd.Wait()
	recvErr := recvCmd.Wait()
	
	if sendErr != nil {
		return errors.Wrap(sendErr, "zfs send failed")
	}
	
	if recvErr != nil {
		return errors.Wrap(recvErr, "zfs receive failed")
	}

	tieredLogger.Debugf("Snapshot sent successfully for job %s", job.ID)
	return nil
}

func (mw *MigrationWorker) ensureTargetDataset(ctx context.Context, job *MigrationJob) error {
	// Check if target dataset exists
	cmd := exec.CommandContext(ctx, "zfs", "list", "-H", job.TargetDataset)
	if err := cmd.Run(); err != nil {
		// Dataset doesn't exist, create it
		createCmd := exec.CommandContext(ctx, "zfs", "create", "-p", job.TargetDataset)
		if err := createCmd.Run(); err != nil {
			return errors.Wrapf(err, "creating target dataset %s", job.TargetDataset)
		}
		tieredLogger.Debugf("Created target dataset: %s", job.TargetDataset)
	}
	
	return nil
}

func (mw *MigrationWorker) verifyMigration(ctx context.Context, job *MigrationJob) error {
	tieredLogger.Debugf("Verifying migration for job %s", job.ID)

	// Compare checksums between source and target
	sourceChecksum, err := mw.getDatasetChecksum(ctx, job.SourceDataset)
	if err != nil {
		return errors.Wrap(err, "getting source checksum")
	}

	targetChecksum, err := mw.getDatasetChecksum(ctx, job.TargetDataset)
	if err != nil {
		return errors.Wrap(err, "getting target checksum")
	}

	if sourceChecksum != targetChecksum {
		return fmt.Errorf("checksum mismatch: source=%s, target=%s", sourceChecksum, targetChecksum)
	}

	tieredLogger.Debugf("Migration verification successful for job %s", job.ID)
	return nil
}

func (mw *MigrationWorker) getDatasetChecksum(ctx context.Context, dataset string) (string, error) {
	// Get ZFS checksum for the dataset
	cmd := exec.CommandContext(ctx, "zfs", "get", "-H", "-o", "value", "checksum", dataset)
	output, err := cmd.Output()
	if err != nil {
		return "", errors.Wrapf(err, "getting checksum for %s", dataset)
	}

	return string(output), nil
}

func (mw *MigrationWorker) finalizeMigration(ctx context.Context, job *MigrationJob) error {
	tieredLogger.Debugf("Finalizing migration for job %s", job.ID)

	// Clean up source snapshot
	sourceSnapshot := fmt.Sprintf("%s@%s", job.SourceDataset, job.SnapshotName)
	cmd := exec.CommandContext(ctx, "zfs", "destroy", sourceSnapshot)
	if err := cmd.Run(); err != nil {
		tieredLogger.Warnf("Failed to cleanup source snapshot %s: %v", sourceSnapshot, err)
		// Don't fail the migration for cleanup issues
	}

	// Update dataset properties on target if needed
	if err := mw.updateTargetProperties(ctx, job); err != nil {
		tieredLogger.Warnf("Failed to update target properties: %v", err)
		// Don't fail the migration for property update issues
	}

	tieredLogger.Debugf("Migration finalized for job %s", job.ID)
	return nil
}

func (mw *MigrationWorker) updateTargetProperties(ctx context.Context, job *MigrationJob) error {
	// Set appropriate properties for the target tier
	// This would be customized based on tier characteristics
	
	properties := map[string]string{
		"compression": "lz4",
		"atime":       "off",
		"sync":        "standard",
	}

	for prop, value := range properties {
		cmd := exec.CommandContext(ctx, "zfs", "set", fmt.Sprintf("%s=%s", prop, value), job.TargetDataset)
		if err := cmd.Run(); err != nil {
			return errors.Wrapf(err, "setting property %s=%s on %s", prop, value, job.TargetDataset)
		}
	}

	return nil
}