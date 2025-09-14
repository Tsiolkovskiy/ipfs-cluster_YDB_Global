// Package zfshttp implements ZFS snapshot management for IPFS Cluster
package zfshttp

import (
	"context"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var snapshotLogger = logging.Logger("zfs-snapshot")

// ZFSSnapshotManager implements the SnapshotManager interface
type ZFSSnapshotManager struct {
	config           *SnapshotConfig
	retentionPolicy  *RetentionPolicy
	
	// State management
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.RWMutex
	
	// Snapshot tracking
	snapshots        map[string][]*Snapshot
	scheduledTasks   map[string]*ScheduledTask
	
	// Metrics
	metrics          *SnapshotMetrics
}

// SnapshotConfig holds configuration for snapshot management
type SnapshotConfig struct {
	AutoSnapshotInterval    time.Duration `json:"auto_snapshot_interval"`
	MaxConcurrentOperations int           `json:"max_concurrent_operations"`
	SnapshotPrefix          string        `json:"snapshot_prefix"`
	CompressionEnabled      bool          `json:"compression_enabled"`
	VerifyChecksums         bool          `json:"verify_checksums"`
	RetryAttempts           int           `json:"retry_attempts"`
	RetryDelay              time.Duration `json:"retry_delay"`
	EnableAutoCleanup       bool          `json:"enable_auto_cleanup"`
	CleanupInterval         time.Duration `json:"cleanup_interval"`
}

// RetentionPolicy defines how long snapshots should be kept
type RetentionPolicy struct {
	HourlySnapshots  int `json:"hourly_snapshots"`   // Keep last N hourly snapshots
	DailySnapshots   int `json:"daily_snapshots"`    // Keep last N daily snapshots
	WeeklySnapshots  int `json:"weekly_snapshots"`   // Keep last N weekly snapshots
	MonthlySnapshots int `json:"monthly_snapshots"`  // Keep last N monthly snapshots
	YearlySnapshots  int `json:"yearly_snapshots"`   // Keep last N yearly snapshots
}

// Snapshot represents a ZFS snapshot
type Snapshot struct {
	Name         string            `json:"name"`
	Dataset      string            `json:"dataset"`
	FullName     string            `json:"full_name"`
	CreatedAt    time.Time         `json:"created_at"`
	Size         int64             `json:"size"`
	Used         int64             `json:"used"`
	Referenced   int64             `json:"referenced"`
	Properties   map[string]string `json:"properties"`
	Type         SnapshotType      `json:"type"`
	Status       SnapshotStatus    `json:"status"`
}

// SnapshotType represents the type of snapshot
type SnapshotType int

const (
	SnapshotTypeManual SnapshotType = iota
	SnapshotTypeScheduled
	SnapshotTypeReplication
	SnapshotTypeBackup
)

// SnapshotStatus represents the status of a snapshot
type SnapshotStatus int

const (
	SnapshotStatusActive SnapshotStatus = iota
	SnapshotStatusPending
	SnapshotStatusDeleting
	SnapshotStatusError
)

// ScheduledTask represents a scheduled snapshot task
type ScheduledTask struct {
	ID          string        `json:"id"`
	Dataset     string        `json:"dataset"`
	Interval    time.Duration `json:"interval"`
	NextRun     time.Time     `json:"next_run"`
	LastRun     time.Time     `json:"last_run"`
	Enabled     bool          `json:"enabled"`
	ticker      *time.Ticker
	stopChan    chan struct{}
}

// SnapshotMetrics holds metrics for snapshot operations
type SnapshotMetrics struct {
	TotalSnapshots       int64     `json:"total_snapshots"`
	SnapshotsCreated     int64     `json:"snapshots_created"`
	SnapshotsDeleted     int64     `json:"snapshots_deleted"`
	FailedOperations     int64     `json:"failed_operations"`
	AverageCreationTime  float64   `json:"average_creation_time"`
	AverageDeletionTime  float64   `json:"average_deletion_time"`
	TotalStorageUsed     int64     `json:"total_storage_used"`
	LastUpdate           time.Time `json:"last_update"`
}

// NewZFSSnapshotManager creates a new ZFS snapshot manager
func NewZFSSnapshotManager(config *SnapshotConfig, retentionPolicy *RetentionPolicy) *ZFSSnapshotManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &ZFSSnapshotManager{
		config:          config,
		retentionPolicy: retentionPolicy,
		ctx:             ctx,
		cancel:          cancel,
		snapshots:       make(map[string][]*Snapshot),
		scheduledTasks:  make(map[string]*ScheduledTask),
		metrics:         &SnapshotMetrics{},
	}
}

// Start initializes and starts the snapshot manager
func (sm *ZFSSnapshotManager) Start() error {
	snapshotLogger.Info("Starting ZFS snapshot manager")
	
	// Load existing snapshots
	if err := sm.loadExistingSnapshots(); err != nil {
		return fmt.Errorf("failed to load existing snapshots: %w", err)
	}
	
	// Start cleanup routine if enabled
	if sm.config.EnableAutoCleanup {
		go sm.cleanupLoop()
	}
	
	// Start metrics collection
	go sm.metricsLoop()
	
	snapshotLogger.Info("ZFS snapshot manager started successfully")
	return nil
}

// Stop gracefully shuts down the snapshot manager
func (sm *ZFSSnapshotManager) Stop() error {
	snapshotLogger.Info("Stopping ZFS snapshot manager")
	
	sm.cancel()
	
	// Stop all scheduled tasks
	sm.mu.Lock()
	for _, task := range sm.scheduledTasks {
		sm.stopScheduledTask(task)
	}
	sm.mu.Unlock()
	
	snapshotLogger.Info("ZFS snapshot manager stopped")
	return nil
}

// CreateSnapshot creates a new ZFS snapshot
func (sm *ZFSSnapshotManager) CreateSnapshot(ctx context.Context, dataset, name string) error {
	snapshotLogger.Infof("Creating snapshot %s for dataset %s", name, dataset)
	
	startTime := time.Now()
	
	// Construct full snapshot name
	fullName := fmt.Sprintf("%s@%s", dataset, name)
	
	// Create ZFS snapshot command
	cmd := exec.CommandContext(ctx, "zfs", "snapshot", fullName)
	
	// Execute command with retry logic
	var err error
	for attempt := 0; attempt <= sm.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			snapshotLogger.Warnf("Retrying snapshot creation (attempt %d/%d)", attempt, sm.config.RetryAttempts)
			time.Sleep(sm.config.RetryDelay)
		}
		
		err = cmd.Run()
		if err == nil {
			break
		}
	}
	
	if err != nil {
		sm.updateMetrics(false, 0, 0)
		return fmt.Errorf("failed to create snapshot after %d attempts: %w", sm.config.RetryAttempts+1, err)
	}
	
	// Get snapshot properties
	snapshot, err := sm.getSnapshotInfo(ctx, dataset, name)
	if err != nil {
		snapshotLogger.Warnf("Failed to get snapshot info: %v", err)
		// Create basic snapshot info
		snapshot = &Snapshot{
			Name:      name,
			Dataset:   dataset,
			FullName:  fullName,
			CreatedAt: time.Now(),
			Type:      SnapshotTypeManual,
			Status:    SnapshotStatusActive,
		}
	}
	
	// Add to tracking
	sm.mu.Lock()
	if sm.snapshots[dataset] == nil {
		sm.snapshots[dataset] = make([]*Snapshot, 0)
	}
	sm.snapshots[dataset] = append(sm.snapshots[dataset], snapshot)
	sm.mu.Unlock()
	
	duration := time.Since(startTime)
	sm.updateMetrics(true, duration.Milliseconds(), 0)
	
	snapshotLogger.Infof("Snapshot %s created successfully in %v", fullName, duration)
	return nil
}

// GetPreviousSnapshot gets the previous snapshot for incremental replication
func (sm *ZFSSnapshotManager) GetPreviousSnapshot(ctx context.Context, dataset, currentSnapshot string) (string, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	snapshots, exists := sm.snapshots[dataset]
	if !exists || len(snapshots) == 0 {
		return "", fmt.Errorf("no snapshots found for dataset %s", dataset)
	}
	
	// Sort snapshots by creation time
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].CreatedAt.Before(snapshots[j].CreatedAt)
	})
	
	// Find current snapshot and return previous one
	for i, snapshot := range snapshots {
		if snapshot.Name == currentSnapshot {
			if i == 0 {
				return "", fmt.Errorf("no previous snapshot found for %s", currentSnapshot)
			}
			return snapshots[i-1].Name, nil
		}
	}
	
	return "", fmt.Errorf("current snapshot %s not found", currentSnapshot)
}

// ListSnapshots lists all snapshots for a dataset
func (sm *ZFSSnapshotManager) ListSnapshots(ctx context.Context, dataset string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "zfs", "list", "-t", "snapshot", "-H", "-o", "name", "-s", "creation", dataset)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	snapshots := make([]string, 0, len(lines))
	
	for _, line := range lines {
		if line == "" {
			continue
		}
		// Extract snapshot name from full path (dataset@snapshot)
		parts := strings.Split(line, "@")
		if len(parts) == 2 {
			snapshots = append(snapshots, parts[1])
		}
	}
	
	return snapshots, nil
}

// DeleteSnapshot deletes a snapshot
func (sm *ZFSSnapshotManager) DeleteSnapshot(ctx context.Context, dataset, snapshot string) error {
	snapshotLogger.Infof("Deleting snapshot %s from dataset %s", snapshot, dataset)
	
	startTime := time.Now()
	fullName := fmt.Sprintf("%s@%s", dataset, snapshot)
	
	// Create ZFS destroy command
	cmd := exec.CommandContext(ctx, "zfs", "destroy", fullName)
	
	// Execute command with retry logic
	var err error
	for attempt := 0; attempt <= sm.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			snapshotLogger.Warnf("Retrying snapshot deletion (attempt %d/%d)", attempt, sm.config.RetryAttempts)
			time.Sleep(sm.config.RetryDelay)
		}
		
		err = cmd.Run()
		if err == nil {
			break
		}
	}
	
	if err != nil {
		sm.updateMetrics(false, 0, 0)
		return fmt.Errorf("failed to delete snapshot after %d attempts: %w", sm.config.RetryAttempts+1, err)
	}
	
	// Remove from tracking
	sm.mu.Lock()
	if snapshots, exists := sm.snapshots[dataset]; exists {
		for i, snap := range snapshots {
			if snap.Name == snapshot {
				sm.snapshots[dataset] = append(snapshots[:i], snapshots[i+1:]...)
				break
			}
		}
	}
	sm.mu.Unlock()
	
	duration := time.Since(startTime)
	sm.updateMetrics(true, 0, duration.Milliseconds())
	
	snapshotLogger.Infof("Snapshot %s deleted successfully in %v", fullName, duration)
	return nil
}

// RollbackToSnapshot rolls back a dataset to a specific snapshot
func (sm *ZFSSnapshotManager) RollbackToSnapshot(ctx context.Context, dataset, snapshot string) error {
	snapshotLogger.Infof("Rolling back dataset %s to snapshot %s", dataset, snapshot)
	
	fullName := fmt.Sprintf("%s@%s", dataset, snapshot)
	
	// Create ZFS rollback command
	cmd := exec.CommandContext(ctx, "zfs", "rollback", "-r", fullName)
	
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to rollback to snapshot %s: %w", fullName, err)
	}
	
	snapshotLogger.Infof("Successfully rolled back dataset %s to snapshot %s", dataset, snapshot)
	return nil
}

// ScheduleAutoSnapshots schedules automatic snapshot creation for a dataset
func (sm *ZFSSnapshotManager) ScheduleAutoSnapshots(dataset string, interval time.Duration) error {
	snapshotLogger.Infof("Scheduling auto snapshots for dataset %s every %v", dataset, interval)
	
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	// Stop existing task if any
	if existingTask, exists := sm.scheduledTasks[dataset]; exists {
		sm.stopScheduledTask(existingTask)
	}
	
	// Create new scheduled task
	task := &ScheduledTask{
		ID:       fmt.Sprintf("auto-%s-%d", dataset, time.Now().Unix()),
		Dataset:  dataset,
		Interval: interval,
		NextRun:  time.Now().Add(interval),
		Enabled:  true,
		ticker:   time.NewTicker(interval),
		stopChan: make(chan struct{}),
	}
	
	sm.scheduledTasks[dataset] = task
	
	// Start the scheduled task
	go sm.runScheduledTask(task)
	
	return nil
}

// StopAutoSnapshots stops automatic snapshot creation for a dataset
func (sm *ZFSSnapshotManager) StopAutoSnapshots(dataset string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	task, exists := sm.scheduledTasks[dataset]
	if !exists {
		return fmt.Errorf("no scheduled task found for dataset %s", dataset)
	}
	
	sm.stopScheduledTask(task)
	delete(sm.scheduledTasks, dataset)
	
	snapshotLogger.Infof("Stopped auto snapshots for dataset %s", dataset)
	return nil
}

// runScheduledTask runs a scheduled snapshot task
func (sm *ZFSSnapshotManager) runScheduledTask(task *ScheduledTask) {
	snapshotLogger.Infof("Starting scheduled task %s for dataset %s", task.ID, task.Dataset)
	
	for {
		select {
		case <-task.ticker.C:
			if task.Enabled {
				sm.createScheduledSnapshot(task)
			}
		case <-task.stopChan:
			snapshotLogger.Infof("Stopping scheduled task %s", task.ID)
			return
		case <-sm.ctx.Done():
			return
		}
	}
}

// createScheduledSnapshot creates a scheduled snapshot
func (sm *ZFSSnapshotManager) createScheduledSnapshot(task *ScheduledTask) {
	snapshotName := fmt.Sprintf("%s%s-%d", 
		sm.config.SnapshotPrefix, 
		"auto", 
		time.Now().Unix())
	
	ctx, cancel := context.WithTimeout(sm.ctx, 5*time.Minute)
	defer cancel()
	
	if err := sm.CreateSnapshot(ctx, task.Dataset, snapshotName); err != nil {
		snapshotLogger.Errorf("Failed to create scheduled snapshot for dataset %s: %v", task.Dataset, err)
		return
	}
	
	task.LastRun = time.Now()
	task.NextRun = time.Now().Add(task.Interval)
	
	// Apply retention policy after creating snapshot
	sm.applyRetentionPolicy(task.Dataset)
}

// stopScheduledTask stops a scheduled task
func (sm *ZFSSnapshotManager) stopScheduledTask(task *ScheduledTask) {
	if task.ticker != nil {
		task.ticker.Stop()
	}
	if task.stopChan != nil {
		close(task.stopChan)
	}
	task.Enabled = false
}

// applyRetentionPolicy applies retention policy to remove old snapshots
func (sm *ZFSSnapshotManager) applyRetentionPolicy(dataset string) {
	snapshotLogger.Debugf("Applying retention policy for dataset %s", dataset)
	
	sm.mu.RLock()
	snapshots, exists := sm.snapshots[dataset]
	sm.mu.RUnlock()
	
	if !exists || len(snapshots) == 0 {
		return
	}
	
	// Group snapshots by type (hourly, daily, weekly, monthly, yearly)
	snapshotGroups := sm.groupSnapshotsByAge(snapshots)
	
	// Apply retention rules for each group
	toDelete := make([]*Snapshot, 0)
	
	if len(snapshotGroups["hourly"]) > sm.retentionPolicy.HourlySnapshots {
		excess := snapshotGroups["hourly"][sm.retentionPolicy.HourlySnapshots:]
		toDelete = append(toDelete, excess...)
	}
	
	if len(snapshotGroups["daily"]) > sm.retentionPolicy.DailySnapshots {
		excess := snapshotGroups["daily"][sm.retentionPolicy.DailySnapshots:]
		toDelete = append(toDelete, excess...)
	}
	
	if len(snapshotGroups["weekly"]) > sm.retentionPolicy.WeeklySnapshots {
		excess := snapshotGroups["weekly"][sm.retentionPolicy.WeeklySnapshots:]
		toDelete = append(toDelete, excess...)
	}
	
	if len(snapshotGroups["monthly"]) > sm.retentionPolicy.MonthlySnapshots {
		excess := snapshotGroups["monthly"][sm.retentionPolicy.MonthlySnapshots:]
		toDelete = append(toDelete, excess...)
	}
	
	if len(snapshotGroups["yearly"]) > sm.retentionPolicy.YearlySnapshots {
		excess := snapshotGroups["yearly"][sm.retentionPolicy.YearlySnapshots:]
		toDelete = append(toDelete, excess...)
	}
	
	// Delete excess snapshots
	for _, snapshot := range toDelete {
		ctx, cancel := context.WithTimeout(sm.ctx, 2*time.Minute)
		if err := sm.DeleteSnapshot(ctx, snapshot.Dataset, snapshot.Name); err != nil {
			snapshotLogger.Errorf("Failed to delete snapshot %s: %v", snapshot.FullName, err)
		}
		cancel()
	}
	
	if len(toDelete) > 0 {
		snapshotLogger.Infof("Deleted %d snapshots for dataset %s according to retention policy", len(toDelete), dataset)
	}
}

// groupSnapshotsByAge groups snapshots by their age category
func (sm *ZFSSnapshotManager) groupSnapshotsByAge(snapshots []*Snapshot) map[string][]*Snapshot {
	groups := map[string][]*Snapshot{
		"hourly":  make([]*Snapshot, 0),
		"daily":   make([]*Snapshot, 0),
		"weekly":  make([]*Snapshot, 0),
		"monthly": make([]*Snapshot, 0),
		"yearly":  make([]*Snapshot, 0),
	}
	
	now := time.Now()
	
	// Sort snapshots by creation time (newest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].CreatedAt.After(snapshots[j].CreatedAt)
	})
	
	for _, snapshot := range snapshots {
		age := now.Sub(snapshot.CreatedAt)
		
		switch {
		case age < 24*time.Hour:
			groups["hourly"] = append(groups["hourly"], snapshot)
		case age < 7*24*time.Hour:
			groups["daily"] = append(groups["daily"], snapshot)
		case age < 30*24*time.Hour:
			groups["weekly"] = append(groups["weekly"], snapshot)
		case age < 365*24*time.Hour:
			groups["monthly"] = append(groups["monthly"], snapshot)
		default:
			groups["yearly"] = append(groups["yearly"], snapshot)
		}
	}
	
	return groups
}

// getSnapshotInfo retrieves detailed information about a snapshot
func (sm *ZFSSnapshotManager) getSnapshotInfo(ctx context.Context, dataset, snapshot string) (*Snapshot, error) {
	fullName := fmt.Sprintf("%s@%s", dataset, snapshot)
	
	// Get snapshot properties
	cmd := exec.CommandContext(ctx, "zfs", "get", "-H", "-p", "-o", "property,value", 
		"creation,used,referenced,compressratio", fullName)
	
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot properties: %w", err)
	}
	
	properties := make(map[string]string)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	
	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			properties[parts[0]] = parts[1]
		}
	}
	
	// Parse creation time
	var createdAt time.Time
	if creationStr, exists := properties["creation"]; exists {
		if timestamp, err := strconv.ParseInt(creationStr, 10, 64); err == nil {
			createdAt = time.Unix(timestamp, 0)
		}
	}
	
	// Parse sizes
	var used, referenced int64
	if usedStr, exists := properties["used"]; exists {
		used, _ = strconv.ParseInt(usedStr, 10, 64)
	}
	if refStr, exists := properties["referenced"]; exists {
		referenced, _ = strconv.ParseInt(refStr, 10, 64)
	}
	
	return &Snapshot{
		Name:       snapshot,
		Dataset:    dataset,
		FullName:   fullName,
		CreatedAt:  createdAt,
		Used:       used,
		Referenced: referenced,
		Properties: properties,
		Type:       SnapshotTypeManual,
		Status:     SnapshotStatusActive,
	}, nil
}

// loadExistingSnapshots loads existing snapshots from ZFS
func (sm *ZFSSnapshotManager) loadExistingSnapshots() error {
	snapshotLogger.Info("Loading existing ZFS snapshots")
	
	// Get all snapshots
	cmd := exec.Command("zfs", "list", "-t", "snapshot", "-H", "-o", "name")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list existing snapshots: %w", err)
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	
	for _, line := range lines {
		if line == "" {
			continue
		}
		
		// Parse dataset@snapshot format
		parts := strings.Split(line, "@")
		if len(parts) != 2 {
			continue
		}
		
		dataset := parts[0]
		snapshotName := parts[1]
		
		// Get snapshot info
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		snapshot, err := sm.getSnapshotInfo(ctx, dataset, snapshotName)
		cancel()
		
		if err != nil {
			snapshotLogger.Warnf("Failed to get info for snapshot %s: %v", line, err)
			continue
		}
		
		// Add to tracking
		if sm.snapshots[dataset] == nil {
			sm.snapshots[dataset] = make([]*Snapshot, 0)
		}
		sm.snapshots[dataset] = append(sm.snapshots[dataset], snapshot)
	}
	
	totalSnapshots := 0
	for _, snapshots := range sm.snapshots {
		totalSnapshots += len(snapshots)
	}
	
	snapshotLogger.Infof("Loaded %d existing snapshots across %d datasets", totalSnapshots, len(sm.snapshots))
	return nil
}

// cleanupLoop runs periodic cleanup operations
func (sm *ZFSSnapshotManager) cleanupLoop() {
	ticker := time.NewTicker(sm.config.CleanupInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			sm.performCleanup()
		case <-sm.ctx.Done():
			return
		}
	}
}

// performCleanup performs cleanup operations
func (sm *ZFSSnapshotManager) performCleanup() {
	snapshotLogger.Debug("Performing snapshot cleanup")
	
	sm.mu.RLock()
	datasets := make([]string, 0, len(sm.snapshots))
	for dataset := range sm.snapshots {
		datasets = append(datasets, dataset)
	}
	sm.mu.RUnlock()
	
	// Apply retention policy to all datasets
	for _, dataset := range datasets {
		sm.applyRetentionPolicy(dataset)
	}
}

// metricsLoop collects and updates metrics
func (sm *ZFSSnapshotManager) metricsLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			sm.collectMetrics()
		case <-sm.ctx.Done():
			return
		}
	}
}

// collectMetrics collects current metrics
func (sm *ZFSSnapshotManager) collectMetrics() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	totalSnapshots := int64(0)
	totalStorage := int64(0)
	
	for _, snapshots := range sm.snapshots {
		totalSnapshots += int64(len(snapshots))
		for _, snapshot := range snapshots {
			totalStorage += snapshot.Used
		}
	}
	
	sm.metrics.TotalSnapshots = totalSnapshots
	sm.metrics.TotalStorageUsed = totalStorage
	sm.metrics.LastUpdate = time.Now()
}

// updateMetrics updates metrics after operations
func (sm *ZFSSnapshotManager) updateMetrics(success bool, creationTime, deletionTime int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	
	if success {
		if creationTime > 0 {
			sm.metrics.SnapshotsCreated++
			// Update average creation time
			if sm.metrics.SnapshotsCreated == 1 {
				sm.metrics.AverageCreationTime = float64(creationTime)
			} else {
				sm.metrics.AverageCreationTime = (sm.metrics.AverageCreationTime*float64(sm.metrics.SnapshotsCreated-1) + float64(creationTime)) / float64(sm.metrics.SnapshotsCreated)
			}
		}
		
		if deletionTime > 0 {
			sm.metrics.SnapshotsDeleted++
			// Update average deletion time
			if sm.metrics.SnapshotsDeleted == 1 {
				sm.metrics.AverageDeletionTime = float64(deletionTime)
			} else {
				sm.metrics.AverageDeletionTime = (sm.metrics.AverageDeletionTime*float64(sm.metrics.SnapshotsDeleted-1) + float64(deletionTime)) / float64(sm.metrics.SnapshotsDeleted)
			}
		}
	} else {
		sm.metrics.FailedOperations++
	}
}

// GetMetrics returns current snapshot metrics
func (sm *ZFSSnapshotManager) GetMetrics() *SnapshotMetrics {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	// Create a copy to avoid race conditions
	metrics := *sm.metrics
	return &metrics
}

// GetSnapshotsByDataset returns all snapshots for a specific dataset
func (sm *ZFSSnapshotManager) GetSnapshotsByDataset(dataset string) []*Snapshot {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	snapshots, exists := sm.snapshots[dataset]
	if !exists {
		return []*Snapshot{}
	}
	
	// Return a copy to avoid race conditions
	result := make([]*Snapshot, len(snapshots))
	copy(result, snapshots)
	return result
}

// GetScheduledTasks returns all scheduled tasks
func (sm *ZFSSnapshotManager) GetScheduledTasks() map[string]*ScheduledTask {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	result := make(map[string]*ScheduledTask)
	for k, v := range sm.scheduledTasks {
		taskCopy := *v
		result[k] = &taskCopy
	}
	return result
}

// VerifySnapshot verifies the integrity of a snapshot
func (sm *ZFSSnapshotManager) VerifySnapshot(ctx context.Context, dataset, snapshot string) error {
	if !sm.config.VerifyChecksums {
		return nil
	}
	
	fullName := fmt.Sprintf("%s@%s", dataset, snapshot)
	
	// Use ZFS scrub to verify snapshot integrity
	cmd := exec.CommandContext(ctx, "zfs", "scrub", fullName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("snapshot verification failed: %w", err)
	}
	
	snapshotLogger.Infof("Snapshot %s verified successfully", fullName)
	return nil
}

// CloneSnapshot creates a clone from a snapshot
func (sm *ZFSSnapshotManager) CloneSnapshot(ctx context.Context, dataset, snapshot, cloneName string) error {
	sourceSnapshot := fmt.Sprintf("%s@%s", dataset, snapshot)
	
	cmd := exec.CommandContext(ctx, "zfs", "clone", sourceSnapshot, cloneName)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to clone snapshot: %w", err)
	}
	
	snapshotLogger.Infof("Successfully cloned snapshot %s to %s", sourceSnapshot, cloneName)
	return nil
}