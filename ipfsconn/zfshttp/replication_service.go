// Package zfshttp implements ZFS replication service for IPFS Cluster
package zfshttp

import (
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var replicationLogger = logging.Logger("zfs-replication")

// SnapshotManager interface for managing ZFS snapshots
type SnapshotManager interface {
	// CreateSnapshot creates a new ZFS snapshot
	CreateSnapshot(ctx context.Context, dataset, name string) error
	// GetPreviousSnapshot gets the previous snapshot for incremental replication
	GetPreviousSnapshot(ctx context.Context, dataset, currentSnapshot string) (string, error)
	// ListSnapshots lists all snapshots for a dataset
	ListSnapshots(ctx context.Context, dataset string) ([]string, error)
	// DeleteSnapshot deletes a snapshot
	DeleteSnapshot(ctx context.Context, dataset, snapshot string) error
	// RollbackToSnapshot rolls back a dataset to a specific snapshot
	RollbackToSnapshot(ctx context.Context, dataset, snapshot string) error
	// GetSnapshotInfo gets information about a snapshot
	GetSnapshotInfo(ctx context.Context, dataset, snapshot string) (*SnapshotInfo, error)
}

// ReplicationService manages ZFS replication between cluster nodes
type ReplicationService struct {
	config           *ReplicationConfig
	snapshotManager  SnapshotManager
	consensusClient  ConsensusClient
	nodeManager      NodeManager
	
	// State management
	ctx              context.Context
	cancel           context.CancelFunc
	mu               sync.RWMutex
	
	// Active replication jobs
	activeJobs       map[string]*ReplicationJob
	jobQueue         chan *ReplicationJob
	
	// Metrics and monitoring
	metrics          *ReplicationMetrics
	lastHealthCheck  time.Time
}

// ReplicationConfig holds configuration for the replication service
type ReplicationConfig struct {
	ReplicationFactor    int           `json:"replication_factor"`
	SnapshotInterval     time.Duration `json:"snapshot_interval"`
	MaxConcurrentJobs    int           `json:"max_concurrent_jobs"`
	CompressionLevel     int           `json:"compression_level"`
	NetworkTimeout       time.Duration `json:"network_timeout"`
	RetryAttempts        int           `json:"retry_attempts"`
	RetryBackoff         time.Duration `json:"retry_backoff"`
	EnableIncremental    bool          `json:"enable_incremental"`
	CrossDatacenterSync  bool          `json:"cross_datacenter_sync"`
}

// ReplicationJob represents a single replication operation
type ReplicationJob struct {
	ID               string            `json:"id"`
	SourceDataset    string            `json:"source_dataset"`
	TargetNodes      []string          `json:"target_nodes"`
	SnapshotName     string            `json:"snapshot_name"`
	IsIncremental    bool              `json:"is_incremental"`
	CompressionType  string            `json:"compression_type"`
	Status           ReplicationStatus `json:"status"`
	Progress         float64           `json:"progress"`
	StartTime        time.Time         `json:"start_time"`
	EndTime          time.Time         `json:"end_time"`
	Error            string            `json:"error,omitempty"`
	BytesTransferred int64             `json:"bytes_transferred"`
	EstimatedSize    int64             `json:"estimated_size"`
}

// ReplicationStatus represents the status of a replication job
type ReplicationStatus int

const (
	ReplicationStatusPending ReplicationStatus = iota
	ReplicationStatusRunning
	ReplicationStatusCompleted
	ReplicationStatusFailed
	ReplicationStatusCancelled
)

// ReplicationMetrics holds metrics for the replication service
type ReplicationMetrics struct {
	TotalJobs           int64     `json:"total_jobs"`
	CompletedJobs       int64     `json:"completed_jobs"`
	FailedJobs          int64     `json:"failed_jobs"`
	ActiveJobs          int64     `json:"active_jobs"`
	AverageJobDuration  float64   `json:"average_job_duration"`
	TotalBytesReplicated int64    `json:"total_bytes_replicated"`
	ReplicationRate     float64   `json:"replication_rate"`
	LastUpdate          time.Time `json:"last_update"`
}

// ConsensusClient interface for IPFS Cluster consensus integration
type ConsensusClient interface {
	// Coordinate replication operations with cluster consensus
	CoordinateReplication(ctx context.Context, job *ReplicationJob) error
	// Get cluster state for replication decisions
	GetClusterState(ctx context.Context) (*ClusterState, error)
	// Register replication event
	RegisterReplicationEvent(ctx context.Context, event *ReplicationEvent) error
}

// NodeManager interface for managing cluster nodes
type NodeManager interface {
	// Get list of available nodes for replication
	GetAvailableNodes(ctx context.Context) ([]Node, error)
	// Check node health and availability
	CheckNodeHealth(ctx context.Context, nodeID string) (*NodeHealth, error)
	// Get node network information
	GetNodeNetworkInfo(ctx context.Context, nodeID string) (*NodeNetworkInfo, error)
	// Get node by ID
	GetNodeByID(ctx context.Context, nodeID string) (*Node, error)
	// Update node status
	UpdateNodeStatus(ctx context.Context, nodeID string, status NodeStatus) error
}

// ClusterState represents the current state of the cluster
type ClusterState struct {
	Nodes          []Node    `json:"nodes"`
	ActivePins     int64     `json:"active_pins"`
	TotalStorage   int64     `json:"total_storage"`
	UsedStorage    int64     `json:"used_storage"`
	LastUpdated    time.Time `json:"last_updated"`
}

// Node represents a cluster node
type Node struct {
	ID              string            `json:"id"`
	Address         string            `json:"address"`
	Status          NodeStatus        `json:"status"`
	Datacenter      string            `json:"datacenter"`
	StorageCapacity int64             `json:"storage_capacity"`
	UsedStorage     int64             `json:"used_storage"`
	ZFSPools        []string          `json:"zfs_pools"`
	LastSeen        time.Time         `json:"last_seen"`
}

// NodeStatus represents the status of a cluster node
type NodeStatus int

const (
	NodeStatusOnline NodeStatus = iota
	NodeStatusOffline
	NodeStatusDegraded
	NodeStatusMaintenance
)

// NodeHealth represents health information for a node
type NodeHealth struct {
	NodeID          string    `json:"node_id"`
	IsHealthy       bool      `json:"is_healthy"`
	CPUUsage        float64   `json:"cpu_usage"`
	MemoryUsage     float64   `json:"memory_usage"`
	DiskUsage       float64   `json:"disk_usage"`
	NetworkLatency  float64   `json:"network_latency"`
	LastCheck       time.Time `json:"last_check"`
}

// NodeNetworkInfo represents network information for a node
type NodeNetworkInfo struct {
	NodeID          string `json:"node_id"`
	IPAddress       string `json:"ip_address"`
	Port            int    `json:"port"`
	Bandwidth       int64  `json:"bandwidth"`
	Protocol        string `json:"protocol"`
}

// ReplicationEvent represents an event in the replication process
type ReplicationEvent struct {
	JobID       string                 `json:"job_id"`
	EventType   ReplicationEventType   `json:"event_type"`
	Timestamp   time.Time              `json:"timestamp"`
	NodeID      string                 `json:"node_id"`
	Details     map[string]interface{} `json:"details"`
}

// ReplicationEventType represents the type of replication event
type ReplicationEventType int

const (
	ReplicationEventStarted ReplicationEventType = iota
	ReplicationEventProgress
	ReplicationEventCompleted
	ReplicationEventFailed
	ReplicationEventCancelled
)

// NewReplicationService creates a new ZFS replication service
func NewReplicationService(config *ReplicationConfig, snapshotManager SnapshotManager, 
	consensusClient ConsensusClient, nodeManager NodeManager) *ReplicationService {
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &ReplicationService{
		config:          config,
		snapshotManager: snapshotManager,
		consensusClient: consensusClient,
		nodeManager:     nodeManager,
		ctx:             ctx,
		cancel:          cancel,
		activeJobs:      make(map[string]*ReplicationJob),
		jobQueue:        make(chan *ReplicationJob, config.MaxConcurrentJobs*2),
		metrics:         &ReplicationMetrics{},
	}
}

// Start initializes and starts the replication service
func (rs *ReplicationService) Start() error {
	replicationLogger.Info("Starting ZFS replication service")
	
	// Start job processing workers
	for i := 0; i < rs.config.MaxConcurrentJobs; i++ {
		go rs.jobWorker(i)
	}
	
	// Start periodic health checks
	go rs.healthCheckLoop()
	
	// Start metrics collection
	go rs.metricsCollectionLoop()
	
	replicationLogger.Info("ZFS replication service started successfully")
	return nil
}

// Stop gracefully shuts down the replication service
func (rs *ReplicationService) Stop() error {
	replicationLogger.Info("Stopping ZFS replication service")
	
	rs.cancel()
	
	// Wait for active jobs to complete or timeout
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	
	for {
		rs.mu.RLock()
		activeCount := len(rs.activeJobs)
		rs.mu.RUnlock()
		
		if activeCount == 0 {
			break
		}
		
		select {
		case <-timeout.C:
			replicationLogger.Warn("Timeout waiting for active jobs to complete")
			return nil
		case <-time.After(1 * time.Second):
			// Continue waiting
		}
	}
	
	replicationLogger.Info("ZFS replication service stopped")
	return nil
}

// ReplicateDataset initiates replication of a dataset to target nodes
func (rs *ReplicationService) ReplicateDataset(ctx context.Context, dataset string, targetNodes []string) (*ReplicationJob, error) {
	replicationLogger.Infof("Starting replication of dataset %s to nodes %v", dataset, targetNodes)
	
	// Create snapshot for replication
	snapshotName := fmt.Sprintf("repl-%d", time.Now().Unix())
	if err := rs.snapshotManager.CreateSnapshot(ctx, dataset, snapshotName); err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}
	
	// Create replication job
	job := &ReplicationJob{
		ID:              generateJobID(),
		SourceDataset:   dataset,
		TargetNodes:     targetNodes,
		SnapshotName:    snapshotName,
		IsIncremental:   rs.config.EnableIncremental,
		CompressionType: "gzip",
		Status:          ReplicationStatusPending,
		StartTime:       time.Now(),
	}
	
	// Coordinate with cluster consensus
	if err := rs.consensusClient.CoordinateReplication(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to coordinate replication: %w", err)
	}
	
	// Add job to queue
	rs.mu.Lock()
	rs.activeJobs[job.ID] = job
	rs.mu.Unlock()
	
	select {
	case rs.jobQueue <- job:
		replicationLogger.Infof("Replication job %s queued successfully", job.ID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	
	return job, nil
}

// ReplicateIncremental performs incremental replication using ZFS send/receive
func (rs *ReplicationService) ReplicateIncremental(ctx context.Context, job *ReplicationJob) error {
	replicationLogger.Infof("Starting incremental replication for job %s", job.ID)
	
	// Get previous snapshot for incremental send
	previousSnapshot, err := rs.snapshotManager.GetPreviousSnapshot(ctx, job.SourceDataset, job.SnapshotName)
	if err != nil {
		replicationLogger.Warnf("No previous snapshot found, falling back to full replication: %v", err)
		return rs.ReplicateFull(ctx, job)
	}
	
	for _, targetNode := range job.TargetNodes {
		if err := rs.sendIncrementalSnapshot(ctx, job, targetNode, previousSnapshot); err != nil {
			return fmt.Errorf("failed to send incremental snapshot to node %s: %w", targetNode, err)
		}
	}
	
	return nil
}

// ReplicateFull performs full replication using ZFS send/receive
func (rs *ReplicationService) ReplicateFull(ctx context.Context, job *ReplicationJob) error {
	replicationLogger.Infof("Starting full replication for job %s", job.ID)
	
	for _, targetNode := range job.TargetNodes {
		if err := rs.sendFullSnapshot(ctx, job, targetNode); err != nil {
			return fmt.Errorf("failed to send full snapshot to node %s: %w", targetNode, err)
		}
	}
	
	return nil
}

// sendIncrementalSnapshot sends an incremental snapshot to a target node
func (rs *ReplicationService) sendIncrementalSnapshot(ctx context.Context, job *ReplicationJob, targetNode, previousSnapshot string) error {
	nodeInfo, err := rs.nodeManager.GetNodeNetworkInfo(ctx, targetNode)
	if err != nil {
		return fmt.Errorf("failed to get node network info: %w", err)
	}
	
	// Construct ZFS send command for incremental replication
	sourceSnapshot := fmt.Sprintf("%s@%s", job.SourceDataset, job.SnapshotName)
	previousSnapshotFull := fmt.Sprintf("%s@%s", job.SourceDataset, previousSnapshot)
	
	cmd := exec.CommandContext(ctx, "zfs", "send", "-i", previousSnapshotFull, sourceSnapshot)
	
	// Add compression if enabled
	if rs.config.CompressionLevel > 0 {
		cmd.Args = append(cmd.Args, "-c")
	}
	
	// Pipe to SSH for remote transfer
	sshCmd := exec.CommandContext(ctx, "ssh", 
		fmt.Sprintf("%s@%s", "zfs-replication", nodeInfo.IPAddress),
		fmt.Sprintf("zfs receive -F %s", job.SourceDataset))
	
	// Connect commands via pipe
	sshCmd.Stdin, err = cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}
	
	// Start both commands
	if err := sshCmd.Start(); err != nil {
		return fmt.Errorf("failed to start SSH command: %w", err)
	}
	
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start ZFS send command: %w", err)
	}
	
	// Wait for completion
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("ZFS send failed: %w", err)
	}
	
	if err := sshCmd.Wait(); err != nil {
		return fmt.Errorf("SSH receive failed: %w", err)
	}
	
	replicationLogger.Infof("Incremental snapshot sent successfully to node %s", targetNode)
	return nil
}

// sendFullSnapshot sends a full snapshot to a target node
func (rs *ReplicationService) sendFullSnapshot(ctx context.Context, job *ReplicationJob, targetNode string) error {
	nodeInfo, err := rs.nodeManager.GetNodeNetworkInfo(ctx, targetNode)
	if err != nil {
		return fmt.Errorf("failed to get node network info: %w", err)
	}
	
	// Construct ZFS send command for full replication
	sourceSnapshot := fmt.Sprintf("%s@%s", job.SourceDataset, job.SnapshotName)
	
	cmd := exec.CommandContext(ctx, "zfs", "send")
	
	// Add compression if enabled
	if rs.config.CompressionLevel > 0 {
		cmd.Args = append(cmd.Args, "-c")
	}
	
	cmd.Args = append(cmd.Args, sourceSnapshot)
	
	// Pipe to SSH for remote transfer
	sshCmd := exec.CommandContext(ctx, "ssh",
		fmt.Sprintf("%s@%s", "zfs-replication", nodeInfo.IPAddress),
		fmt.Sprintf("zfs receive -F %s", job.SourceDataset))
	
	// Connect commands via pipe
	sshCmd.Stdin, err = cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}
	
	// Start both commands
	if err := sshCmd.Start(); err != nil {
		return fmt.Errorf("failed to start SSH command: %w", err)
	}
	
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start ZFS send command: %w", err)
	}
	
	// Wait for completion
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("ZFS send failed: %w", err)
	}
	
	if err := sshCmd.Wait(); err != nil {
		return fmt.Errorf("SSH receive failed: %w", err)
	}
	
	replicationLogger.Infof("Full snapshot sent successfully to node %s", targetNode)
	return nil
}

// jobWorker processes replication jobs from the queue
func (rs *ReplicationService) jobWorker(workerID int) {
	replicationLogger.Infof("Starting replication worker %d", workerID)
	
	for {
		select {
		case job := <-rs.jobQueue:
			rs.processJob(job)
		case <-rs.ctx.Done():
			replicationLogger.Infof("Stopping replication worker %d", workerID)
			return
		}
	}
}

// processJob processes a single replication job
func (rs *ReplicationService) processJob(job *ReplicationJob) {
	replicationLogger.Infof("Processing replication job %s", job.ID)
	
	// Update job status
	rs.mu.Lock()
	job.Status = ReplicationStatusRunning
	rs.mu.Unlock()
	
	// Register start event
	event := &ReplicationEvent{
		JobID:     job.ID,
		EventType: ReplicationEventStarted,
		Timestamp: time.Now(),
		Details:   map[string]interface{}{"dataset": job.SourceDataset},
	}
	rs.consensusClient.RegisterReplicationEvent(rs.ctx, event)
	
	var err error
	if job.IsIncremental {
		err = rs.ReplicateIncremental(rs.ctx, job)
	} else {
		err = rs.ReplicateFull(rs.ctx, job)
	}
	
	// Update job completion status
	rs.mu.Lock()
	job.EndTime = time.Now()
	if err != nil {
		job.Status = ReplicationStatusFailed
		job.Error = err.Error()
		replicationLogger.Errorf("Replication job %s failed: %v", job.ID, err)
	} else {
		job.Status = ReplicationStatusCompleted
		replicationLogger.Infof("Replication job %s completed successfully", job.ID)
	}
	rs.mu.Unlock()
	
	// Register completion event
	eventType := ReplicationEventCompleted
	if err != nil {
		eventType = ReplicationEventFailed
	}
	
	event = &ReplicationEvent{
		JobID:     job.ID,
		EventType: eventType,
		Timestamp: time.Now(),
		Details:   map[string]interface{}{"error": job.Error},
	}
	rs.consensusClient.RegisterReplicationEvent(rs.ctx, event)
	
	// Update metrics
	rs.updateMetrics(job)
	
	// Clean up completed job after delay
	go func() {
		time.Sleep(5 * time.Minute)
		rs.mu.Lock()
		delete(rs.activeJobs, job.ID)
		rs.mu.Unlock()
	}()
}

// GetJobStatus returns the status of a replication job
func (rs *ReplicationService) GetJobStatus(jobID string) (*ReplicationJob, error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	
	job, exists := rs.activeJobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job %s not found", jobID)
	}
	
	return job, nil
}

// ListActiveJobs returns all active replication jobs
func (rs *ReplicationService) ListActiveJobs() []*ReplicationJob {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	
	jobs := make([]*ReplicationJob, 0, len(rs.activeJobs))
	for _, job := range rs.activeJobs {
		jobs = append(jobs, job)
	}
	
	return jobs
}

// CancelJob cancels a running replication job
func (rs *ReplicationService) CancelJob(jobID string) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	
	job, exists := rs.activeJobs[jobID]
	if !exists {
		return fmt.Errorf("job %s not found", jobID)
	}
	
	if job.Status != ReplicationStatusRunning && job.Status != ReplicationStatusPending {
		return fmt.Errorf("job %s cannot be cancelled (status: %v)", jobID, job.Status)
	}
	
	job.Status = ReplicationStatusCancelled
	replicationLogger.Infof("Replication job %s cancelled", jobID)
	
	return nil
}

// GetMetrics returns current replication metrics
func (rs *ReplicationService) GetMetrics() *ReplicationMetrics {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	
	// Create a copy to avoid race conditions
	metrics := *rs.metrics
	return &metrics
}

// healthCheckLoop performs periodic health checks
func (rs *ReplicationService) healthCheckLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rs.performHealthCheck()
		case <-rs.ctx.Done():
			return
		}
	}
}

// performHealthCheck checks the health of the replication service
func (rs *ReplicationService) performHealthCheck() {
	rs.mu.Lock()
	rs.lastHealthCheck = time.Now()
	rs.mu.Unlock()
	
	// Check for stuck jobs
	for _, job := range rs.activeJobs {
		if job.Status == ReplicationStatusRunning {
			duration := time.Since(job.StartTime)
			if duration > 2*time.Hour { // Configurable timeout
				replicationLogger.Warnf("Job %s has been running for %v, may be stuck", job.ID, duration)
			}
		}
	}
}

// metricsCollectionLoop collects and updates metrics
func (rs *ReplicationService) metricsCollectionLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			rs.collectMetrics()
		case <-rs.ctx.Done():
			return
		}
	}
}

// collectMetrics collects current metrics
func (rs *ReplicationService) collectMetrics() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	
	rs.metrics.ActiveJobs = int64(len(rs.activeJobs))
	rs.metrics.LastUpdate = time.Now()
}

// updateMetrics updates metrics after job completion
func (rs *ReplicationService) updateMetrics(job *ReplicationJob) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	
	rs.metrics.TotalJobs++
	
	if job.Status == ReplicationStatusCompleted {
		rs.metrics.CompletedJobs++
		duration := job.EndTime.Sub(job.StartTime).Seconds()
		rs.metrics.AverageJobDuration = (rs.metrics.AverageJobDuration*float64(rs.metrics.CompletedJobs-1) + duration) / float64(rs.metrics.CompletedJobs)
		rs.metrics.TotalBytesReplicated += job.BytesTransferred
	} else if job.Status == ReplicationStatusFailed {
		rs.metrics.FailedJobs++
	}
}

// generateJobID generates a unique job ID
func generateJobID() string {
	return fmt.Sprintf("repl-%d-%d", time.Now().Unix(), time.Now().Nanosecond())
}

// ExecuteReplicationJob executes a replication job directly
func (rs *ReplicationService) ExecuteReplicationJob(ctx context.Context, job *ReplicationJob) error {
	replicationLogger.Infof("Executing replication job: %s", job.ID)
	
	// Update job status
	rs.mu.Lock()
	job.Status = ReplicationStatusRunning
	job.StartTime = time.Now()
	rs.activeJobs[job.ID] = job
	rs.mu.Unlock()
	
	var err error
	if job.IsIncremental {
		err = rs.ReplicateIncremental(ctx, job)
	} else {
		err = rs.ReplicateFull(ctx, job)
	}
	
	// Update completion status
	rs.mu.Lock()
	job.EndTime = time.Now()
	if err != nil {
		job.Status = ReplicationStatusFailed
		job.Error = err.Error()
	} else {
		job.Status = ReplicationStatusCompleted
	}
	rs.mu.Unlock()
	
	// Update metrics
	rs.updateMetrics(job)
	
	return err
}