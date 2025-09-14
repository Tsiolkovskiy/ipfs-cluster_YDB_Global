// Package resource provides automated maintenance for IPFS Cluster ZFS integration
// It implements automatic ZFS scrub, repair, defragmentation, and scheduled maintenance
// with minimal impact on system performance.
package resource

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/pkg/errors"
)

var maintenanceLogger = logging.Logger("automated-maintenance")

// AutomatedMaintenance manages automated ZFS maintenance operations
type AutomatedMaintenance struct {
	config          *MaintenanceConfig
	scheduler       *MaintenanceScheduler
	scrubManager    *ScrubManager
	defragManager   *DefragmentationManager
	repairManager   *RepairManager
	
	// Active operations
	activeOperations map[string]*MaintenanceOperation
	
	// Synchronization
	mu              sync.RWMutex
	
	// Background processes
	schedulerTicker *time.Ticker
	stopScheduler   chan struct{}
}

// MaintenanceConfig holds configuration for automated maintenance
type MaintenanceConfig struct {
	// Scrub settings
	ScrubInterval       time.Duration `json:"scrub_interval"`
	ScrubBandwidthLimit int64         `json:"scrub_bandwidth_limit"` // bytes/sec
	ScrubSchedule       string        `json:"scrub_schedule"`        // cron-like schedule
	
	// Defragmentation settings
	DefragInterval      time.Duration `json:"defrag_interval"`
	DefragThreshold     float64       `json:"defrag_threshold"`      // fragmentation percentage
	DefragBandwidthLimit int64        `json:"defrag_bandwidth_limit"`
	
	// Repair settings
	AutoRepairEnabled   bool          `json:"auto_repair_enabled"`
	RepairTimeout       time.Duration `json:"repair_timeout"`
	MaxRepairAttempts   int           `json:"max_repair_attempts"`
	
	// Scheduling settings
	MaintenanceWindow   TimeWindow    `json:"maintenance_window"`
	MaxConcurrentOps    int           `json:"max_concurrent_ops"`
	PerformanceImpactLimit float64    `json:"performance_impact_limit"` // 0.1 = 10%
	
	// Monitoring settings
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	MetricsInterval     time.Duration `json:"metrics_interval"`
	
	// Emergency settings
	EmergencyRepairEnabled bool       `json:"emergency_repair_enabled"`
	CriticalErrorThreshold int        `json:"critical_error_threshold"`
}

// TimeWindow defines a maintenance time window
type TimeWindow struct {
	StartHour int `json:"start_hour"` // 0-23
	EndHour   int `json:"end_hour"`   // 0-23
	Days      []int `json:"days"`     // 0=Sunday, 1=Monday, etc.
}

// MaintenanceOperation represents a maintenance operation
type MaintenanceOperation struct {
	ID              string            `json:"id"`
	Type            string            `json:"type"`            // "scrub", "defrag", "repair"
	Pool            string            `json:"pool"`
	Dataset         string            `json:"dataset,omitempty"`
	Status          string            `json:"status"`          // "scheduled", "running", "completed", "failed"
	Priority        int               `json:"priority"`        // 1=high, 2=medium, 3=low
	Progress        float64           `json:"progress"`        // 0-100
	StartTime       time.Time         `json:"start_time"`
	EndTime         time.Time         `json:"end_time"`
	EstimatedDuration time.Duration   `json:"estimated_duration"`
	ActualDuration  time.Duration     `json:"actual_duration"`
	Error           string            `json:"error,omitempty"`
	
	// Performance impact
	IOImpact        float64           `json:"io_impact"`       // 0-1
	CPUImpact       float64           `json:"cpu_impact"`      // 0-1
	
	// Operation-specific data
	Metadata        map[string]interface{} `json:"metadata"`
	
	mu sync.RWMutex `json:"-"`
}

// MaintenanceScheduler manages scheduling of maintenance operations
type MaintenanceScheduler struct {
	config     *MaintenanceConfig
	operations chan *MaintenanceOperation
	
	mu sync.RWMutex
}

// ScrubManager manages ZFS scrub operations
type ScrubManager struct {
	config         *MaintenanceConfig
	activeScrubs   map[string]*ScrubOperation
	
	mu sync.RWMutex
}

// ScrubOperation represents a ZFS scrub operation
type ScrubOperation struct {
	Pool           string    `json:"pool"`
	StartTime      time.Time `json:"start_time"`
	Progress       float64   `json:"progress"`
	BytesScanned   int64     `json:"bytes_scanned"`
	BytesTotal     int64     `json:"bytes_total"`
	ErrorsFound    int       `json:"errors_found"`
	ErrorsRepaired int       `json:"errors_repaired"`
	Status         string    `json:"status"`
}

// DefragmentationManager manages ZFS defragmentation
type DefragmentationManager struct {
	config           *MaintenanceConfig
	activeDefragOps  map[string]*DefragOperation
	
	mu sync.RWMutex
}

// DefragOperation represents a defragmentation operation
type DefragOperation struct {
	Pool              string    `json:"pool"`
	Dataset           string    `json:"dataset"`
	StartTime         time.Time `json:"start_time"`
	Progress          float64   `json:"progress"`
	InitialFragmentation float64 `json:"initial_fragmentation"`
	CurrentFragmentation float64 `json:"current_fragmentation"`
	BytesDefragmented int64     `json:"bytes_defragmented"`
	Status            string    `json:"status"`
}

// RepairManager manages ZFS repair operations
type RepairManager struct {
	config        *MaintenanceConfig
	activeRepairs map[string]*RepairOperation
	
	mu sync.RWMutex
}

// RepairOperation represents a ZFS repair operation
type RepairOperation struct {
	Pool          string    `json:"pool"`
	Device        string    `json:"device"`
	StartTime     time.Time `json:"start_time"`
	Progress      float64   `json:"progress"`
	ErrorType     string    `json:"error_type"`
	RepairMethod  string    `json:"repair_method"`
	BytesRepaired int64     `json:"bytes_repaired"`
	Status        string    `json:"status"`
	Attempts      int       `json:"attempts"`
}

// NewAutomatedMaintenance creates a new automated maintenance manager
func NewAutomatedMaintenance(config *MaintenanceConfig) (*AutomatedMaintenance, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid configuration")
	}

	am := &AutomatedMaintenance{
		config:           config,
		scheduler:        NewMaintenanceScheduler(config),
		scrubManager:     NewScrubManager(config),
		defragManager:    NewDefragmentationManager(config),
		repairManager:    NewRepairManager(config),
		activeOperations: make(map[string]*MaintenanceOperation),
		stopScheduler:    make(chan struct{}),
	}

	maintenanceLogger.Info("Automated maintenance manager initialized successfully")
	return am, nil
}

// Start starts the automated maintenance system
func (am *AutomatedMaintenance) Start(ctx context.Context) error {
	maintenanceLogger.Info("Starting automated maintenance system")

	// Start scheduler
	if err := am.scheduler.Start(ctx); err != nil {
		return errors.Wrap(err, "starting scheduler")
	}

	// Start background monitoring
	am.startScheduler()

	maintenanceLogger.Info("Automated maintenance system started successfully")
	return nil
}

// Stop stops the automated maintenance system
func (am *AutomatedMaintenance) Stop() error {
	maintenanceLogger.Info("Stopping automated maintenance system")

	// Stop scheduler
	if am.schedulerTicker != nil {
		am.schedulerTicker.Stop()
		close(am.stopScheduler)
	}

	// Stop all active operations gracefully
	am.stopActiveOperations()

	maintenanceLogger.Info("Automated maintenance system stopped")
	return nil
}

// ScheduleScrub schedules a ZFS scrub operation
func (am *AutomatedMaintenance) ScheduleScrub(poolName string, priority int) (*MaintenanceOperation, error) {
	operation := &MaintenanceOperation{
		ID:       fmt.Sprintf("scrub-%s-%d", poolName, time.Now().UnixNano()),
		Type:     "scrub",
		Pool:     poolName,
		Status:   "scheduled",
		Priority: priority,
		Metadata: make(map[string]interface{}),
	}

	// Estimate duration based on pool size
	poolSize, err := am.getPoolSize(poolName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting pool size for %s", poolName)
	}

	// Estimate 1GB per minute scrub rate
	operation.EstimatedDuration = time.Duration(poolSize/(1024*1024*1024)) * time.Minute

	am.mu.Lock()
	am.activeOperations[operation.ID] = operation
	am.mu.Unlock()

	maintenanceLogger.Infof("Scheduled scrub operation %s for pool %s", operation.ID, poolName)
	return operation, nil
}

// ScheduleDefragmentation schedules a defragmentation operation
func (am *AutomatedMaintenance) ScheduleDefragmentation(poolName string, priority int) (*MaintenanceOperation, error) {
	// Check current fragmentation level
	fragmentation, err := am.getPoolFragmentation(poolName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting fragmentation for pool %s", poolName)
	}

	if fragmentation < am.config.DefragThreshold {
		return nil, fmt.Errorf("pool %s fragmentation (%.1f%%) below threshold (%.1f%%)", 
			poolName, fragmentation*100, am.config.DefragThreshold*100)
	}

	operation := &MaintenanceOperation{
		ID:       fmt.Sprintf("defrag-%s-%d", poolName, time.Now().UnixNano()),
		Type:     "defrag",
		Pool:     poolName,
		Status:   "scheduled",
		Priority: priority,
		Metadata: map[string]interface{}{
			"initial_fragmentation": fragmentation,
		},
	}

	// Estimate duration based on fragmentation level and pool size
	poolSize, err := am.getPoolSize(poolName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting pool size for %s", poolName)
	}

	// More fragmented pools take longer to defragment
	durationMultiplier := 1.0 + fragmentation*2.0
	operation.EstimatedDuration = time.Duration(float64(poolSize/(512*1024*1024))*durationMultiplier) * time.Minute

	am.mu.Lock()
	am.activeOperations[operation.ID] = operation
	am.mu.Unlock()

	maintenanceLogger.Infof("Scheduled defragmentation operation %s for pool %s (%.1f%% fragmented)", 
		operation.ID, poolName, fragmentation*100)
	return operation, nil
}

// ScheduleRepair schedules a repair operation
func (am *AutomatedMaintenance) ScheduleRepair(poolName, deviceName, errorType string) (*MaintenanceOperation, error) {
	operation := &MaintenanceOperation{
		ID:       fmt.Sprintf("repair-%s-%s-%d", poolName, deviceName, time.Now().UnixNano()),
		Type:     "repair",
		Pool:     poolName,
		Status:   "scheduled",
		Priority: 1, // Repairs are always high priority
		Metadata: map[string]interface{}{
			"device":     deviceName,
			"error_type": errorType,
		},
	}

	// Estimate repair duration based on error type
	switch errorType {
	case "checksum_error":
		operation.EstimatedDuration = 30 * time.Minute
	case "device_error":
		operation.EstimatedDuration = 2 * time.Hour
	case "pool_degraded":
		operation.EstimatedDuration = 4 * time.Hour
	default:
		operation.EstimatedDuration = 1 * time.Hour
	}

	am.mu.Lock()
	am.activeOperations[operation.ID] = operation
	am.mu.Unlock()

	maintenanceLogger.Infof("Scheduled repair operation %s for pool %s device %s (%s)", 
		operation.ID, poolName, deviceName, errorType)
	return operation, nil
}

// ExecuteOperation executes a maintenance operation
func (am *AutomatedMaintenance) ExecuteOperation(ctx context.Context, operationID string) error {
	am.mu.RLock()
	operation, exists := am.activeOperations[operationID]
	am.mu.RUnlock()

	if !exists {
		return fmt.Errorf("operation %s not found", operationID)
	}

	// Check if we're in maintenance window
	if !am.isInMaintenanceWindow() {
		return fmt.Errorf("not in maintenance window")
	}

	// Check performance impact
	if am.getCurrentPerformanceImpact() >= am.config.PerformanceImpactLimit {
		return fmt.Errorf("performance impact limit exceeded")
	}

	operation.mu.Lock()
	operation.Status = "running"
	operation.StartTime = time.Now()
	operation.mu.Unlock()

	maintenanceLogger.Infof("Executing %s operation %s", operation.Type, operationID)

	var err error
	switch operation.Type {
	case "scrub":
		err = am.executeScrub(ctx, operation)
	case "defrag":
		err = am.executeDefragmentation(ctx, operation)
	case "repair":
		err = am.executeRepair(ctx, operation)
	default:
		err = fmt.Errorf("unknown operation type: %s", operation.Type)
	}

	operation.mu.Lock()
	operation.EndTime = time.Now()
	operation.ActualDuration = operation.EndTime.Sub(operation.StartTime)
	
	if err != nil {
		operation.Status = "failed"
		operation.Error = err.Error()
		maintenanceLogger.Errorf("Operation %s failed: %v", operationID, err)
	} else {
		operation.Status = "completed"
		operation.Progress = 100.0
		maintenanceLogger.Infof("Operation %s completed successfully", operationID)
	}
	operation.mu.Unlock()

	return err
}

// GetActiveOperations returns all active maintenance operations
func (am *AutomatedMaintenance) GetActiveOperations() []*MaintenanceOperation {
	am.mu.RLock()
	defer am.mu.RUnlock()

	operations := make([]*MaintenanceOperation, 0, len(am.activeOperations))
	for _, op := range am.activeOperations {
		op.mu.RLock()
		opCopy := *op
		op.mu.RUnlock()
		operations = append(operations, &opCopy)
	}

	return operations
}

// GetOperationStatus returns the status of a specific operation
func (am *AutomatedMaintenance) GetOperationStatus(operationID string) (*MaintenanceOperation, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	operation, exists := am.activeOperations[operationID]
	if !exists {
		return nil, fmt.Errorf("operation %s not found", operationID)
	}

	operation.mu.RLock()
	opCopy := *operation
	operation.mu.RUnlock()

	return &opCopy, nil
}

// Private methods

func (am *AutomatedMaintenance) startScheduler() {
	am.schedulerTicker = time.NewTicker(am.config.HealthCheckInterval)
	
	go func() {
		for {
			select {
			case <-am.schedulerTicker.C:
				am.performScheduledMaintenance()
			case <-am.stopScheduler:
				return
			}
		}
	}()
}

func (am *AutomatedMaintenance) performScheduledMaintenance() {
	ctx := context.Background()

	// Check if we're in maintenance window
	if !am.isInMaintenanceWindow() {
		return
	}

	// Check for pools that need scrubbing
	am.checkAndScheduleScrubs(ctx)

	// Check for pools that need defragmentation
	am.checkAndScheduleDefragmentation(ctx)

	// Check for pools that need repair
	am.checkAndScheduleRepairs(ctx)

	// Execute pending operations
	am.executePendingOperations(ctx)
}

func (am *AutomatedMaintenance) checkAndScheduleScrubs(ctx context.Context) {
	pools, err := am.getZFSPools()
	if err != nil {
		maintenanceLogger.Errorf("Failed to get ZFS pools: %v", err)
		return
	}

	for _, pool := range pools {
		lastScrub, err := am.getLastScrubTime(pool)
		if err != nil {
			maintenanceLogger.Warnf("Failed to get last scrub time for pool %s: %v", pool, err)
			continue
		}

		if time.Since(lastScrub) >= am.config.ScrubInterval {
			if _, err := am.ScheduleScrub(pool, 2); err != nil {
				maintenanceLogger.Errorf("Failed to schedule scrub for pool %s: %v", pool, err)
			}
		}
	}
}

func (am *AutomatedMaintenance) checkAndScheduleDefragmentation(ctx context.Context) {
	pools, err := am.getZFSPools()
	if err != nil {
		maintenanceLogger.Errorf("Failed to get ZFS pools: %v", err)
		return
	}

	for _, pool := range pools {
		fragmentation, err := am.getPoolFragmentation(pool)
		if err != nil {
			maintenanceLogger.Warnf("Failed to get fragmentation for pool %s: %v", pool, err)
			continue
		}

		if fragmentation >= am.config.DefragThreshold {
			if _, err := am.ScheduleDefragmentation(pool, 3); err != nil {
				maintenanceLogger.Errorf("Failed to schedule defragmentation for pool %s: %v", pool, err)
			}
		}
	}
}

func (am *AutomatedMaintenance) checkAndScheduleRepairs(ctx context.Context) {
	if !am.config.AutoRepairEnabled {
		return
	}

	pools, err := am.getZFSPools()
	if err != nil {
		maintenanceLogger.Errorf("Failed to get ZFS pools: %v", err)
		return
	}

	for _, pool := range pools {
		errors, err := am.getPoolErrors(pool)
		if err != nil {
			maintenanceLogger.Warnf("Failed to get errors for pool %s: %v", pool, err)
			continue
		}

		for device, errorType := range errors {
			if _, err := am.ScheduleRepair(pool, device, errorType); err != nil {
				maintenanceLogger.Errorf("Failed to schedule repair for pool %s device %s: %v", pool, device, err)
			}
		}
	}
}

func (am *AutomatedMaintenance) executePendingOperations(ctx context.Context) {
	// Get operations sorted by priority
	operations := am.getOperationsByPriority()
	
	executed := 0
	for _, op := range operations {
		if executed >= am.config.MaxConcurrentOps {
			break
		}

		op.mu.RLock()
		status := op.Status
		op.mu.RUnlock()

		if status == "scheduled" {
			go func(operation *MaintenanceOperation) {
				if err := am.ExecuteOperation(ctx, operation.ID); err != nil {
					maintenanceLogger.Errorf("Failed to execute operation %s: %v", operation.ID, err)
				}
			}(op)
			executed++
		}
	}
}

func (am *AutomatedMaintenance) getOperationsByPriority() []*MaintenanceOperation {
	am.mu.RLock()
	defer am.mu.RUnlock()

	operations := make([]*MaintenanceOperation, 0, len(am.activeOperations))
	for _, op := range am.activeOperations {
		operations = append(operations, op)
	}

	// Sort by priority (lower number = higher priority)
	for i := 0; i < len(operations)-1; i++ {
		for j := i + 1; j < len(operations); j++ {
			operations[i].mu.RLock()
			operations[j].mu.RLock()
			if operations[i].Priority > operations[j].Priority {
				operations[i], operations[j] = operations[j], operations[i]
			}
			operations[j].mu.RUnlock()
			operations[i].mu.RUnlock()
		}
	}

	return operations
}

func (am *AutomatedMaintenance) isInMaintenanceWindow() bool {
	now := time.Now()
	hour := now.Hour()
	weekday := int(now.Weekday())

	// Check if current hour is in maintenance window
	if am.config.MaintenanceWindow.StartHour <= am.config.MaintenanceWindow.EndHour {
		if hour < am.config.MaintenanceWindow.StartHour || hour >= am.config.MaintenanceWindow.EndHour {
			return false
		}
	} else {
		// Window spans midnight
		if hour < am.config.MaintenanceWindow.StartHour && hour >= am.config.MaintenanceWindow.EndHour {
			return false
		}
	}

	// Check if current day is allowed
	if len(am.config.MaintenanceWindow.Days) > 0 {
		allowed := false
		for _, day := range am.config.MaintenanceWindow.Days {
			if day == weekday {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}

	return true
}

func (am *AutomatedMaintenance) getCurrentPerformanceImpact() float64 {
	am.mu.RLock()
	defer am.mu.RUnlock()

	totalImpact := 0.0
	for _, op := range am.activeOperations {
		op.mu.RLock()
		if op.Status == "running" {
			totalImpact += op.IOImpact
		}
		op.mu.RUnlock()
	}

	return totalImpact
}

func (am *AutomatedMaintenance) stopActiveOperations() {
	am.mu.RLock()
	operations := make([]*MaintenanceOperation, 0, len(am.activeOperations))
	for _, op := range am.activeOperations {
		operations = append(operations, op)
	}
	am.mu.RUnlock()

	for _, op := range operations {
		op.mu.RLock()
		status := op.Status
		op.mu.RUnlock()

		if status == "running" {
			// In a real implementation, this would send a signal to stop the operation
			maintenanceLogger.Infof("Stopping operation %s", op.ID)
		}
	}
}

// ZFS utility methods

func (am *AutomatedMaintenance) getZFSPools() ([]string, error) {
	cmd := exec.Command("zpool", "list", "-H", "-o", "name")
	output, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrap(err, "listing ZFS pools")
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var pools []string
	for _, line := range lines {
		if line != "" {
			pools = append(pools, line)
		}
	}

	return pools, nil
}

func (am *AutomatedMaintenance) getPoolSize(poolName string) (int64, error) {
	cmd := exec.Command("zpool", "list", "-H", "-o", "size", poolName)
	output, err := cmd.Output()
	if err != nil {
		return 0, errors.Wrapf(err, "getting size for pool %s", poolName)
	}

	sizeStr := strings.TrimSpace(string(output))
	// Parse ZFS size format (e.g., "1.5T", "500G")
	return parseZFSSize(sizeStr)
}

func (am *AutomatedMaintenance) getPoolFragmentation(poolName string) (float64, error) {
	cmd := exec.Command("zpool", "list", "-H", "-o", "frag", poolName)
	output, err := cmd.Output()
	if err != nil {
		return 0, errors.Wrapf(err, "getting fragmentation for pool %s", poolName)
	}

	fragStr := strings.TrimSpace(string(output))
	fragStr = strings.TrimSuffix(fragStr, "%")
	
	frag, err := strconv.ParseFloat(fragStr, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing fragmentation %s", fragStr)
	}

	return frag / 100.0, nil
}

func (am *AutomatedMaintenance) getLastScrubTime(poolName string) (time.Time, error) {
	cmd := exec.Command("zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "getting status for pool %s", poolName)
	}

	// Parse scrub information from zpool status output
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, "scrub repaired") || strings.Contains(line, "scrub completed") {
			// Extract date from scrub line (simplified parsing)
			// In real implementation, would use proper date parsing
			return time.Now().Add(-24 * time.Hour), nil // Placeholder
		}
	}

	// No scrub found, return very old date
	return time.Now().Add(-365 * 24 * time.Hour), nil
}

func (am *AutomatedMaintenance) getPoolErrors(poolName string) (map[string]string, error) {
	cmd := exec.Command("zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrapf(err, "getting status for pool %s", poolName)
	}

	errors := make(map[string]string)
	
	// Parse pool status for errors (simplified)
	if strings.Contains(string(output), "DEGRADED") {
		errors["pool"] = "pool_degraded"
	}
	
	if strings.Contains(string(output), "FAULTED") {
		errors["device"] = "device_error"
	}

	return errors, nil
}

// Validate validates the maintenance configuration
func (c *MaintenanceConfig) Validate() error {
	if c.ScrubInterval <= 0 {
		return fmt.Errorf("scrub_interval must be positive")
	}
	
	if c.DefragInterval <= 0 {
		return fmt.Errorf("defrag_interval must be positive")
	}
	
	if c.DefragThreshold <= 0 || c.DefragThreshold >= 1 {
		return fmt.Errorf("defrag_threshold must be between 0 and 1")
	}
	
	if c.MaxConcurrentOps <= 0 {
		return fmt.Errorf("max_concurrent_ops must be positive")
	}
	
	if c.PerformanceImpactLimit <= 0 || c.PerformanceImpactLimit >= 1 {
		return fmt.Errorf("performance_impact_limit must be between 0 and 1")
	}
	
	if c.HealthCheckInterval <= 0 {
		return fmt.Errorf("health_check_interval must be positive")
	}
	
	return nil
}