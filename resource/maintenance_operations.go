package resource

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// NewMaintenanceScheduler creates a new maintenance scheduler
func NewMaintenanceScheduler(config *MaintenanceConfig) *MaintenanceScheduler {
	return &MaintenanceScheduler{
		config:     config,
		operations: make(chan *MaintenanceOperation, 100),
	}
}

// Start starts the maintenance scheduler
func (ms *MaintenanceScheduler) Start(ctx context.Context) error {
	maintenanceLogger.Info("Starting maintenance scheduler")
	return nil
}

// NewScrubManager creates a new scrub manager
func NewScrubManager(config *MaintenanceConfig) *ScrubManager {
	return &ScrubManager{
		config:       config,
		activeScrubs: make(map[string]*ScrubOperation),
	}
}

// NewDefragmentationManager creates a new defragmentation manager
func NewDefragmentationManager(config *MaintenanceConfig) *DefragmentationManager {
	return &DefragmentationManager{
		config:          config,
		activeDefragOps: make(map[string]*DefragOperation),
	}
}

// NewRepairManager creates a new repair manager
func NewRepairManager(config *MaintenanceConfig) *RepairManager {
	return &RepairManager{
		config:        config,
		activeRepairs: make(map[string]*RepairOperation),
	}
}

// executeScrub executes a ZFS scrub operation
func (am *AutomatedMaintenance) executeScrub(ctx context.Context, operation *MaintenanceOperation) error {
	maintenanceLogger.Infof("Starting scrub for pool %s", operation.Pool)

	// Create scrub operation
	scrubOp := &ScrubOperation{
		Pool:      operation.Pool,
		StartTime: time.Now(),
		Status:    "running",
	}

	am.scrubManager.mu.Lock()
	am.scrubManager.activeScrubs[operation.Pool] = scrubOp
	am.scrubManager.mu.Unlock()

	// Start ZFS scrub
	cmd := exec.CommandContext(ctx, "zpool", "scrub", operation.Pool)
	if err := cmd.Run(); err != nil {
		scrubOp.Status = "failed"
		return errors.Wrapf(err, "starting scrub for pool %s", operation.Pool)
	}

	// Monitor scrub progress
	return am.monitorScrubProgress(ctx, operation, scrubOp)
}

// monitorScrubProgress monitors the progress of a scrub operation
func (am *AutomatedMaintenance) monitorScrubProgress(ctx context.Context, operation *MaintenanceOperation, scrubOp *ScrubOperation) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			progress, err := am.getScrubProgress(operation.Pool)
			if err != nil {
				maintenanceLogger.Warnf("Failed to get scrub progress for pool %s: %v", operation.Pool, err)
				continue
			}

			operation.mu.Lock()
			operation.Progress = progress.Progress
			operation.mu.Unlock()

			scrubOp.Progress = progress.Progress
			scrubOp.BytesScanned = progress.BytesScanned
			scrubOp.BytesTotal = progress.BytesTotal
			scrubOp.ErrorsFound = progress.ErrorsFound
			scrubOp.ErrorsRepaired = progress.ErrorsRepaired

			if progress.Progress >= 100.0 {
				scrubOp.Status = "completed"
				maintenanceLogger.Infof("Scrub completed for pool %s", operation.Pool)
				
				// Clean up
				am.scrubManager.mu.Lock()
				delete(am.scrubManager.activeScrubs, operation.Pool)
				am.scrubManager.mu.Unlock()
				
				return nil
			}
		}
	}
}

// ScrubProgress represents scrub progress information
type ScrubProgress struct {
	Progress       float64 `json:"progress"`
	BytesScanned   int64   `json:"bytes_scanned"`
	BytesTotal     int64   `json:"bytes_total"`
	ErrorsFound    int     `json:"errors_found"`
	ErrorsRepaired int     `json:"errors_repaired"`
}

// getScrubProgress gets the current scrub progress for a pool
func (am *AutomatedMaintenance) getScrubProgress(poolName string) (*ScrubProgress, error) {
	cmd := exec.Command("zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrapf(err, "getting scrub status for pool %s", poolName)
	}

	progress := &ScrubProgress{}
	
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		if strings.Contains(line, "scan:") && strings.Contains(line, "scrub") {
			// Parse scrub progress line
			if strings.Contains(line, "in progress") {
				// Extract progress percentage
				if idx := strings.Index(line, "%"); idx > 0 {
					start := idx - 1
					for start > 0 && (line[start] >= '0' && line[start] <= '9' || line[start] == '.') {
						start--
					}
					if start < idx-1 {
						if pct, err := strconv.ParseFloat(line[start+1:idx], 64); err == nil {
							progress.Progress = pct
						}
					}
				}
			} else if strings.Contains(line, "completed") {
				progress.Progress = 100.0
			}
		}
		
		if strings.Contains(line, "errors:") {
			// Parse error count
			parts := strings.Split(line, ":")
			if len(parts) > 1 {
				errorStr := strings.TrimSpace(parts[1])
				if errorStr != "No known data errors" {
					// Parse error count (simplified)
					progress.ErrorsFound = 1
				}
			}
		}
	}

	return progress, nil
}

// executeDefragmentation executes a defragmentation operation
func (am *AutomatedMaintenance) executeDefragmentation(ctx context.Context, operation *MaintenanceOperation) error {
	maintenanceLogger.Infof("Starting defragmentation for pool %s", operation.Pool)

	// Get initial fragmentation
	initialFrag, err := am.getPoolFragmentation(operation.Pool)
	if err != nil {
		return errors.Wrapf(err, "getting initial fragmentation for pool %s", operation.Pool)
	}

	// Create defrag operation
	defragOp := &DefragOperation{
		Pool:                 operation.Pool,
		StartTime:            time.Now(),
		Status:               "running",
		InitialFragmentation: initialFrag,
		CurrentFragmentation: initialFrag,
	}

	am.defragManager.mu.Lock()
	am.defragManager.activeDefragOps[operation.Pool] = defragOp
	am.defragManager.mu.Unlock()

	// Perform defragmentation using ZFS scrub with repair
	if err := am.performDefragmentation(ctx, operation, defragOp); err != nil {
		defragOp.Status = "failed"
		return err
	}

	defragOp.Status = "completed"
	maintenanceLogger.Infof("Defragmentation completed for pool %s", operation.Pool)

	// Clean up
	am.defragManager.mu.Lock()
	delete(am.defragManager.activeDefragOps, operation.Pool)
	am.defragManager.mu.Unlock()

	return nil
}

// performDefragmentation performs the actual defragmentation
func (am *AutomatedMaintenance) performDefragmentation(ctx context.Context, operation *MaintenanceOperation, defragOp *DefragOperation) error {
	// Step 1: Perform scrub to identify fragmented areas
	cmd := exec.CommandContext(ctx, "zpool", "scrub", operation.Pool)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "starting scrub for defragmentation of pool %s", operation.Pool)
	}

	// Step 2: Monitor progress and fragmentation improvement
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	startTime := time.Now()
	maxDuration := operation.EstimatedDuration * 2 // Allow up to 2x estimated time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check if we've exceeded maximum duration
			if time.Since(startTime) > maxDuration {
				return fmt.Errorf("defragmentation timeout exceeded for pool %s", operation.Pool)
			}

			// Get current fragmentation
			currentFrag, err := am.getPoolFragmentation(operation.Pool)
			if err != nil {
				maintenanceLogger.Warnf("Failed to get current fragmentation: %v", err)
				continue
			}

			defragOp.CurrentFragmentation = currentFrag

			// Calculate progress based on fragmentation improvement
			fragImprovement := defragOp.InitialFragmentation - currentFrag
			maxImprovement := defragOp.InitialFragmentation - am.config.DefragThreshold
			
			if maxImprovement > 0 {
				progress := (fragImprovement / maxImprovement) * 100.0
				if progress > 100.0 {
					progress = 100.0
				}
				
				operation.mu.Lock()
				operation.Progress = progress
				operation.mu.Unlock()
				
				defragOp.Progress = progress
			}

			// Check if defragmentation is complete
			if currentFrag <= am.config.DefragThreshold {
				operation.mu.Lock()
				operation.Progress = 100.0
				operation.mu.Unlock()
				return nil
			}

			// Check scrub status
			scrubProgress, err := am.getScrubProgress(operation.Pool)
			if err == nil && scrubProgress.Progress >= 100.0 {
				// Scrub completed, check if we need another pass
				if currentFrag > am.config.DefragThreshold && fragImprovement > 0.01 {
					// Start another scrub pass
					cmd := exec.CommandContext(ctx, "zpool", "scrub", operation.Pool)
					if err := cmd.Run(); err != nil {
						return errors.Wrapf(err, "starting additional scrub pass for pool %s", operation.Pool)
					}
				}
			}
		}
	}
}

// executeRepair executes a repair operation
func (am *AutomatedMaintenance) executeRepair(ctx context.Context, operation *MaintenanceOperation) error {
	deviceName := operation.Metadata["device"].(string)
	errorType := operation.Metadata["error_type"].(string)
	
	maintenanceLogger.Infof("Starting repair for pool %s device %s (%s)", 
		operation.Pool, deviceName, errorType)

	// Create repair operation
	repairOp := &RepairOperation{
		Pool:         operation.Pool,
		Device:       deviceName,
		StartTime:    time.Now(),
		Status:       "running",
		ErrorType:    errorType,
		RepairMethod: am.selectRepairMethod(errorType),
		Attempts:     1,
	}

	am.repairManager.mu.Lock()
	am.repairManager.activeRepairs[fmt.Sprintf("%s-%s", operation.Pool, deviceName)] = repairOp
	am.repairManager.mu.Unlock()

	// Perform repair based on error type
	var err error
	switch errorType {
	case "checksum_error":
		err = am.repairChecksumErrors(ctx, operation, repairOp)
	case "device_error":
		err = am.repairDeviceErrors(ctx, operation, repairOp)
	case "pool_degraded":
		err = am.repairDegradedPool(ctx, operation, repairOp)
	default:
		err = fmt.Errorf("unknown error type: %s", errorType)
	}

	if err != nil {
		repairOp.Status = "failed"
		
		// Retry if we haven't exceeded max attempts
		if repairOp.Attempts < am.config.MaxRepairAttempts {
			repairOp.Attempts++
			maintenanceLogger.Infof("Retrying repair for pool %s device %s (attempt %d)", 
				operation.Pool, deviceName, repairOp.Attempts)
			return am.executeRepair(ctx, operation)
		}
		
		return err
	}

	repairOp.Status = "completed"
	operation.mu.Lock()
	operation.Progress = 100.0
	operation.mu.Unlock()

	maintenanceLogger.Infof("Repair completed for pool %s device %s", operation.Pool, deviceName)

	// Clean up
	am.repairManager.mu.Lock()
	delete(am.repairManager.activeRepairs, fmt.Sprintf("%s-%s", operation.Pool, deviceName))
	am.repairManager.mu.Unlock()

	return nil
}

// selectRepairMethod selects the appropriate repair method for an error type
func (am *AutomatedMaintenance) selectRepairMethod(errorType string) string {
	switch errorType {
	case "checksum_error":
		return "scrub_repair"
	case "device_error":
		return "device_replace"
	case "pool_degraded":
		return "resilver"
	default:
		return "generic_repair"
	}
}

// repairChecksumErrors repairs checksum errors
func (am *AutomatedMaintenance) repairChecksumErrors(ctx context.Context, operation *MaintenanceOperation, repairOp *RepairOperation) error {
	// Clear errors first
	cmd := exec.CommandContext(ctx, "zpool", "clear", operation.Pool)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "clearing errors for pool %s", operation.Pool)
	}

	// Perform scrub to repair checksum errors
	cmd = exec.CommandContext(ctx, "zpool", "scrub", operation.Pool)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "starting repair scrub for pool %s", operation.Pool)
	}

	// Monitor repair progress
	return am.monitorRepairProgress(ctx, operation, repairOp)
}

// repairDeviceErrors repairs device errors
func (am *AutomatedMaintenance) repairDeviceErrors(ctx context.Context, operation *MaintenanceOperation, repairOp *RepairOperation) error {
	// For device errors, we might need to replace the device
	// This is a simplified implementation
	
	// First try to clear the error
	cmd := exec.CommandContext(ctx, "zpool", "clear", operation.Pool, repairOp.Device)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "clearing device errors for %s in pool %s", repairOp.Device, operation.Pool)
	}

	// Monitor to see if errors persist
	return am.monitorRepairProgress(ctx, operation, repairOp)
}

// repairDegradedPool repairs a degraded pool
func (am *AutomatedMaintenance) repairDegradedPool(ctx context.Context, operation *MaintenanceOperation, repairOp *RepairOperation) error {
	// For degraded pools, start resilver process
	cmd := exec.CommandContext(ctx, "zpool", "clear", operation.Pool)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "clearing pool errors for %s", operation.Pool)
	}

	// Check if resilver starts automatically, otherwise trigger scrub
	cmd = exec.CommandContext(ctx, "zpool", "scrub", operation.Pool)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "starting resilver for pool %s", operation.Pool)
	}

	return am.monitorRepairProgress(ctx, operation, repairOp)
}

// monitorRepairProgress monitors repair progress
func (am *AutomatedMaintenance) monitorRepairProgress(ctx context.Context, operation *MaintenanceOperation, repairOp *RepairOperation) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check if repair timeout exceeded
			if time.Since(startTime) > am.config.RepairTimeout {
				return fmt.Errorf("repair timeout exceeded for pool %s", operation.Pool)
			}

			// Check pool health
			healthy, err := am.isPoolHealthy(operation.Pool)
			if err != nil {
				maintenanceLogger.Warnf("Failed to check pool health: %v", err)
				continue
			}

			if healthy {
				operation.mu.Lock()
				operation.Progress = 100.0
				operation.mu.Unlock()
				return nil
			}

			// Update progress based on time elapsed (simplified)
			elapsed := time.Since(startTime)
			progress := (float64(elapsed) / float64(am.config.RepairTimeout)) * 100.0
			if progress > 95.0 {
				progress = 95.0 // Don't show 100% until actually complete
			}

			operation.mu.Lock()
			operation.Progress = progress
			operation.mu.Unlock()

			repairOp.Progress = progress
		}
	}
}

// isPoolHealthy checks if a pool is healthy
func (am *AutomatedMaintenance) isPoolHealthy(poolName string) (bool, error) {
	cmd := exec.Command("zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return false, errors.Wrapf(err, "checking health of pool %s", poolName)
	}

	status := string(output)
	return strings.Contains(status, "state: ONLINE") && 
		   !strings.Contains(status, "DEGRADED") && 
		   !strings.Contains(status, "FAULTED"), nil
}