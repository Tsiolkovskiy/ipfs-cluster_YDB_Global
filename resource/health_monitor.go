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

// HealthMonitor monitors the health of ZFS pools and disks
type HealthMonitor struct {
	config *Config
}

// HealthStatus represents the health status of a component
type HealthStatus struct {
	Component   string                 `json:"component"`
	Status      string                 `json:"status"`
	Issues      []HealthIssue          `json:"issues"`
	Metrics     map[string]interface{} `json:"metrics"`
	LastChecked time.Time              `json:"last_checked"`
}

// HealthIssue represents a specific health issue
type HealthIssue struct {
	Severity    string    `json:"severity"`
	Type        string    `json:"type"`
	Description string    `json:"description"`
	Timestamp   time.Time `json:"timestamp"`
	Resolved    bool      `json:"resolved"`
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(config *Config) *HealthMonitor {
	return &HealthMonitor{
		config: config,
	}
}

// CheckPoolHealth checks the health of a ZFS pool
func (hm *HealthMonitor) CheckPoolHealth(ctx context.Context, poolName string) error {
	logger.Debugf("Checking health of pool %s", poolName)

	// Get pool status
	cmd := exec.CommandContext(ctx, "zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrapf(err, "getting pool status for %s", poolName)
	}

	status := hm.parsePoolStatus(string(output))
	
	// Check for critical issues
	if status.Status == "DEGRADED" || status.Status == "FAULTED" {
		logger.Warnf("Pool %s is in %s state", poolName, status.Status)
		
		// Attempt automatic recovery if configured
		if hm.config.AutoReplaceFailed {
			return hm.attemptPoolRecovery(ctx, poolName, status)
		}
	}

	// Check fragmentation levels
	if err := hm.checkFragmentation(ctx, poolName); err != nil {
		logger.Warnf("Fragmentation check failed for pool %s: %v", poolName, err)
	}

	return nil
}

// CheckDiskHealth checks the health of a physical disk
func (hm *HealthMonitor) CheckDiskHealth(ctx context.Context, device string) error {
	logger.Debugf("Checking health of disk %s", device)

	// Check SMART attributes
	smartData, err := hm.getSMARTData(ctx, device)
	if err != nil {
		return errors.Wrapf(err, "getting SMART data for %s", device)
	}

	// Analyze SMART data for potential issues
	issues := hm.analyzeSMARTData(smartData)
	
	if len(issues) > 0 {
		logger.Warnf("Disk %s has %d health issues", device, len(issues))
		
		// Check if any issues are critical
		for _, issue := range issues {
			if issue.Severity == "CRITICAL" {
				logger.Errorf("Critical issue detected on disk %s: %s", device, issue.Description)
				
				// Mark disk for replacement if configured
				if hm.config.AutoReplaceFailed {
					return hm.scheduleDiskReplacement(ctx, device, issue)
				}
			}
		}
	}

	return nil
}

// parsePoolStatus parses zpool status output
func (hm *HealthMonitor) parsePoolStatus(output string) *HealthStatus {
	status := &HealthStatus{
		Component:   "pool",
		Issues:      []HealthIssue{},
		Metrics:     make(map[string]interface{}),
		LastChecked: time.Now(),
	}

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		if strings.Contains(line, "state:") {
			parts := strings.Split(line, ":")
			if len(parts) > 1 {
				status.Status = strings.TrimSpace(parts[1])
			}
		}
		
		if strings.Contains(line, "errors:") {
			parts := strings.Split(line, ":")
			if len(parts) > 1 {
				errorStr := strings.TrimSpace(parts[1])
				if errorStr != "No known data errors" {
					issue := HealthIssue{
						Severity:    "WARNING",
						Type:        "DATA_ERROR",
						Description: errorStr,
						Timestamp:   time.Now(),
						Resolved:    false,
					}
					status.Issues = append(status.Issues, issue)
				}
			}
		}
	}

	return status
}

// getSMARTData retrieves SMART data for a disk
func (hm *HealthMonitor) getSMARTData(ctx context.Context, device string) (map[string]interface{}, error) {
	cmd := exec.CommandContext(ctx, "smartctl", "-A", device)
	output, err := cmd.Output()
	if err != nil {
		// smartctl may return non-zero exit code even for successful reads
		// Check if we got any output
		if len(output) == 0 {
			return nil, errors.Wrapf(err, "no SMART data available for %s", device)
		}
	}

	return hm.parseSMARTOutput(string(output)), nil
}

// parseSMARTOutput parses smartctl output
func (hm *HealthMonitor) parseSMARTOutput(output string) map[string]interface{} {
	smartData := make(map[string]interface{})
	
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		// Parse SMART attributes (simplified)
		if strings.Contains(line, "Temperature_Celsius") {
			fields := strings.Fields(line)
			if len(fields) >= 10 {
				if temp, err := strconv.Atoi(fields[9]); err == nil {
					smartData["temperature"] = temp
				}
			}
		}
		
		if strings.Contains(line, "Reallocated_Sector_Ct") {
			fields := strings.Fields(line)
			if len(fields) >= 10 {
				if sectors, err := strconv.ParseInt(fields[9], 10, 64); err == nil {
					smartData["reallocated_sectors"] = sectors
				}
			}
		}
		
		if strings.Contains(line, "Current_Pending_Sector") {
			fields := strings.Fields(line)
			if len(fields) >= 10 {
				if sectors, err := strconv.ParseInt(fields[9], 10, 64); err == nil {
					smartData["pending_sectors"] = sectors
				}
			}
		}
		
		if strings.Contains(line, "Power_On_Hours") {
			fields := strings.Fields(line)
			if len(fields) >= 10 {
				if hours, err := strconv.ParseInt(fields[9], 10, 64); err == nil {
					smartData["power_on_hours"] = hours
				}
			}
		}
	}
	
	return smartData
}

// analyzeSMARTData analyzes SMART data for potential issues
func (hm *HealthMonitor) analyzeSMARTData(smartData map[string]interface{}) []HealthIssue {
	var issues []HealthIssue
	
	// Check temperature
	if temp, ok := smartData["temperature"].(int); ok {
		if temp >= hm.config.AlertThresholds.TemperatureCritical {
			issues = append(issues, HealthIssue{
				Severity:    "CRITICAL",
				Type:        "HIGH_TEMPERATURE",
				Description: fmt.Sprintf("Disk temperature is %d°C (critical threshold: %d°C)", 
					temp, hm.config.AlertThresholds.TemperatureCritical),
				Timestamp:   time.Now(),
				Resolved:    false,
			})
		} else if temp >= hm.config.AlertThresholds.TemperatureWarning {
			issues = append(issues, HealthIssue{
				Severity:    "WARNING",
				Type:        "HIGH_TEMPERATURE",
				Description: fmt.Sprintf("Disk temperature is %d°C (warning threshold: %d°C)", 
					temp, hm.config.AlertThresholds.TemperatureWarning),
				Timestamp:   time.Now(),
				Resolved:    false,
			})
		}
	}
	
	// Check reallocated sectors
	if sectors, ok := smartData["reallocated_sectors"].(int64); ok && sectors > 0 {
		severity := "WARNING"
		if sectors > 100 {
			severity = "CRITICAL"
		}
		
		issues = append(issues, HealthIssue{
			Severity:    severity,
			Type:        "REALLOCATED_SECTORS",
			Description: fmt.Sprintf("Disk has %d reallocated sectors", sectors),
			Timestamp:   time.Now(),
			Resolved:    false,
		})
	}
	
	// Check pending sectors
	if sectors, ok := smartData["pending_sectors"].(int64); ok && sectors > 0 {
		issues = append(issues, HealthIssue{
			Severity:    "WARNING",
			Type:        "PENDING_SECTORS",
			Description: fmt.Sprintf("Disk has %d pending sectors", sectors),
			Timestamp:   time.Now(),
			Resolved:    false,
		})
	}
	
	return issues
}

// checkFragmentation checks pool fragmentation levels
func (hm *HealthMonitor) checkFragmentation(ctx context.Context, poolName string) error {
	cmd := exec.CommandContext(ctx, "zpool", "list", "-H", "-o", "frag", poolName)
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrapf(err, "getting fragmentation for pool %s", poolName)
	}

	fragStr := strings.TrimSpace(string(output))
	fragStr = strings.TrimSuffix(fragStr, "%")
	
	frag, err := strconv.ParseFloat(fragStr, 64)
	if err != nil {
		return errors.Wrapf(err, "parsing fragmentation value %s", fragStr)
	}

	fragRatio := frag / 100.0
	
	if fragRatio >= hm.config.AlertThresholds.FragmentationWarning {
		logger.Warnf("Pool %s fragmentation is %.1f%% (threshold: %.1f%%)", 
			poolName, frag, hm.config.AlertThresholds.FragmentationWarning*100)
		
		// Schedule defragmentation if fragmentation is too high
		if fragRatio >= 0.5 { // 50% fragmentation
			return hm.scheduleDefragmentation(ctx, poolName)
		}
	}

	return nil
}

// attemptPoolRecovery attempts to recover a degraded pool
func (hm *HealthMonitor) attemptPoolRecovery(ctx context.Context, poolName string, status *HealthStatus) error {
	logger.Infof("Attempting recovery for pool %s", poolName)

	// Try to clear errors first
	cmd := exec.CommandContext(ctx, "zpool", "clear", poolName)
	if err := cmd.Run(); err != nil {
		logger.Warnf("Failed to clear pool errors for %s: %v", poolName, err)
	}

	// Check if pool is now healthy
	cmd = exec.CommandContext(ctx, "zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrapf(err, "checking pool status after clear")
	}

	newStatus := hm.parsePoolStatus(string(output))
	if newStatus.Status == "ONLINE" {
		logger.Infof("Pool %s recovered successfully", poolName)
		return nil
	}

	// If still degraded, may need disk replacement
	logger.Warnf("Pool %s still degraded after clear, may need disk replacement", poolName)
	return fmt.Errorf("pool recovery failed, manual intervention required")
}

// scheduleDiskReplacement schedules a disk for replacement
func (hm *HealthMonitor) scheduleDiskReplacement(ctx context.Context, device string, issue HealthIssue) error {
	logger.Infof("Scheduling disk %s for replacement due to: %s", device, issue.Description)
	
	// In a real implementation, this would:
	// 1. Mark the disk as needing replacement
	// 2. Find a suitable replacement disk
	// 3. Schedule the replacement operation
	// 4. Notify administrators
	
	return nil
}

// scheduleDefragmentation schedules pool defragmentation
func (hm *HealthMonitor) scheduleDefragmentation(ctx context.Context, poolName string) error {
	logger.Infof("Scheduling defragmentation for pool %s", poolName)
	
	// In a real implementation, this would:
	// 1. Schedule a scrub operation during low-usage hours
	// 2. Consider rebalancing data across devices
	// 3. Optimize dataset properties
	
	return nil
}