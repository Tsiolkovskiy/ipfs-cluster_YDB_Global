package zfs

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ZFSManager handles ZFS operations for the datastore
type ZFSManager struct {
	config *Config
}

// NewZFSManager creates a new ZFS manager instance
func NewZFSManager(cfg *Config) *ZFSManager {
	return &ZFSManager{
		config: cfg,
	}
}

// CreateDataset creates a new ZFS dataset with optimized properties
func (zm *ZFSManager) CreateDataset(datasetPath string) error {
	// Check if dataset already exists
	exists, err := zm.DatasetExists(datasetPath)
	if err != nil {
		return errors.Wrap(err, "checking dataset existence")
	}
	if exists {
		return nil // Dataset already exists
	}

	// Create the dataset with optimized properties
	cmd := exec.Command("zfs", "create", datasetPath)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "creating dataset %s", datasetPath)
	}

	// Set ZFS properties for optimal IPFS performance
	properties := map[string]string{
		"compression":    zm.config.Compression,
		"recordsize":     zm.config.RecordSize,
		"sync":           zm.config.Sync,
		"atime":          boolToOnOff(zm.config.ATime),
		"dedup":          boolToOnOff(zm.config.Deduplication),
		"xattr":          "sa",           // Store extended attributes in system attributes
		"dnodesize":      "auto",         // Automatic dnode sizing
		"relatime":       "on",           // Relative access time updates
		"logbias":        "throughput",   // Optimize for throughput over latency
		"primarycache":   "all",          // Cache both metadata and data
		"secondarycache": "all",          // Use L2ARC for both metadata and data
	}

	for prop, value := range properties {
		if err := zm.SetProperty(datasetPath, prop, value); err != nil {
			return errors.Wrapf(err, "setting property %s=%s", prop, value)
		}
	}

	// Enable encryption if configured
	if zm.config.Encryption {
		if err := zm.enableEncryption(datasetPath); err != nil {
			return errors.Wrap(err, "enabling encryption")
		}
	}

	return nil
}

// DatasetExists checks if a ZFS dataset exists
func (zm *ZFSManager) DatasetExists(datasetPath string) (bool, error) {
	cmd := exec.Command("zfs", "list", "-H", "-o", "name", datasetPath)
	err := cmd.Run()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				return false, nil // Dataset doesn't exist
			}
		}
		return false, errors.Wrapf(err, "checking dataset existence %s", datasetPath)
	}
	return true, nil
}

// SetProperty sets a ZFS property on a dataset
func (zm *ZFSManager) SetProperty(datasetPath, property, value string) error {
	cmd := exec.Command("zfs", "set", fmt.Sprintf("%s=%s", property, value), datasetPath)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "setting property %s=%s on %s", property, value, datasetPath)
	}
	return nil
}

// GetProperty gets a ZFS property from a dataset
func (zm *ZFSManager) GetProperty(datasetPath, property string) (string, error) {
	cmd := exec.Command("zfs", "get", "-H", "-o", "value", property, datasetPath)
	output, err := cmd.Output()
	if err != nil {
		return "", errors.Wrapf(err, "getting property %s from %s", property, datasetPath)
	}
	return strings.TrimSpace(string(output)), nil
}

// CreateSnapshot creates a ZFS snapshot
func (zm *ZFSManager) CreateSnapshot(datasetPath, snapshotName string) error {
	fullSnapshotName := fmt.Sprintf("%s@%s", datasetPath, snapshotName)
	cmd := exec.Command("zfs", "snapshot", fullSnapshotName)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "creating snapshot %s", fullSnapshotName)
	}
	return nil
}

// ListSnapshots lists all snapshots for a dataset
func (zm *ZFSManager) ListSnapshots(datasetPath string) ([]string, error) {
	cmd := exec.Command("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", datasetPath)
	output, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrapf(err, "listing snapshots for %s", datasetPath)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var snapshots []string
	for _, line := range lines {
		if line != "" && strings.Contains(line, "@") {
			snapshots = append(snapshots, line)
		}
	}
	return snapshots, nil
}

// DeleteSnapshot deletes a ZFS snapshot
func (zm *ZFSManager) DeleteSnapshot(snapshotName string) error {
	cmd := exec.Command("zfs", "destroy", snapshotName)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "deleting snapshot %s", snapshotName)
	}
	return nil
}

// GetCompressionRatio returns the compression ratio for a dataset
func (zm *ZFSManager) GetCompressionRatio(datasetPath string) (float64, error) {
	ratioStr, err := zm.GetProperty(datasetPath, "compressratio")
	if err != nil {
		return 0, err
	}

	// Parse compression ratio (format: "2.34x")
	ratioStr = strings.TrimSuffix(ratioStr, "x")
	ratio, err := strconv.ParseFloat(ratioStr, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing compression ratio %s", ratioStr)
	}

	return ratio, nil
}

// GetDeduplicationRatio returns the deduplication ratio for a dataset
func (zm *ZFSManager) GetDeduplicationRatio(datasetPath string) (float64, error) {
	ratioStr, err := zm.GetProperty(datasetPath, "dedupratio")
	if err != nil {
		return 0, err
	}

	// Parse deduplication ratio (format: "1.23x")
	ratioStr = strings.TrimSuffix(ratioStr, "x")
	ratio, err := strconv.ParseFloat(ratioStr, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing deduplication ratio %s", ratioStr)
	}

	return ratio, nil
}

// GetUsedSpace returns the used space for a dataset in bytes
func (zm *ZFSManager) GetUsedSpace(datasetPath string) (int64, error) {
	usedStr, err := zm.GetProperty(datasetPath, "used")
	if err != nil {
		return 0, err
	}

	// Parse used space (can be in various units: B, K, M, G, T, P)
	used, err := parseZFSSize(usedStr)
	if err != nil {
		return 0, errors.Wrapf(err, "parsing used space %s", usedStr)
	}

	return used, nil
}

// OptimizeDataset performs ZFS optimization operations
func (zm *ZFSManager) OptimizeDataset(ctx context.Context, datasetPath string) error {
	// Get current performance metrics
	compressionRatio, err := zm.GetCompressionRatio(datasetPath)
	if err != nil {
		return errors.Wrap(err, "getting compression ratio")
	}

	// Adjust record size based on compression efficiency
	if compressionRatio < 1.5 && zm.config.RecordSize == "128K" {
		// Poor compression, try larger record size
		if err := zm.SetProperty(datasetPath, "recordsize", "1M"); err != nil {
			return errors.Wrap(err, "adjusting record size")
		}
	}

	// Create automatic snapshot if enabled
	if zm.config.SnapshotInterval > 0 {
		snapshotName := fmt.Sprintf("auto-%d", time.Now().Unix())
		if err := zm.CreateSnapshot(datasetPath, snapshotName); err != nil {
			return errors.Wrap(err, "creating automatic snapshot")
		}

		// Clean up old snapshots
		if err := zm.cleanupOldSnapshots(datasetPath); err != nil {
			return errors.Wrap(err, "cleaning up old snapshots")
		}
	}

	return nil
}

// enableEncryption enables ZFS encryption on a dataset
func (zm *ZFSManager) enableEncryption(datasetPath string) error {
	// Note: This is a simplified implementation
	// In production, proper key management should be implemented
	properties := map[string]string{
		"encryption":  "aes-256-gcm",
		"keyformat":   zm.config.KeyFormat,
		"keylocation": "prompt", // In production, use proper key management
	}

	for prop, value := range properties {
		if err := zm.SetProperty(datasetPath, prop, value); err != nil {
			return errors.Wrapf(err, "setting encryption property %s=%s", prop, value)
		}
	}

	return nil
}

// cleanupOldSnapshots removes old snapshots beyond the retention limit
func (zm *ZFSManager) cleanupOldSnapshots(datasetPath string) error {
	if zm.config.MaxSnapshots <= 0 {
		return nil // No cleanup needed
	}

	snapshots, err := zm.ListSnapshots(datasetPath)
	if err != nil {
		return err
	}

	// Remove oldest snapshots if we exceed the limit
	if len(snapshots) > zm.config.MaxSnapshots {
		toDelete := len(snapshots) - zm.config.MaxSnapshots
		for i := 0; i < toDelete; i++ {
			if err := zm.DeleteSnapshot(snapshots[i]); err != nil {
				return errors.Wrapf(err, "deleting old snapshot %s", snapshots[i])
			}
		}
	}

	return nil
}

// Helper functions

func boolToOnOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func parseZFSSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "0" || sizeStr == "-" {
		return 0, nil
	}

	// Extract numeric part and unit
	var numStr string
	var unit string
	
	for i, r := range sizeStr {
		if r >= '0' && r <= '9' || r == '.' {
			numStr += string(r)
		} else {
			unit = sizeStr[i:]
			break
		}
	}

	if numStr == "" {
		return 0, fmt.Errorf("invalid size format: %s", sizeStr)
	}

	size, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, err
	}

	// Convert based on unit
	switch strings.ToUpper(unit) {
	case "", "B":
		return int64(size), nil
	case "K", "KB":
		return int64(size * 1024), nil
	case "M", "MB":
		return int64(size * 1024 * 1024), nil
	case "G", "GB":
		return int64(size * 1024 * 1024 * 1024), nil
	case "T", "TB":
		return int64(size * 1024 * 1024 * 1024 * 1024), nil
	case "P", "PB":
		return int64(size * 1024 * 1024 * 1024 * 1024 * 1024), nil
	default:
		return 0, fmt.Errorf("unknown unit: %s", unit)
	}
}