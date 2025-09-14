// Package zfshttp implements an IPFS Cluster IPFSConnector component
// optimized for ZFS storage systems. It provides enhanced functionality
// for working with ZFS datasets, snapshots, compression, and deduplication.
package zfshttp

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs-cluster/ipfs-cluster/api"
	"github.com/ipfs-cluster/ipfs-cluster/ipfsconn/ipfshttp"

	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("zfshttp")

// ZFSConnector implements the IPFSConnector interface with ZFS optimizations
type ZFSConnector struct {
	// Embed the standard HTTP connector for base functionality
	*ipfshttp.Connector

	// ZFS-specific fields
	config         *Config
	datasetManager *DatasetManager
	zfsMetrics     *ZFSMetrics
	
	// Context management
	ctx    context.Context
	cancel context.CancelFunc
	
	// Synchronization
	mu       sync.RWMutex
	shutdown bool
}



// ZFSPinMetadata extends pin information with ZFS-specific data
type ZFSPinMetadata struct {
	CID             string    `json:"cid"`
	Dataset         string    `json:"dataset"`
	ZFSPath         string    `json:"zfs_path"`
	Checksum        string    `json:"checksum"`
	CompressionType string    `json:"compression_type"`
	Size            int64     `json:"size"`
	CreatedAt       time.Time `json:"created_at"`
	LastAccessed    time.Time `json:"last_accessed"`
	AccessCount     int64     `json:"access_count"`
}

// NewZFSConnector creates a new ZFS-optimized IPFS connector
func NewZFSConnector(cfg *Config) (*ZFSConnector, error) {
	// Create base HTTP connector
	baseConnector, err := ipfshttp.NewConnector(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create base connector: %w", err)
	}

	// Initialize dataset manager
	datasetManager, err := NewDatasetManager(cfg.ZFSConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create dataset manager: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	connector := &ZFSConnector{
		Connector:      baseConnector,
		config:         cfg,
		datasetManager: datasetManager,
		zfsMetrics:     &ZFSMetrics{LastUpdate: time.Now()},
		ctx:            ctx,
		cancel:         cancel,
	}

	// Initialize ZFS metrics collection
	go connector.metricsCollectionLoop()

	return connector, nil
}

// Pin implements the IPFSConnector interface with ZFS optimizations
func (zc *ZFSConnector) Pin(ctx context.Context, pin api.Pin) error {
	start := time.Now()
	defer func() {
		zc.updatePinMetrics(time.Since(start))
	}()

	zc.mu.RLock()
	if zc.shutdown {
		zc.mu.RUnlock()
		return fmt.Errorf("connector is shutting down")
	}
	zc.mu.RUnlock()

	// Get optimal dataset for this pin
	dataset, err := zc.datasetManager.GetDatasetForCID(pin.Cid.String())
	if err != nil {
		return fmt.Errorf("failed to get dataset for CID %s: %w", pin.Cid.String(), err)
	}

	// Store ZFS metadata before pinning
	metadata := &ZFSPinMetadata{
		CID:             pin.Cid.String(),
		Dataset:         dataset.Name,
		ZFSPath:         filepath.Join(dataset.MountPoint, pin.Cid.String()),
		CompressionType: dataset.CompressionType,
		CreatedAt:       time.Now(),
		AccessCount:     0,
	}

	// Store metadata in ZFS dataset
	if err := zc.storePinMetadata(metadata); err != nil {
		logger.Warnf("Failed to store ZFS metadata for pin %s: %s", pin.Cid.String(), err)
	}

	// Perform the actual pin operation using base connector
	if err := zc.Connector.Pin(ctx, pin); err != nil {
		// Clean up metadata on failure
		zc.deletePinMetadata(pin.Cid.String())
		return err
	}

	// Update dataset statistics
	zc.datasetManager.UpdatePinCount(dataset.Name, 1)

	logger.Debugf("Successfully pinned %s to ZFS dataset %s", pin.Cid.String(), dataset.Name)
	return nil
}

// Unpin implements the IPFSConnector interface with ZFS optimizations
func (zc *ZFSConnector) Unpin(ctx context.Context, c api.Cid) error {
	start := time.Now()
	defer func() {
		zc.updatePinMetrics(time.Since(start))
	}()

	zc.mu.RLock()
	if zc.shutdown {
		zc.mu.RUnlock()
		return fmt.Errorf("connector is shutting down")
	}
	zc.mu.RUnlock()

	// Get pin metadata before unpinning
	metadata, err := zc.getPinMetadata(c.String())
	if err != nil {
		logger.Warnf("Failed to get ZFS metadata for unpin %s: %s", c.String(), err)
	}

	// Perform the actual unpin operation
	if err := zc.Connector.Unpin(ctx, c); err != nil {
		return err
	}

	// Clean up ZFS metadata
	if err := zc.deletePinMetadata(c.String()); err != nil {
		logger.Warnf("Failed to delete ZFS metadata for %s: %s", c.String(), err)
	}

	// Update dataset statistics
	if metadata != nil {
		zc.datasetManager.UpdatePinCount(metadata.Dataset, -1)
	}

	logger.Debugf("Successfully unpinned %s from ZFS", c.String())
	return nil
}

// PinLs implements the IPFSConnector interface with ZFS optimizations
func (zc *ZFSConnector) PinLs(ctx context.Context, typeFilters []string, out chan<- api.IPFSPinInfo) error {
	zc.mu.RLock()
	if zc.shutdown {
		zc.mu.RUnlock()
		return fmt.Errorf("connector is shutting down")
	}
	zc.mu.RUnlock()

	// Use base connector for pin listing but enhance with ZFS metadata
	enhancedOut := make(chan api.IPFSPinInfo, 100)
	
	// Start goroutine to enhance pin info with ZFS metadata
	go func() {
		defer close(out)
		for pinInfo := range enhancedOut {
			// Enhance with ZFS metadata if available
			if metadata, err := zc.getPinMetadata(pinInfo.Cid.String()); err == nil {
				// Add ZFS-specific information to pin info
				// This could be done through custom fields or metadata
				logger.Debugf("Enhanced pin %s with ZFS metadata from dataset %s", 
					pinInfo.Cid.String(), metadata.Dataset)
			}
			out <- pinInfo
		}
	}()

	return zc.Connector.PinLs(ctx, typeFilters, enhancedOut)
}

// GetZFSMetrics returns current ZFS performance metrics
func (zc *ZFSConnector) GetZFSMetrics() *ZFSMetrics {
	zc.zfsMetrics.mu.RLock()
	defer zc.zfsMetrics.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	return &ZFSMetrics{
		CompressionRatio:       zc.zfsMetrics.CompressionRatio,
		DeduplicationRatio:     zc.zfsMetrics.DeduplicationRatio,
		ARCHitRatio:           zc.zfsMetrics.ARCHitRatio,
		FragmentationLevel:    zc.zfsMetrics.FragmentationLevel,
		PinOperationsPerSecond: zc.zfsMetrics.PinOperationsPerSecond,
		AverageLatency:        zc.zfsMetrics.AverageLatency,
		LastUpdate:            zc.zfsMetrics.LastUpdate,
	}
}

// CreateSnapshot creates a ZFS snapshot of the specified dataset
func (zc *ZFSConnector) CreateSnapshot(dataset, name string) error {
	return zc.datasetManager.CreateSnapshot(dataset, name)
}

// ListSnapshots returns available snapshots for a dataset
func (zc *ZFSConnector) ListSnapshots(dataset string) ([]string, error) {
	return zc.datasetManager.ListSnapshots(dataset)
}

// RollbackToSnapshot rolls back a dataset to a specific snapshot
func (zc *ZFSConnector) RollbackToSnapshot(dataset, snapshot string) error {
	return zc.datasetManager.RollbackToSnapshot(dataset, snapshot)
}

// Shutdown gracefully shuts down the ZFS connector
func (zc *ZFSConnector) Shutdown(ctx context.Context) error {
	zc.mu.Lock()
	zc.shutdown = true
	zc.mu.Unlock()

	// Cancel context to stop background goroutines
	zc.cancel()

	// Shutdown dataset manager
	if zc.datasetManager != nil {
		if err := zc.datasetManager.Shutdown(ctx); err != nil {
			logger.Errorf("Error shutting down dataset manager: %s", err)
		}
	}

	// Shutdown base connector
	return zc.Connector.Shutdown(ctx)
}

// storePinMetadata stores pin metadata in ZFS dataset
func (zc *ZFSConnector) storePinMetadata(metadata *ZFSPinMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metadataPath := filepath.Join(zc.config.ZFSConfig.MetadataPath, metadata.CID+".json")
	return zc.datasetManager.WriteFile(metadataPath, data)
}

// getPinMetadata retrieves pin metadata from ZFS dataset
func (zc *ZFSConnector) getPinMetadata(cid string) (*ZFSPinMetadata, error) {
	metadataPath := filepath.Join(zc.config.ZFSConfig.MetadataPath, cid+".json")
	data, err := zc.datasetManager.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata ZFSPinMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &metadata, nil
}

// deletePinMetadata removes pin metadata from ZFS dataset
func (zc *ZFSConnector) deletePinMetadata(cid string) error {
	metadataPath := filepath.Join(zc.config.ZFSConfig.MetadataPath, cid+".json")
	return zc.datasetManager.DeleteFile(metadataPath)
}

// updatePinMetrics updates performance metrics for pin operations
func (zc *ZFSConnector) updatePinMetrics(latency time.Duration) {
	zc.zfsMetrics.mu.Lock()
	defer zc.zfsMetrics.mu.Unlock()

	// Simple moving average for latency
	if zc.zfsMetrics.AverageLatency == 0 {
		zc.zfsMetrics.AverageLatency = latency
	} else {
		zc.zfsMetrics.AverageLatency = (zc.zfsMetrics.AverageLatency + latency) / 2
	}

	// Update operations per second (simplified calculation)
	now := time.Now()
	if !zc.zfsMetrics.LastUpdate.IsZero() {
		timeDiff := now.Sub(zc.zfsMetrics.LastUpdate).Seconds()
		if timeDiff > 0 {
			zc.zfsMetrics.PinOperationsPerSecond = 1.0 / timeDiff
		}
	}
	zc.zfsMetrics.LastUpdate = now
}

// metricsCollectionLoop periodically collects ZFS metrics
func (zc *ZFSConnector) metricsCollectionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			zc.collectZFSMetrics()
		case <-zc.ctx.Done():
			return
		}
	}
}

// collectZFSMetrics collects current ZFS system metrics
func (zc *ZFSConnector) collectZFSMetrics() {
	zc.zfsMetrics.mu.Lock()
	defer zc.zfsMetrics.mu.Unlock()

	// Collect compression ratio
	if ratio, err := zc.getCompressionRatio(); err == nil {
		zc.zfsMetrics.CompressionRatio = ratio
	}

	// Collect deduplication ratio
	if ratio, err := zc.getDeduplicationRatio(); err == nil {
		zc.zfsMetrics.DeduplicationRatio = ratio
	}

	// Collect ARC hit ratio
	if ratio, err := zc.getARCHitRatio(); err == nil {
		zc.zfsMetrics.ARCHitRatio = ratio
	}

	// Collect fragmentation level
	if level, err := zc.getFragmentationLevel(); err == nil {
		zc.zfsMetrics.FragmentationLevel = level
	}

	zc.zfsMetrics.LastUpdate = time.Now()
}

// getCompressionRatio gets the current ZFS compression ratio
func (zc *ZFSConnector) getCompressionRatio() (float64, error) {
	cmd := exec.Command("zfs", "get", "-H", "-p", "compressratio", zc.config.ZFSConfig.PoolName)
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get compression ratio: %w", err)
	}

	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return 0, fmt.Errorf("unexpected zfs output format")
	}

	ratio, err := strconv.ParseFloat(strings.TrimSuffix(fields[2], "x"), 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse compression ratio: %w", err)
	}

	return ratio, nil
}

// getDeduplicationRatio gets the current ZFS deduplication ratio
func (zc *ZFSConnector) getDeduplicationRatio() (float64, error) {
	cmd := exec.Command("zpool", "get", "-H", "-p", "dedupratio", zc.config.ZFSConfig.PoolName)
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get deduplication ratio: %w", err)
	}

	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return 0, fmt.Errorf("unexpected zpool output format")
	}

	ratio, err := strconv.ParseFloat(strings.TrimSuffix(fields[2], "x"), 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse deduplication ratio: %w", err)
	}

	return ratio, nil
}

// getARCHitRatio gets the current ARC hit ratio
func (zc *ZFSConnector) getARCHitRatio() (float64, error) {
	cmd := exec.Command("cat", "/proc/spl/kstat/zfs/arcstats")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to read ARC stats: %w", err)
	}

	lines := strings.Split(string(output), "\n")
	var hits, misses int64

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 3 {
			switch fields[0] {
			case "hits":
				hits, _ = strconv.ParseInt(fields[2], 10, 64)
			case "misses":
				misses, _ = strconv.ParseInt(fields[2], 10, 64)
			}
		}
	}

	total := hits + misses
	if total == 0 {
		return 0, nil
	}

	return float64(hits) / float64(total) * 100, nil
}

// getFragmentationLevel gets the current fragmentation level
func (zc *ZFSConnector) getFragmentationLevel() (float64, error) {
	cmd := exec.Command("zpool", "get", "-H", "-p", "fragmentation", zc.config.ZFSConfig.PoolName)
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to get fragmentation level: %w", err)
	}

	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return 0, fmt.Errorf("unexpected zpool output format")
	}

	level, err := strconv.ParseFloat(strings.TrimSuffix(fields[2], "%"), 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse fragmentation level: %w", err)
	}

	return level, nil
}