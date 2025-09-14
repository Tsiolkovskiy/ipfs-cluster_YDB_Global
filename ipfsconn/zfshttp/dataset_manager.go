package zfshttp

import (
	"context"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var dmLogger = logging.Logger("zfs-dataset-manager")

// DatasetManager manages ZFS datasets for IPFS Cluster
type DatasetManager struct {
	config   *ZFSConfig
	datasets map[string]*Dataset
	shards   map[string]*Shard
	mu       sync.RWMutex
	
	// Metrics and monitoring
	totalPins    int64
	totalSize    int64
	lastOptimized time.Time
	
	// Shutdown coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Dataset represents a ZFS dataset used for storing IPFS pins
type Dataset struct {
	Name            string    `json:"name"`
	MountPoint      string    `json:"mount_point"`
	CompressionType string    `json:"compression_type"`
	RecordSize      string    `json:"record_size"`
	PinCount        int64     `json:"pin_count"`
	TotalSize       int64     `json:"total_size"`
	CreatedAt       time.Time `json:"created_at"`
	LastAccessed    time.Time `json:"last_accessed"`
	
	// ZFS properties
	CompressionRatio   float64 `json:"compression_ratio"`
	DeduplicationRatio float64 `json:"deduplication_ratio"`
	FragmentationLevel float64 `json:"fragmentation_level"`
	
	mu sync.RWMutex
}

// Shard represents a logical grouping of pins within datasets
type Shard struct {
	ID              string    `json:"id"`
	Dataset         string    `json:"dataset"`
	PinCount        int64     `json:"pin_count"`
	MaxCapacity     int64     `json:"max_capacity"`
	CompressionType string    `json:"compression_type"`
	CreatedAt       time.Time `json:"created_at"`
	LastRebalanced  time.Time `json:"last_rebalanced"`
	
	mu sync.RWMutex
}

// NewDatasetManager creates a new ZFS dataset manager
func NewDatasetManager(config *ZFSConfig) (*DatasetManager, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	dm := &DatasetManager{
		config:   config,
		datasets: make(map[string]*Dataset),
		shards:   make(map[string]*Shard),
		ctx:      ctx,
		cancel:   cancel,
	}
	
	// Initialize existing datasets
	if err := dm.discoverExistingDatasets(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to discover existing datasets: %w", err)
	}
	
	// Start background optimization if enabled
	if config.AutoOptimization {
		dm.wg.Add(1)
		go dm.optimizationLoop()
	}
	
	return dm, nil
}

// GetDatasetForCID returns the optimal dataset for storing a given CID
func (dm *DatasetManager) GetDatasetForCID(cid string) (*Dataset, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	
	// Use sharding strategy to determine dataset
	switch dm.config.ShardingStrategy {
	case "consistent_hash":
		return dm.getDatasetByConsistentHash(cid)
	case "round_robin":
		return dm.getDatasetByRoundRobin()
	case "size_based":
		return dm.getDatasetBySizeOptimization()
	default:
		return dm.getDatasetByConsistentHash(cid)
	}
}

// CreateDataset creates a new ZFS dataset
func (dm *DatasetManager) CreateDataset(name string) (*Dataset, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	// Check if dataset already exists
	if _, exists := dm.datasets[name]; exists {
		return dm.datasets[name], nil
	}
	
	fullName := fmt.Sprintf("%s/%s", dm.config.PoolName, name)
	mountPoint := filepath.Join(dm.config.BasePath, name)
	
	// Create ZFS dataset
	cmd := exec.Command("zfs", "create", 
		"-o", fmt.Sprintf("compression=%s", dm.config.CompressionType),
		"-o", fmt.Sprintf("recordsize=%s", dm.config.RecordSize),
		"-o", fmt.Sprintf("mountpoint=%s", mountPoint),
		fullName)
	
	if dm.config.EnableDeduplication {
		cmd.Args = append(cmd.Args, "-o", "dedup=on")
	}
	
	if dm.config.EnableEncryption && dm.config.EncryptionKey != "" {
		cmd.Args = append(cmd.Args, "-o", "encryption=aes-256-gcm", 
			"-o", "keyformat=passphrase", "-o", fmt.Sprintf("keylocation=file://%s", dm.config.EncryptionKey))
	}
	
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to create ZFS dataset %s: %w", fullName, err)
	}
	
	// Create dataset object
	dataset := &Dataset{
		Name:            fullName,
		MountPoint:      mountPoint,
		CompressionType: dm.config.CompressionType,
		RecordSize:      dm.config.RecordSize,
		CreatedAt:       time.Now(),
		LastAccessed:    time.Now(),
	}
	
	dm.datasets[name] = dataset
	
	dmLogger.Infof("Created ZFS dataset: %s at %s", fullName, mountPoint)
	return dataset, nil
}

// UpdatePinCount updates the pin count for a dataset
func (dm *DatasetManager) UpdatePinCount(datasetName string, delta int64) {
	dm.mu.RLock()
	dataset, exists := dm.datasets[datasetName]
	dm.mu.RUnlock()
	
	if !exists {
		dmLogger.Warnf("Dataset %s not found for pin count update", datasetName)
		return
	}
	
	dataset.mu.Lock()
	dataset.PinCount += delta
	dataset.LastAccessed = time.Now()
	dataset.mu.Unlock()
	
	// Update global counters
	dm.mu.Lock()
	dm.totalPins += delta
	dm.mu.Unlock()
	
	// Check if dataset needs to be split due to size
	if dataset.PinCount > dm.config.MaxPinsPerDataset {
		go dm.considerDatasetSplit(datasetName)
	}
}

// CreateSnapshot creates a ZFS snapshot
func (dm *DatasetManager) CreateSnapshot(dataset, name string) error {
	snapshotName := fmt.Sprintf("%s@%s", dataset, name)
	cmd := exec.Command("zfs", "snapshot", snapshotName)
	
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to create snapshot %s: %w", snapshotName, err)
	}
	
	dmLogger.Infof("Created ZFS snapshot: %s", snapshotName)
	return nil
}

// ListSnapshots lists available snapshots for a dataset
func (dm *DatasetManager) ListSnapshots(dataset string) ([]string, error) {
	cmd := exec.Command("zfs", "list", "-H", "-t", "snapshot", "-o", "name", "-s", "creation", dataset)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots for %s: %w", dataset, err)
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	snapshots := make([]string, 0, len(lines))
	
	for _, line := range lines {
		if line != "" {
			snapshots = append(snapshots, line)
		}
	}
	
	return snapshots, nil
}

// RollbackToSnapshot rolls back a dataset to a specific snapshot
func (dm *DatasetManager) RollbackToSnapshot(dataset, snapshot string) error {
	snapshotName := fmt.Sprintf("%s@%s", dataset, snapshot)
	cmd := exec.Command("zfs", "rollback", "-r", snapshotName)
	
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to rollback to snapshot %s: %w", snapshotName, err)
	}
	
	dmLogger.Infof("Rolled back dataset %s to snapshot %s", dataset, snapshot)
	return nil
}

// WriteFile writes data to a file in the ZFS dataset
func (dm *DatasetManager) WriteFile(path string, data []byte) error {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	
	// Write file atomically
	tempPath := path + ".tmp"
	if err := ioutil.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp file %s: %w", tempPath, err)
	}
	
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath) // Clean up on failure
		return fmt.Errorf("failed to rename temp file %s to %s: %w", tempPath, path, err)
	}
	
	return nil
}

// ReadFile reads data from a file in the ZFS dataset
func (dm *DatasetManager) ReadFile(path string) ([]byte, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}
	return data, nil
}

// DeleteFile deletes a file from the ZFS dataset
func (dm *DatasetManager) DeleteFile(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file %s: %w", path, err)
	}
	return nil
}

// Shutdown gracefully shuts down the dataset manager
func (dm *DatasetManager) Shutdown(ctx context.Context) error {
	dm.cancel()
	
	// Wait for background goroutines to finish
	done := make(chan struct{})
	go func() {
		dm.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		dmLogger.Info("Dataset manager shutdown completed")
		return nil
	case <-ctx.Done():
		dmLogger.Warn("Dataset manager shutdown timed out")
		return ctx.Err()
	}
}

// getDatasetByConsistentHash selects dataset using consistent hashing
func (dm *DatasetManager) getDatasetByConsistentHash(cid string) (*Dataset, error) {
	if len(dm.datasets) == 0 {
		// Create first dataset
		return dm.CreateDataset("shard-0")
	}
	
	// Use CRC32 hash for consistent distribution
	hash := crc32.ChecksumIEEE([]byte(cid))
	shardIndex := int(hash) % len(dm.datasets)
	
	// Get dataset by index
	i := 0
	for _, dataset := range dm.datasets {
		if i == shardIndex {
			return dataset, nil
		}
		i++
	}
	
	// Fallback to first dataset
	for _, dataset := range dm.datasets {
		return dataset, nil
	}
	
	return nil, fmt.Errorf("no datasets available")
}

// getDatasetByRoundRobin selects dataset using round-robin
func (dm *DatasetManager) getDatasetByRoundRobin() (*Dataset, error) {
	if len(dm.datasets) == 0 {
		return dm.CreateDataset("shard-0")
	}
	
	// Find dataset with minimum pin count
	var minDataset *Dataset
	var minCount int64 = -1
	
	for _, dataset := range dm.datasets {
		dataset.mu.RLock()
		count := dataset.PinCount
		dataset.mu.RUnlock()
		
		if minCount == -1 || count < minCount {
			minCount = count
			minDataset = dataset
		}
	}
	
	return minDataset, nil
}

// getDatasetBySizeOptimization selects dataset based on size optimization
func (dm *DatasetManager) getDatasetBySizeOptimization() (*Dataset, error) {
	if len(dm.datasets) == 0 {
		return dm.CreateDataset("shard-0")
	}
	
	// Find dataset with best compression ratio and available space
	var bestDataset *Dataset
	var bestScore float64 = -1
	
	for _, dataset := range dm.datasets {
		dataset.mu.RLock()
		pinCount := dataset.PinCount
		compressionRatio := dataset.CompressionRatio
		dataset.mu.RUnlock()
		
		// Calculate score based on available space and compression efficiency
		availableSpace := float64(dm.config.MaxPinsPerDataset - pinCount)
		score := availableSpace * compressionRatio
		
		if bestScore == -1 || score > bestScore {
			bestScore = score
			bestDataset = dataset
		}
	}
	
	return bestDataset, nil
}

// discoverExistingDatasets discovers existing ZFS datasets
func (dm *DatasetManager) discoverExistingDatasets() error {
	cmd := exec.Command("zfs", "list", "-H", "-o", "name,mountpoint", "-t", "filesystem", dm.config.PoolName)
	output, err := cmd.Output()
	if err != nil {
		// Pool might not exist yet, which is okay
		dmLogger.Infof("No existing ZFS datasets found for pool %s", dm.config.PoolName)
		return nil
	}
	
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		
		name := fields[0]
		mountPoint := fields[1]
		
		// Skip the root pool dataset
		if name == dm.config.PoolName {
			continue
		}
		
		// Extract shard name
		parts := strings.Split(name, "/")
		if len(parts) < 2 {
			continue
		}
		shardName := parts[len(parts)-1]
		
		dataset := &Dataset{
			Name:            name,
			MountPoint:      mountPoint,
			CompressionType: dm.config.CompressionType,
			RecordSize:      dm.config.RecordSize,
			CreatedAt:       time.Now(), // We don't have the actual creation time
			LastAccessed:    time.Now(),
		}
		
		dm.datasets[shardName] = dataset
		dmLogger.Infof("Discovered existing ZFS dataset: %s at %s", name, mountPoint)
	}
	
	return nil
}

// considerDatasetSplit considers splitting a dataset when it becomes too large
func (dm *DatasetManager) considerDatasetSplit(datasetName string) {
	dm.mu.RLock()
	dataset, exists := dm.datasets[datasetName]
	dm.mu.RUnlock()
	
	if !exists {
		return
	}
	
	dataset.mu.RLock()
	pinCount := dataset.PinCount
	dataset.mu.RUnlock()
	
	if pinCount > dm.config.MaxPinsPerDataset {
		dmLogger.Infof("Dataset %s has %d pins, considering split (max: %d)", 
			datasetName, pinCount, dm.config.MaxPinsPerDataset)
		
		// Create new dataset for future pins
		newShardName := fmt.Sprintf("shard-%d", time.Now().Unix())
		if _, err := dm.CreateDataset(newShardName); err != nil {
			dmLogger.Errorf("Failed to create new dataset %s: %s", newShardName, err)
		}
	}
}

// optimizationLoop runs periodic optimization tasks
func (dm *DatasetManager) optimizationLoop() {
	defer dm.wg.Done()
	
	ticker := time.NewTicker(time.Hour) // Run optimization every hour
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			dm.runOptimization()
		case <-dm.ctx.Done():
			return
		}
	}
}

// runOptimization performs dataset optimization tasks
func (dm *DatasetManager) runOptimization() {
	dmLogger.Info("Running dataset optimization")
	
	dm.mu.RLock()
	datasets := make([]*Dataset, 0, len(dm.datasets))
	for _, dataset := range dm.datasets {
		datasets = append(datasets, dataset)
	}
	dm.mu.RUnlock()
	
	for _, dataset := range datasets {
		// Update dataset metrics
		if err := dm.updateDatasetMetrics(dataset); err != nil {
			dmLogger.Warnf("Failed to update metrics for dataset %s: %s", dataset.Name, err)
		}
		
		// Consider defragmentation if fragmentation is high
		dataset.mu.RLock()
		fragmentation := dataset.FragmentationLevel
		dataset.mu.RUnlock()
		
		if fragmentation > 30.0 { // 30% fragmentation threshold
			dmLogger.Infof("Dataset %s has high fragmentation (%.1f%%), scheduling defrag", 
				dataset.Name, fragmentation)
			go dm.defragmentDataset(dataset.Name)
		}
	}
	
	dm.mu.Lock()
	dm.lastOptimized = time.Now()
	dm.mu.Unlock()
}

// updateDatasetMetrics updates performance metrics for a dataset
func (dm *DatasetManager) updateDatasetMetrics(dataset *Dataset) error {
	// Get compression ratio
	cmd := exec.Command("zfs", "get", "-H", "-p", "compressratio", dataset.Name)
	if output, err := cmd.Output(); err == nil {
		fields := strings.Fields(string(output))
		if len(fields) >= 3 {
			if ratio, err := strconv.ParseFloat(strings.TrimSuffix(fields[2], "x"), 64); err == nil {
				dataset.mu.Lock()
				dataset.CompressionRatio = ratio
				dataset.mu.Unlock()
			}
		}
	}
	
	// Get used space
	cmd = exec.Command("zfs", "get", "-H", "-p", "used", dataset.Name)
	if output, err := cmd.Output(); err == nil {
		fields := strings.Fields(string(output))
		if len(fields) >= 3 {
			if size, err := strconv.ParseInt(fields[2], 10, 64); err == nil {
				dataset.mu.Lock()
				dataset.TotalSize = size
				dataset.mu.Unlock()
			}
		}
	}
	
	return nil
}

// defragmentDataset performs defragmentation on a dataset
func (dm *DatasetManager) defragmentDataset(datasetName string) {
	dmLogger.Infof("Starting defragmentation for dataset %s", datasetName)
	
	// Create a snapshot before defragmentation
	snapshotName := fmt.Sprintf("defrag-%d", time.Now().Unix())
	if err := dm.CreateSnapshot(datasetName, snapshotName); err != nil {
		dmLogger.Errorf("Failed to create snapshot before defrag: %s", err)
		return
	}
	
	// Run ZFS scrub to defragment
	cmd := exec.Command("zpool", "scrub", dm.config.PoolName)
	if err := cmd.Run(); err != nil {
		dmLogger.Errorf("Failed to run scrub for defragmentation: %s", err)
		return
	}
	
	dmLogger.Infof("Defragmentation completed for dataset %s", datasetName)
}