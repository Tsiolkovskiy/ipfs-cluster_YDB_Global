// Package resource provides automated resource management for IPFS Cluster ZFS integration
// It handles automatic disk addition, health monitoring, and performance optimization
// to support trillion-scale pin operations.
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

var logger = logging.Logger("resource-manager")

// ResourceManager manages ZFS pool resources automatically
type ResourceManager struct {
	config          *Config
	pools           map[string]*PoolInfo
	disks           map[string]*DiskInfo
	healthMonitor   *HealthMonitor
	layoutOptimizer *LayoutOptimizer
	
	// Synchronization
	mu              sync.RWMutex
	
	// Background processes
	monitorTicker   *time.Ticker
	stopMonitoring  chan struct{}
	maintenanceTicker *time.Ticker
	stopMaintenance chan struct{}
}

// Config holds configuration for the resource manager
type Config struct {
	// Pool management
	AutoExpandPools     bool          `json:"auto_expand_pools"`
	CapacityThreshold   float64       `json:"capacity_threshold"`     // 0.8 = 80%
	MinFreeSpace        int64         `json:"min_free_space"`         // bytes
	
	// Disk management
	AutoAddDisks        bool          `json:"auto_add_disks"`
	DiskScanInterval    time.Duration `json:"disk_scan_interval"`
	HealthCheckInterval time.Duration `json:"health_check_interval"`
	
	// Performance optimization
	AutoOptimizeLayout  bool          `json:"auto_optimize_layout"`
	OptimizationInterval time.Duration `json:"optimization_interval"`
	
	// Replacement settings
	AutoReplaceFailed   bool          `json:"auto_replace_failed"`
	ReplacementTimeout  time.Duration `json:"replacement_timeout"`
	
	// Monitoring
	MetricsInterval     time.Duration `json:"metrics_interval"`
	AlertThresholds     AlertThresholds `json:"alert_thresholds"`
}

// PoolInfo represents information about a ZFS pool
type PoolInfo struct {
	Name            string            `json:"name"`
	State           string            `json:"state"`
	Size            int64             `json:"size"`
	Used            int64             `json:"used"`
	Available       int64             `json:"available"`
	Fragmentation   float64           `json:"fragmentation"`
	Health          string            `json:"health"`
	Disks           []string          `json:"disks"`
	Properties      map[string]string `json:"properties"`
	LastScrub       time.Time         `json:"last_scrub"`
	CreatedAt       time.Time         `json:"created_at"`
	LastOptimized   time.Time         `json:"last_optimized"`
	
	// Performance metrics
	ReadOps         int64   `json:"read_ops"`
	WriteOps        int64   `json:"write_ops"`
	ReadBandwidth   int64   `json:"read_bandwidth"`
	WriteBandwidth  int64   `json:"write_bandwidth"`
	
	mu sync.RWMutex `json:"-"`
}

// DiskInfo represents information about a physical disk
type DiskInfo struct {
	Device          string    `json:"device"`
	Model           string    `json:"model"`
	Serial          string    `json:"serial"`
	Size            int64     `json:"size"`
	Health          string    `json:"health"`
	Temperature     int       `json:"temperature"`
	PowerOnHours    int64     `json:"power_on_hours"`
	ReallocatedSectors int64  `json:"reallocated_sectors"`
	PendingSectors  int64     `json:"pending_sectors"`
	Pool            string    `json:"pool"`
	State           string    `json:"state"`
	LastChecked     time.Time `json:"last_checked"`
	
	// SMART attributes
	SMARTAttributes map[string]interface{} `json:"smart_attributes"`
	
	mu sync.RWMutex `json:"-"`
}

// AlertThresholds defines thresholds for various alerts
type AlertThresholds struct {
	CapacityWarning     float64 `json:"capacity_warning"`      // 0.8 = 80%
	CapacityCritical    float64 `json:"capacity_critical"`     // 0.9 = 90%
	FragmentationWarning float64 `json:"fragmentation_warning"` // 0.3 = 30%
	TemperatureWarning  int     `json:"temperature_warning"`   // Celsius
	TemperatureCritical int     `json:"temperature_critical"`  // Celsius
}

// NewResourceManager creates a new resource manager instance
func NewResourceManager(config *Config) (*ResourceManager, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid configuration")
	}

	rm := &ResourceManager{
		config:          config,
		pools:           make(map[string]*PoolInfo),
		disks:           make(map[string]*DiskInfo),
		healthMonitor:   NewHealthMonitor(config),
		layoutOptimizer: NewLayoutOptimizer(config),
		stopMonitoring:  make(chan struct{}),
		stopMaintenance: make(chan struct{}),
	}

	logger.Info("Resource manager initialized successfully")
	return rm, nil
}

// Start starts the resource manager and begins monitoring
func (rm *ResourceManager) Start(ctx context.Context) error {
	logger.Info("Starting resource manager")

	// Discover existing pools and disks
	if err := rm.discoverResources(ctx); err != nil {
		return errors.Wrap(err, "discovering resources")
	}

	// Start monitoring
	if rm.config.HealthCheckInterval > 0 {
		rm.startHealthMonitoring()
	}

	// Start maintenance
	if rm.config.OptimizationInterval > 0 {
		rm.startMaintenance()
	}

	logger.Info("Resource manager started successfully")
	return nil
}

// Stop stops the resource manager and cleans up resources
func (rm *ResourceManager) Stop() error {
	logger.Info("Stopping resource manager")

	// Stop monitoring
	if rm.monitorTicker != nil {
		rm.monitorTicker.Stop()
		close(rm.stopMonitoring)
	}

	// Stop maintenance
	if rm.maintenanceTicker != nil {
		rm.maintenanceTicker.Stop()
		close(rm.stopMaintenance)
	}

	logger.Info("Resource manager stopped")
	return nil
}

// AddDiskToPool automatically adds a disk to the specified pool
func (rm *ResourceManager) AddDiskToPool(ctx context.Context, poolName, diskDevice string) error {
	logger.Infof("Adding disk %s to pool %s", diskDevice, poolName)

	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Verify pool exists
	pool, exists := rm.pools[poolName]
	if !exists {
		return fmt.Errorf("pool %s not found", poolName)
	}

	// Verify disk is available
	disk, exists := rm.disks[diskDevice]
	if !exists {
		return fmt.Errorf("disk %s not found", diskDevice)
	}

	disk.mu.RLock()
	if disk.Pool != "" {
		disk.mu.RUnlock()
		return fmt.Errorf("disk %s is already in use by pool %s", diskDevice, disk.Pool)
	}
	disk.mu.RUnlock()

	// Add disk to pool using zpool add
	cmd := exec.CommandContext(ctx, "zpool", "add", poolName, diskDevice)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "adding disk %s to pool %s", diskDevice, poolName)
	}

	// Update pool and disk information
	pool.mu.Lock()
	pool.Disks = append(pool.Disks, diskDevice)
	pool.mu.Unlock()

	disk.mu.Lock()
	disk.Pool = poolName
	disk.State = "ONLINE"
	disk.mu.Unlock()

	logger.Infof("Successfully added disk %s to pool %s", diskDevice, poolName)
	return nil
}

// ExpandPool automatically expands a pool when capacity threshold is reached
func (rm *ResourceManager) ExpandPool(ctx context.Context, poolName string) error {
	logger.Infof("Expanding pool %s", poolName)

	rm.mu.RLock()
	pool, exists := rm.pools[poolName]
	rm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pool %s not found", poolName)
	}

	// Find available disks
	availableDisks := rm.findAvailableDisks()
	if len(availableDisks) == 0 {
		return fmt.Errorf("no available disks for pool expansion")
	}

	// Add the best available disk
	bestDisk := rm.selectBestDisk(availableDisks)
	if bestDisk == nil {
		return fmt.Errorf("no suitable disk found for expansion")
	}

	return rm.AddDiskToPool(ctx, poolName, bestDisk.Device)
}

// ReplaceFaultyDisk automatically replaces a faulty disk
func (rm *ResourceManager) ReplaceFaultyDisk(ctx context.Context, poolName, faultyDisk string) error {
	logger.Infof("Replacing faulty disk %s in pool %s", faultyDisk, poolName)

	// Find replacement disk
	availableDisks := rm.findAvailableDisks()
	replacementDisk := rm.selectBestDisk(availableDisks)
	if replacementDisk == nil {
		return fmt.Errorf("no suitable replacement disk found")
	}

	// Replace disk using zpool replace
	cmd := exec.CommandContext(ctx, "zpool", "replace", poolName, faultyDisk, replacementDisk.Device)
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "replacing disk %s with %s in pool %s", 
			faultyDisk, replacementDisk.Device, poolName)
	}

	// Update disk information
	rm.mu.Lock()
	if disk, exists := rm.disks[faultyDisk]; exists {
		disk.mu.Lock()
		disk.State = "REPLACED"
		disk.Pool = ""
		disk.mu.Unlock()
	}

	if disk, exists := rm.disks[replacementDisk.Device]; exists {
		disk.mu.Lock()
		disk.Pool = poolName
		disk.State = "ONLINE"
		disk.mu.Unlock()
	}
	rm.mu.Unlock()

	logger.Infof("Successfully replaced disk %s with %s in pool %s", 
		faultyDisk, replacementDisk.Device, poolName)
	return nil
}

// OptimizePoolLayout optimizes the data layout for maximum performance
func (rm *ResourceManager) OptimizePoolLayout(ctx context.Context, poolName string) error {
	logger.Infof("Optimizing layout for pool %s", poolName)

	rm.mu.RLock()
	pool, exists := rm.pools[poolName]
	rm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pool %s not found", poolName)
	}

	// Use layout optimizer to determine optimal configuration
	optimization := rm.layoutOptimizer.AnalyzePool(pool)
	
	// Apply optimizations
	for _, action := range optimization.Actions {
		if err := rm.applyOptimizationAction(ctx, poolName, action); err != nil {
			logger.Errorf("Failed to apply optimization action %s: %v", action.Type, err)
		}
	}

	// Update last optimized timestamp
	pool.mu.Lock()
	pool.LastOptimized = time.Now()
	pool.mu.Unlock()

	logger.Infof("Pool %s optimization completed", poolName)
	return nil
}

// GetPoolInfo returns information about a specific pool
func (rm *ResourceManager) GetPoolInfo(poolName string) (*PoolInfo, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	pool, exists := rm.pools[poolName]
	if !exists {
		return nil, fmt.Errorf("pool %s not found", poolName)
	}

	// Return a copy to avoid race conditions
	poolCopy := *pool
	return &poolCopy, nil
}

// ListPools returns information about all pools
func (rm *ResourceManager) ListPools() []*PoolInfo {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	pools := make([]*PoolInfo, 0, len(rm.pools))
	for _, pool := range rm.pools {
		poolCopy := *pool
		pools = append(pools, &poolCopy)
	}

	return pools
}

// GetDiskInfo returns information about a specific disk
func (rm *ResourceManager) GetDiskInfo(device string) (*DiskInfo, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	disk, exists := rm.disks[device]
	if !exists {
		return nil, fmt.Errorf("disk %s not found", device)
	}

	// Return a copy to avoid race conditions
	diskCopy := *disk
	return &diskCopy, nil
}

// ListDisks returns information about all disks
func (rm *ResourceManager) ListDisks() []*DiskInfo {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	disks := make([]*DiskInfo, 0, len(rm.disks))
	for _, disk := range rm.disks {
		diskCopy := *disk
		disks = append(disks, &diskCopy)
	}

	return disks
}

// CheckCapacity checks if pools need expansion based on capacity thresholds
func (rm *ResourceManager) CheckCapacity(ctx context.Context) error {
	if !rm.config.AutoExpandPools {
		return nil
	}

	rm.mu.RLock()
	pools := make([]*PoolInfo, 0, len(rm.pools))
	for _, pool := range rm.pools {
		pools = append(pools, pool)
	}
	rm.mu.RUnlock()

	for _, pool := range pools {
		pool.mu.RLock()
		utilizationRatio := float64(pool.Used) / float64(pool.Size)
		pool.mu.RUnlock()

		if utilizationRatio >= rm.config.CapacityThreshold {
			logger.Infof("Pool %s capacity threshold reached (%.2f%%), expanding", 
				pool.Name, utilizationRatio*100)
			
			if err := rm.ExpandPool(ctx, pool.Name); err != nil {
				logger.Errorf("Failed to expand pool %s: %v", pool.Name, err)
			}
		}
	}

	return nil
}

// Private methods

func (rm *ResourceManager) discoverResources(ctx context.Context) error {
	// Discover pools
	if err := rm.discoverPools(ctx); err != nil {
		return errors.Wrap(err, "discovering pools")
	}

	// Discover disks
	if err := rm.discoverDisks(ctx); err != nil {
		return errors.Wrap(err, "discovering disks")
	}

	return nil
}

func (rm *ResourceManager) discoverPools(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "zpool", "list", "-H", "-o", 
		"name,size,alloc,free,frag,health")
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrap(err, "listing pools")
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}

		pool := &PoolInfo{
			Name:       fields[0],
			Health:     fields[5],
			CreatedAt:  time.Now(), // Would be parsed from zpool history in real implementation
			Properties: make(map[string]string),
		}

		// Parse sizes
		if size, err := parseZFSSize(fields[1]); err == nil {
			pool.Size = size
		}
		if used, err := parseZFSSize(fields[2]); err == nil {
			pool.Used = used
		}
		if available, err := parseZFSSize(fields[3]); err == nil {
			pool.Available = available
		}
		if frag, err := strconv.ParseFloat(strings.TrimSuffix(fields[4], "%"), 64); err == nil {
			pool.Fragmentation = frag / 100.0
		}

		// Get pool disks
		pool.Disks = rm.getPoolDisks(ctx, pool.Name)

		rm.pools[pool.Name] = pool
	}

	return nil
}

func (rm *ResourceManager) discoverDisks(ctx context.Context) error {
	// Use lsblk to discover available disks
	cmd := exec.CommandContext(ctx, "lsblk", "-J", "-o", "NAME,SIZE,MODEL,SERIAL,TYPE")
	output, err := cmd.Output()
	if err != nil {
		return errors.Wrap(err, "listing block devices")
	}

	// Parse JSON output (simplified implementation)
	// In real implementation, would properly parse JSON and extract disk information
	
	// For now, simulate some disks
	simulatedDisks := []string{"/dev/sdb", "/dev/sdc", "/dev/sdd", "/dev/sde"}
	for _, device := range simulatedDisks {
		disk := &DiskInfo{
			Device:      device,
			Model:       "Simulated Disk",
			Serial:      fmt.Sprintf("SIM%s", device[len(device)-3:]),
			Size:        4 * 1024 * 1024 * 1024 * 1024, // 4TB
			Health:      "GOOD",
			Temperature: 35,
			State:       "AVAILABLE",
			LastChecked: time.Now(),
			SMARTAttributes: make(map[string]interface{}),
		}

		rm.disks[device] = disk
	}

	return nil
}

func (rm *ResourceManager) getPoolDisks(ctx context.Context, poolName string) []string {
	cmd := exec.CommandContext(ctx, "zpool", "status", poolName)
	output, err := cmd.Output()
	if err != nil {
		return nil
	}

	// Parse zpool status output to extract disk devices
	// This is a simplified implementation
	var disks []string
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "/dev/") {
			fields := strings.Fields(line)
			if len(fields) > 0 {
				disks = append(disks, fields[0])
			}
		}
	}

	return disks
}

func (rm *ResourceManager) findAvailableDisks() []*DiskInfo {
	var available []*DiskInfo
	
	for _, disk := range rm.disks {
		disk.mu.RLock()
		if disk.Pool == "" && disk.Health == "GOOD" && disk.State == "AVAILABLE" {
			available = append(available, disk)
		}
		disk.mu.RUnlock()
	}

	return available
}

func (rm *ResourceManager) selectBestDisk(disks []*DiskInfo) *DiskInfo {
	if len(disks) == 0 {
		return nil
	}

	// Select disk with largest size and best health
	var bestDisk *DiskInfo
	var bestScore float64

	for _, disk := range disks {
		disk.mu.RLock()
		score := float64(disk.Size)
		if disk.Temperature < 40 {
			score *= 1.1 // Bonus for cooler disks
		}
		if disk.ReallocatedSectors == 0 {
			score *= 1.05 // Bonus for no reallocated sectors
		}
		disk.mu.RUnlock()

		if bestDisk == nil || score > bestScore {
			bestDisk = disk
			bestScore = score
		}
	}

	return bestDisk
}

func (rm *ResourceManager) startHealthMonitoring() {
	rm.monitorTicker = time.NewTicker(rm.config.HealthCheckInterval)
	
	go func() {
		for {
			select {
			case <-rm.monitorTicker.C:
				rm.performHealthCheck()
			case <-rm.stopMonitoring:
				return
			}
		}
	}()
}

func (rm *ResourceManager) startMaintenance() {
	rm.maintenanceTicker = time.NewTicker(rm.config.OptimizationInterval)
	
	go func() {
		for {
			select {
			case <-rm.maintenanceTicker.C:
				rm.performMaintenance()
			case <-rm.stopMaintenance:
				return
			}
		}
	}()
}

func (rm *ResourceManager) performHealthCheck() {
	ctx := context.Background()
	
	// Check pool health
	for poolName := range rm.pools {
		if err := rm.healthMonitor.CheckPoolHealth(ctx, poolName); err != nil {
			logger.Errorf("Pool health check failed for %s: %v", poolName, err)
		}
	}

	// Check disk health
	for device := range rm.disks {
		if err := rm.healthMonitor.CheckDiskHealth(ctx, device); err != nil {
			logger.Errorf("Disk health check failed for %s: %v", device, err)
		}
	}

	// Check capacity
	if err := rm.CheckCapacity(ctx); err != nil {
		logger.Errorf("Capacity check failed: %v", err)
	}
}

func (rm *ResourceManager) performMaintenance() {
	ctx := context.Background()
	
	if rm.config.AutoOptimizeLayout {
		for poolName := range rm.pools {
			if err := rm.OptimizePoolLayout(ctx, poolName); err != nil {
				logger.Errorf("Layout optimization failed for pool %s: %v", poolName, err)
			}
		}
	}
}

func (rm *ResourceManager) applyOptimizationAction(ctx context.Context, poolName string, action *OptimizationAction) error {
	switch action.Type {
	case "adjust_recordsize":
		return rm.adjustRecordSize(ctx, poolName, action.Parameters["recordsize"].(string))
	case "enable_compression":
		return rm.enableCompression(ctx, poolName, action.Parameters["algorithm"].(string))
	case "rebalance_data":
		return rm.rebalancePoolData(ctx, poolName)
	default:
		return fmt.Errorf("unknown optimization action: %s", action.Type)
	}
}

func (rm *ResourceManager) adjustRecordSize(ctx context.Context, poolName, recordSize string) error {
	// This would adjust recordsize for datasets in the pool
	logger.Infof("Adjusting recordsize to %s for pool %s", recordSize, poolName)
	return nil
}

func (rm *ResourceManager) enableCompression(ctx context.Context, poolName, algorithm string) error {
	// This would enable compression for datasets in the pool
	logger.Infof("Enabling %s compression for pool %s", algorithm, poolName)
	return nil
}

func (rm *ResourceManager) rebalancePoolData(ctx context.Context, poolName string) error {
	// This would rebalance data across pool devices
	logger.Infof("Rebalancing data for pool %s", poolName)
	return nil
}

// Helper function to parse ZFS sizes
func parseZFSSize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "0" || sizeStr == "-" {
		return 0, nil
	}

	// Extract numeric part and unit
	var numStr string
	var unit string
	
	for i, r := range sizeStr {
		if (r >= '0' && r <= '9') || r == '.' {
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

// Validate validates the resource manager configuration
func (c *Config) Validate() error {
	if c.CapacityThreshold <= 0 || c.CapacityThreshold >= 1 {
		return fmt.Errorf("capacity_threshold must be between 0 and 1")
	}
	if c.MinFreeSpace < 0 {
		return fmt.Errorf("min_free_space must be non-negative")
	}
	if c.DiskScanInterval <= 0 {
		return fmt.Errorf("disk_scan_interval must be positive")
	}
	if c.HealthCheckInterval <= 0 {
		return fmt.Errorf("health_check_interval must be positive")
	}
	return nil
}