package resource

import (
	"context"
	"testing"
	"time"
)

func TestNewResourceManager(t *testing.T) {
	config := DefaultConfig()
	
	rm, err := NewResourceManager(config)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}
	
	if rm == nil {
		t.Fatal("Resource manager is nil")
	}
	
	if rm.config != config {
		t.Error("Config not set correctly")
	}
	
	if rm.pools == nil {
		t.Error("Pools map not initialized")
	}
	
	if rm.disks == nil {
		t.Error("Disks map not initialized")
	}
}

func TestResourceManagerValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name:        "valid config",
			config:      DefaultConfig(),
			expectError: false,
		},
		{
			name: "invalid capacity threshold",
			config: &Config{
				CapacityThreshold:   1.5, // Invalid: > 1
				MinFreeSpace:        1024,
				DiskScanInterval:    time.Minute,
				HealthCheckInterval: time.Minute,
			},
			expectError: true,
		},
		{
			name: "negative min free space",
			config: &Config{
				CapacityThreshold:   0.8,
				MinFreeSpace:        -1024, // Invalid: negative
				DiskScanInterval:    time.Minute,
				HealthCheckInterval: time.Minute,
			},
			expectError: true,
		},
		{
			name: "zero disk scan interval",
			config: &Config{
				CapacityThreshold:   0.8,
				MinFreeSpace:        1024,
				DiskScanInterval:    0, // Invalid: zero
				HealthCheckInterval: time.Minute,
			},
			expectError: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewResourceManager(tt.config)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestPoolOperations(t *testing.T) {
	config := DefaultConfig()
	rm, err := NewResourceManager(config)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}
	
	// Test adding a mock pool
	pool := &PoolInfo{
		Name:      "test-pool",
		State:     "ONLINE",
		Size:      1024 * 1024 * 1024 * 1024, // 1TB
		Used:      512 * 1024 * 1024 * 1024,  // 512GB
		Available: 512 * 1024 * 1024 * 1024,  // 512GB
		Health:    "ONLINE",
		Disks:     []string{"/dev/sda", "/dev/sdb"},
		Properties: make(map[string]string),
		CreatedAt: time.Now(),
	}
	
	rm.mu.Lock()
	rm.pools[pool.Name] = pool
	rm.mu.Unlock()
	
	// Test GetPoolInfo
	retrievedPool, err := rm.GetPoolInfo("test-pool")
	if err != nil {
		t.Errorf("Failed to get pool info: %v", err)
	}
	
	if retrievedPool.Name != pool.Name {
		t.Errorf("Expected pool name %s, got %s", pool.Name, retrievedPool.Name)
	}
	
	// Test ListPools
	pools := rm.ListPools()
	if len(pools) != 1 {
		t.Errorf("Expected 1 pool, got %d", len(pools))
	}
	
	// Test non-existent pool
	_, err = rm.GetPoolInfo("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent pool")
	}
}

func TestDiskOperations(t *testing.T) {
	config := DefaultConfig()
	rm, err := NewResourceManager(config)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}
	
	// Test adding a mock disk
	disk := &DiskInfo{
		Device:      "/dev/sdc",
		Model:       "Test Disk",
		Serial:      "TEST123",
		Size:        1024 * 1024 * 1024 * 1024, // 1TB
		Health:      "GOOD",
		Temperature: 35,
		State:       "AVAILABLE",
		LastChecked: time.Now(),
		SMARTAttributes: make(map[string]interface{}),
	}
	
	rm.mu.Lock()
	rm.disks[disk.Device] = disk
	rm.mu.Unlock()
	
	// Test GetDiskInfo
	retrievedDisk, err := rm.GetDiskInfo("/dev/sdc")
	if err != nil {
		t.Errorf("Failed to get disk info: %v", err)
	}
	
	if retrievedDisk.Device != disk.Device {
		t.Errorf("Expected disk device %s, got %s", disk.Device, retrievedDisk.Device)
	}
	
	// Test ListDisks
	disks := rm.ListDisks()
	if len(disks) != 1 {
		t.Errorf("Expected 1 disk, got %d", len(disks))
	}
	
	// Test non-existent disk
	_, err = rm.GetDiskInfo("/dev/nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent disk")
	}
}

func TestCapacityCheck(t *testing.T) {
	config := DefaultConfig()
	config.CapacityThreshold = 0.7 // 70% threshold for testing
	
	rm, err := NewResourceManager(config)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}
	
	// Add a pool that exceeds threshold
	pool := &PoolInfo{
		Name:      "test-pool",
		Size:      1000,
		Used:      800, // 80% used, exceeds 70% threshold
		Available: 200,
		Health:    "ONLINE",
		Disks:     []string{"/dev/sda"},
		Properties: make(map[string]string),
	}
	
	rm.mu.Lock()
	rm.pools[pool.Name] = pool
	rm.mu.Unlock()
	
	// Add an available disk for expansion
	disk := &DiskInfo{
		Device: "/dev/sdb",
		Size:   1000,
		Health: "GOOD",
		State:  "AVAILABLE",
		Pool:   "", // Available
	}
	
	rm.mu.Lock()
	rm.disks[disk.Device] = disk
	rm.mu.Unlock()
	
	ctx := context.Background()
	
	// This would normally trigger expansion, but we're testing the logic
	err = rm.CheckCapacity(ctx)
	if err != nil {
		t.Errorf("Capacity check failed: %v", err)
	}
}

func TestFindAvailableDisks(t *testing.T) {
	config := DefaultConfig()
	rm, err := NewResourceManager(config)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}
	
	// Add available and unavailable disks
	disks := []*DiskInfo{
		{
			Device: "/dev/sda",
			Health: "GOOD",
			State:  "AVAILABLE",
			Pool:   "", // Available
		},
		{
			Device: "/dev/sdb",
			Health: "GOOD",
			State:  "AVAILABLE",
			Pool:   "existing-pool", // Not available
		},
		{
			Device: "/dev/sdc",
			Health: "FAILED",
			State:  "AVAILABLE",
			Pool:   "", // Not healthy
		},
		{
			Device: "/dev/sdd",
			Health: "GOOD",
			State:  "AVAILABLE",
			Pool:   "", // Available
		},
	}
	
	rm.mu.Lock()
	for _, disk := range disks {
		rm.disks[disk.Device] = disk
	}
	rm.mu.Unlock()
	
	available := rm.findAvailableDisks()
	
	// Should find 2 available disks (/dev/sda and /dev/sdd)
	if len(available) != 2 {
		t.Errorf("Expected 2 available disks, got %d", len(available))
	}
	
	// Check that the right disks are found
	foundDevices := make(map[string]bool)
	for _, disk := range available {
		foundDevices[disk.Device] = true
	}
	
	if !foundDevices["/dev/sda"] || !foundDevices["/dev/sdd"] {
		t.Error("Wrong disks identified as available")
	}
}

func TestSelectBestDisk(t *testing.T) {
	config := DefaultConfig()
	rm, err := NewResourceManager(config)
	if err != nil {
		t.Fatalf("Failed to create resource manager: %v", err)
	}
	
	disks := []*DiskInfo{
		{
			Device:      "/dev/sda",
			Size:        1000,
			Temperature: 45,
			ReallocatedSectors: 0,
		},
		{
			Device:      "/dev/sdb",
			Size:        2000, // Larger
			Temperature: 35,   // Cooler
			ReallocatedSectors: 0,
		},
		{
			Device:      "/dev/sdc",
			Size:        1500,
			Temperature: 55, // Hotter
			ReallocatedSectors: 5, // Has reallocated sectors
		},
	}
	
	best := rm.selectBestDisk(disks)
	
	if best == nil {
		t.Fatal("No best disk selected")
	}
	
	// Should select /dev/sdb (largest, coolest, no reallocated sectors)
	if best.Device != "/dev/sdb" {
		t.Errorf("Expected /dev/sdb as best disk, got %s", best.Device)
	}
}

func TestParseZFSSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"1024", 1024, false},
		{"1K", 1024, false},
		{"1M", 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"1T", 1024 * 1024 * 1024 * 1024, false},
		{"1.5G", int64(1.5 * 1024 * 1024 * 1024), false},
		{"0", 0, false},
		{"-", 0, false},
		{"invalid", 0, true},
		{"1X", 0, true},
	}
	
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := parseZFSSize(tt.input)
			
			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error for input %s", tt.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input %s: %v", tt.input, err)
				}
				if result != tt.expected {
					t.Errorf("Expected %d, got %d for input %s", tt.expected, result, tt.input)
				}
			}
		})
	}
}