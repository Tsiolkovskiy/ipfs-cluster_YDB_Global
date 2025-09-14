package sharding

import (
	"fmt"
	"time"
)

// MockZFSManager implements ZFSManager interface for testing
type MockZFSManager struct {
	snapshots    map[string][]string // dataset -> snapshot names
	datasets     map[string]int64    // dataset -> size
	failOperations map[string]bool   // operation -> should fail
}

// NewMockZFSManager creates a new mock ZFS manager
func NewMockZFSManager() *MockZFSManager {
	return &MockZFSManager{
		snapshots:      make(map[string][]string),
		datasets:       make(map[string]int64),
		failOperations: make(map[string]bool),
	}
}

// CreateSnapshot creates a mock snapshot
func (m *MockZFSManager) CreateSnapshot(dataset, name string) error {
	if m.failOperations["CreateSnapshot"] {
		return fmt.Errorf("mock: create snapshot failed")
	}

	if m.snapshots[dataset] == nil {
		m.snapshots[dataset] = make([]string, 0)
	}
	m.snapshots[dataset] = append(m.snapshots[dataset], name)
	
	// Simulate operation time
	time.Sleep(10 * time.Millisecond)
	return nil
}

// SendSnapshot simulates sending a snapshot
func (m *MockZFSManager) SendSnapshot(dataset, snapshot, target string) error {
	if m.failOperations["SendSnapshot"] {
		return fmt.Errorf("mock: send snapshot failed")
	}

	// Check if snapshot exists
	found := false
	for _, snap := range m.snapshots[dataset] {
		if snap == snapshot {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("mock: snapshot %s not found in dataset %s", snapshot, dataset)
	}

	// Simulate transfer time
	time.Sleep(50 * time.Millisecond)
	return nil
}

// ReceiveSnapshot simulates receiving a snapshot
func (m *MockZFSManager) ReceiveSnapshot(source string) error {
	if m.failOperations["ReceiveSnapshot"] {
		return fmt.Errorf("mock: receive snapshot failed")
	}

	// Simulate receive time
	time.Sleep(30 * time.Millisecond)
	return nil
}

// DestroySnapshot destroys a mock snapshot
func (m *MockZFSManager) DestroySnapshot(dataset, snapshot string) error {
	if m.failOperations["DestroySnapshot"] {
		return fmt.Errorf("mock: destroy snapshot failed")
	}

	snapshots := m.snapshots[dataset]
	for i, snap := range snapshots {
		if snap == snapshot {
			m.snapshots[dataset] = append(snapshots[:i], snapshots[i+1:]...)
			break
		}
	}
	
	time.Sleep(5 * time.Millisecond)
	return nil
}

// VerifyDataIntegrity simulates data integrity verification
func (m *MockZFSManager) VerifyDataIntegrity(dataset string) error {
	if m.failOperations["VerifyDataIntegrity"] {
		return fmt.Errorf("mock: data integrity check failed")
	}

	// Simulate verification time
	time.Sleep(20 * time.Millisecond)
	return nil
}

// GetDatasetSize returns the mock size of a dataset
func (m *MockZFSManager) GetDatasetSize(dataset string) (int64, error) {
	if m.failOperations["GetDatasetSize"] {
		return 0, fmt.Errorf("mock: get dataset size failed")
	}

	size, exists := m.datasets[dataset]
	if !exists {
		size = 1024 * 1024 * 1024 // Default 1GB
		m.datasets[dataset] = size
	}

	return size, nil
}

// SetFailOperation configures an operation to fail for testing
func (m *MockZFSManager) SetFailOperation(operation string, shouldFail bool) {
	m.failOperations[operation] = shouldFail
}

// GetSnapshots returns all snapshots for a dataset
func (m *MockZFSManager) GetSnapshots(dataset string) []string {
	return m.snapshots[dataset]
}

// SetDatasetSize sets the size of a dataset
func (m *MockZFSManager) SetDatasetSize(dataset string, size int64) {
	m.datasets[dataset] = size
}