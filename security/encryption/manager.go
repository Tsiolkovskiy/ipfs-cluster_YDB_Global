// Package encryption provides ZFS native encryption integration with IPFS Cluster
package encryption

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var logger = logging.Logger("encryption")

// EncryptionManager manages ZFS encryption integration with IPFS Cluster
type EncryptionManager struct {
	config          *Config
	keyProvider     KeyProvider
	rotationPolicy  *KeyRotationPolicy
	auditLogger     *AuditLogger
	
	// Key management
	activeKeys      map[string]*EncryptionKey
	keysMutex       sync.RWMutex
	
	// Context management
	ctx             context.Context
	cancel          context.CancelFunc
	
	// Background tasks
	rotationTicker  *time.Ticker
	wg              sync.WaitGroup
}

// EncryptionKey represents an encryption key with metadata
type EncryptionKey struct {
	ID              string    `json:"id"`
	Dataset         string    `json:"dataset"`
	KeyData         []byte    `json:"-"` // Never serialize key data
	Algorithm       string    `json:"algorithm"`
	CreatedAt       time.Time `json:"created_at"`
	LastUsed        time.Time `json:"last_used"`
	RotationDue     time.Time `json:"rotation_due"`
	Status          KeyStatus `json:"status"`
	Version         int       `json:"version"`
}

// KeyStatus represents the status of an encryption key
type KeyStatus int

const (
	KeyStatusActive KeyStatus = iota
	KeyStatusRotating
	KeyStatusDeprecated
	KeyStatusRevoked
)

// String returns string representation of KeyStatus
func (ks KeyStatus) String() string {
	switch ks {
	case KeyStatusActive:
		return "active"
	case KeyStatusRotating:
		return "rotating"
	case KeyStatusDeprecated:
		return "deprecated"
	case KeyStatusRevoked:
		return "revoked"
	default:
		return "unknown"
	}
}

// KeyProvider interface for key management backends
type KeyProvider interface {
	// GenerateKey generates a new encryption key
	GenerateKey(dataset string, algorithm string) (*EncryptionKey, error)
	
	// StoreKey securely stores an encryption key
	StoreKey(key *EncryptionKey) error
	
	// RetrieveKey retrieves an encryption key by ID
	RetrieveKey(keyID string) (*EncryptionKey, error)
	
	// ListKeys lists all keys for a dataset
	ListKeys(dataset string) ([]*EncryptionKey, error)
	
	// RevokeKey revokes an encryption key
	RevokeKey(keyID string) error
	
	// BackupKeys creates a backup of all keys
	BackupKeys(backupPath string) error
}

// KeyRotationPolicy defines key rotation parameters
type KeyRotationPolicy struct {
	// RotationInterval defines how often keys should be rotated
	RotationInterval time.Duration `json:"rotation_interval"`
	
	// GracePeriod defines how long old keys remain valid after rotation
	GracePeriod time.Duration `json:"grace_period"`
	
	// MaxKeyAge defines maximum age before forced rotation
	MaxKeyAge time.Duration `json:"max_key_age"`
	
	// AutoRotate enables automatic key rotation
	AutoRotate bool `json:"auto_rotate"`
	
	// NotifyBeforeRotation defines notification period before rotation
	NotifyBeforeRotation time.Duration `json:"notify_before_rotation"`
}

// NewEncryptionManager creates a new encryption manager
func NewEncryptionManager(config *Config, keyProvider KeyProvider) (*EncryptionManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	
	if keyProvider == nil {
		return nil, fmt.Errorf("key provider cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	auditLogger, err := NewAuditLogger(config.AuditConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create audit logger: %w", err)
	}

	em := &EncryptionManager{
		config:         config,
		keyProvider:    keyProvider,
		rotationPolicy: config.RotationPolicy,
		auditLogger:    auditLogger,
		activeKeys:     make(map[string]*EncryptionKey),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Load existing keys
	if err := em.loadExistingKeys(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load existing keys: %w", err)
	}

	// Start background tasks
	if config.RotationPolicy.AutoRotate {
		em.startKeyRotationScheduler()
	}

	logger.Info("Encryption manager initialized successfully")
	return em, nil
}

// EnableDatasetEncryption enables ZFS native encryption for a dataset
func (em *EncryptionManager) EnableDatasetEncryption(dataset string, algorithm string) error {
	em.auditLogger.LogOperation("enable_encryption", map[string]interface{}{
		"dataset":   dataset,
		"algorithm": algorithm,
		"timestamp": time.Now(),
	})

	// Generate new encryption key
	key, err := em.keyProvider.GenerateKey(dataset, algorithm)
	if err != nil {
		em.auditLogger.LogError("key_generation_failed", err, map[string]interface{}{
			"dataset": dataset,
		})
		return fmt.Errorf("failed to generate encryption key: %w", err)
	}

	// Store key securely
	if err := em.keyProvider.StoreKey(key); err != nil {
		em.auditLogger.LogError("key_storage_failed", err, map[string]interface{}{
			"key_id":  key.ID,
			"dataset": dataset,
		})
		return fmt.Errorf("failed to store encryption key: %w", err)
	}

	// Enable ZFS encryption
	if err := em.enableZFSEncryption(dataset, key); err != nil {
		em.auditLogger.LogError("zfs_encryption_failed", err, map[string]interface{}{
			"key_id":  key.ID,
			"dataset": dataset,
		})
		return fmt.Errorf("failed to enable ZFS encryption: %w", err)
	}

	// Add to active keys
	em.keysMutex.Lock()
	em.activeKeys[key.ID] = key
	em.keysMutex.Unlock()

	logger.Infof("Successfully enabled encryption for dataset %s with key %s", dataset, key.ID)
	return nil
}

// RotateDatasetKey rotates the encryption key for a dataset without downtime
func (em *EncryptionManager) RotateDatasetKey(dataset string) error {
	em.auditLogger.LogOperation("rotate_key", map[string]interface{}{
		"dataset":   dataset,
		"timestamp": time.Now(),
	})

	// Get current key
	currentKey, err := em.getCurrentKeyForDataset(dataset)
	if err != nil {
		return fmt.Errorf("failed to get current key for dataset %s: %w", dataset, err)
	}

	// Mark current key as rotating
	em.keysMutex.Lock()
	currentKey.Status = KeyStatusRotating
	em.keysMutex.Unlock()

	// Generate new key
	newKey, err := em.keyProvider.GenerateKey(dataset, currentKey.Algorithm)
	if err != nil {
		// Restore current key status
		em.keysMutex.Lock()
		currentKey.Status = KeyStatusActive
		em.keysMutex.Unlock()
		
		em.auditLogger.LogError("key_rotation_failed", err, map[string]interface{}{
			"dataset":     dataset,
			"current_key": currentKey.ID,
		})
		return fmt.Errorf("failed to generate new key: %w", err)
	}

	newKey.Version = currentKey.Version + 1

	// Store new key
	if err := em.keyProvider.StoreKey(newKey); err != nil {
		em.auditLogger.LogError("new_key_storage_failed", err, map[string]interface{}{
			"dataset": dataset,
			"new_key": newKey.ID,
		})
		return fmt.Errorf("failed to store new key: %w", err)
	}

	// Perform ZFS key rotation
	if err := em.rotateZFSKey(dataset, currentKey, newKey); err != nil {
		em.auditLogger.LogError("zfs_key_rotation_failed", err, map[string]interface{}{
			"dataset":     dataset,
			"current_key": currentKey.ID,
			"new_key":     newKey.ID,
		})
		return fmt.Errorf("failed to rotate ZFS key: %w", err)
	}

	// Update key statuses
	em.keysMutex.Lock()
	currentKey.Status = KeyStatusDeprecated
	newKey.Status = KeyStatusActive
	em.activeKeys[newKey.ID] = newKey
	em.keysMutex.Unlock()

	// Schedule old key cleanup after grace period
	go em.scheduleKeyCleanup(currentKey, em.rotationPolicy.GracePeriod)

	logger.Infof("Successfully rotated key for dataset %s from %s to %s", 
		dataset, currentKey.ID, newKey.ID)
	return nil
}

// GetDatasetEncryptionStatus returns encryption status for a dataset
func (em *EncryptionManager) GetDatasetEncryptionStatus(dataset string) (*EncryptionStatus, error) {
	// Check ZFS encryption status
	cmd := exec.Command("zfs", "get", "-H", "-p", "encryption", dataset)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get encryption status: %w", err)
	}

	fields := strings.Fields(string(output))
	if len(fields) < 3 {
		return nil, fmt.Errorf("unexpected zfs output format")
	}

	encryptionEnabled := fields[2] != "off"
	
	status := &EncryptionStatus{
		Dataset:   dataset,
		Enabled:   encryptionEnabled,
		Timestamp: time.Now(),
	}

	if encryptionEnabled {
		// Get current key info
		if key, err := em.getCurrentKeyForDataset(dataset); err == nil {
			status.KeyID = key.ID
			status.Algorithm = key.Algorithm
			status.KeyVersion = key.Version
			status.LastRotation = key.CreatedAt
			status.NextRotation = key.RotationDue
		}
	}

	return status, nil
}

// ListDatasetKeys returns all keys for a dataset
func (em *EncryptionManager) ListDatasetKeys(dataset string) ([]*EncryptionKey, error) {
	return em.keyProvider.ListKeys(dataset)
}

// BackupKeys creates a backup of all encryption keys
func (em *EncryptionManager) BackupKeys(backupPath string) error {
	em.auditLogger.LogOperation("backup_keys", map[string]interface{}{
		"backup_path": backupPath,
		"timestamp":   time.Now(),
	})

	if err := em.keyProvider.BackupKeys(backupPath); err != nil {
		em.auditLogger.LogError("backup_failed", err, map[string]interface{}{
			"backup_path": backupPath,
		})
		return fmt.Errorf("failed to backup keys: %w", err)
	}

	logger.Infof("Successfully backed up encryption keys to %s", backupPath)
	return nil
}

// Shutdown gracefully shuts down the encryption manager
func (em *EncryptionManager) Shutdown(ctx context.Context) error {
	logger.Info("Shutting down encryption manager")
	
	// Cancel context to stop background tasks
	em.cancel()
	
	// Stop rotation ticker
	if em.rotationTicker != nil {
		em.rotationTicker.Stop()
	}
	
	// Wait for background tasks to complete
	done := make(chan struct{})
	go func() {
		em.wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		logger.Info("All background tasks completed")
	case <-ctx.Done():
		logger.Warn("Shutdown timeout reached, forcing exit")
	}
	
	// Close audit logger
	if em.auditLogger != nil {
		em.auditLogger.Close()
	}
	
	return nil
}

// enableZFSEncryption enables ZFS native encryption for a dataset
func (em *EncryptionManager) enableZFSEncryption(dataset string, key *EncryptionKey) error {
	// Create key file temporarily
	keyFile := filepath.Join(os.TempDir(), fmt.Sprintf("zfs-key-%s", key.ID))
	defer os.Remove(keyFile)
	
	if err := os.WriteFile(keyFile, key.KeyData, 0600); err != nil {
		return fmt.Errorf("failed to write key file: %w", err)
	}
	
	// Enable encryption
	cmd := exec.Command("zfs", "create", 
		"-o", "encryption="+key.Algorithm,
		"-o", "keyformat=raw",
		"-o", "keylocation=file://"+keyFile,
		dataset)
	
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to enable ZFS encryption: %w, output: %s", err, output)
	}
	
	return nil
}

// rotateZFSKey performs ZFS key rotation
func (em *EncryptionManager) rotateZFSKey(dataset string, oldKey, newKey *EncryptionKey) error {
	// Create new key file
	newKeyFile := filepath.Join(os.TempDir(), fmt.Sprintf("zfs-key-%s", newKey.ID))
	defer os.Remove(newKeyFile)
	
	if err := os.WriteFile(newKeyFile, newKey.KeyData, 0600); err != nil {
		return fmt.Errorf("failed to write new key file: %w", err)
	}
	
	// Change key
	cmd := exec.Command("zfs", "change-key", 
		"-o", "keylocation=file://"+newKeyFile,
		dataset)
	
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to rotate ZFS key: %w, output: %s", err, output)
	}
	
	return nil
}

// getCurrentKeyForDataset returns the current active key for a dataset
func (em *EncryptionManager) getCurrentKeyForDataset(dataset string) (*EncryptionKey, error) {
	keys, err := em.keyProvider.ListKeys(dataset)
	if err != nil {
		return nil, err
	}
	
	for _, key := range keys {
		if key.Status == KeyStatusActive {
			return key, nil
		}
	}
	
	return nil, fmt.Errorf("no active key found for dataset %s", dataset)
}

// loadExistingKeys loads existing keys from the key provider
func (em *EncryptionManager) loadExistingKeys() error {
	// This would typically load keys from all datasets
	// For now, we'll implement a basic version
	logger.Info("Loading existing encryption keys")
	return nil
}

// startKeyRotationScheduler starts the automatic key rotation scheduler
func (em *EncryptionManager) startKeyRotationScheduler() {
	em.rotationTicker = time.NewTicker(em.rotationPolicy.RotationInterval)
	
	em.wg.Add(1)
	go func() {
		defer em.wg.Done()
		defer em.rotationTicker.Stop()
		
		for {
			select {
			case <-em.rotationTicker.C:
				em.checkAndRotateKeys()
			case <-em.ctx.Done():
				return
			}
		}
	}()
	
	logger.Info("Key rotation scheduler started")
}

// checkAndRotateKeys checks for keys that need rotation and rotates them
func (em *EncryptionManager) checkAndRotateKeys() {
	em.keysMutex.RLock()
	keysToRotate := make([]*EncryptionKey, 0)
	
	for _, key := range em.activeKeys {
		if time.Now().After(key.RotationDue) {
			keysToRotate = append(keysToRotate, key)
		}
	}
	em.keysMutex.RUnlock()
	
	for _, key := range keysToRotate {
		logger.Infof("Auto-rotating key %s for dataset %s", key.ID, key.Dataset)
		if err := em.RotateDatasetKey(key.Dataset); err != nil {
			logger.Errorf("Failed to auto-rotate key %s: %s", key.ID, err)
		}
	}
}

// scheduleKeyCleanup schedules cleanup of deprecated keys after grace period
func (em *EncryptionManager) scheduleKeyCleanup(key *EncryptionKey, gracePeriod time.Duration) {
	time.Sleep(gracePeriod)
	
	em.keysMutex.Lock()
	delete(em.activeKeys, key.ID)
	em.keysMutex.Unlock()
	
	// Mark key as revoked
	key.Status = KeyStatusRevoked
	if err := em.keyProvider.StoreKey(key); err != nil {
		logger.Errorf("Failed to update key status to revoked: %s", err)
	}
	
	logger.Infof("Cleaned up deprecated key %s after grace period", key.ID)
}

// EncryptionStatus represents the encryption status of a dataset
type EncryptionStatus struct {
	Dataset      string    `json:"dataset"`
	Enabled      bool      `json:"enabled"`
	KeyID        string    `json:"key_id,omitempty"`
	Algorithm    string    `json:"algorithm,omitempty"`
	KeyVersion   int       `json:"key_version,omitempty"`
	LastRotation time.Time `json:"last_rotation,omitempty"`
	NextRotation time.Time `json:"next_rotation,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
}