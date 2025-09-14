package encryption

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEncryptionManager(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "encryption-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test configuration
	config := &Config{
		DefaultAlgorithm: "aes-256-gcm",
		KeyProvider: KeyProviderConfig{
			Type: "file",
			Config: map[string]interface{}{
				"key_dir": filepath.Join(tempDir, "keys"),
			},
		},
		RotationPolicy: &KeyRotationPolicy{
			RotationInterval:     time.Hour,
			GracePeriod:         time.Minute,
			MaxKeyAge:           24 * time.Hour,
			AutoRotate:          false, // Disable for testing
			NotifyBeforeRotation: time.Minute,
		},
		AuditConfig: &AuditConfig{
			Enabled: true,
			LogFile: filepath.Join(tempDir, "audit.log"),
		},
	}

	// Create key provider
	keyProvider, err := NewFileKeyProvider(filepath.Join(tempDir, "keys"))
	if err != nil {
		t.Fatalf("Failed to create key provider: %v", err)
	}

	// Create encryption manager
	em, err := NewEncryptionManager(config, keyProvider)
	if err != nil {
		t.Fatalf("Failed to create encryption manager: %v", err)
	}
	defer em.Shutdown(context.Background())

	t.Run("EnableDatasetEncryption", func(t *testing.T) {
		// This test would require actual ZFS commands, so we'll skip it in the test environment
		// In a real environment with ZFS, this would work
		t.Skip("Skipping ZFS integration test - requires ZFS to be installed")
	})

	t.Run("RotateDatasetKey", func(t *testing.T) {
		// This test would require actual ZFS commands, so we'll skip it in the test environment
		t.Skip("Skipping ZFS integration test - requires ZFS to be installed")
	})

	t.Run("BackupKeys", func(t *testing.T) {
		backupDir := filepath.Join(tempDir, "backup")
		err := em.BackupKeys(backupDir)
		if err != nil {
			t.Errorf("Failed to backup keys: %v", err)
		}

		// Verify backup directory exists
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			t.Errorf("Backup directory was not created")
		}
	})
}

func TestFileKeyProvider(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "keyprovider-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	keyDir := filepath.Join(tempDir, "keys")
	provider, err := NewFileKeyProvider(keyDir)
	if err != nil {
		t.Fatalf("Failed to create file key provider: %v", err)
	}

	t.Run("GenerateAndStoreKey", func(t *testing.T) {
		dataset := "test/dataset"
		algorithm := "aes-256-gcm"

		// Generate key
		key, err := provider.GenerateKey(dataset, algorithm)
		if err != nil {
			t.Fatalf("Failed to generate key: %v", err)
		}

		if key.Dataset != dataset {
			t.Errorf("Expected dataset %s, got %s", dataset, key.Dataset)
		}

		if key.Algorithm != algorithm {
			t.Errorf("Expected algorithm %s, got %s", algorithm, key.Algorithm)
		}

		if len(key.KeyData) != 32 { // AES-256 = 32 bytes
			t.Errorf("Expected key data length 32, got %d", len(key.KeyData))
		}

		// Store key
		err = provider.StoreKey(key)
		if err != nil {
			t.Fatalf("Failed to store key: %v", err)
		}

		// Retrieve key
		retrievedKey, err := provider.RetrieveKey(key.ID)
		if err != nil {
			t.Fatalf("Failed to retrieve key: %v", err)
		}

		if retrievedKey.ID != key.ID {
			t.Errorf("Expected key ID %s, got %s", key.ID, retrievedKey.ID)
		}

		if string(retrievedKey.KeyData) != string(key.KeyData) {
			t.Errorf("Key data mismatch")
		}
	})

	t.Run("ListKeys", func(t *testing.T) {
		dataset := "test/dataset2"

		// Generate multiple keys
		key1, _ := provider.GenerateKey(dataset, "aes-256-gcm")
		key2, _ := provider.GenerateKey(dataset, "aes-128-gcm")

		provider.StoreKey(key1)
		provider.StoreKey(key2)

		// List keys
		keys, err := provider.ListKeys(dataset)
		if err != nil {
			t.Fatalf("Failed to list keys: %v", err)
		}

		if len(keys) != 2 {
			t.Errorf("Expected 2 keys, got %d", len(keys))
		}
	})

	t.Run("RevokeKey", func(t *testing.T) {
		dataset := "test/dataset3"

		// Generate and store key
		key, _ := provider.GenerateKey(dataset, "aes-256-gcm")
		provider.StoreKey(key)

		// Revoke key
		err := provider.RevokeKey(key.ID)
		if err != nil {
			t.Fatalf("Failed to revoke key: %v", err)
		}

		// Verify key is revoked
		keys, err := provider.ListKeys(dataset)
		if err != nil {
			t.Fatalf("Failed to list keys: %v", err)
		}

		for _, k := range keys {
			if k.ID == key.ID && k.Status != KeyStatusRevoked {
				t.Errorf("Key should be revoked but has status %s", k.Status.String())
			}
		}
	})
}

func TestAuditLogger(t *testing.T) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "audit-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	config := &AuditConfig{
		Enabled: true,
		LogFile: filepath.Join(tempDir, "audit.log"),
		MaxSize: 1, // 1MB for testing
	}

	logger, err := NewAuditLogger(config)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()

	t.Run("LogOperation", func(t *testing.T) {
		logger.LogOperation("test_operation", map[string]interface{}{
			"test_param": "test_value",
		})

		// Verify log file exists
		if _, err := os.Stat(config.LogFile); os.IsNotExist(err) {
			t.Errorf("Audit log file was not created")
		}
	})

	t.Run("LogError", func(t *testing.T) {
		testErr := fmt.Errorf("test error")
		logger.LogError("test_error_operation", testErr, map[string]interface{}{
			"error_context": "test context",
		})

		// Log file should still exist and be writable
		if _, err := os.Stat(config.LogFile); os.IsNotExist(err) {
			t.Errorf("Audit log file should exist after error logging")
		}
	})

	t.Run("LogSecurityViolation", func(t *testing.T) {
		logger.LogSecurityViolation("unauthorized_access", "test_user", "192.168.1.1", map[string]interface{}{
			"attempted_resource": "test_dataset",
		})

		// Verify log file still exists
		if _, err := os.Stat(config.LogFile); os.IsNotExist(err) {
			t.Errorf("Audit log file should exist after security violation logging")
		}
	})
}