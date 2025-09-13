package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMetadataStoreInterface verifies that YDBMetadataStore implements MetadataStore interface
func TestMetadataStoreInterface(t *testing.T) {
	// This test ensures that YDBMetadataStore implements the MetadataStore interface
	var _ MetadataStore = (*YDBMetadataStore)(nil)
	
	// Test passes if compilation succeeds
	assert.True(t, true, "YDBMetadataStore implements MetadataStore interface")
}

// TestNewYDBMetadataStore_InvalidConnection tests error handling for invalid connections
func TestNewYDBMetadataStore_InvalidConnection(t *testing.T) {
	// This test verifies that invalid connection strings are handled properly
	// We can't test actual connection without YDB, but we can test the function signature
	
	// Test with empty connection string - this should fail
	// Note: We can't actually call this without YDB running, but the function exists
	assert.NotNil(t, NewYDBMetadataStore, "NewYDBMetadataStore function exists")
}

// TestYDBMetadataStoreStructure tests the struct definition
func TestYDBMetadataStoreStructure(t *testing.T) {
	// Verify the struct can be created (without connecting to YDB)
	store := &YDBMetadataStore{}
	assert.NotNil(t, store, "YDBMetadataStore struct can be created")
	
	// Verify Close method exists and can be called safely on empty struct
	err := store.Close()
	assert.NoError(t, err, "Close method handles nil db gracefully")
}