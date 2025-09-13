package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// MemoryCheckpointStore implements CheckpointStore using in-memory storage
// This is suitable for development and testing. For production, use a persistent store.
type MemoryCheckpointStore struct {
	mu          sync.RWMutex
	checkpoints map[string]*WorkflowExecution
}

// NewMemoryCheckpointStore creates a new memory-based checkpoint store
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{
		checkpoints: make(map[string]*WorkflowExecution),
	}
}

// SaveCheckpoint saves a workflow execution checkpoint
func (m *MemoryCheckpointStore) SaveCheckpoint(ctx context.Context, execution *WorkflowExecution) error {
	if execution == nil {
		return fmt.Errorf("execution cannot be nil")
	}
	
	// Deep copy the execution to avoid concurrent modification issues
	executionCopy, err := m.deepCopyExecution(execution)
	if err != nil {
		return fmt.Errorf("failed to copy execution: %w", err)
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.checkpoints[execution.ID] = executionCopy
	return nil
}

// LoadCheckpoint loads a workflow execution checkpoint
func (m *MemoryCheckpointStore) LoadCheckpoint(ctx context.Context, executionID string) (*WorkflowExecution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	execution, exists := m.checkpoints[executionID]
	if !exists {
		return nil, fmt.Errorf("checkpoint not found for execution: %s", executionID)
	}
	
	// Return a deep copy to avoid concurrent modification issues
	return m.deepCopyExecution(execution)
}

// DeleteCheckpoint deletes a workflow execution checkpoint
func (m *MemoryCheckpointStore) DeleteCheckpoint(ctx context.Context, executionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	delete(m.checkpoints, executionID)
	return nil
}

// ListCheckpoints lists workflow execution checkpoints based on filter
func (m *MemoryCheckpointStore) ListCheckpoints(ctx context.Context, filter *HistoryFilter) ([]*WorkflowExecution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	var results []*WorkflowExecution
	
	for _, execution := range m.checkpoints {
		if m.matchesFilter(execution, filter) {
			executionCopy, err := m.deepCopyExecution(execution)
			if err != nil {
				continue // Skip this execution if copy fails
			}
			results = append(results, executionCopy)
		}
	}
	
	// Apply limit and offset
	if filter != nil {
		if filter.Offset > 0 && filter.Offset < len(results) {
			results = results[filter.Offset:]
		} else if filter.Offset >= len(results) {
			results = []*WorkflowExecution{}
		}
		
		if filter.Limit > 0 && filter.Limit < len(results) {
			results = results[:filter.Limit]
		}
	}
	
	return results, nil
}

// deepCopyExecution creates a deep copy of a workflow execution
func (m *MemoryCheckpointStore) deepCopyExecution(execution *WorkflowExecution) (*WorkflowExecution, error) {
	// Use JSON marshaling/unmarshaling for deep copy
	// This is not the most efficient method but it's simple and reliable
	data, err := json.Marshal(execution)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal execution: %w", err)
	}
	
	var copy WorkflowExecution
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil, fmt.Errorf("failed to unmarshal execution: %w", err)
	}
	
	return &copy, nil
}

// matchesFilter checks if an execution matches the given filter
func (m *MemoryCheckpointStore) matchesFilter(execution *WorkflowExecution, filter *HistoryFilter) bool {
	if filter == nil {
		return true
	}
	
	// Check status filter
	if filter.Status != "" && execution.Status != filter.Status {
		return false
	}
	
	// Check type filter
	if filter.Type != "" && execution.Definition.Type != filter.Type {
		return false
	}
	
	// Check start time filter
	if filter.StartTime != nil && execution.StartedAt.Before(*filter.StartTime) {
		return false
	}
	
	// Check end time filter
	if filter.EndTime != nil {
		if execution.CompletedAt == nil {
			return false
		}
		if execution.CompletedAt.After(*filter.EndTime) {
			return false
		}
	}
	
	return true
}

// GetCheckpointCount returns the total number of checkpoints stored
func (m *MemoryCheckpointStore) GetCheckpointCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.checkpoints)
}

// Clear removes all checkpoints (useful for testing)
func (m *MemoryCheckpointStore) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints = make(map[string]*WorkflowExecution)
}