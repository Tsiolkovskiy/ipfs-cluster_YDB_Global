package sharding

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TierManager handles automatic data movement between storage tiers
type TierManager struct {
	config      *LoadBalancerConfig
	tiers       map[string]*StorageTier
	accessLog   *AccessLog
	moveQueue   []*TierMoveOperation
	mutex       sync.RWMutex
}

// NewTierManager creates a new tier manager
func NewTierManager(config *LoadBalancerConfig) *TierManager {
	tm := &TierManager{
		config:    config,
		tiers:     make(map[string]*StorageTier),
		accessLog: NewAccessLog(),
		moveQueue: make([]*TierMoveOperation, 0),
	}

	// Initialize storage tiers
	if config.HotTierConfig != nil {
		tm.tiers["hot"] = &StorageTier{
			Config:           config.HotTierConfig,
			Shards:           make([]*Shard, 0),
			PerformanceScore: config.HotTierConfig.PerformanceScore,
			CostScore:        config.HotTierConfig.CostPerGB,
		}
	}

	if config.WarmTierConfig != nil {
		tm.tiers["warm"] = &StorageTier{
			Config:           config.WarmTierConfig,
			Shards:           make([]*Shard, 0),
			PerformanceScore: config.WarmTierConfig.PerformanceScore,
			CostScore:        config.WarmTierConfig.CostPerGB,
		}
	}

	if config.ColdTierConfig != nil {
		tm.tiers["cold"] = &StorageTier{
			Config:           config.ColdTierConfig,
			Shards:           make([]*Shard, 0),
			PerformanceScore: config.ColdTierConfig.PerformanceScore,
			CostScore:        config.ColdTierConfig.CostPerGB,
		}
	}

	return tm
}

// RecordAccess records data access for tiering decisions
func (tm *TierManager) RecordAccess(cid string, shardID string) {
	tm.accessLog.RecordAccess(cid, shardID)
}

// PerformTiering analyzes access patterns and moves data between tiers
func (tm *TierManager) PerformTiering(ctx context.Context) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Analyze access patterns
	moves := tm.analyzeAndPlanMoves()
	
	// Execute pending moves
	for _, move := range moves {
		if err := tm.executeTierMove(ctx, move); err != nil {
			lbLogger.Errorf("Tier move failed: %v", err)
			continue
		}
	}

	return nil
}

// MoveToTier moves data to a specific tier
func (tm *TierManager) MoveToTier(ctx context.Context, cid string, targetTier string) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Validate target tier exists
	if _, exists := tm.tiers[targetTier]; !exists {
		return fmt.Errorf("target tier %s does not exist", targetTier)
	}

	// Get current tier for CID
	entry := tm.accessLog.GetEntry(cid)
	if entry == nil {
		return fmt.Errorf("no access record found for CID %s", cid)
	}

	if entry.CurrentTier == targetTier {
		return nil // Already in target tier
	}

	// Create move operation
	move := &TierMoveOperation{
		CID:           cid,
		SourceTier:    entry.CurrentTier,
		TargetTier:    targetTier,
		Reason:        "manual_move",
		ScheduledTime: time.Now(),
		Priority:      1,
	}

	return tm.executeTierMove(ctx, move)
}

// GetStatus returns current tiering status
func (tm *TierManager) GetStatus() *TieringStatus {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	status := &TieringStatus{
		Timestamp:     time.Now(),
		TierBreakdown: make(map[string]*TierBreakdown),
		PendingMoves:  make([]*TierMoveOperation, len(tm.moveQueue)),
	}

	// Calculate tier breakdowns
	for tierName, tier := range tm.tiers {
		breakdown := &TierBreakdown{
			TierName:    tierName,
			DataSize:    tier.UsedCapacity,
			Utilization: float64(tier.UsedCapacity) / float64(tier.CurrentCapacity) * 100,
			CostPerMonth: float64(tier.UsedCapacity) * tier.CostScore * 30, // Rough monthly cost
		}

		// Count pins in this tier
		for _, shard := range tier.Shards {
			shard.mutex.RLock()
			breakdown.PinCount += shard.PinCount
			shard.mutex.RUnlock()
		}

		status.TierBreakdown[tierName] = breakdown
		status.TotalData += tier.UsedCapacity
	}

	// Copy pending moves
	copy(status.PendingMoves, tm.moveQueue)

	return status
}

// Helper methods

func (tm *TierManager) analyzeAndPlanMoves() []*TierMoveOperation {
	var moves []*TierMoveOperation
	now := time.Now()

	entries := tm.accessLog.GetAllEntries()
	for _, entry := range entries {
		targetTier := tm.determineOptimalTier(entry, now)
		if targetTier != entry.CurrentTier {
			move := &TierMoveOperation{
				CID:           entry.CID,
				SourceTier:    entry.CurrentTier,
				TargetTier:    targetTier,
				Reason:        tm.getMoveReason(entry, targetTier),
				ScheduledTime: now,
				Priority:      tm.getMovePriority(entry, targetTier),
			}
			moves = append(moves, move)
		}
	}

	return moves
}

func (tm *TierManager) determineOptimalTier(entry *AccessEntry, now time.Time) string {
	timeSinceLastAccess := now.Sub(entry.LastAccess)

	// Hot data: accessed recently and frequently
	if timeSinceLastAccess < tm.config.HotDataThreshold && entry.AccessCount > 10 {
		return "hot"
	}

	// Cold data: not accessed for a long time
	if timeSinceLastAccess > tm.config.ColdDataThreshold {
		return "cold"
	}

	// Warm data: everything else
	return "warm"
}

func (tm *TierManager) getMoveReason(entry *AccessEntry, targetTier string) string {
	now := time.Now()
	timeSinceLastAccess := now.Sub(entry.LastAccess)

	switch targetTier {
	case "hot":
		return fmt.Sprintf("frequent_access_%d_times", entry.AccessCount)
	case "cold":
		return fmt.Sprintf("inactive_%s", timeSinceLastAccess.String())
	case "warm":
		return "moderate_access_pattern"
	default:
		return "optimization"
	}
}

func (tm *TierManager) getMovePriority(entry *AccessEntry, targetTier string) int {
	// Higher priority for moves that save more cost or improve performance
	switch targetTier {
	case "hot":
		if entry.AccessCount > 100 {
			return 1 // High priority for very active data
		}
		return 2
	case "cold":
		return 3 // Lower priority for cold moves
	case "warm":
		return 2 // Medium priority
	default:
		return 3
	}
}

func (tm *TierManager) executeTierMove(ctx context.Context, move *TierMoveOperation) error {
	lbLogger.Infof("Moving CID %s from %s to %s tier (reason: %s)", 
		move.CID, move.SourceTier, move.TargetTier, move.Reason)

	// In a real implementation, this would:
	// 1. Locate the current shard containing the CID
	// 2. Find an appropriate shard in the target tier
	// 3. Use ZFS send/receive to move the data
	// 4. Update metadata and access logs
	// 5. Verify data integrity

	// Update access log
	tm.accessLog.UpdateTier(move.CID, move.TargetTier)

	return nil
}

// AccessLog implementation

// NewAccessLog creates a new access log
func NewAccessLog() *AccessLog {
	return &AccessLog{
		entries: make(map[string]*AccessEntry),
	}
}

// RecordAccess records an access to a CID
func (al *AccessLog) RecordAccess(cid string, shardID string) {
	al.mutex.Lock()
	defer al.mutex.Unlock()

	entry, exists := al.entries[cid]
	if !exists {
		entry = &AccessEntry{
			CID:           cid,
			FirstAccess:   time.Now(),
			AccessPattern: "unknown",
			CurrentTier:   "warm", // Default tier
		}
		al.entries[cid] = entry
	}

	entry.AccessCount++
	entry.LastAccess = time.Now()

	// Update access pattern
	al.updateAccessPattern(entry)
}

// GetEntry returns access entry for a CID
func (al *AccessLog) GetEntry(cid string) *AccessEntry {
	al.mutex.RLock()
	defer al.mutex.RUnlock()

	entry, exists := al.entries[cid]
	if !exists {
		return nil
	}

	// Return a copy
	entryCopy := *entry
	return &entryCopy
}

// GetAllEntries returns all access entries
func (al *AccessLog) GetAllEntries() []*AccessEntry {
	al.mutex.RLock()
	defer al.mutex.RUnlock()

	entries := make([]*AccessEntry, 0, len(al.entries))
	for _, entry := range al.entries {
		entryCopy := *entry
		entries = append(entries, &entryCopy)
	}

	return entries
}

// UpdateTier updates the current tier for a CID
func (al *AccessLog) UpdateTier(cid string, tier string) {
	al.mutex.Lock()
	defer al.mutex.Unlock()

	if entry, exists := al.entries[cid]; exists {
		entry.CurrentTier = tier
	}
}

func (al *AccessLog) updateAccessPattern(entry *AccessEntry) {
	now := time.Now()
	timeSinceFirst := now.Sub(entry.FirstAccess)
	
	if timeSinceFirst == 0 {
		return
	}

	accessRate := float64(entry.AccessCount) / timeSinceFirst.Hours()

	if accessRate > 1.0 { // More than 1 access per hour
		entry.AccessPattern = "hot"
	} else if accessRate > 0.1 { // More than 1 access per 10 hours
		entry.AccessPattern = "warm"
	} else {
		entry.AccessPattern = "cold"
	}
}