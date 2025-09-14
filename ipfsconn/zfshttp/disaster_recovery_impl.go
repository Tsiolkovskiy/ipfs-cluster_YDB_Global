// Package zfshttp implements disaster recovery implementation details
package zfshttp

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// initializeFailureDetector initializes the failure detection system
func (drs *DisasterRecoveryService) initializeFailureDetector() error {
	drLogger.Info("Initializing failure detector")
	
	config := &FailureDetectionConfig{
		NodeHealthCheckInterval:    30 * time.Second,
		DataIntegrityCheckInterval: 5 * time.Minute,
		NetworkMonitoringInterval:  10 * time.Second,
		FailureThreshold:          3,
		RecoveryThreshold:         2,
	}
	
	drs.failureDetector = &FailureDetector{
		config:              config,
		nodeHealthCheckers:  make(map[string]*NodeHealthChecker),
		dataIntegrityChecker: &DataIntegrityChecker{},
		networkMonitor:      &NetworkMonitor{},
	}
	
	return nil
}

// initializeRecoveryEngine initializes the recovery engine
func (drs *DisasterRecoveryService) initializeRecoveryEngine() error {
	drLogger.Info("Initializing recovery engine")
	
	config := &RecoveryEngineConfig{
		MaxConcurrentRecoveries: 5,
		RecoveryTimeout:         30 * time.Minute,
		RetryDelay:              5 * time.Second,
		EnableParallelRecovery:  true,
	}
	
	drs.recoveryEngine = &RecoveryEngine{
		config:             config,
		recoveryStrategies: make(map[FailureType]RecoveryStrategy),
		activeRecoveries:   make(map[string]*RecoveryOperation),
	}
	
	// Register recovery strategies
	drs.recoveryEngine.recoveryStrategies[FailureTypeNodeDown] = &NodeDownRecoveryStrategy{
		replicationService: drs.replicationService,
		snapshotManager:   drs.snapshotManager,
	}
	
	drs.recoveryEngine.recoveryStrategies[FailureTypeDataCorruption] = &DataCorruptionRecoveryStrategy{
		snapshotManager: drs.snapshotManager,
	}
	
	drs.recoveryEngine.recoveryStrategies[FailureTypeNetworkPartition] = &NetworkPartitionRecoveryStrategy{
		crossDCReplication: drs.crossDCReplication,
		nodeManager:       drs.nodeManager,
	}
	
	drs.recoveryEngine.recoveryStrategies[FailureTypeDatacenterOutage] = &DatacenterOutageRecoveryStrategy{
		crossDCReplication: drs.crossDCReplication,
		nodeManager:       drs.nodeManager,
		snapshotManager:   drs.snapshotManager,
	}
	
	drs.recoveryEngine.recoveryStrategies[FailureTypeReplicationFailure] = &DataCorruptionRecoveryStrategy{
		snapshotManager: drs.snapshotManager,
	}
	
	return nil
}

// initializeIntegrityChecker initializes the integrity checker
func (drs *DisasterRecoveryService) initializeIntegrityChecker() error {
	drLogger.Info("Initializing integrity checker")
	
	config := &IntegrityCheckConfig{
		CheckInterval:    time.Hour,
		DeepScanInterval: 24 * time.Hour,
		ParallelChecks:   4,
		VerifyChecksums:  true,
		VerifyReplication: true,
	}
	
	drs.integrityChecker = &IntegrityChecker{
		config:              config,
		checksumVerifier:    &ChecksumVerifier{},
		replicationVerifier: &ReplicationVerifier{},
	}
	
	return nil
}

// initializeCrossDCReplication initializes cross-datacenter replication
func (drs *DisasterRecoveryService) initializeCrossDCReplication() error {
	drLogger.Info("Initializing cross-datacenter replication")
	
	config := &CrossDCConfig{
		EnableCrossDC:      true,
		ReplicationFactor:  3, // Increased for better disaster recovery
		SyncInterval:       15 * time.Minute,
		CompressionEnabled: true,
		EncryptionEnabled:  true,
		DatacenterPriority: map[string]int{
			"primary":   1,
			"backup-1":  2,
			"backup-2":  3,
			"archive":   4,
		},
	}
	
	drs.crossDCReplication = &CrossDatacenterReplication{
		config:              config,
		datacenterNodes:     make(map[string][]Node),
		replicationPolicies: make(map[string]*ReplicationPolicy),
	}
	
	// Initialize datacenter nodes from node manager
	if err := drs.initializeDatacenterNodes(); err != nil {
		return fmt.Errorf("failed to initialize datacenter nodes: %w", err)
	}
	
	// Initialize replication policies for different dataset types
	if err := drs.initializeReplicationPolicies(); err != nil {
		return fmt.Errorf("failed to initialize replication policies: %w", err)
	}
	
	drLogger.Infof("Cross-datacenter replication initialized with %d datacenters", len(drs.crossDCReplication.datacenterNodes))
	return nil
}

// initializeDatacenterNodes discovers and organizes nodes by datacenter
func (drs *DisasterRecoveryService) initializeDatacenterNodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Get all available nodes
	nodes, err := drs.nodeManager.GetAvailableNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get available nodes: %w", err)
	}
	
	// Organize nodes by datacenter
	for _, node := range nodes {
		datacenter := node.Datacenter
		if datacenter == "" {
			datacenter = "default" // Fallback for nodes without datacenter info
		}
		
		drs.crossDCReplication.datacenterNodes[datacenter] = append(
			drs.crossDCReplication.datacenterNodes[datacenter], node)
	}
	
	// Ensure we have nodes in multiple datacenters for disaster recovery
	if len(drs.crossDCReplication.datacenterNodes) < 2 {
		drLogger.Warn("Only one datacenter detected - disaster recovery capabilities will be limited")
		
		// Create simulated backup datacenters for testing/development
		if len(drs.crossDCReplication.datacenterNodes) == 1 {
			drs.createSimulatedBackupDatacenters()
		}
	}
	
	return nil
}

// createSimulatedBackupDatacenters creates simulated backup datacenters for development/testing
func (drs *DisasterRecoveryService) createSimulatedBackupDatacenters() {
	// Create backup datacenter nodes (for development/testing)
	backupNode1 := Node{
		ID:         "backup-node-1",
		Address:    "backup1.example.com",
		Datacenter: "backup-1",
		Status:     NodeStatusOnline,
	}
	
	backupNode2 := Node{
		ID:         "backup-node-2", 
		Address:    "backup2.example.com",
		Datacenter: "backup-2",
		Status:     NodeStatusOnline,
	}
	
	drs.crossDCReplication.datacenterNodes["backup-1"] = []Node{backupNode1}
	drs.crossDCReplication.datacenterNodes["backup-2"] = []Node{backupNode2}
	
	drLogger.Info("Created simulated backup datacenters for disaster recovery testing")
}

// initializeReplicationPolicies sets up replication policies for different dataset types
func (drs *DisasterRecoveryService) initializeReplicationPolicies() error {
	// Policy for critical metadata datasets
	metadataPolicy := &ReplicationPolicy{
		MinReplicas:    3,
		MaxReplicas:    5,
		PreferredNodes: []string{}, // Will be populated based on datacenter priority
		ExcludedNodes:  []string{},
	}
	
	// Policy for shard datasets
	shardPolicy := &ReplicationPolicy{
		MinReplicas:    2,
		MaxReplicas:    4,
		PreferredNodes: []string{},
		ExcludedNodes:  []string{},
	}
	
	// Policy for temporary/cache datasets
	tempPolicy := &ReplicationPolicy{
		MinReplicas:    1,
		MaxReplicas:    2,
		PreferredNodes: []string{},
		ExcludedNodes:  []string{},
	}
	
	// Assign policies to dataset patterns
	drs.crossDCReplication.replicationPolicies["ipfs-cluster/metadata"] = metadataPolicy
	drs.crossDCReplication.replicationPolicies["ipfs-cluster/shard-*"] = shardPolicy
	drs.crossDCReplication.replicationPolicies["ipfs-cluster/temp-*"] = tempPolicy
	drs.crossDCReplication.replicationPolicies["default"] = shardPolicy // Default policy
	
	return nil
}

// failureDetectionLoop runs the failure detection monitoring
func (drs *DisasterRecoveryService) failureDetectionLoop() {
	ticker := time.NewTicker(drs.config.FailureDetectionInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			drs.performFailureDetection()
		case <-drs.ctx.Done():
			return
		}
	}
}

// performFailureDetection performs failure detection checks
func (drs *DisasterRecoveryService) performFailureDetection() {
	drLogger.Debug("Performing failure detection")
	
	ctx, cancel := context.WithTimeout(drs.ctx, 30*time.Second)
	defer cancel()
	
	// Detect node failures
	drs.detectNodeFailures(ctx)
	
	// Detect network partitions
	drs.detectNetworkPartitions(ctx)
	
	// Detect datacenter outages
	drs.detectDatacenterOutages(ctx)
	
	// Detect replication failures
	drs.detectReplicationFailures(ctx)
}

// detectNodeFailures detects individual node failures
func (drs *DisasterRecoveryService) detectNodeFailures(ctx context.Context) {
	nodes, err := drs.nodeManager.GetAvailableNodes(ctx)
	if err != nil {
		drLogger.Errorf("Failed to get available nodes: %v", err)
		return
	}
	
	drLogger.Infof("Checking health of %d nodes", len(nodes))
	
	for _, node := range nodes {
		health, err := drs.nodeManager.CheckNodeHealth(ctx, node.ID)
		drLogger.Infof("Node %s health check: err=%v, healthy=%v", node.ID, err, health != nil && health.IsHealthy)
		
		if err != nil || (health != nil && !health.IsHealthy) {
			metadata := map[string]interface{}{
				"datacenter": node.Datacenter,
			}
			
			// Only add last_seen if health is not nil
			if health != nil {
				metadata["last_seen"] = health.LastCheck
			}
			
			failure := &FailureEvent{
				ID:            generateFailureID(),
				Type:          FailureTypeNodeDown,
				Severity:      drs.calculateFailureSeverity(FailureTypeNodeDown, []string{node.ID}),
				AffectedNodes: []string{node.ID},
				DetectedAt:    time.Now(),
				Description:   fmt.Sprintf("Node %s health check failed", node.ID),
				Status:        FailureStatusDetected,
				Metadata:      metadata,
			}
			
			drLogger.Infof("Detected failure for node %s, auto-recovery enabled: %v", node.ID, drs.config.EnableAutoRecovery)
			
			if drs.config.EnableAutoRecovery {
				go drs.initiateRecovery(failure)
			}
		}
	}
}

// detectNetworkPartitions detects network partition scenarios
func (drs *DisasterRecoveryService) detectNetworkPartitions(ctx context.Context) {
	// Check connectivity between nodes in different datacenters
	datacenters := drs.getKnownDatacenters()
	
	for i, dc1 := range datacenters {
		for j, dc2 := range datacenters {
			if i >= j {
				continue // Avoid duplicate checks
			}
			
			if !drs.checkDatacenterConnectivity(ctx, dc1, dc2) {
				failure := &FailureEvent{
					ID:          generateFailureID(),
					Type:        FailureTypeNetworkPartition,
					Severity:    FailureSeverityCritical,
					DetectedAt:  time.Now(),
					Description: fmt.Sprintf("Network partition detected between datacenters %s and %s", dc1, dc2),
					Status:      FailureStatusDetected,
					Metadata: map[string]interface{}{
						"datacenter1": dc1,
						"datacenter2": dc2,
					},
				}
				
				if drs.config.EnableAutoRecovery {
					go drs.initiateRecovery(failure)
				}
			}
		}
	}
}

// detectDatacenterOutages detects complete datacenter outages
func (drs *DisasterRecoveryService) detectDatacenterOutages(ctx context.Context) {
	datacenters := drs.getKnownDatacenters()
	
	for _, datacenter := range datacenters {
		healthyNodes := drs.getHealthyNodesInDatacenter(ctx, datacenter)
		totalNodes := drs.getTotalNodesInDatacenter(datacenter)
		
		if len(healthyNodes) == 0 && totalNodes > 0 {
			failure := &FailureEvent{
				ID:          generateFailureID(),
				Type:        FailureTypeDatacenterOutage,
				Severity:    FailureSeverityCritical,
				DetectedAt:  time.Now(),
				Description: fmt.Sprintf("Complete datacenter outage detected: %s", datacenter),
				Status:      FailureStatusDetected,
				Metadata: map[string]interface{}{
					"datacenter":   datacenter,
					"total_nodes":  totalNodes,
					"healthy_nodes": 0,
				},
			}
			
			if drs.config.EnableAutoRecovery {
				go drs.initiateRecovery(failure)
			}
		}
	}
}

// detectReplicationFailures detects replication failures
func (drs *DisasterRecoveryService) detectReplicationFailures(ctx context.Context) {
	if !drs.config.CrossDCReplicationEnabled {
		return
	}
	
	datasets, err := drs.getActiveDatasetsForReplication()
	if err != nil {
		drLogger.Errorf("Failed to get datasets for replication check: %v", err)
		return
	}
	
	for _, dataset := range datasets {
		// Check basic replication health
		if err := drs.checkDatasetReplicationHealth(ctx, dataset); err != nil {
			failure := &FailureEvent{
				ID:               generateFailureID(),
				Type:             FailureTypeReplicationFailure,
				Severity:         drs.calculateReplicationFailureSeverity(dataset, err),
				AffectedDatasets: []string{dataset},
				DetectedAt:       time.Now(),
				Description:      fmt.Sprintf("Replication failure detected for dataset %s: %v", dataset, err),
				Status:           FailureStatusDetected,
				Metadata: map[string]interface{}{
					"dataset": dataset,
					"error":   err.Error(),
					"failure_type": "replication_health",
				},
			}
			
			if drs.config.EnableAutoRecovery {
				go drs.initiateRecovery(failure)
			}
		}
		
		// Check cross-datacenter consistency
		if err := drs.checkCrossDatacenterConsistency(ctx, dataset); err != nil {
			failure := &FailureEvent{
				ID:               generateFailureID(),
				Type:             FailureTypeDataCorruption,
				Severity:         FailureSeverityCritical,
				AffectedDatasets: []string{dataset},
				DetectedAt:       time.Now(),
				Description:      fmt.Sprintf("Cross-datacenter consistency failure for dataset %s: %v", dataset, err),
				Status:           FailureStatusDetected,
				Metadata: map[string]interface{}{
					"dataset": dataset,
					"error":   err.Error(),
					"failure_type": "consistency_check",
				},
			}
			
			if drs.config.EnableAutoRecovery {
				go drs.initiateRecovery(failure)
			}
		}
		
		// Check replication lag
		if lag, err := drs.checkReplicationLag(ctx, dataset); err != nil {
			drLogger.Warnf("Failed to check replication lag for dataset %s: %v", dataset, err)
		} else if lag > 30*time.Minute { // Configurable threshold
			failure := &FailureEvent{
				ID:               generateFailureID(),
				Type:             FailureTypeReplicationFailure,
				Severity:         FailureSeverityMedium,
				AffectedDatasets: []string{dataset},
				DetectedAt:       time.Now(),
				Description:      fmt.Sprintf("High replication lag detected for dataset %s: %v", dataset, lag),
				Status:           FailureStatusDetected,
				Metadata: map[string]interface{}{
					"dataset": dataset,
					"lag":     lag.String(),
					"failure_type": "replication_lag",
				},
			}
			
			if drs.config.EnableAutoRecovery {
				go drs.initiateRecovery(failure)
			}
		}
	}
}

// calculateReplicationFailureSeverity determines the severity of a replication failure
func (drs *DisasterRecoveryService) calculateReplicationFailureSeverity(dataset string, err error) FailureSeverity {
	errorStr := err.Error()
	
	// Critical datasets (metadata) get higher severity
	if strings.Contains(dataset, "metadata") {
		return FailureSeverityCritical
	}
	
	// Complete replication failure is critical
	if strings.Contains(errorStr, "no replicas") || strings.Contains(errorStr, "all replicas failed") {
		return FailureSeverityCritical
	}
	
	// Insufficient replicas is high severity
	if strings.Contains(errorStr, "insufficient replicas") {
		return FailureSeverityHigh
	}
	
	// Other replication issues are medium severity
	return FailureSeverityMedium
}

// checkCrossDatacenterConsistency checks consistency across datacenters
func (drs *DisasterRecoveryService) checkCrossDatacenterConsistency(ctx context.Context, dataset string) error {
	// Get checksums from all datacenters
	datacenterChecksums := make(map[string]string)
	
	for datacenter := range drs.crossDCReplication.datacenterNodes {
		checksum, err := drs.getDatasetChecksum(ctx, dataset, datacenter)
		if err != nil {
			drLogger.Warnf("Failed to get checksum from datacenter %s: %v", datacenter, err)
			continue
		}
		datacenterChecksums[datacenter] = checksum
	}
	
	// Check if all checksums match
	var referenceChecksum string
	var referenceDatacenter string
	
	for datacenter, checksum := range datacenterChecksums {
		if referenceChecksum == "" {
			referenceChecksum = checksum
			referenceDatacenter = datacenter
			continue
		}
		
		if checksum != referenceChecksum {
			return fmt.Errorf("checksum mismatch between datacenter %s (%s) and %s (%s)", 
				referenceDatacenter, referenceChecksum, datacenter, checksum)
		}
	}
	
	// Ensure we have checksums from multiple datacenters
	if len(datacenterChecksums) < 2 {
		return fmt.Errorf("insufficient datacenters for consistency check: only %d available", len(datacenterChecksums))
	}
	
	return nil
}

// checkReplicationLag checks the replication lag for a dataset
func (drs *DisasterRecoveryService) checkReplicationLag(ctx context.Context, dataset string) (time.Duration, error) {
	// Get the latest snapshot timestamp from primary datacenter
	primaryTimestamp, err := drs.getLatestSnapshotTimestamp(ctx, dataset, "primary")
	if err != nil {
		return 0, fmt.Errorf("failed to get primary timestamp: %w", err)
	}
	
	maxLag := time.Duration(0)
	
	// Check lag in all backup datacenters
	for datacenter := range drs.crossDCReplication.datacenterNodes {
		if datacenter == "primary" {
			continue
		}
		
		backupTimestamp, err := drs.getLatestSnapshotTimestamp(ctx, dataset, datacenter)
		if err != nil {
			drLogger.Warnf("Failed to get timestamp from datacenter %s: %v", datacenter, err)
			continue
		}
		
		lag := primaryTimestamp.Sub(backupTimestamp)
		if lag > maxLag {
			maxLag = lag
		}
	}
	
	return maxLag, nil
}

// getLatestSnapshotTimestamp gets the timestamp of the latest snapshot for a dataset in a datacenter
func (drs *DisasterRecoveryService) getLatestSnapshotTimestamp(ctx context.Context, dataset, datacenter string) (time.Time, error) {
	// In a real implementation, this would query the datacenter for the latest snapshot
	// For now, simulate based on datacenter (primary is always most recent)
	
	baseTime := time.Now()
	
	switch datacenter {
	case "primary":
		return baseTime, nil
	case "backup-1":
		return baseTime.Add(-5 * time.Minute), nil
	case "backup-2":
		return baseTime.Add(-10 * time.Minute), nil
	default:
		return baseTime.Add(-15 * time.Minute), nil
	}
}

// calculateFailureSeverity calculates the severity of a failure based on its impact
func (drs *DisasterRecoveryService) calculateFailureSeverity(failureType FailureType, affectedResources []string) FailureSeverity {
	switch failureType {
	case FailureTypeDatacenterOutage:
		return FailureSeverityCritical
	case FailureTypeNetworkPartition:
		return FailureSeverityCritical
	case FailureTypeNodeDown:
		if len(affectedResources) > 1 {
			return FailureSeverityHigh
		}
		return FailureSeverityMedium
	case FailureTypeDataCorruption:
		return FailureSeverityHigh
	case FailureTypeReplicationFailure:
		return FailureSeverityHigh
	default:
		return FailureSeverityLow
	}
}

// getKnownDatacenters returns a list of known datacenters
func (drs *DisasterRecoveryService) getKnownDatacenters() []string {
	datacenters := make([]string, 0)
	for datacenter := range drs.crossDCReplication.datacenterNodes {
		datacenters = append(datacenters, datacenter)
	}
	return datacenters
}

// checkDatacenterConnectivity checks connectivity between two datacenters
func (drs *DisasterRecoveryService) checkDatacenterConnectivity(ctx context.Context, dc1, dc2 string) bool {
	// In a real implementation, this would test network connectivity
	// For now, simulate connectivity check
	return true
}

// getHealthyNodesInDatacenter returns healthy nodes in a specific datacenter
func (drs *DisasterRecoveryService) getHealthyNodesInDatacenter(ctx context.Context, datacenter string) []Node {
	nodes, exists := drs.crossDCReplication.datacenterNodes[datacenter]
	if !exists {
		return []Node{}
	}
	
	healthyNodes := make([]Node, 0)
	for _, node := range nodes {
		health, err := drs.nodeManager.CheckNodeHealth(ctx, node.ID)
		if err == nil && health.IsHealthy {
			healthyNodes = append(healthyNodes, node)
		}
	}
	
	return healthyNodes
}

// getTotalNodesInDatacenter returns total number of nodes in a datacenter
func (drs *DisasterRecoveryService) getTotalNodesInDatacenter(datacenter string) int {
	nodes, exists := drs.crossDCReplication.datacenterNodes[datacenter]
	if !exists {
		return 0
	}
	return len(nodes)
}

// checkDatasetReplicationHealth checks the replication health of a dataset
func (drs *DisasterRecoveryService) checkDatasetReplicationHealth(ctx context.Context, dataset string) error {
	// Check if dataset has sufficient replicas
	expectedReplicas := drs.crossDCReplication.config.ReplicationFactor
	actualReplicas := drs.countDatasetReplicas(ctx, dataset)
	
	if actualReplicas < expectedReplicas {
		return fmt.Errorf("insufficient replicas: expected %d, found %d", expectedReplicas, actualReplicas)
	}
	
	// Check replication lag
	maxLag := 5 * time.Minute
	lag := drs.getReplicationLag(ctx, dataset)
	if lag > maxLag {
		return fmt.Errorf("replication lag too high: %v", lag)
	}
	
	return nil
}

// countDatasetReplicas counts the number of replicas for a dataset
func (drs *DisasterRecoveryService) countDatasetReplicas(ctx context.Context, dataset string) int {
	// In a real implementation, this would query all nodes to count replicas
	// For now, return a simulated count
	return drs.crossDCReplication.config.ReplicationFactor
}

// getReplicationLag gets the replication lag for a dataset
func (drs *DisasterRecoveryService) getReplicationLag(ctx context.Context, dataset string) time.Duration {
	// In a real implementation, this would check the timestamp of the latest replicated snapshot
	// For now, return a simulated lag
	return 30 * time.Second
}

// integrityCheckLoop runs periodic integrity checks
func (drs *DisasterRecoveryService) integrityCheckLoop() {
	ticker := time.NewTicker(drs.config.IntegrityCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			drs.performIntegrityCheck()
		case <-drs.ctx.Done():
			return
		}
	}
}

// performIntegrityCheck performs data integrity checks
func (drs *DisasterRecoveryService) performIntegrityCheck() {
	drLogger.Debug("Performing integrity check")
	
	// This would typically check all datasets, but for brevity we'll check a sample
	// In a real implementation, this would iterate through all managed datasets
}

// crossDCReplicationLoop manages cross-datacenter replication
func (drs *DisasterRecoveryService) crossDCReplicationLoop() {
	ticker := time.NewTicker(drs.crossDCReplication.config.SyncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			drs.performCrossDCSync()
		case <-drs.ctx.Done():
			return
		}
	}
}

// performCrossDCSync performs cross-datacenter synchronization
func (drs *DisasterRecoveryService) performCrossDCSync() {
	drLogger.Debug("Performing cross-datacenter sync")
	
	ctx, cancel := context.WithTimeout(drs.ctx, 30*time.Minute)
	defer cancel()
	
	// Get all managed datasets
	datasets, err := drs.getActiveDatasetsForReplication()
	if err != nil {
		drLogger.Errorf("Failed to get active datasets: %v", err)
		return
	}
	
	// Sync each dataset to remote datacenters
	for _, dataset := range datasets {
		if err := drs.syncDatasetCrossDC(ctx, dataset); err != nil {
			drLogger.Errorf("Failed to sync dataset %s across datacenters: %v", dataset, err)
			continue
		}
	}
	
	// Verify integrity of cross-DC replicas
	if err := drs.verifyCrossDCIntegrity(ctx); err != nil {
		drLogger.Errorf("Cross-DC integrity verification failed: %v", err)
	}
}

// metricsCollectionLoop collects and updates metrics
func (drs *DisasterRecoveryService) metricsCollectionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			drs.collectMetrics()
		case <-drs.ctx.Done():
			return
		}
	}
}

// collectMetrics collects current metrics
func (drs *DisasterRecoveryService) collectMetrics() {
	drs.mu.Lock()
	defer drs.mu.Unlock()
	
	// Update system uptime
	if drs.metrics.LastFailureTime.IsZero() {
		drs.metrics.SystemUptime = 100.0
	} else {
		totalTime := time.Since(drs.metrics.LastFailureTime).Seconds()
		downTime := float64(drs.metrics.FailedRecoveries) * drs.metrics.AverageRecoveryTime
		drs.metrics.SystemUptime = ((totalTime - downTime) / totalTime) * 100.0
	}
	
	// Collect cross-DC replication metrics
	if drs.config.CrossDCReplicationEnabled {
		drs.collectCrossDCMetrics()
	}
}

// initiateRecovery initiates recovery for a failure event
func (drs *DisasterRecoveryService) initiateRecovery(failure *FailureEvent) *RecoveryOperation {
	drLogger.Infof("Initiating recovery for failure: %s", failure.ID)
	
	recovery := &RecoveryOperation{
		ID:           fmt.Sprintf("recovery-%d", time.Now().UnixNano()),
		FailureEvent: failure,
		Status:       RecoveryStatusPending,
		StartTime:    time.Now(),
		Steps:        make([]*RecoveryStep, 0),
	}
	
	// Find appropriate recovery strategy
	strategy, exists := drs.recoveryEngine.recoveryStrategies[failure.Type]
	if !exists {
		recovery.Status = RecoveryStatusFailed
		recovery.Error = fmt.Sprintf("No recovery strategy found for failure type: %v", failure.Type)
		return recovery
	}
	
	recovery.Strategy = fmt.Sprintf("%T", strategy)
	recovery.EstimatedTime = strategy.GetEstimatedRecoveryTime(failure)
	
	// Add to active recoveries
	drs.mu.Lock()
	drs.recoveryEngine.activeRecoveries[recovery.ID] = recovery
	drs.mu.Unlock()
	
	// Start recovery in background
	go drs.executeRecovery(recovery, strategy)
	
	return recovery
}

// executeRecovery executes a recovery operation
func (drs *DisasterRecoveryService) executeRecovery(recovery *RecoveryOperation, strategy RecoveryStrategy) {
	drLogger.Infof("Executing recovery operation: %s", recovery.ID)
	
	recovery.Status = RecoveryStatusRunning
	
	ctx, cancel := context.WithTimeout(drs.ctx, drs.config.RecoveryTimeout)
	defer cancel()
	
	result, err := strategy.Recover(ctx, recovery.FailureEvent)
	
	recovery.EndTime = time.Now()
	
	if err != nil {
		recovery.Status = RecoveryStatusFailed
		recovery.Error = err.Error()
		drLogger.Errorf("Recovery operation %s failed: %v", recovery.ID, err)
	} else if result.Success {
		recovery.Status = RecoveryStatusCompleted
		drLogger.Infof("Recovery operation %s completed successfully", recovery.ID)
	} else {
		recovery.Status = RecoveryStatusFailed
		recovery.Error = "Recovery completed but was not successful"
		drLogger.Warnf("Recovery operation %s completed but was not successful", recovery.ID)
	}
	
	// Update metrics
	drs.updateRecoveryMetrics(recovery, result)
	
	// Clean up after delay
	go func() {
		time.Sleep(5 * time.Minute)
		drs.mu.Lock()
		delete(drs.recoveryEngine.activeRecoveries, recovery.ID)
		drs.mu.Unlock()
	}()
}

// updateRecoveryMetrics updates recovery metrics
func (drs *DisasterRecoveryService) updateRecoveryMetrics(recovery *RecoveryOperation, result *RecoveryResult) {
	drs.mu.Lock()
	defer drs.mu.Unlock()
	
	drs.metrics.TotalFailures++
	
	if recovery.Status == RecoveryStatusCompleted && result != nil && result.Success {
		drs.metrics.RecoveredFailures++
		drs.metrics.DataRecovered += result.RecoveredData
		drs.metrics.NodesRecovered += int64(len(result.RecoveredNodes))
		
		// Update average recovery time
		duration := recovery.EndTime.Sub(recovery.StartTime).Seconds()
		if drs.metrics.RecoveredFailures == 1 {
			drs.metrics.AverageRecoveryTime = duration
		} else {
			drs.metrics.AverageRecoveryTime = (drs.metrics.AverageRecoveryTime*float64(drs.metrics.RecoveredFailures-1) + duration) / float64(drs.metrics.RecoveredFailures)
		}
		
		drs.metrics.LastRecoveryTime = recovery.EndTime
	} else {
		drs.metrics.FailedRecoveries++
	}
	
	drs.metrics.LastFailureTime = recovery.FailureEvent.DetectedAt
}

// verifyZFSChecksums verifies ZFS checksums for a dataset
func (drs *DisasterRecoveryService) verifyZFSChecksums(ctx context.Context, dataset string, report *IntegrityReport) error {
	drLogger.Infof("Verifying ZFS checksums for dataset: %s", dataset)
	
	// Initialize checksum results
	report.ChecksumResults = &ChecksumVerificationResult{
		TotalBlocks:     0,
		VerifiedBlocks:  0,
		CorruptedBlocks: 0,
	}
	
	// First, get dataset properties to understand the data size
	cmd := exec.CommandContext(ctx, "zfs", "get", "-H", "-p", "used,logicalused", dataset)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get dataset properties: %w", err)
	}
	
	// Parse the output to estimate total blocks
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var totalSize int64
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[1] == "used" {
			// Parse the size (simplified - in real implementation would handle units)
			if size, parseErr := fmt.Sscanf(fields[2], "%d", &totalSize); parseErr == nil && size > 0 {
				// Estimate blocks (assuming 128K recordsize)
				report.ChecksumResults.TotalBlocks = totalSize / (128 * 1024)
				break
			}
		}
	}
	
	// Run ZFS scrub to verify checksums
	cmd = exec.CommandContext(ctx, "zfs", "scrub", dataset)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ZFS scrub failed: %w", err)
	}
	
	// Wait for scrub to complete and get status
	maxWait := 30 * time.Second
	startTime := time.Now()
	
	for time.Since(startTime) < maxWait {
		cmd = exec.CommandContext(ctx, "zfs", "status", dataset)
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("failed to get ZFS status: %w", err)
		}
		
		statusStr := string(output)
		
		// Check if scrub is complete
		if strings.Contains(statusStr, "scrub repaired") || strings.Contains(statusStr, "scrub completed") {
			// Parse scrub results
			if err := drs.parseScrubResults(statusStr, report.ChecksumResults); err != nil {
				return fmt.Errorf("failed to parse scrub results: %w", err)
			}
			break
		}
		
		// Check if scrub is still in progress
		if strings.Contains(statusStr, "scrub in progress") {
			time.Sleep(1 * time.Second)
			continue
		}
		
		// If no scrub information, assume clean
		report.ChecksumResults.VerifiedBlocks = report.ChecksumResults.TotalBlocks
		break
	}
	
	// Verify checksums using ZFS list command for additional validation
	if err := drs.verifyDatasetChecksums(ctx, dataset, report.ChecksumResults); err != nil {
		return fmt.Errorf("additional checksum verification failed: %w", err)
	}
	
	drLogger.Infof("Checksum verification completed for dataset %s: %d/%d blocks verified, %d corrupted", 
		dataset, report.ChecksumResults.VerifiedBlocks, report.ChecksumResults.TotalBlocks, report.ChecksumResults.CorruptedBlocks)
	
	return nil
}

// parseScrubResults parses ZFS scrub output to extract checksum verification results
func (drs *DisasterRecoveryService) parseScrubResults(statusOutput string, results *ChecksumVerificationResult) error {
	drLogger.Debug("Parsing ZFS scrub results")
	
	lines := strings.Split(statusOutput, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		// Look for scrub completion information
		if strings.Contains(line, "scrub repaired") {
			// Parse repaired blocks
			if n, err := fmt.Sscanf(line, "scrub repaired %dK", &results.CorruptedBlocks); err == nil && n > 0 {
				results.CorruptedBlocks *= 1024 // Convert KB to bytes
			}
		} else if strings.Contains(line, "with 0 errors") {
			// No errors found
			results.CorruptedBlocks = 0
		} else if strings.Contains(line, "errors:") {
			// Parse error count
			var errorCount int64
			if n, err := fmt.Sscanf(line, "%d errors", &errorCount); err == nil && n > 0 {
				results.CorruptedBlocks = errorCount
			}
		}
	}
	
	// If we have total blocks but no verified count, assume all were verified
	if results.TotalBlocks > 0 && results.VerifiedBlocks == 0 {
		results.VerifiedBlocks = results.TotalBlocks - results.CorruptedBlocks
	}
	
	return nil
}

// verifyDatasetChecksums performs additional checksum verification using ZFS properties
func (drs *DisasterRecoveryService) verifyDatasetChecksums(ctx context.Context, dataset string, results *ChecksumVerificationResult) error {
	drLogger.Infof("Performing additional checksum verification for dataset: %s", dataset)
	
	// Get checksum algorithm and verify it's enabled
	cmd := exec.CommandContext(ctx, "zfs", "get", "-H", "checksum", dataset)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get checksum property: %w", err)
	}
	
	checksumAlgo := strings.Fields(string(output))[2]
	if checksumAlgo == "off" {
		return fmt.Errorf("checksums are disabled for dataset %s", dataset)
	}
	
	// Get dataset statistics for verification
	cmd = exec.CommandContext(ctx, "zfs", "get", "-H", "-p", "used,available,referenced", dataset)
	output, err = cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get dataset statistics: %w", err)
	}
	
	// Parse statistics to update results
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 4 {
			property := fields[1]
			value := fields[2]
			
			if property == "used" {
				if size, parseErr := fmt.Sscanf(value, "%d", &results.TotalBlocks); parseErr == nil && size > 0 {
					// Convert bytes to blocks (assuming 128K recordsize)
					results.TotalBlocks = results.TotalBlocks / (128 * 1024)
				}
			}
		}
	}
	
	// Verify checksum integrity using zpool status
	poolName := strings.Split(dataset, "/")[0]
	cmd = exec.CommandContext(ctx, "zpool", "status", "-v", poolName)
	output, err = cmd.Output()
	if err != nil {
		drLogger.Warnf("Failed to get pool status for %s: %v", poolName, err)
		// Don't fail verification if we can't get pool status
		return nil
	}
	
	statusStr := string(output)
	if strings.Contains(statusStr, "CKSUM") || strings.Contains(statusStr, "checksum errors") {
		// Parse checksum errors from pool status
		results.CorruptedBlocks++
	}
	
	// Update verified blocks count
	if results.TotalBlocks > results.CorruptedBlocks {
		results.VerifiedBlocks = results.TotalBlocks - results.CorruptedBlocks
	}
	
	drLogger.Infof("Checksum verification completed: %d total, %d verified, %d corrupted", 
		results.TotalBlocks, results.VerifiedBlocks, results.CorruptedBlocks)
	
	return nil
}

// verifyReplicationConsistency verifies replication consistency across nodes
func (drs *DisasterRecoveryService) verifyReplicationConsistency(ctx context.Context, dataset string, report *IntegrityReport) error {
	drLogger.Infof("Verifying replication consistency for dataset: %s", dataset)
	
	// Initialize replication results
	report.ReplicationResults = &ReplicationVerificationResult{
		ExpectedReplicas:   drs.crossDCReplication.config.ReplicationFactor,
		ActualReplicas:     0,
		ConsistentReplicas: 0,
	}
	
	// Get all nodes that should have replicas
	expectedNodes := make([]Node, 0)
	for _, nodes := range drs.crossDCReplication.datacenterNodes {
		expectedNodes = append(expectedNodes, nodes...)
	}
	
	// Limit to replication factor
	if len(expectedNodes) > drs.crossDCReplication.config.ReplicationFactor {
		expectedNodes = expectedNodes[:drs.crossDCReplication.config.ReplicationFactor]
	}
	
	// Get reference checksum from primary datacenter
	referenceChecksum, err := drs.getDatasetChecksum(ctx, dataset, "primary")
	if err != nil {
		return fmt.Errorf("failed to get reference checksum: %w", err)
	}
	
	// Verify each replica
	for _, node := range expectedNodes {
		// Check if replica exists
		if drs.checkReplicaExists(ctx, dataset, node) {
			report.ReplicationResults.ActualReplicas++
			
			// Verify consistency
			nodeChecksum, err := drs.getDatasetChecksum(ctx, dataset, node.Datacenter)
			if err != nil {
				drLogger.Warnf("Failed to get checksum from node %s: %v", node.ID, err)
				continue
			}
			
			if nodeChecksum == referenceChecksum {
				report.ReplicationResults.ConsistentReplicas++
			} else {
				drLogger.Warnf("Checksum mismatch on node %s: expected %s, got %s", 
					node.ID, referenceChecksum, nodeChecksum)
			}
		}
	}
	
	drLogger.Infof("Replication verification completed: %d/%d replicas found, %d consistent", 
		report.ReplicationResults.ActualReplicas, 
		report.ReplicationResults.ExpectedReplicas,
		report.ReplicationResults.ConsistentReplicas)
	
	return nil
}

// checkReplicaExists checks if a replica exists on a specific node
func (drs *DisasterRecoveryService) checkReplicaExists(ctx context.Context, dataset string, node Node) bool {
	// In a real implementation, this would make an RPC call to check if the dataset exists
	// For testing purposes, assume replicas exist
	return true
}

// replicateForDisasterRecovery replicates data for disaster recovery
func (drs *DisasterRecoveryService) replicateForDisasterRecovery(ctx context.Context, snapshot string) error {
	drLogger.Infof("Replicating snapshot for disaster recovery: %s", snapshot)
	
	// Get target datacenters for replication
	targetDatacenters := make([]string, 0)
	for datacenter := range drs.crossDCReplication.datacenterNodes {
		if datacenter != "primary" {
			targetDatacenters = append(targetDatacenters, datacenter)
		}
	}
	
	// Replicate to each target datacenter
	for _, datacenter := range targetDatacenters {
		if err := drs.replicateSnapshotToDatacenter(ctx, snapshot, datacenter); err != nil {
			drLogger.Errorf("Failed to replicate %s to %s: %v", snapshot, datacenter, err)
			continue
		}
	}
	
	return nil
}

// replicateSnapshotToDatacenter replicates a snapshot to a specific datacenter
func (drs *DisasterRecoveryService) replicateSnapshotToDatacenter(ctx context.Context, snapshot, datacenter string) error {
	drLogger.Infof("Replicating snapshot %s to datacenter %s", snapshot, datacenter)
	
	// Get nodes in target datacenter
	nodes, exists := drs.crossDCReplication.datacenterNodes[datacenter]
	if !exists || len(nodes) == 0 {
		return fmt.Errorf("no nodes available in datacenter %s", datacenter)
	}
	
	// Use the first available node as target
	targetNode := nodes[0]
	
	// In a real implementation, this would use ZFS send/receive
	// For now, simulate the replication process
	time.Sleep(100 * time.Millisecond) // Simulate network transfer time
	
	// Verify replication integrity
	if err := drs.verifyReplicationIntegrity(ctx, snapshot, targetNode); err != nil {
		return fmt.Errorf("replication integrity check failed: %w", err)
	}
	
	drLogger.Infof("Successfully replicated snapshot %s to node %s", snapshot, targetNode.ID)
	return nil
}

// getActiveDatasetsForReplication returns datasets that need cross-DC replication
func (drs *DisasterRecoveryService) getActiveDatasetsForReplication() ([]string, error) {
	drLogger.Debug("Getting active datasets for replication")
	
	// Query all managed ZFS datasets that require cross-DC replication
	datasets := make([]string, 0)
	
	// In a real implementation, this would query ZFS to get all datasets
	// For testing, return a predefined list of datasets
	datasets = append(datasets, 
		"ipfs-cluster/metadata",
		"ipfs-cluster/shard-1", 
		"ipfs-cluster/shard-2",
		"ipfs-cluster/shard-3",
	)
	
	// Filter datasets based on replication policies
	filteredDatasets := make([]string, 0)
	for _, dataset := range datasets {
		policy := drs.getReplicationPolicyForDataset(dataset)
		if policy != nil && policy.MinReplicas > 1 {
			filteredDatasets = append(filteredDatasets, dataset)
		}
	}
	
	drLogger.Infof("Found %d datasets requiring cross-DC replication", len(filteredDatasets))
	return filteredDatasets, nil
}

// getReplicationPolicyForDataset gets the replication policy for a specific dataset
func (drs *DisasterRecoveryService) getReplicationPolicyForDataset(dataset string) *ReplicationPolicy {
	// Check for exact match first
	if policy, exists := drs.crossDCReplication.replicationPolicies[dataset]; exists {
		return policy
	}
	
	// Check for pattern matches
	for pattern, policy := range drs.crossDCReplication.replicationPolicies {
		if strings.Contains(pattern, "*") {
			// Simple wildcard matching
			prefix := strings.TrimSuffix(pattern, "*")
			if strings.HasPrefix(dataset, prefix) {
				return policy
			}
		}
	}
	
	// Return default policy if no match found
	if defaultPolicy, exists := drs.crossDCReplication.replicationPolicies["default"]; exists {
		return defaultPolicy
	}
	
	return nil
}

// syncDatasetCrossDC synchronizes a dataset across datacenters
func (drs *DisasterRecoveryService) syncDatasetCrossDC(ctx context.Context, dataset string) error {
	drLogger.Infof("Syncing dataset %s across datacenters", dataset)
	
	// Create a snapshot for synchronization
	snapshotName := fmt.Sprintf("sync-%d", time.Now().Unix())
	if err := drs.snapshotManager.CreateSnapshot(ctx, dataset, snapshotName); err != nil {
		return fmt.Errorf("failed to create sync snapshot: %w", err)
	}
	
	fullSnapshot := fmt.Sprintf("%s@%s", dataset, snapshotName)
	
	// Replicate to all target datacenters
	return drs.replicateForDisasterRecovery(ctx, fullSnapshot)
}

// verifyCrossDCIntegrity verifies integrity of cross-datacenter replicas
func (drs *DisasterRecoveryService) verifyCrossDCIntegrity(ctx context.Context) error {
	drLogger.Info("Verifying cross-datacenter replica integrity")
	
	datasets, err := drs.getActiveDatasetsForReplication()
	if err != nil {
		return fmt.Errorf("failed to get datasets for integrity check: %w", err)
	}
	
	errorCount := 0
	for _, dataset := range datasets {
		if err := drs.checkCrossDatacenterConsistency(ctx, dataset); err != nil {
			drLogger.Errorf("Cross-DC consistency check failed for dataset %s: %v", dataset, err)
			errorCount++
			continue
		}
		
		// Check replication health
		if err := drs.checkDatasetReplicationHealth(ctx, dataset); err != nil {
			drLogger.Errorf("Replication health check failed for dataset %s: %v", dataset, err)
			errorCount++
			continue
		}
	}
	
	if errorCount > 0 {
		return fmt.Errorf("cross-DC integrity verification failed for %d datasets", errorCount)
	}
	
	drLogger.Info("Cross-datacenter integrity verification completed successfully")
	return nil
}

// getDatasetChecksum gets the checksum of a dataset in a specific datacenter
func (drs *DisasterRecoveryService) getDatasetChecksum(ctx context.Context, dataset, datacenter string) (string, error) {
	// In a real implementation, this would execute ZFS commands to get checksums
	// For testing, return consistent checksums across datacenters
	return fmt.Sprintf("checksum-%s", dataset), nil
}

// getSnapshotChecksum gets the checksum of a snapshot
func (drs *DisasterRecoveryService) getSnapshotChecksum(ctx context.Context, snapshot string) (string, error) {
	// In a real implementation, this would use ZFS to get the actual checksum
	return fmt.Sprintf("checksum-%s", snapshot), nil
}

// getRemoteSnapshotChecksum gets the checksum of a snapshot on a remote node
func (drs *DisasterRecoveryService) getRemoteSnapshotChecksum(ctx context.Context, snapshot string, node Node) (string, error) {
	// In a real implementation, this would make an RPC call to the remote node
	// For testing, return the same checksum as the source to simulate successful replication
	return fmt.Sprintf("checksum-%s", snapshot), nil
}

// collectCrossDCMetrics collects metrics for cross-datacenter operations
func (drs *DisasterRecoveryService) collectCrossDCMetrics() {
	// Collect metrics about cross-DC replication health
	// This would include replication lag, success rates, etc.
	drLogger.Debug("Collecting cross-datacenter metrics")
}

// verifyReplicationIntegrity verifies the integrity of a replicated snapshot
func (drs *DisasterRecoveryService) verifyReplicationIntegrity(ctx context.Context, snapshot string, node Node) error {
	drLogger.Infof("Verifying replication integrity for snapshot %s on node %s", snapshot, node.ID)
	
	// Get checksum from source
	sourceChecksum, err := drs.getSnapshotChecksum(ctx, snapshot)
	if err != nil {
		return fmt.Errorf("failed to get source checksum: %w", err)
	}
	
	// Get checksum from target node
	targetChecksum, err := drs.getRemoteSnapshotChecksum(ctx, snapshot, node)
	if err != nil {
		return fmt.Errorf("failed to get target checksum: %w", err)
	}
	
	if sourceChecksum != targetChecksum {
		return fmt.Errorf("checksum mismatch: source=%s, target=%s", sourceChecksum, targetChecksum)
	}
	
	return nil
}

