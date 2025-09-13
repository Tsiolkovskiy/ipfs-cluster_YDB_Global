package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/global-data-controller/gdc/internal/models"
)

// TestYDBMetadataStore_Integration runs integration tests against a real YDB instance
// Set YDB_CONNECTION_STRING environment variable to run these tests
func TestYDBMetadataStore_Integration(t *testing.T) {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("YDB_CONNECTION_STRING not set, skipping integration tests")
	}

	ctx := context.Background()
	store, err := NewYDBMetadataStore(ctx, connectionString)
	require.NoError(t, err)
	defer store.Close()

	// Initialize schema (in real deployment this would be done separately)
	err = store.InitializeSchema(ctx)
	require.NoError(t, err)

	t.Run("Zone Operations", func(t *testing.T) {
		testZoneOperations(t, ctx, store)
	})

	t.Run("Cluster Operations", func(t *testing.T) {
		testClusterOperations(t, ctx, store)
	})

	t.Run("Policy Operations", func(t *testing.T) {
		testPolicyOperations(t, ctx, store)
	})

	t.Run("Shard Operations", func(t *testing.T) {
		testShardOperations(t, ctx, store)
	})

	t.Run("Topology Operations", func(t *testing.T) {
		testTopologyOperations(t, ctx, store)
	})
}

func testZoneOperations(t *testing.T, ctx context.Context, store *YDBMetadataStore) {
	// Create test zone
	zone := &models.Zone{
		ID:     "zone-test-1",
		Name:   "Test Zone 1",
		Region: "us-east-1",
		Status: models.ZoneStatusActive,
		Capabilities: map[string]string{
			"storage_type": "ssd",
			"bandwidth":    "10gbps",
		},
		Coordinates: &models.GeoCoordinates{
			Latitude:  40.7128,
			Longitude: -74.0060,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Test RegisterZone
	err := store.RegisterZone(ctx, zone)
	require.NoError(t, err)

	// Test GetZone
	retrievedZone, err := store.GetZone(ctx, zone.ID)
	require.NoError(t, err)
	assert.Equal(t, zone.ID, retrievedZone.ID)
	assert.Equal(t, zone.Name, retrievedZone.Name)
	assert.Equal(t, zone.Region, retrievedZone.Region)
	assert.Equal(t, zone.Status, retrievedZone.Status)
	assert.Equal(t, zone.Capabilities, retrievedZone.Capabilities)
	assert.NotNil(t, retrievedZone.Coordinates)
	assert.InDelta(t, zone.Coordinates.Latitude, retrievedZone.Coordinates.Latitude, 0.0001)
	assert.InDelta(t, zone.Coordinates.Longitude, retrievedZone.Coordinates.Longitude, 0.0001)

	// Test UpdateZoneStatus
	err = store.UpdateZoneStatus(ctx, zone.ID, models.ZoneStatusMaintenance)
	require.NoError(t, err)

	updatedZone, err := store.GetZone(ctx, zone.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ZoneStatusMaintenance, updatedZone.Status)

	// Test zone without coordinates
	zoneNoCoords := &models.Zone{
		ID:        "zone-test-2",
		Name:      "Test Zone 2",
		Region:    "eu-west-1",
		Status:    models.ZoneStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err = store.RegisterZone(ctx, zoneNoCoords)
	require.NoError(t, err)

	retrievedZoneNoCoords, err := store.GetZone(ctx, zoneNoCoords.ID)
	require.NoError(t, err)
	assert.Nil(t, retrievedZoneNoCoords.Coordinates)
}

func testClusterOperations(t *testing.T, ctx context.Context, store *YDBMetadataStore) {
	// First create a zone for the cluster
	zone := &models.Zone{
		ID:        "zone-cluster-test",
		Name:      "Cluster Test Zone",
		Region:    "us-west-2",
		Status:    models.ZoneStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	err := store.RegisterZone(ctx, zone)
	require.NoError(t, err)

	// Create test cluster with nodes
	cluster := &models.Cluster{
		ID:       "cluster-test-1",
		ZoneID:   zone.ID,
		Name:     "Test Cluster 1",
		Endpoint: "https://cluster1.example.com:9094",
		Status:   models.ClusterStatusHealthy,
		Version:  "1.0.0",
		Capabilities: map[string]string{
			"pin_method": "recursive",
			"max_size":   "1TB",
		},
		Nodes: []*models.Node{
			{
				ID:        "node-1",
				ClusterID: "cluster-test-1",
				Address:   "/ip4/192.168.1.10/tcp/4001",
				Status:    models.NodeStatusOnline,
				Resources: &models.NodeResources{
					CPUCores:     8,
					MemoryBytes:  32 * 1024 * 1024 * 1024, // 32GB
					StorageBytes: 2 * 1024 * 1024 * 1024 * 1024, // 2TB
					NetworkMbps:  1000,
				},
				Metrics: &models.NodeMetrics{
					CPUUsage:       0.45,
					MemoryUsage:    0.60,
					StorageUsage:   0.30,
					NetworkInMbps:  150.5,
					NetworkOutMbps: 200.3,
					PinnedObjects:  1500,
					LastUpdated:    time.Now(),
				},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			{
				ID:        "node-2",
				ClusterID: "cluster-test-1",
				Address:   "/ip4/192.168.1.11/tcp/4001",
				Status:    models.NodeStatusOnline,
				Resources: &models.NodeResources{
					CPUCores:     16,
					MemoryBytes:  64 * 1024 * 1024 * 1024, // 64GB
					StorageBytes: 4 * 1024 * 1024 * 1024 * 1024, // 4TB
					NetworkMbps:  1000,
				},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Test RegisterCluster
	err = store.RegisterCluster(ctx, cluster)
	require.NoError(t, err)

	// Test GetCluster
	retrievedCluster, err := store.GetCluster(ctx, cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, cluster.ID, retrievedCluster.ID)
	assert.Equal(t, cluster.ZoneID, retrievedCluster.ZoneID)
	assert.Equal(t, cluster.Name, retrievedCluster.Name)
	assert.Equal(t, cluster.Endpoint, retrievedCluster.Endpoint)
	assert.Equal(t, cluster.Status, retrievedCluster.Status)
	assert.Equal(t, cluster.Version, retrievedCluster.Version)
	assert.Equal(t, cluster.Capabilities, retrievedCluster.Capabilities)
	assert.Len(t, retrievedCluster.Nodes, 2)

	// Check first node with metrics
	node1 := retrievedCluster.Nodes[0]
	assert.Equal(t, "node-1", node1.ID)
	assert.Equal(t, cluster.ID, node1.ClusterID)
	assert.NotNil(t, node1.Resources)
	assert.Equal(t, 8, node1.Resources.CPUCores)
	assert.NotNil(t, node1.Metrics)
	assert.InDelta(t, 0.45, node1.Metrics.CPUUsage, 0.01)

	// Check second node without metrics
	node2 := retrievedCluster.Nodes[1]
	assert.Equal(t, "node-2", node2.ID)
	assert.NotNil(t, node2.Resources)
	assert.Equal(t, 16, node2.Resources.CPUCores)
	assert.Nil(t, node2.Metrics)

	// Test UpdateClusterStatus
	err = store.UpdateClusterStatus(ctx, cluster.ID, models.ClusterStatusDegraded)
	require.NoError(t, err)

	updatedCluster, err := store.GetCluster(ctx, cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, models.ClusterStatusDegraded, updatedCluster.Status)

	// Test UpdateNodeMetrics
	newMetrics := &models.NodeMetrics{
		CPUUsage:       0.75,
		MemoryUsage:    0.80,
		StorageUsage:   0.35,
		NetworkInMbps:  200.0,
		NetworkOutMbps: 250.0,
		PinnedObjects:  2000,
		LastUpdated:    time.Now(),
	}

	err = store.UpdateNodeMetrics(ctx, "node-2", newMetrics)
	require.NoError(t, err)

	updatedCluster, err = store.GetCluster(ctx, cluster.ID)
	require.NoError(t, err)
	
	var updatedNode *models.Node
	for _, node := range updatedCluster.Nodes {
		if node.ID == "node-2" {
			updatedNode = node
			break
		}
	}
	require.NotNil(t, updatedNode)
	require.NotNil(t, updatedNode.Metrics)
	assert.InDelta(t, 0.75, updatedNode.Metrics.CPUUsage, 0.01)
	assert.Equal(t, int64(2000), updatedNode.Metrics.PinnedObjects)
}

func testPolicyOperations(t *testing.T, ctx context.Context, store *YDBMetadataStore) {
	// Create test policy
	policy := &models.Policy{
		ID:      "policy-test-1",
		Name:    "Test Replication Policy",
		Version: 1,
		Rules: map[string]string{
			"replication_factor": "3",
			"placement_zones":    "us-east-1,us-west-2,eu-west-1",
			"storage_class":      "standard",
		},
		Metadata: map[string]string{
			"description": "Test policy for replication",
			"owner":       "test-team",
		},
		CreatedAt: time.Now(),
		CreatedBy: "test-user",
	}

	// Test StorePolicyVersion
	err := store.StorePolicyVersion(ctx, policy)
	require.NoError(t, err)

	// Test GetPolicy
	retrievedPolicy, err := store.GetPolicy(ctx, policy.ID, policy.Version)
	require.NoError(t, err)
	assert.Equal(t, policy.ID, retrievedPolicy.ID)
	assert.Equal(t, policy.Name, retrievedPolicy.Name)
	assert.Equal(t, policy.Version, retrievedPolicy.Version)
	assert.Equal(t, policy.Rules, retrievedPolicy.Rules)
	assert.Equal(t, policy.Metadata, retrievedPolicy.Metadata)
	assert.Equal(t, policy.CreatedBy, retrievedPolicy.CreatedBy)

	// Test GetActivePolicies
	activePolicies, err := store.GetActivePolicies(ctx)
	require.NoError(t, err)
	assert.Len(t, activePolicies, 1)
	assert.Equal(t, policy.ID, activePolicies[0].ID)

	// Create a new version of the same policy
	policyV2 := &models.Policy{
		ID:      policy.ID,
		Name:    policy.Name,
		Version: 2,
		Rules: map[string]string{
			"replication_factor": "4",
			"placement_zones":    "us-east-1,us-west-2,eu-west-1,ap-southeast-1",
			"storage_class":      "premium",
		},
		Metadata: policy.Metadata,
		CreatedAt: time.Now(),
		CreatedBy: "test-user",
	}

	err = store.StorePolicyVersion(ctx, policyV2)
	require.NoError(t, err)

	// Active policies should now return the new version
	activePolicies, err = store.GetActivePolicies(ctx)
	require.NoError(t, err)
	assert.Len(t, activePolicies, 1)
	assert.Equal(t, 2, activePolicies[0].Version)

	// Old version should still be retrievable
	oldPolicy, err := store.GetPolicy(ctx, policy.ID, 1)
	require.NoError(t, err)
	assert.Equal(t, 1, oldPolicy.Version)
	assert.Equal(t, "3", oldPolicy.Rules["replication_factor"])
}

func testShardOperations(t *testing.T, ctx context.Context, store *YDBMetadataStore) {
	// First create zones for shard assignments
	zones := []*models.Zone{
		{
			ID:        "zone-shard-1",
			Name:      "Shard Zone 1",
			Region:    "us-east-1",
			Status:    models.ZoneStatusActive,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			ID:        "zone-shard-2",
			Name:      "Shard Zone 2",
			Region:    "us-west-2",
			Status:    models.ZoneStatusActive,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	for _, zone := range zones {
		err := store.RegisterZone(ctx, zone)
		require.NoError(t, err)
	}

	// Create shard assignments
	assignments := []*models.ShardAssignment{
		{
			ShardID:    "shard-001",
			ZoneID:     "zone-shard-1",
			Status:     "active",
			AssignedAt: time.Now(),
			UpdatedAt:  time.Now(),
		},
		{
			ShardID:    "shard-002",
			ZoneID:     "zone-shard-2",
			Status:     "active",
			AssignedAt: time.Now(),
			UpdatedAt:  time.Now(),
		},
		{
			ShardID:    "shard-003",
			ZoneID:     "zone-shard-1",
			Status:     "migrating",
			AssignedAt: time.Now(),
			UpdatedAt:  time.Now(),
		},
	}

	// Test UpdateShardOwnership
	err := store.UpdateShardOwnership(ctx, assignments)
	require.NoError(t, err)

	// Test GetShardDistribution
	distribution, err := store.GetShardDistribution(ctx)
	require.NoError(t, err)
	assert.Len(t, distribution.Assignments, 3)

	// Check specific assignments
	shard001 := distribution.Assignments["shard-001"]
	require.NotNil(t, shard001)
	assert.Equal(t, "zone-shard-1", shard001.ZoneID)
	assert.Equal(t, "active", shard001.Status)

	shard003 := distribution.Assignments["shard-003"]
	require.NotNil(t, shard003)
	assert.Equal(t, "migrating", shard003.Status)

	// Update shard status
	updatedAssignments := []*models.ShardAssignment{
		{
			ShardID:    "shard-003",
			ZoneID:     "zone-shard-2",
			Status:     "active",
			AssignedAt: assignments[2].AssignedAt,
			UpdatedAt:  time.Now(),
		},
	}

	err = store.UpdateShardOwnership(ctx, updatedAssignments)
	require.NoError(t, err)

	// Verify update
	distribution, err = store.GetShardDistribution(ctx)
	require.NoError(t, err)
	shard003Updated := distribution.Assignments["shard-003"]
	require.NotNil(t, shard003Updated)
	assert.Equal(t, "zone-shard-2", shard003Updated.ZoneID)
	assert.Equal(t, "active", shard003Updated.Status)
}

func testTopologyOperations(t *testing.T, ctx context.Context, store *YDBMetadataStore) {
	// Create a comprehensive topology
	zone := &models.Zone{
		ID:        "zone-topology-test",
		Name:      "Topology Test Zone",
		Region:    "ap-southeast-1",
		Status:    models.ZoneStatusActive,
		Capabilities: map[string]string{
			"storage_type": "nvme",
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err := store.RegisterZone(ctx, zone)
	require.NoError(t, err)

	cluster := &models.Cluster{
		ID:       "cluster-topology-test",
		ZoneID:   zone.ID,
		Name:     "Topology Test Cluster",
		Endpoint: "https://topology-cluster.example.com:9094",
		Status:   models.ClusterStatusHealthy,
		Version:  "1.1.0",
		Nodes: []*models.Node{
			{
				ID:        "node-topology-1",
				ClusterID: "cluster-topology-test",
				Address:   "/ip4/10.0.1.10/tcp/4001",
				Status:    models.NodeStatusOnline,
				Resources: &models.NodeResources{
					CPUCores:     4,
					MemoryBytes:  16 * 1024 * 1024 * 1024,
					StorageBytes: 1024 * 1024 * 1024 * 1024,
					NetworkMbps:  100,
				},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err = store.RegisterCluster(ctx, cluster)
	require.NoError(t, err)

	// Test GetZoneTopology
	topology, err := store.GetZoneTopology(ctx)
	require.NoError(t, err)
	assert.NotNil(t, topology)

	// Check that our test zone and cluster are included
	testZone, exists := topology.Zones[zone.ID]
	require.True(t, exists)
	assert.Equal(t, zone.Name, testZone.Name)
	assert.Equal(t, zone.Region, testZone.Region)

	testCluster, exists := topology.Clusters[cluster.ID]
	require.True(t, exists)
	assert.Equal(t, cluster.Name, testCluster.Name)
	assert.Equal(t, cluster.ZoneID, testCluster.ZoneID)
	assert.Len(t, testCluster.Nodes, 1)

	// Verify node data
	node := testCluster.Nodes[0]
	assert.Equal(t, "node-topology-1", node.ID)
	assert.NotNil(t, node.Resources)
	assert.Equal(t, 4, node.Resources.CPUCores)
}

// TestYDBMetadataStore_ErrorHandling tests error conditions
func TestYDBMetadataStore_ErrorHandling(t *testing.T) {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		t.Skip("YDB_CONNECTION_STRING not set, skipping integration tests")
	}

	ctx := context.Background()
	store, err := NewYDBMetadataStore(ctx, connectionString)
	require.NoError(t, err)
	defer store.Close()

	t.Run("GetNonExistentZone", func(t *testing.T) {
		_, err := store.GetZone(ctx, "non-existent-zone")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "zone not found")
	})

	t.Run("GetNonExistentCluster", func(t *testing.T) {
		_, err := store.GetCluster(ctx, "non-existent-cluster")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cluster not found")
	})

	t.Run("GetNonExistentPolicy", func(t *testing.T) {
		_, err := store.GetPolicy(ctx, "non-existent-policy", 1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "policy not found")
	})

	t.Run("UpdateNonExistentZoneStatus", func(t *testing.T) {
		// This should not error in YDB as UPDATE with no matching rows is valid
		err := store.UpdateZoneStatus(ctx, "non-existent-zone", models.ZoneStatusOffline)
		assert.NoError(t, err)
	})
}

// BenchmarkYDBMetadataStore_Operations benchmarks common operations
func BenchmarkYDBMetadataStore_Operations(b *testing.B) {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		b.Skip("YDB_CONNECTION_STRING not set, skipping benchmark")
	}

	ctx := context.Background()
	store, err := NewYDBMetadataStore(ctx, connectionString)
	require.NoError(b, err)
	defer store.Close()

	// Setup test data
	zone := &models.Zone{
		ID:        "bench-zone",
		Name:      "Benchmark Zone",
		Region:    "us-central-1",
		Status:    models.ZoneStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	err = store.RegisterZone(ctx, zone)
	require.NoError(b, err)

	b.Run("GetZone", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := store.GetZone(ctx, zone.ID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("UpdateZoneStatus", func(b *testing.B) {
		statuses := []models.ZoneStatus{
			models.ZoneStatusActive,
			models.ZoneStatusMaintenance,
			models.ZoneStatusDegraded,
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			status := statuses[i%len(statuses)]
			err := store.UpdateZoneStatus(ctx, zone.ID, status)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetZoneTopology", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := store.GetZoneTopology(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}