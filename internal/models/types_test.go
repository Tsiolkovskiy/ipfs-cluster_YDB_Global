package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZone(t *testing.T) {
	t.Run("Valid Zone Creation", func(t *testing.T) {
		zone := &Zone{
			ID:     "zone-msk-1",
			Name:   "Moscow Zone 1",
			Region: "moscow",
			Status: ZoneStatusActive,
			Capabilities: map[string]string{
				"storage_type": "ssd",
				"bandwidth":    "10gbps",
			},
			Coordinates: &GeoCoordinates{
				Latitude:  55.7558,
				Longitude: 37.6176,
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		assert.Equal(t, "zone-msk-1", zone.ID)
		assert.Equal(t, "Moscow Zone 1", zone.Name)
		assert.Equal(t, "moscow", zone.Region)
		assert.Equal(t, ZoneStatusActive, zone.Status)
		assert.NotNil(t, zone.Coordinates)
		assert.Equal(t, 55.7558, zone.Coordinates.Latitude)
		assert.Equal(t, 37.6176, zone.Coordinates.Longitude)
	})

	t.Run("Zone Status Constants", func(t *testing.T) {
		assert.Equal(t, ZoneStatus("active"), ZoneStatusActive)
		assert.Equal(t, ZoneStatus("degraded"), ZoneStatusDegraded)
		assert.Equal(t, ZoneStatus("maintenance"), ZoneStatusMaintenance)
		assert.Equal(t, ZoneStatus("offline"), ZoneStatusOffline)
	})

	t.Run("Zone JSON Serialization", func(t *testing.T) {
		zone := &Zone{
			ID:     "zone-test",
			Name:   "Test Zone",
			Region: "test",
			Status: ZoneStatusActive,
			Capabilities: map[string]string{
				"test": "value",
			},
			CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		}

		data, err := json.Marshal(zone)
		require.NoError(t, err)

		var unmarshaled Zone
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, zone.ID, unmarshaled.ID)
		assert.Equal(t, zone.Name, unmarshaled.Name)
		assert.Equal(t, zone.Region, unmarshaled.Region)
		assert.Equal(t, zone.Status, unmarshaled.Status)
		assert.Equal(t, zone.Capabilities, unmarshaled.Capabilities)
	})
}

func TestCluster(t *testing.T) {
	t.Run("Valid Cluster Creation", func(t *testing.T) {
		cluster := &Cluster{
			ID:       "cluster-1",
			ZoneID:   "zone-msk-1",
			Name:     "IPFS Cluster 1",
			Endpoint: "https://cluster1.example.com:9094",
			Status:   ClusterStatusHealthy,
			Version:  "1.1.4",
			Nodes: []*Node{
				{
					ID:        "node-1",
					ClusterID: "cluster-1",
					Address:   "192.168.1.10",
					Status:    NodeStatusOnline,
				},
			},
			Capabilities: map[string]string{
				"max_storage": "10TB",
				"replication": "true",
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		assert.Equal(t, "cluster-1", cluster.ID)
		assert.Equal(t, "zone-msk-1", cluster.ZoneID)
		assert.Equal(t, ClusterStatusHealthy, cluster.Status)
		assert.Len(t, cluster.Nodes, 1)
		assert.Equal(t, "node-1", cluster.Nodes[0].ID)
	})

	t.Run("Cluster Status Constants", func(t *testing.T) {
		assert.Equal(t, ClusterStatus("healthy"), ClusterStatusHealthy)
		assert.Equal(t, ClusterStatus("degraded"), ClusterStatusDegraded)
		assert.Equal(t, ClusterStatus("offline"), ClusterStatusOffline)
	})
}

func TestNode(t *testing.T) {
	t.Run("Valid Node Creation", func(t *testing.T) {
		node := &Node{
			ID:        "node-1",
			ClusterID: "cluster-1",
			Address:   "192.168.1.10",
			Status:    NodeStatusOnline,
			Resources: &NodeResources{
				CPUCores:     8,
				MemoryBytes:  32 * 1024 * 1024 * 1024, // 32GB
				StorageBytes: 10 * 1024 * 1024 * 1024 * 1024, // 10TB
				NetworkMbps:  1000,
			},
			Metrics: &NodeMetrics{
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
		}

		assert.Equal(t, "node-1", node.ID)
		assert.Equal(t, "cluster-1", node.ClusterID)
		assert.Equal(t, NodeStatusOnline, node.Status)
		assert.NotNil(t, node.Resources)
		assert.NotNil(t, node.Metrics)
		assert.Equal(t, int64(32*1024*1024*1024), node.Resources.MemoryBytes)
		assert.Equal(t, 0.45, node.Metrics.CPUUsage)
	})

	t.Run("Node Status Constants", func(t *testing.T) {
		assert.Equal(t, NodeStatus("online"), NodeStatusOnline)
		assert.Equal(t, NodeStatus("offline"), NodeStatusOffline)
		assert.Equal(t, NodeStatus("draining"), NodeStatusDraining)
	})

	t.Run("Node Resources Validation", func(t *testing.T) {
		resources := &NodeResources{
			CPUCores:     16,
			MemoryBytes:  64 * 1024 * 1024 * 1024, // 64GB
			StorageBytes: 20 * 1024 * 1024 * 1024 * 1024, // 20TB
			NetworkMbps:  10000, // 10Gbps
		}

		assert.Equal(t, 16, resources.CPUCores)
		assert.Equal(t, int64(64*1024*1024*1024), resources.MemoryBytes)
		assert.Equal(t, int64(20*1024*1024*1024*1024), resources.StorageBytes)
		assert.Equal(t, 10000, resources.NetworkMbps)
	})

	t.Run("Node Metrics Validation", func(t *testing.T) {
		metrics := &NodeMetrics{
			CPUUsage:       0.75,
			MemoryUsage:    0.85,
			StorageUsage:   0.40,
			NetworkInMbps:  500.0,
			NetworkOutMbps: 750.0,
			PinnedObjects:  2500,
			LastUpdated:    time.Now(),
		}

		// Validate usage percentages are within valid range
		assert.True(t, metrics.CPUUsage >= 0.0 && metrics.CPUUsage <= 1.0)
		assert.True(t, metrics.MemoryUsage >= 0.0 && metrics.MemoryUsage <= 1.0)
		assert.True(t, metrics.StorageUsage >= 0.0 && metrics.StorageUsage <= 1.0)
		assert.True(t, metrics.PinnedObjects >= 0)
	})
}

func TestContent(t *testing.T) {
	t.Run("Valid Content Creation", func(t *testing.T) {
		content := &Content{
			CID:  "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			Size: 1024 * 1024, // 1MB
			Type: "application/json",
			Replicas: []*Replica{
				{
					NodeID:     "node-1",
					ClusterID:  "cluster-1",
					ZoneID:     "zone-msk-1",
					Status:     ReplicaStatusPinned,
					CreatedAt:  time.Now(),
					VerifiedAt: nil,
				},
			},
			Policies: []string{"policy-rf-3", "policy-geo-diverse"},
			Status:   ContentStatusPinned,
			Metadata: map[string]string{
				"content_type": "json",
				"priority":     "high",
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		assert.Equal(t, "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG", content.CID)
		assert.Equal(t, int64(1024*1024), content.Size)
		assert.Equal(t, ContentStatusPinned, content.Status)
		assert.Len(t, content.Replicas, 1)
		assert.Len(t, content.Policies, 2)
	})

	t.Run("Content Status Constants", func(t *testing.T) {
		assert.Equal(t, ContentStatus("pending"), ContentStatusPending)
		assert.Equal(t, ContentStatus("pinned"), ContentStatusPinned)
		assert.Equal(t, ContentStatus("replicated"), ContentStatusReplicated)
		assert.Equal(t, ContentStatus("failed"), ContentStatusFailed)
	})
}

func TestReplica(t *testing.T) {
	t.Run("Valid Replica Creation", func(t *testing.T) {
		now := time.Now()
		replica := &Replica{
			NodeID:     "node-1",
			ClusterID:  "cluster-1",
			ZoneID:     "zone-msk-1",
			Status:     ReplicaStatusVerified,
			CreatedAt:  now,
			VerifiedAt: &now,
		}

		assert.Equal(t, "node-1", replica.NodeID)
		assert.Equal(t, "cluster-1", replica.ClusterID)
		assert.Equal(t, "zone-msk-1", replica.ZoneID)
		assert.Equal(t, ReplicaStatusVerified, replica.Status)
		assert.NotNil(t, replica.VerifiedAt)
	})

	t.Run("Replica Status Constants", func(t *testing.T) {
		assert.Equal(t, ReplicaStatus("pending"), ReplicaStatusPending)
		assert.Equal(t, ReplicaStatus("pinned"), ReplicaStatusPinned)
		assert.Equal(t, ReplicaStatus("failed"), ReplicaStatusFailed)
		assert.Equal(t, ReplicaStatus("verified"), ReplicaStatusVerified)
	})
}

func TestPolicy(t *testing.T) {
	t.Run("Valid Policy Creation", func(t *testing.T) {
		policy := &Policy{
			ID:      "policy-rf-3",
			Name:    "Replication Factor 3",
			Version: 1,
			Rules: map[string]string{
				"replication_factor": "3",
				"zone_diversity":     "true",
			},
			Metadata: map[string]string{
				"description": "Standard RF=3 policy with zone diversity",
				"category":    "replication",
			},
			CreatedAt: time.Now(),
			CreatedBy: "admin@example.com",
		}

		assert.Equal(t, "policy-rf-3", policy.ID)
		assert.Equal(t, "Replication Factor 3", policy.Name)
		assert.Equal(t, 1, policy.Version)
		assert.Equal(t, "3", policy.Rules["replication_factor"])
		assert.Equal(t, "admin@example.com", policy.CreatedBy)
	})

	t.Run("Policy JSON Serialization", func(t *testing.T) {
		policy := &Policy{
			ID:      "test-policy",
			Name:    "Test Policy",
			Version: 2,
			Rules: map[string]string{
				"rule1": "value1",
			},
			Metadata: map[string]string{
				"meta1": "value1",
			},
			CreatedAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			CreatedBy: "test@example.com",
		}

		data, err := json.Marshal(policy)
		require.NoError(t, err)

		var unmarshaled Policy
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, policy.ID, unmarshaled.ID)
		assert.Equal(t, policy.Name, unmarshaled.Name)
		assert.Equal(t, policy.Version, unmarshaled.Version)
		assert.Equal(t, policy.Rules, unmarshaled.Rules)
		assert.Equal(t, policy.Metadata, unmarshaled.Metadata)
		assert.Equal(t, policy.CreatedBy, unmarshaled.CreatedBy)
	})
}

func TestPinPlan(t *testing.T) {
	t.Run("Valid PinPlan Creation", func(t *testing.T) {
		plan := &PinPlan{
			ID:  "plan-123",
			CID: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
			Assignments: []*NodeAssignment{
				{
					NodeID:    "node-1",
					ClusterID: "cluster-1",
					ZoneID:    "zone-msk-1",
					Priority:  1,
				},
				{
					NodeID:    "node-2",
					ClusterID: "cluster-2",
					ZoneID:    "zone-nn-1",
					Priority:  2,
				},
			},
			Cost: &CostEstimate{
				StorageCost: 10.50,
				EgressCost:  5.25,
				ComputeCost: 2.75,
				TotalCost:   18.50,
				Currency:    "USD",
			},
			CreatedAt: time.Now(),
		}

		assert.Equal(t, "plan-123", plan.ID)
		assert.Equal(t, "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG", plan.CID)
		assert.Len(t, plan.Assignments, 2)
		assert.NotNil(t, plan.Cost)
		assert.Equal(t, 18.50, plan.Cost.TotalCost)
		assert.Equal(t, "USD", plan.Cost.Currency)
	})
}

func TestTopology(t *testing.T) {
	t.Run("Valid Topology Creation", func(t *testing.T) {
		zone1 := &Zone{
			ID:     "zone-1",
			Name:   "Zone 1",
			Region: "region-1",
			Status: ZoneStatusActive,
		}

		cluster1 := &Cluster{
			ID:     "cluster-1",
			ZoneID: "zone-1",
			Name:   "Cluster 1",
			Status: ClusterStatusHealthy,
		}

		topology := &Topology{
			Zones: map[string]*Zone{
				"zone-1": zone1,
			},
			Clusters: map[string]*Cluster{
				"cluster-1": cluster1,
			},
			UpdatedAt: time.Now(),
		}

		assert.Len(t, topology.Zones, 1)
		assert.Len(t, topology.Clusters, 1)
		assert.Equal(t, zone1, topology.Zones["zone-1"])
		assert.Equal(t, cluster1, topology.Clusters["cluster-1"])
	})
}

func TestShardAssignment(t *testing.T) {
	t.Run("Valid ShardAssignment Creation", func(t *testing.T) {
		assignment := &ShardAssignment{
			ShardID:    "shard-001",
			ZoneID:     "zone-msk-1",
			Status:     "active",
			AssignedAt: time.Now(),
			UpdatedAt:  time.Now(),
		}

		assert.Equal(t, "shard-001", assignment.ShardID)
		assert.Equal(t, "zone-msk-1", assignment.ZoneID)
		assert.Equal(t, "active", assignment.Status)
	})
}

func TestShardDistribution(t *testing.T) {
	t.Run("Valid ShardDistribution Creation", func(t *testing.T) {
		assignment1 := &ShardAssignment{
			ShardID: "shard-001",
			ZoneID:  "zone-msk-1",
			Status:  "active",
		}

		assignment2 := &ShardAssignment{
			ShardID: "shard-002",
			ZoneID:  "zone-nn-1",
			Status:  "active",
		}

		distribution := &ShardDistribution{
			Assignments: map[string]*ShardAssignment{
				"shard-001": assignment1,
				"shard-002": assignment2,
			},
			UpdatedAt: time.Now(),
		}

		assert.Len(t, distribution.Assignments, 2)
		assert.Equal(t, assignment1, distribution.Assignments["shard-001"])
		assert.Equal(t, assignment2, distribution.Assignments["shard-002"])
	})
}

func TestGeoCoordinates(t *testing.T) {
	t.Run("Valid GeoCoordinates", func(t *testing.T) {
		coords := &GeoCoordinates{
			Latitude:  55.7558,  // Moscow
			Longitude: 37.6176,
		}

		assert.Equal(t, 55.7558, coords.Latitude)
		assert.Equal(t, 37.6176, coords.Longitude)
	})

	t.Run("GeoCoordinates JSON Serialization", func(t *testing.T) {
		coords := &GeoCoordinates{
			Latitude:  40.7128,  // New York
			Longitude: -74.0060,
		}

		data, err := json.Marshal(coords)
		require.NoError(t, err)

		var unmarshaled GeoCoordinates
		err = json.Unmarshal(data, &unmarshaled)
		require.NoError(t, err)

		assert.Equal(t, coords.Latitude, unmarshaled.Latitude)
		assert.Equal(t, coords.Longitude, unmarshaled.Longitude)
	})
}