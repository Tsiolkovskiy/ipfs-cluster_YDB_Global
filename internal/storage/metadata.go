package storage

import (
	"context"

	"github.com/global-data-controller/gdc/internal/models"
)

// MetadataStore defines the interface for metadata storage operations
type MetadataStore interface {
	// Zone operations
	RegisterZone(ctx context.Context, zone *models.Zone) error
	GetZoneTopology(ctx context.Context) (*models.Topology, error)
	
	// Cluster operations
	RegisterCluster(ctx context.Context, cluster *models.Cluster) error
	
	// Policy operations
	StorePolicyVersion(ctx context.Context, policy *models.Policy) error
	GetActivePolicies(ctx context.Context) ([]*models.Policy, error)
	
	// Shard operations
	UpdateShardOwnership(ctx context.Context, assignments []*models.ShardAssignment) error
	GetShardDistribution(ctx context.Context) (*models.ShardDistribution, error)
}