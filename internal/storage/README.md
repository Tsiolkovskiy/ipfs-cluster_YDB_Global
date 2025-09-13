# YDB MetadataStore Implementation

This package implements the MetadataStore interface using YDB (Yandex Database) as the backend storage system.

## Overview

The YDB MetadataStore provides persistent storage for Global Data Controller metadata including:

- **Zones**: Geographic zones containing IPFS clusters
- **Clusters**: IPFS cluster information and configuration
- **Nodes**: Individual nodes within clusters with resources and metrics
- **Policies**: Replication and placement policies with versioning
- **Shard Ownership**: Distribution of data shards across zones

## Architecture

### Database Schema

The YDB schema includes the following tables:

- `zones` - Geographic zone information
- `clusters` - IPFS cluster metadata
- `nodes` - Node information and resources
- `node_metrics` - Real-time node performance metrics
- `policies` - Policy definitions with versioning
- `active_policies` - Currently active policy versions
- `shard_ownership` - Shard-to-zone assignments
- `content` - Content metadata (for future use)
- `replicas` - Content replica information (for future use)

### Key Features

1. **ACID Transactions**: All operations use YDB transactions for consistency
2. **Efficient Queries**: Optimized queries with proper indexing
3. **JSON Support**: Complex data structures stored as JSON
4. **Null Safety**: Proper handling of optional fields
5. **Error Handling**: Comprehensive error handling and logging

## Usage

### Basic Setup

```go
import "github.com/global-data-controller/gdc/internal/storage"

// Create a new YDB metadata store
ctx := context.Background()
store, err := storage.NewYDBMetadataStore(ctx, "grpc://localhost:2136/local")
if err != nil {
    log.Fatal(err)
}
defer store.Close()
```

### Zone Operations

```go
// Register a new zone
zone := &models.Zone{
    ID:     "zone-1",
    Name:   "US East 1",
    Region: "us-east-1",
    Status: models.ZoneStatusActive,
    Coordinates: &models.GeoCoordinates{
        Latitude:  40.7128,
        Longitude: -74.0060,
    },
    CreatedAt: time.Now(),
    UpdatedAt: time.Now(),
}

err = store.RegisterZone(ctx, zone)
if err != nil {
    log.Fatal(err)
}

// Retrieve zone
retrievedZone, err := store.GetZone(ctx, "zone-1")
if err != nil {
    log.Fatal(err)
}

// Update zone status
err = store.UpdateZoneStatus(ctx, "zone-1", models.ZoneStatusMaintenance)
if err != nil {
    log.Fatal(err)
}
```

### Cluster Operations

```go
// Register a cluster with nodes
cluster := &models.Cluster{
    ID:       "cluster-1",
    ZoneID:   "zone-1",
    Name:     "Primary Cluster",
    Endpoint: "https://cluster1.example.com:9094",
    Status:   models.ClusterStatusHealthy,
    Nodes: []*models.Node{
        {
            ID:        "node-1",
            ClusterID: "cluster-1",
            Address:   "/ip4/192.168.1.10/tcp/4001",
            Status:    models.NodeStatusOnline,
            Resources: &models.NodeResources{
                CPUCores:     8,
                MemoryBytes:  32 * 1024 * 1024 * 1024,
                StorageBytes: 2 * 1024 * 1024 * 1024 * 1024,
                NetworkMbps:  1000,
            },
        },
    },
    CreatedAt: time.Now(),
    UpdatedAt: time.Now(),
}

err = store.RegisterCluster(ctx, cluster)
if err != nil {
    log.Fatal(err)
}
```

### Policy Operations

```go
// Store a policy version
policy := &models.Policy{
    ID:      "policy-1",
    Name:    "Standard Replication",
    Version: 1,
    Rules: map[string]string{
        "replication_factor": "3",
        "placement_zones":    "us-east-1,us-west-2,eu-west-1",
    },
    CreatedAt: time.Now(),
    CreatedBy: "admin",
}

err = store.StorePolicyVersion(ctx, policy)
if err != nil {
    log.Fatal(err)
}

// Get active policies
activePolicies, err := store.GetActivePolicies(ctx)
if err != nil {
    log.Fatal(err)
}
```

### Topology Operations

```go
// Get complete topology
topology, err := store.GetZoneTopology(ctx)
if err != nil {
    log.Fatal(err)
}

// Access zones and clusters
for zoneID, zone := range topology.Zones {
    fmt.Printf("Zone: %s (%s)\n", zone.Name, zoneID)
}

for clusterID, cluster := range topology.Clusters {
    fmt.Printf("Cluster: %s in zone %s\n", cluster.Name, cluster.ZoneID)
}
```

## Testing

### Unit Tests

Run unit tests that don't require YDB:

```bash
go test -v ./internal/storage/... -run "TestMetadataStoreInterface|TestYDBMetadataStoreStructure"
```

### Integration Tests

To run integration tests, you need a running YDB instance:

1. Start YDB using Docker:
   ```bash
   make ydb-start
   ```

2. Setup the schema:
   ```bash
   make ydb-setup
   ```

3. Run integration tests:
   ```bash
   make ydb-test
   ```

Or run all steps together:
```bash
make ydb-test-setup
```

### Test Environment Variables

- `YDB_CONNECTION_STRING`: Connection string for YDB (e.g., `grpc://localhost:2136/local`)

## Schema Management

### Initial Setup

The database schema is defined in `schema.sql` and can be applied using:

```bash
go run ./scripts/setup-ydb-schema.go <connection_string>
```

### Schema Evolution

When adding new fields or tables:

1. Update `schema.sql` with new DDL statements
2. Update the corresponding Go structs in `internal/models/`
3. Update the CRUD operations in `metadata.go`
4. Add tests for the new functionality

## Performance Considerations

### Indexing

The schema includes strategic indexes for common query patterns:

- `idx_clusters_zone_id` - For zone-based cluster queries
- `idx_nodes_cluster_id` - For cluster-based node queries
- `idx_policies_name` - For policy name lookups
- `idx_shard_ownership_zone_id` - For zone-based shard queries

### Query Optimization

- Use prepared statements for repeated queries
- Batch operations when possible (e.g., `UpdateShardOwnership`)
- Use transactions for multi-table operations
- Leverage YDB's distributed nature for read scaling

### Connection Management

- Connection pooling is handled by the YDB Go SDK
- Use context for timeout and cancellation
- Properly close connections in production

## Error Handling

The implementation provides comprehensive error handling:

- **Connection Errors**: Wrapped with context about the operation
- **Query Errors**: Include the failing query for debugging
- **Data Errors**: JSON marshaling/unmarshaling errors are caught
- **Not Found**: Specific error messages for missing resources

## Monitoring and Observability

### Metrics

The YDB Go SDK provides built-in metrics that can be exported to Prometheus:

- Connection pool statistics
- Query execution times
- Error rates
- Transaction statistics

### Logging

All operations include structured logging with:

- Operation context
- Query parameters (sanitized)
- Execution times
- Error details

### Tracing

The implementation supports distributed tracing through context propagation.

## Security

### Authentication

YDB supports multiple authentication methods:

- Anonymous (for local development)
- Static credentials
- IAM tokens (for Yandex Cloud)
- Service account keys

### Authorization

- Database-level permissions
- Table-level access control
- Row-level security (if needed)

### Encryption

- TLS encryption for client-server communication
- Encryption at rest (YDB feature)

## Deployment

### Local Development

Use the provided Docker Compose configuration:

```bash
docker-compose -f docker-compose.ydb.yml up -d
```

### Production

For production deployment:

1. Use a managed YDB service or deploy YDB cluster
2. Configure proper authentication and TLS
3. Set up monitoring and alerting
4. Configure backup and disaster recovery
5. Tune connection pool settings

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Check network connectivity and YDB service status
2. **Schema Errors**: Ensure schema is properly initialized
3. **Permission Denied**: Verify authentication credentials
4. **Query Failures**: Check YDB logs for detailed error information

### Debug Mode

Enable debug logging by setting the YDB SDK log level:

```go
import "github.com/ydb-platform/ydb-go-sdk/v3/log"

// Enable debug logging
ydb.WithLogger(log.Default(log.DEBUG))
```

## Future Enhancements

Planned improvements:

1. **Connection Pooling**: Advanced connection pool configuration
2. **Query Caching**: Cache frequently accessed data
3. **Batch Operations**: More efficient bulk operations
4. **Schema Migrations**: Automated schema version management
5. **Read Replicas**: Support for read-only replicas
6. **Partitioning**: Table partitioning for large datasets