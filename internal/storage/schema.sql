-- YDB Schema for Global Data Controller Metadata Store

-- Zones table - stores geographic zones information
CREATE TABLE zones (
    id Utf8 NOT NULL,
    name Utf8 NOT NULL,
    region Utf8 NOT NULL,
    status Utf8 NOT NULL,
    capabilities Json,
    coordinates_latitude Double,
    coordinates_longitude Double,
    created_at Timestamp NOT NULL,
    updated_at Timestamp NOT NULL,
    PRIMARY KEY (id)
);

-- Clusters table - stores IPFS cluster information
CREATE TABLE clusters (
    id Utf8 NOT NULL,
    zone_id Utf8 NOT NULL,
    name Utf8 NOT NULL,
    endpoint Utf8 NOT NULL,
    status Utf8 NOT NULL,
    version Utf8,
    capabilities Json,
    created_at Timestamp NOT NULL,
    updated_at Timestamp NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_clusters_zone_id GLOBAL ON (zone_id)
);

-- Nodes table - stores individual node information within clusters
CREATE TABLE nodes (
    id Utf8 NOT NULL,
    cluster_id Utf8 NOT NULL,
    address Utf8 NOT NULL,
    status Utf8 NOT NULL,
    cpu_cores Uint32,
    memory_bytes Uint64,
    storage_bytes Uint64,
    network_mbps Uint32,
    created_at Timestamp NOT NULL,
    updated_at Timestamp NOT NULL,
    PRIMARY KEY (id),
    INDEX idx_nodes_cluster_id GLOBAL ON (cluster_id)
);

-- Node metrics table - stores current metrics for nodes
CREATE TABLE node_metrics (
    node_id Utf8 NOT NULL,
    cpu_usage Double NOT NULL,
    memory_usage Double NOT NULL,
    storage_usage Double NOT NULL,
    network_in_mbps Double NOT NULL,
    network_out_mbps Double NOT NULL,
    pinned_objects Uint64 NOT NULL,
    last_updated Timestamp NOT NULL,
    PRIMARY KEY (node_id)
);

-- Policies table - stores replication and placement policies
CREATE TABLE policies (
    id Utf8 NOT NULL,
    name Utf8 NOT NULL,
    version Uint32 NOT NULL,
    rules Json NOT NULL,
    metadata Json,
    created_at Timestamp NOT NULL,
    created_by Utf8 NOT NULL,
    PRIMARY KEY (id, version),
    INDEX idx_policies_name GLOBAL ON (name)
);

-- Active policies table - tracks which policy versions are currently active
CREATE TABLE active_policies (
    policy_id Utf8 NOT NULL,
    version Uint32 NOT NULL,
    activated_at Timestamp NOT NULL,
    PRIMARY KEY (policy_id)
);

-- Shard ownership table - tracks which zones own which shards
CREATE TABLE shard_ownership (
    shard_id Utf8 NOT NULL,
    zone_id Utf8 NOT NULL,
    status Utf8 NOT NULL,
    assigned_at Timestamp NOT NULL,
    updated_at Timestamp NOT NULL,
    PRIMARY KEY (shard_id),
    INDEX idx_shard_ownership_zone_id GLOBAL ON (zone_id)
);

-- Content table - stores information about content in the system
CREATE TABLE content (
    cid Utf8 NOT NULL,
    size Uint64 NOT NULL,
    type Utf8 NOT NULL,
    policies Json,
    status Utf8 NOT NULL,
    metadata Json,
    created_at Timestamp NOT NULL,
    updated_at Timestamp NOT NULL,
    PRIMARY KEY (cid)
);

-- Replicas table - stores information about content replicas
CREATE TABLE replicas (
    cid Utf8 NOT NULL,
    node_id Utf8 NOT NULL,
    cluster_id Utf8 NOT NULL,
    zone_id Utf8 NOT NULL,
    status Utf8 NOT NULL,
    created_at Timestamp NOT NULL,
    verified_at Timestamp,
    PRIMARY KEY (cid, node_id),
    INDEX idx_replicas_node_id GLOBAL ON (node_id),
    INDEX idx_replicas_cluster_id GLOBAL ON (cluster_id),
    INDEX idx_replicas_zone_id GLOBAL ON (zone_id)
);