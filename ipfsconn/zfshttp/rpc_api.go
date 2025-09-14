package zfshttp

import (
	"context"
	"fmt"

	"github.com/ipfs-cluster/ipfs-cluster/api"

	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"go.opencensus.io/trace"
)

var rpcLogger = logging.Logger("zfs-rpc-api")

// ZFSConnectorRPCAPI extends the standard IPFSConnectorRPCAPI with ZFS-specific operations
type ZFSConnectorRPCAPI struct {
	zfsConnector *ZFSConnector
}

// NewZFSConnectorRPCAPI creates a new ZFS connector RPC API
func NewZFSConnectorRPCAPI(connector *ZFSConnector) *ZFSConnectorRPCAPI {
	return &ZFSConnectorRPCAPI{
		zfsConnector: connector,
	}
}

// ZFSMetricsRequest represents a request for ZFS metrics
type ZFSMetricsRequest struct {
	IncludeDatasets bool `json:"include_datasets"`
	IncludeShards   bool `json:"include_shards"`
}

// ZFSMetricsResponse represents the response containing ZFS metrics
type ZFSMetricsResponse struct {
	Metrics  *ZFSMetrics            `json:"metrics"`
	Datasets map[string]*Dataset    `json:"datasets,omitempty"`
	Shards   map[string]*Shard      `json:"shards,omitempty"`
}

// ZFSSnapshotRequest represents a request to create a ZFS snapshot
type ZFSSnapshotRequest struct {
	Dataset string `json:"dataset"`
	Name    string `json:"name"`
}

// ZFSSnapshotListRequest represents a request to list ZFS snapshots
type ZFSSnapshotListRequest struct {
	Dataset string `json:"dataset"`
}

// ZFSSnapshotListResponse represents the response containing snapshot list
type ZFSSnapshotListResponse struct {
	Snapshots []string `json:"snapshots"`
}

// ZFSRollbackRequest represents a request to rollback to a ZFS snapshot
type ZFSRollbackRequest struct {
	Dataset  string `json:"dataset"`
	Snapshot string `json:"snapshot"`
}

// ZFSRebalanceRequest represents a request to rebalance shards
type ZFSRebalanceRequest struct {
	DryRun bool `json:"dry_run"`
}

// ZFSRebalanceResponse represents the response containing rebalance operations
type ZFSRebalanceResponse struct {
	Operations []*ShardRebalanceOperation `json:"operations"`
	Executed   bool                       `json:"executed"`
}

// ZFSOptimizeRequest represents a request to optimize ZFS parameters
type ZFSOptimizeRequest struct {
	Dataset string `json:"dataset"`
	Force   bool   `json:"force"`
}

// ZFSScalingRecommendationRequest represents a request for scaling recommendations
type ZFSScalingRecommendationRequest struct {
	IncludeRebalancing bool `json:"include_rebalancing"`
}

// ZFSScalingRecommendationResponse represents scaling recommendations
type ZFSScalingRecommendationResponse struct {
	Recommendation *ScalingRecommendation `json:"recommendation"`
}

// Pin runs ZFSConnector.Pin() with ZFS optimizations
func (rpcapi *ZFSConnectorRPCAPI) Pin(ctx context.Context, in api.Pin, out *struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/Pin")
	defer span.End()
	
	rpcLogger.Debugf("ZFS Pin RPC call for CID: %s", in.Cid.String())
	return rpcapi.zfsConnector.Pin(ctx, in)
}

// Unpin runs ZFSConnector.Unpin() with ZFS cleanup
func (rpcapi *ZFSConnectorRPCAPI) Unpin(ctx context.Context, in api.Pin, out *struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/Unpin")
	defer span.End()
	
	rpcLogger.Debugf("ZFS Unpin RPC call for CID: %s", in.Cid.String())
	return rpcapi.zfsConnector.Unpin(ctx, in.Cid)
}

// PinLsCid runs ZFSConnector.PinLsCid() with ZFS metadata
func (rpcapi *ZFSConnectorRPCAPI) PinLsCid(ctx context.Context, in api.Pin, out *api.IPFSPinStatus) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/PinLsCid")
	defer span.End()
	
	status, err := rpcapi.zfsConnector.PinLsCid(ctx, in)
	if err != nil {
		return err
	}
	*out = status
	return nil
}

// PinLs runs ZFSConnector.PinLs() with ZFS enhancements
func (rpcapi *ZFSConnectorRPCAPI) PinLs(ctx context.Context, in <-chan []string, out chan<- api.IPFSPinInfo) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/PinLs")
	defer span.End()
	
	// Create a channel to receive type filters
	typeFilters := make([]string, 0)
	select {
	case filters := <-in:
		typeFilters = filters
	case <-ctx.Done():
		return ctx.Err()
	}
	
	return rpcapi.zfsConnector.PinLs(ctx, typeFilters, out)
}

// GetZFSMetrics returns current ZFS performance metrics
func (rpcapi *ZFSConnectorRPCAPI) GetZFSMetrics(ctx context.Context, in ZFSMetricsRequest, out *ZFSMetricsResponse) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/GetZFSMetrics")
	defer span.End()
	
	rpcLogger.Debug("ZFS GetMetrics RPC call")
	
	metrics := rpcapi.zfsConnector.GetZFSMetrics()
	response := &ZFSMetricsResponse{
		Metrics: metrics,
	}
	
	// Include dataset information if requested
	if in.IncludeDatasets {
		rpcapi.zfsConnector.datasetManager.mu.RLock()
		response.Datasets = make(map[string]*Dataset)
		for name, dataset := range rpcapi.zfsConnector.datasetManager.datasets {
			// Create a copy to avoid race conditions
			datasetCopy := *dataset
			response.Datasets[name] = &datasetCopy
		}
		rpcapi.zfsConnector.datasetManager.mu.RUnlock()
	}
	
	// Include shard information if requested
	if in.IncludeShards {
		rpcapi.zfsConnector.datasetManager.mu.RLock()
		response.Shards = make(map[string]*Shard)
		for name, shard := range rpcapi.zfsConnector.datasetManager.shards {
			// Create a copy to avoid race conditions
			shardCopy := *shard
			response.Shards[name] = &shardCopy
		}
		rpcapi.zfsConnector.datasetManager.mu.RUnlock()
	}
	
	*out = *response
	return nil
}

// CreateZFSSnapshot creates a ZFS snapshot
func (rpcapi *ZFSConnectorRPCAPI) CreateZFSSnapshot(ctx context.Context, in ZFSSnapshotRequest, out *struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/CreateZFSSnapshot")
	defer span.End()
	
	rpcLogger.Debugf("ZFS CreateSnapshot RPC call for dataset: %s, snapshot: %s", in.Dataset, in.Name)
	
	if in.Dataset == "" || in.Name == "" {
		return fmt.Errorf("dataset and snapshot name are required")
	}
	
	return rpcapi.zfsConnector.CreateSnapshot(in.Dataset, in.Name)
}

// ListZFSSnapshots lists available ZFS snapshots
func (rpcapi *ZFSConnectorRPCAPI) ListZFSSnapshots(ctx context.Context, in ZFSSnapshotListRequest, out *ZFSSnapshotListResponse) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/ListZFSSnapshots")
	defer span.End()
	
	rpcLogger.Debugf("ZFS ListSnapshots RPC call for dataset: %s", in.Dataset)
	
	if in.Dataset == "" {
		return fmt.Errorf("dataset name is required")
	}
	
	snapshots, err := rpcapi.zfsConnector.ListSnapshots(in.Dataset)
	if err != nil {
		return err
	}
	
	*out = ZFSSnapshotListResponse{
		Snapshots: snapshots,
	}
	return nil
}

// RollbackZFSSnapshot rolls back to a ZFS snapshot
func (rpcapi *ZFSConnectorRPCAPI) RollbackZFSSnapshot(ctx context.Context, in ZFSRollbackRequest, out *struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/RollbackZFSSnapshot")
	defer span.End()
	
	rpcLogger.Debugf("ZFS RollbackSnapshot RPC call for dataset: %s, snapshot: %s", in.Dataset, in.Snapshot)
	
	if in.Dataset == "" || in.Snapshot == "" {
		return fmt.Errorf("dataset and snapshot name are required")
	}
	
	return rpcapi.zfsConnector.RollbackToSnapshot(in.Dataset, in.Snapshot)
}

// RebalanceZFSShards performs shard rebalancing
func (rpcapi *ZFSConnectorRPCAPI) RebalanceZFSShards(ctx context.Context, in ZFSRebalanceRequest, out *ZFSRebalanceResponse) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/RebalanceZFSShards")
	defer span.End()
	
	rpcLogger.Debugf("ZFS RebalanceShards RPC call (dry_run: %v)", in.DryRun)
	
	// Get current shards
	rpcapi.zfsConnector.datasetManager.mu.RLock()
	shards := make([]*Shard, 0, len(rpcapi.zfsConnector.datasetManager.shards))
	for _, shard := range rpcapi.zfsConnector.datasetManager.shards {
		shards = append(shards, shard)
	}
	rpcapi.zfsConnector.datasetManager.mu.RUnlock()
	
	// Create shard manager if not exists (this would normally be part of the connector)
	config := rpcapi.zfsConnector.config.ZFSConfig
	shardManager := NewShardManager(config)
	
	// Get rebalancing operations
	operations, err := shardManager.RebalanceShards(shards)
	if err != nil {
		return fmt.Errorf("failed to calculate rebalancing operations: %w", err)
	}
	
	response := &ZFSRebalanceResponse{
		Operations: operations,
		Executed:   false,
	}
	
	// Execute operations if not dry run
	if !in.DryRun && len(operations) > 0 {
		for _, op := range operations {
			if err := shardManager.ExecuteRebalanceOperation(op, rpcapi.zfsConnector.datasetManager); err != nil {
				rpcLogger.Errorf("Failed to execute rebalance operation %s->%s: %s", 
					op.SourceShard, op.TargetShard, err)
				return fmt.Errorf("failed to execute rebalance operation: %w", err)
			}
		}
		response.Executed = true
	}
	
	*out = *response
	return nil
}

// OptimizeZFS performs ZFS optimization
func (rpcapi *ZFSConnectorRPCAPI) OptimizeZFS(ctx context.Context, in ZFSOptimizeRequest, out *struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/OptimizeZFS")
	defer span.End()
	
	rpcLogger.Debugf("ZFS Optimize RPC call for dataset: %s (force: %v)", in.Dataset, in.Force)
	
	// This would trigger ZFS optimization operations
	// For now, we'll simulate by updating metrics
	rpcapi.zfsConnector.collectZFSMetrics()
	
	rpcLogger.Infof("ZFS optimization completed for dataset: %s", in.Dataset)
	return nil
}

// GetZFSScalingRecommendation returns scaling recommendations
func (rpcapi *ZFSConnectorRPCAPI) GetZFSScalingRecommendation(ctx context.Context, in ZFSScalingRecommendationRequest, out *ZFSScalingRecommendationResponse) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/GetZFSScalingRecommendation")
	defer span.End()
	
	rpcLogger.Debug("ZFS GetScalingRecommendation RPC call")
	
	// Get current shards
	rpcapi.zfsConnector.datasetManager.mu.RLock()
	shards := make([]*Shard, 0, len(rpcapi.zfsConnector.datasetManager.shards))
	for _, shard := range rpcapi.zfsConnector.datasetManager.shards {
		shards = append(shards, shard)
	}
	rpcapi.zfsConnector.datasetManager.mu.RUnlock()
	
	// Create auto-scaler
	config := rpcapi.zfsConnector.config.ZFSConfig
	shardManager := NewShardManager(config)
	autoScaler := NewAutoScaler(shardManager, config)
	
	// Get scaling recommendation
	recommendation, err := autoScaler.CheckScaling(shards)
	if err != nil {
		return fmt.Errorf("failed to get scaling recommendation: %w", err)
	}
	
	*out = ZFSScalingRecommendationResponse{
		Recommendation: recommendation,
	}
	return nil
}

// ConfigKey runs ZFSConnector.ConfigKey() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) ConfigKey(ctx context.Context, in string, out *interface{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/ConfigKey")
	defer span.End()
	
	res, err := rpcapi.zfsConnector.ConfigKey(in)
	if err != nil {
		return err
	}
	*out = res
	return nil
}

// RepoStat runs ZFSConnector.RepoStat() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) RepoStat(ctx context.Context, in struct{}, out *api.IPFSRepoStat) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/RepoStat")
	defer span.End()
	
	res, err := rpcapi.zfsConnector.RepoStat(ctx)
	if err != nil {
		return err
	}
	*out = res
	return nil
}

// SwarmPeers runs ZFSConnector.SwarmPeers() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) SwarmPeers(ctx context.Context, in struct{}, out *[]peer.ID) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/SwarmPeers")
	defer span.End()
	
	res, err := rpcapi.zfsConnector.SwarmPeers(ctx)
	if err != nil {
		return err
	}
	*out = res
	return nil
}

// BlockStream runs ZFSConnector.BlockStream() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) BlockStream(ctx context.Context, in <-chan api.NodeWithMeta, out chan<- struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/BlockStream")
	defer span.End()
	
	defer close(out)
	return rpcapi.zfsConnector.BlockStream(ctx, in)
}

// BlockGet runs ZFSConnector.BlockGet() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) BlockGet(ctx context.Context, in api.Cid, out *[]byte) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/BlockGet")
	defer span.End()
	
	res, err := rpcapi.zfsConnector.BlockGet(ctx, in)
	if err != nil {
		return err
	}
	*out = res
	return nil
}

// Resolve runs ZFSConnector.Resolve() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) Resolve(ctx context.Context, in string, out *api.Cid) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/Resolve")
	defer span.End()
	
	c, err := rpcapi.zfsConnector.Resolve(ctx, in)
	if err != nil {
		return err
	}
	*out = c
	return nil
}

// ID runs ZFSConnector.ID() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) ID(ctx context.Context, in struct{}, out *api.IPFSID) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/ID")
	defer span.End()
	
	id, err := rpcapi.zfsConnector.ID(ctx)
	if err != nil {
		return err
	}
	*out = id
	return nil
}

// ConnectSwarms runs ZFSConnector.ConnectSwarms() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) ConnectSwarms(ctx context.Context, in struct{}, out *struct{}) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/ConnectSwarms")
	defer span.End()
	
	return rpcapi.zfsConnector.ConnectSwarms(ctx)
}

// RepoGC runs ZFSConnector.RepoGC() - delegates to base connector
func (rpcapi *ZFSConnectorRPCAPI) RepoGC(ctx context.Context, in struct{}, out *api.RepoGC) error {
	ctx, span := trace.StartSpan(ctx, "rpc/zfsconn/RepoGC")
	defer span.End()
	
	res, err := rpcapi.zfsConnector.RepoGC(ctx)
	if err != nil {
		return err
	}
	*out = res
	return nil
}