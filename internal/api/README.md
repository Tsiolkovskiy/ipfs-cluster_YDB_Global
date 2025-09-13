# API Gateway

This package implements the API Gateway for the Global Data Controller (GDC), providing both HTTP/REST and gRPC interfaces for managing distributed IPFS clusters and content replication.

## Features

### HTTP/REST API
- **RESTful endpoints** for all major operations
- **Request validation** with comprehensive error handling
- **Rate limiting** per client IP (configurable)
- **CORS support** for web applications
- **OpenAPI 3.0 specification** with complete documentation
- **JSON request/response** format with proper content-type handling

### gRPC API
- **High-performance gRPC** server for internal service communication
- **Request logging** and error handling
- **Type-safe** protocol buffer interfaces (when proto files are available)
- **Streaming support** for real-time operations

### Security & Middleware
- **Rate limiting**: Configurable per-IP rate limiting with token bucket algorithm
- **CORS**: Cross-Origin Resource Sharing support
- **Request validation**: Comprehensive input validation for all endpoints
- **Error handling**: Structured error responses with proper HTTP status codes
- **Logging**: Request/response logging with correlation IDs

## API Endpoints

### Health & Monitoring
- `GET /health` - Service health check
- `GET /ready` - Service readiness check

### Zone Management
- `GET /api/v1/zones` - List all zones
- `POST /api/v1/zones` - Create a new zone
- `GET /api/v1/zones/{id}` - Get zone details
- `PUT /api/v1/zones/{id}` - Update zone
- `DELETE /api/v1/zones/{id}` - Delete zone

### Cluster Management
- `GET /api/v1/clusters` - List clusters (optionally filtered by zone)
- `POST /api/v1/clusters` - Create a new cluster
- `GET /api/v1/clusters/{id}` - Get cluster details
- `PUT /api/v1/clusters/{id}` - Update cluster
- `DELETE /api/v1/clusters/{id}` - Delete cluster

### Content Management
- `POST /api/v1/content/pin` - Pin content to the network
- `DELETE /api/v1/content/unpin/{cid}` - Unpin content
- `GET /api/v1/content/status/{cid}` - Get content pin status
- `GET /api/v1/content` - List pinned content with filtering

### Policy Management
- `GET /api/v1/policies` - List all policies
- `POST /api/v1/policies` - Create a new policy
- `GET /api/v1/policies/{id}` - Get policy details
- `PUT /api/v1/policies/{id}` - Update policy
- `DELETE /api/v1/policies/{id}` - Delete policy

### Topology
- `GET /api/v1/topology` - Get global topology information

### Documentation
- `GET /api/v1/openapi.json` - OpenAPI specification

## Usage

### Basic Setup

```go
package main

import (
    "context"
    "log"
    
    "github.com/global-data-controller/gdc/internal/api"
    "go.uber.org/zap"
)

func main() {
    // Create logger
    logger, err := zap.NewProduction()
    if err != nil {
        log.Fatal(err)
    }
    
    // Create your services implementation
    services := NewYourServices() // implements api.Services interface
    
    // Create HTTP Gateway
    httpGateway := api.NewHTTPGateway(services, logger)
    
    // Create gRPC Server
    grpcServer := api.NewGRPCServer(services, logger)
    
    ctx := context.Background()
    
    // Start servers
    if err := httpGateway.Start(ctx, ":8080"); err != nil {
        logger.Fatal("Failed to start HTTP gateway", zap.Error(err))
    }
    
    if err := grpcServer.Start(ctx, ":9090"); err != nil {
        logger.Fatal("Failed to start gRPC server", zap.Error(err))
    }
    
    // Servers are now running
    logger.Info("API Gateway started", 
        zap.String("http", ":8080"),
        zap.String("grpc", ":9090"))
    
    // Graceful shutdown
    defer func() {
        httpGateway.Stop(ctx)
        grpcServer.Stop(ctx)
    }()
}
```

### Services Interface

The API Gateway requires a `Services` interface implementation that provides the business logic:

```go
type Services interface {
    // Zone management
    GetZones(ctx context.Context) ([]*models.Zone, error)
    GetZone(ctx context.Context, id string) (*models.Zone, error)
    CreateZone(ctx context.Context, zone *models.Zone) error
    UpdateZone(ctx context.Context, id string, zone *models.Zone) error
    DeleteZone(ctx context.Context, id string) error

    // Cluster management
    GetClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error)
    GetCluster(ctx context.Context, id string) (*models.Cluster, error)
    CreateCluster(ctx context.Context, cluster *models.Cluster) error
    UpdateCluster(ctx context.Context, id string, cluster *models.Cluster) error
    DeleteCluster(ctx context.Context, id string) error

    // Content management
    PinContent(ctx context.Context, request *PinRequest) (*models.PinPlan, error)
    UnpinContent(ctx context.Context, cid string) error
    GetContentStatus(ctx context.Context, cid string) (*models.PinStatus, error)
    ListContent(ctx context.Context, filter *ContentFilter) ([]*models.Content, error)

    // Policy management
    GetPolicies(ctx context.Context) ([]*models.Policy, error)
    GetPolicy(ctx context.Context, id string) (*models.Policy, error)
    CreatePolicy(ctx context.Context, policy *models.Policy) error
    UpdatePolicy(ctx context.Context, id string, policy *models.Policy) error
    DeletePolicy(ctx context.Context, id string) error

    // Topology
    GetTopology(ctx context.Context) (*models.Topology, error)
}
```

### Rate Limiting Configuration

The rate limiter can be configured when creating the gateway:

```go
// Default: 100 requests per minute per IP, burst of 10
gateway := api.NewHTTPGateway(services, logger)

// Custom rate limiting
gateway.limiter = api.NewRateLimiter(
    rate.Every(time.Second), // 1 request per second
    5,                       // burst of 5
)
```

### Example Requests

#### Create a Zone
```bash
curl -X POST http://localhost:8080/api/v1/zones \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Moscow Primary",
    "region": "RU-MSK",
    "status": "active",
    "capabilities": {
      "storage_type": "ssd",
      "network_tier": "premium"
    },
    "coordinates": {
      "latitude": 55.7558,
      "longitude": 37.6176
    }
  }'
```

#### Pin Content
```bash
curl -X POST http://localhost:8080/api/v1/content/pin \
  -H "Content-Type: application/json" \
  -d '{
    "cid": "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG",
    "size": 1048576,
    "policies": ["default-replication"],
    "constraints": {
      "zones": "msk,nn",
      "storage_class": "ssd"
    },
    "priority": 1
  }'
```

#### Get Content Status
```bash
curl http://localhost:8080/api/v1/content/status/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG
```

## Error Handling

The API uses structured error responses:

```json
{
  "code": "VALIDATION_ERROR",
  "message": "Validation failed",
  "details": {
    "error": "name is required"
  }
}
```

### Error Codes
- `INTERNAL_SERVER_ERROR` - Internal server errors
- `VALIDATION_ERROR` - Request validation failures
- `RATE_LIMIT_EXCEEDED` - Rate limit exceeded
- `METHOD_NOT_ALLOWED` - HTTP method not allowed
- `ZONE_NOT_FOUND` - Zone not found
- `CLUSTER_NOT_FOUND` - Cluster not found
- `CONTENT_NOT_FOUND` - Content not found
- `POLICY_NOT_FOUND` - Policy not found

## Testing

The package includes comprehensive tests covering:

- **Unit tests** for all handlers and middleware
- **Integration tests** for full request flows
- **Mock services** for isolated testing
- **Rate limiting tests**
- **Validation tests**
- **Error handling tests**

Run tests:
```bash
go test ./internal/api/... -v
```

## OpenAPI Documentation

The API provides a complete OpenAPI 3.0 specification available at:
- `GET /api/v1/openapi.json` - JSON format
- `internal/api/openapi.yaml` - YAML source file

The specification includes:
- Complete endpoint documentation
- Request/response schemas
- Example requests and responses
- Error response formats
- Authentication requirements (when implemented)

## Architecture

The API Gateway follows a clean architecture pattern:

```
┌─────────────────┐    ┌─────────────────┐
│   HTTP Client   │    │   gRPC Client   │
└─────────┬───────┘    └─────────┬───────┘
          │                      │
          ▼                      ▼
┌─────────────────┐    ┌─────────────────┐
│  HTTP Gateway   │    │  gRPC Server    │
│  - Routing      │    │  - Type Safety  │
│  - Validation   │    │  - Performance  │
│  - Rate Limit   │    │  - Streaming    │
│  - CORS         │    │  - Logging      │
└─────────┬───────┘    └─────────┬───────┘
          │                      │
          └──────────┬───────────┘
                     ▼
          ┌─────────────────┐
          │    Services     │
          │   Interface     │
          └─────────┬───────┘
                    ▼
          ┌─────────────────┐
          │ Business Logic  │
          │ (Policy Engine, │
          │  Scheduler,     │
          │  Orchestrator)  │
          └─────────────────┘
```

## Future Enhancements

- **Authentication**: SPIFFE/SPIRE integration for mTLS
- **Authorization**: OPA/Rego policy enforcement
- **GraphQL**: GraphQL endpoint for flexible queries
- **WebSocket**: Real-time updates for status changes
- **Metrics**: Prometheus metrics for monitoring
- **Tracing**: OpenTelemetry distributed tracing
- **Caching**: Redis caching for frequently accessed data