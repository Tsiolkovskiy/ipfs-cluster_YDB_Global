# SPIFFE/SPIRE Setup Guide

This guide explains how to set up and use SPIFFE/SPIRE authentication in the Global Data Controller (GDC).

## Overview

SPIFFE (Secure Production Identity Framework For Everyone) provides a secure identity framework for distributed systems. SPIRE (SPIFFE Runtime Environment) is the production-ready implementation that issues and manages SPIFFE identities.

The GDC uses SPIFFE/SPIRE for:
- **Service-to-service authentication** using mTLS with automatically rotated certificates
- **Identity verification** based on SPIFFE IDs
- **Zero-trust security** where every service must authenticate
- **Automatic certificate management** without manual certificate handling

## Quick Start

### 1. Start SPIRE Infrastructure

```bash
# Start SPIRE server and agent
make spire-start

# Setup registration entries for GDC services
make spire-setup

# Check status
make spire-status
```

### 2. Start GDC with SPIFFE Authentication

```bash
# Start development environment with SPIFFE
make dev-spiffe
```

### 3. Test SPIFFE Integration

```bash
# Run integration tests
make test-spiffe
```

## Detailed Setup

### Prerequisites

- Docker and Docker Compose
- PowerShell (for Windows) or Bash (for Linux/macOS)
- Go 1.21+ for development

### Step 1: SPIRE Infrastructure

The SPIRE infrastructure consists of:
- **SPIRE Server**: Issues and manages SPIFFE identities
- **SPIRE Agent**: Runs on each node and provides workload API
- **OIDC Discovery Provider**: (Optional) For web-based authentication

Start the infrastructure:

```bash
docker-compose -f docker-compose.spire.yml up -d spire-server spire-agent
```

### Step 2: Registration Entries

SPIRE requires registration entries to define which workloads can receive which SPIFFE IDs. Run the setup script:

**Windows:**
```powershell
.\scripts\setup-spire-entries.ps1
```

**Linux/macOS:**
```bash
./scripts/setup-spire-entries.sh
```

This creates registration entries for all GDC services:
- `spiffe://gdc.local/gdc-server`
- `spiffe://gdc.local/policy-engine`
- `spiffe://gdc.local/scheduler`
- `spiffe://gdc.local/orchestrator`
- And more...

### Step 3: Configure GDC

Set environment variables or update configuration:

```bash
export GDC_SECURITY_TLS_ENABLED=true
export GDC_SECURITY_SPIFFE_SOCKET_PATH="unix:///tmp/spire-agent/public/api.sock"
export GDC_SECURITY_TRUST_DOMAIN="gdc.local"
export GDC_SECURITY_SERVICE_ID="spiffe://gdc.local/gdc-server"
```

Or in `configs/config.yaml`:

```yaml
security:
  spiffe_socket_path: "unix:///tmp/spire-agent/public/api.sock"
  tls_enabled: true
  trust_domain: "gdc.local"
  service_id: "spiffe://gdc.local/gdc-server"
  authorized_ids:
    - "spiffe://gdc.local/scheduler"
    - "spiffe://gdc.local/orchestrator"
    - "spiffe://gdc.local/policy-engine"
```

### Step 4: Start GDC Services

```bash
# Start with SPIFFE authentication
docker-compose -f docker-compose.dev.yml -f docker-compose.spire.yml --profile spiffe up -d
```

## Usage Examples

### Server with SPIFFE Authentication

```go
package main

import (
    "context"
    "github.com/global-data-controller/gdc/internal/auth"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewProduction()
    
    // Create SPIFFE auth service
    authConfig := &auth.Config{
        SocketPath: "unix:///tmp/spire-agent/public/api.sock",
    }
    
    authService, err := auth.NewSPIFFEAuthService(authConfig, logger)
    if err != nil {
        logger.Fatal("Failed to create auth service", zap.Error(err))
    }
    
    // Initialize
    ctx := context.Background()
    if err := authService.Initialize(ctx); err != nil {
        logger.Fatal("Failed to initialize auth service", zap.Error(err))
    }
    defer authService.Close()
    
    // Create authenticated server
    serverConfig := &auth.ServerConfig{
        TrustDomain: "gdc.local",
        ServiceID:   "spiffe://gdc.local/my-service",
        AuthorizedIDs: []string{
            "spiffe://gdc.local/client-service",
        },
    }
    
    authenticatedServer := auth.NewAuthenticatedServer(authService, serverConfig, logger)
    
    // Create gRPC server with mTLS
    grpcServer, err := authenticatedServer.NewGRPCServer(ctx)
    if err != nil {
        logger.Fatal("Failed to create gRPC server", zap.Error(err))
    }
    
    // Start server
    authenticatedServer.ListenAndServeGRPC(ctx, ":9443", grpcServer)
}
```

### Client with SPIFFE Authentication

```go
package main

import (
    "context"
    "github.com/global-data-controller/gdc/internal/auth"
    "github.com/spiffe/go-spiffe/v2/spiffeid"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewProduction()
    
    // Create SPIFFE auth service
    authService, _ := auth.NewSPIFFEAuthService(&auth.Config{
        SocketPath: "unix:///tmp/spire-agent/public/api.sock",
    }, logger)
    
    ctx := context.Background()
    authService.Initialize(ctx)
    defer authService.Close()
    
    // Create SPIFFE client
    spiffeClient := auth.NewSPIFFEClient(authService, logger)
    
    // Connect to another service
    targetServerID := spiffeid.Must("spiffe://gdc.local/target-service")
    conn, err := spiffeClient.NewGRPCConnection(ctx, "target-service:9443", targetServerID)
    if err != nil {
        logger.Fatal("Failed to connect", zap.Error(err))
    }
    defer conn.Close()
    
    // Use the connection for gRPC calls
    // client := pb.NewServiceClient(conn)
}
```

### HTTP Handler with Identity

```go
func protectedHandler(w http.ResponseWriter, r *http.Request) {
    // Extract authenticated identity
    identity, err := auth.RequireIdentity(r.Context())
    if err != nil {
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return
    }
    
    // Use the identity
    fmt.Fprintf(w, "Hello, %s!", identity.SPIFFEID.String())
}
```

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GDC_SECURITY_SPIFFE_SOCKET_PATH` | SPIRE agent socket path | `unix:///tmp/spire-agent/public/api.sock` |
| `GDC_SECURITY_TLS_ENABLED` | Enable TLS/mTLS | `false` |
| `GDC_SECURITY_TRUST_DOMAIN` | SPIFFE trust domain | `gdc.local` |
| `GDC_SECURITY_SERVICE_ID` | Service SPIFFE ID | `spiffe://gdc.local/gdc-server` |
| `GDC_SECURITY_AUTH_ENABLED` | Enable authentication | `false` |
| `GDC_SECURITY_AUTH_METHOD` | Authentication method | `spiffe` |

### Configuration File

```yaml
security:
  # SPIFFE/SPIRE configuration
  spiffe_socket_path: "unix:///tmp/spire-agent/public/api.sock"
  tls_enabled: true
  trust_domain: "gdc.local"
  service_id: "spiffe://gdc.local/gdc-server"
  
  # Authentication
  auth_enabled: true
  auth_method: "spiffe"  # "spiffe", "tls", "none"
  
  # Authorization
  authz_enabled: true
  authz_method: "opa"    # "opa", "rbac", "none"
  
  # Authorized client SPIFFE IDs
  authorized_ids:
    - "spiffe://gdc.local/scheduler"
    - "spiffe://gdc.local/orchestrator"
    - "spiffe://gdc.local/policy-engine"
    - "spiffe://gdc.local/cluster-adapter"
```

## SPIRE Configuration

### Server Configuration (`configs/spire/server/server.conf`)

```hcl
server {
    bind_address = "0.0.0.0"
    bind_port = "8081"
    trust_domain = "gdc.local"
    data_dir = "/opt/spire/data/server"
    log_level = "INFO"
    default_svid_ttl = "1h"
    ca_ttl = "168h"
}

plugins {
    DataStore "sql" {
        plugin_data {
            database_type = "sqlite3"
            connection_string = "/opt/spire/data/server/datastore.sqlite3"
        }
    }
    
    NodeAttestor "join_token" {
        plugin_data {}
    }
    
    KeyManager "disk" {
        plugin_data {
            keys_path = "/opt/spire/data/server/keys.json"
        }
    }
}
```

### Agent Configuration (`configs/spire/agent/agent.conf`)

```hcl
agent {
    data_dir = "/opt/spire/data/agent"
    log_level = "INFO"
    server_address = "spire-server"
    server_port = "8081"
    socket_path = "/tmp/spire-agent/public/api.sock"
    trust_domain = "gdc.local"
    insecure_bootstrap = true  # For development only
}

plugins {
    NodeAttestor "join_token" {
        plugin_data {}
    }
    
    KeyManager "memory" {
        plugin_data {}
    }
    
    WorkloadAttestor "docker" {
        plugin_data {
            docker_socket_path = "/var/run/docker.sock"
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **SPIRE Agent Connection Failed**
   ```
   Error: failed to create X509Source: connection refused
   ```
   - Check if SPIRE agent is running: `docker ps | grep spire-agent`
   - Verify socket path: `ls -la /tmp/spire-agent/public/api.sock`
   - Check agent logs: `make spire-logs`

2. **Certificate Verification Failed**
   ```
   Error: certificate verification failed: x509: certificate signed by unknown authority
   ```
   - Ensure both services are in the same trust domain
   - Check registration entries: `make spire-entries`
   - Verify trust bundle is up to date

3. **Authentication Failed**
   ```
   Error: authentication failed: no client certificates found
   ```
   - Verify mTLS is properly configured
   - Check if client has valid SVID: `docker exec gdc-spire-agent /opt/spire/bin/spire-agent api fetch`
   - Ensure client certificate is being sent

### Debugging Commands

```bash
# Check SPIRE status
make spire-status

# List registration entries
make spire-entries

# List agents
make spire-agents

# View logs
make spire-logs

# Test SVID fetch
docker exec gdc-spire-agent /opt/spire/bin/spire-agent api fetch -socketPath /tmp/spire-agent/public/api.sock

# Validate server health
docker exec gdc-spire-server /opt/spire/bin/spire-server healthcheck -socketPath /tmp/spire-server/private/api.sock
```

### Development vs Production

**Development:**
- Uses `insecure_bootstrap = true`
- Self-signed certificates
- Permissive policies
- Local Docker setup

**Production:**
- Proper node attestation (TPM, AWS IID, etc.)
- CA-signed certificates
- Strict authorization policies
- Kubernetes or VM deployment
- Monitoring and alerting

## Security Considerations

1. **Trust Domain Isolation**: Use separate trust domains for different environments
2. **Certificate Rotation**: SPIRE automatically rotates certificates
3. **Access Control**: Define specific SPIFFE IDs for fine-grained access
4. **Network Security**: Use network policies to restrict traffic
5. **Monitoring**: Monitor certificate expiration and renewal events

## Integration with Other Systems

### Kubernetes

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: spire-agent-config
data:
  agent.conf: |
    agent {
        data_dir = "/run/spire"
        log_level = "DEBUG"
        server_address = "spire-server"
        server_port = "8081"
        socket_path = "/run/spire/sockets/agent.sock"
        trust_domain = "gdc.local"
    }
```

### Service Mesh (Istio)

SPIFFE/SPIRE can integrate with Istio for service mesh authentication:

```yaml
apiVersion: install.istio.io/v1alpha1
kind: IstioOperator
metadata:
  name: control-plane
spec:
  values:
    pilot:
      env:
        EXTERNAL_ISTIOD: true
        SPIFFE_BUNDLE_ENDPOINTS: "gdc.local|https://spire-server:8081"
```

## Performance Considerations

- **Certificate Caching**: SVIDs are cached and automatically renewed
- **Connection Pooling**: Reuse gRPC connections when possible
- **Batch Operations**: Group multiple operations to reduce overhead
- **Monitoring**: Track authentication latency and success rates

## References

- [SPIFFE Specification](https://github.com/spiffe/spiffe)
- [SPIRE Documentation](https://spiffe.io/docs/latest/spire/)
- [Go-SPIFFE Library](https://github.com/spiffe/go-spiffe)
- [SPIFFE/SPIRE Examples](https://github.com/spiffe/spire-examples)