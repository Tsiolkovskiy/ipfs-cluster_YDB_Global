# SPIFFE/SPIRE Authentication

This package provides SPIFFE/SPIRE based authentication and mTLS for the Global Data Controller (GDC). It implements secure service-to-service communication using SPIFFE Verifiable Identity Documents (SVIDs).

## Overview

SPIFFE (Secure Production Identity Framework For Everyone) provides a secure identity framework for distributed systems. SPIRE (SPIFFE Runtime Environment) is the production-ready implementation that issues and manages SPIFFE identities.

### Key Features

- **mTLS Authentication**: Automatic mutual TLS using SPIFFE SVIDs
- **Identity Verification**: Validates SPIFFE identities from client certificates
- **gRPC Integration**: Interceptors for gRPC servers and clients
- **HTTP Integration**: Middleware for HTTP servers and clients
- **Trust Domain Management**: Supports multi-trust-domain scenarios
- **Automatic Certificate Rotation**: Leverages SPIRE's automatic certificate rotation

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   GDC Service   │    │   SPIRE Agent   │    │  SPIRE Server   │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Auth Service│◄┼────┼►│ Workload API│ │    │ │ Registration│ │
│ └─────────────┘ │    │ └─────────────┘ │    │ │   Entries   │ │
│                 │    │                 │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │                 │
│ │gRPC/HTTP    │ │    │ │    SVIDs    │ │    │                 │
│ │Servers      │ │    │ │             │ │    │                 │
│ └─────────────┘ │    │ └─────────────┘ │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Configuration

### Environment Variables

```bash
# SPIRE Agent socket path
GDC_SECURITY_SPIFFE_SOCKET_PATH="unix:///tmp/spire-agent/public/api.sock"

# Trust domain
GDC_SECURITY_TRUST_DOMAIN="gdc.local"

# Service SPIFFE ID
GDC_SECURITY_SERVICE_ID="spiffe://gdc.local/gdc-server"

# Authorized client SPIFFE IDs (comma-separated)
GDC_SECURITY_AUTHORIZED_IDS="spiffe://gdc.local/scheduler,spiffe://gdc.local/orchestrator"
```

### Configuration File

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

## Usage

### Basic Setup

```go
package main

import (
    "context"
    "log"
    
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
        log.Fatal(err)
    }
    
    // Initialize connection to SPIRE agent
    ctx := context.Background()
    if err := authService.Initialize(ctx); err != nil {
        log.Fatal(err)
    }
    defer authService.Close()
}
```

### gRPC Server with SPIFFE Authentication

```go
// Create server configuration
serverConfig := &auth.ServerConfig{
    TrustDomain: "gdc.local",
    ServiceID:   "spiffe://gdc.local/gdc-server",
    AuthorizedIDs: []string{
        "spiffe://gdc.local/scheduler",
        "spiffe://gdc.local/orchestrator",
    },
}

// Create authenticated server
authenticatedServer := auth.NewAuthenticatedServer(authService, serverConfig, logger)

// Create gRPC server with SPIFFE mTLS
grpcServer, err := authenticatedServer.NewGRPCServer(ctx)
if err != nil {
    log.Fatal(err)
}

// Register your services
pb.RegisterYourServiceServer(grpcServer, yourServiceImpl)

// Start server
go authenticatedServer.ListenAndServeGRPC(ctx, ":9090", grpcServer)
```

### gRPC Client with SPIFFE Authentication

```go
// Create SPIFFE client
spiffeClient := auth.NewSPIFFEClient(authService, logger)

// Connect to another service
targetServerID, _ := spiffeid.FromString("spiffe://gdc.local/scheduler")
conn, err := spiffeClient.NewGRPCConnection(ctx, "scheduler:9090", targetServerID)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

// Use the connection
client := pb.NewSchedulerServiceClient(conn)
response, err := client.ComputePlacement(ctx, &pb.PlacementRequest{})
```

### HTTP Server with SPIFFE Authentication

```go
// Create HTTP handler
handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    // Extract authenticated identity
    identity, err := auth.RequireIdentity(r.Context())
    if err != nil {
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return
    }
    
    fmt.Fprintf(w, "Hello, %s!", identity.SPIFFEID.String())
})

// Create HTTPS server with SPIFFE mTLS
httpServer, err := authenticatedServer.NewHTTPServer(ctx, handler)
if err != nil {
    log.Fatal(err)
}

// Start server
go authenticatedServer.ListenAndServeHTTPS(ctx, ":8443", httpServer)
```

### HTTP Client with SPIFFE Authentication

```go
// Create HTTP client
targetServerID, _ := spiffeid.FromString("spiffe://gdc.local/api-gateway")
httpClient, err := spiffeClient.NewHTTPClient(ctx, targetServerID)
if err != nil {
    log.Fatal(err)
}

// Make authenticated request
resp, err := httpClient.Get("https://api-gateway:8443/api/v1/health")
if err != nil {
    log.Fatal(err)
}
defer resp.Body.Close()
```

## SPIRE Setup

### 1. SPIRE Server Configuration

```hcl
server {
    bind_address = "0.0.0.0"
    bind_port = "8081"
    trust_domain = "gdc.local"
    data_dir = "/opt/spire/data/server"
    log_level = "DEBUG"
    ca_ttl = "168h"
    default_svid_ttl = "1h"
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

    Notifier "k8sbundle" {
        plugin_data {
            namespace = "spire"
            config_map = "trust-bundle"
        }
    }
}
```

### 2. SPIRE Agent Configuration

```hcl
agent {
    data_dir = "/opt/spire/data/agent"
    log_level = "DEBUG"
    server_address = "spire-server"
    server_port = "8081"
    socket_path = "/tmp/spire-agent/public/api.sock"
    trust_domain = "gdc.local"
    trust_bundle_path = "/opt/spire/conf/agent/bootstrap.crt"
}

plugins {
    NodeAttestor "join_token" {
        plugin_data {
            token_path = "/opt/spire/conf/agent/token"
        }
    }

    KeyManager "memory" {
        plugin_data {}
    }

    WorkloadAttestor "unix" {
        plugin_data {}
    }
}
```

### 3. Registration Entries

```bash
# Register GDC server
spire-server entry create \
    -spiffeID spiffe://gdc.local/gdc-server \
    -parentID spiffe://gdc.local/spire/agent/join_token/$(hostname) \
    -selector unix:uid:1000

# Register scheduler service
spire-server entry create \
    -spiffeID spiffe://gdc.local/scheduler \
    -parentID spiffe://gdc.local/spire/agent/join_token/$(hostname) \
    -selector unix:uid:1000

# Register orchestrator service
spire-server entry create \
    -spiffeID spiffe://gdc.local/orchestrator \
    -parentID spiffe://gdc.local/spire/agent/join_token/$(hostname) \
    -selector unix:uid:1000
```

## Security Considerations

### Trust Domain Isolation

- Each environment (dev, staging, prod) should use separate trust domains
- Cross-trust-domain communication requires explicit federation setup
- Trust domain names should follow DNS naming conventions

### Certificate Rotation

- SPIRE automatically rotates SVIDs before expiration
- Applications should handle certificate rotation gracefully
- Monitor certificate expiration and renewal events

### Access Control

- Use specific SPIFFE IDs for fine-grained access control
- Implement authorization policies based on SPIFFE identity
- Regularly audit and rotate registration entries

### Network Security

- SPIFFE provides identity, but network segmentation is still important
- Use firewalls and network policies to restrict traffic
- Monitor network traffic for anomalies

## Authorization with OPA/Rego

The auth package now includes comprehensive authorization using Open Policy Agent (OPA) with Rego policies. This provides fine-grained access control with support for RBAC (Role-Based Access Control) and ABAC (Attribute-Based Access Control).

### Authorization Features

- **Role-Based Access Control (RBAC)**: Users are assigned roles, and roles have specific permissions
- **Attribute-Based Access Control (ABAC)**: Access decisions based on attributes like time, location, resource properties
- **Policy-as-Code**: Authorization policies written in Rego language
- **Runtime Policy Management**: Policies can be updated without service restart
- **Audit Trail**: All authorization decisions are logged with reasons
- **Emergency Access**: Special emergency roles for critical situations
- **Maintenance Windows**: Restrict sensitive operations during maintenance

### Default Roles and Permissions

| Role | Permissions | Description |
|------|-------------|-------------|
| `admin` | `*:*` | Full access to all resources and operations |
| `operator` | Content operations, read access | Can manage content (pin/unpin/replicate) and read system state |
| `policy_admin` | Policy management, read access | Can create/update/delete policies |
| `cluster_admin` | Cluster management, read access | Can manage clusters (create/update/delete/drain) |
| `zone_admin` | Zone management, read access | Can manage zones and maintenance windows |
| `monitor` | Read-only access | Can read metrics, health, status, and topology |
| `service` | Service-to-service communication | For internal service communication |

### Authorization Usage

#### Basic Authorization Check

```go
// Get the authorization service
authzService := authService.GetAuthorizationService()

// Create authorization request
request := &auth.AuthorizationRequest{
    Identity: identity,
    Resource: "policies",
    Action:   "create",
    Context: map[string]interface{}{
        "client_ip": "192.168.1.100",
        "time": time.Now().Unix(),
    },
}

// Check authorization
result, err := authzService.Authorize(ctx, request)
if err != nil {
    log.Fatal(err)
}

if result.Allowed {
    fmt.Printf("Access granted: %s\n", result.Reason)
} else {
    fmt.Printf("Access denied: %s\n", result.Reason)
}
```

#### Role Management

```go
// Assign role to user
err := authzService.AssignRole(ctx, "user123", "operator")

// Revoke role from user
err := authzService.RevokeRole(ctx, "user123", "operator")
```

#### Maintenance Windows

```go
// Set maintenance window
start := time.Now()
end := start.Add(2 * time.Hour)
err := authzService.SetMaintenanceWindow(ctx, start, end, "Database upgrade")
```

#### Custom Policies

```go
// Define custom policy
customPolicy := `
package authz

import rego.v1

# Allow operations only from specific IP ranges
allow := {"allow": true, "reason": "Authorized IP range"} if {
    input.context.client_ip
    net.cidr_contains("10.0.0.0/8", input.context.client_ip)
}
`

// Load custom policy
policies := map[string]string{
    "ip_restriction": customPolicy,
}
err := authzService.LoadPolicies(ctx, policies)
```

### gRPC Server with Authorization

```go
// Create server with both authentication and authorization interceptors
server := grpc.NewServer(
    grpc.ChainUnaryInterceptor(
        authService.GRPCAuthInterceptor(),    // Authentication first
        authService.GRPCAuthzInterceptor(),   // Then authorization
    ),
    grpc.ChainStreamInterceptor(
        authService.GRPCStreamAuthInterceptor(),
        authService.GRPCStreamAuthzInterceptor(),
    ),
)
```

### HTTP Server with Authorization

```go
// Chain middlewares: Authentication -> Authorization -> Handler
protectedHandler := authService.HTTPAuthMiddleware()(
    authService.HTTPAuthzMiddleware()(yourHandler),
)

server := &http.Server{
    Addr:    ":8443",
    Handler: protectedHandler,
    TLSConfig: tlsConfig, // SPIFFE mTLS config
}
```

### Policy Examples

#### Time-Based Access Control

```rego
package authz

import rego.v1

# Allow operations only during business hours
allow := {"allow": true, "reason": "Business hours access"} if {
    input.action in ["create", "update", "delete"]
    current_hour := time.clock(input.time)[0]
    current_hour >= 9
    current_hour <= 17
}
```

#### Resource-Specific Permissions

```rego
package authz

import rego.v1

# Allow users to access only their own resources
allow := {"allow": true, "reason": "Own resource access"} if {
    startswith(input.resource, sprintf("users/%s/", [input.identity.subject]))
}

# Allow admins to access all user resources
allow := {"allow": true, "reason": "Admin access"} if {
    startswith(input.resource, "users/")
    user_roles := data.roles[input.identity.subject]
    "admin" in user_roles
}
```

#### Emergency Access

```rego
package authz

import rego.v1

# Allow emergency access with valid token
allow := {"allow": true, "reason": "Emergency access"} if {
    input.context.emergency_token
    input.context.emergency_token == data.emergency_tokens[input.identity.subject]
    user_roles := data.roles[input.identity.subject]
    "emergency" in user_roles
}
```

## Troubleshooting

### Common Issues

1. **Connection to SPIRE Agent Failed**
   ```
   Error: failed to create X509Source: connection refused
   ```
   - Check if SPIRE agent is running
   - Verify socket path configuration
   - Check file permissions on socket

2. **Certificate Verification Failed**
   ```
   Error: certificate verification failed: x509: certificate signed by unknown authority
   ```
   - Verify trust bundle is up to date
   - Check if SPIFFE ID is properly registered
   - Ensure client and server are in same trust domain

3. **Authentication Failed**
   ```
   Error: authentication failed: no client certificates found
   ```
   - Verify mTLS is properly configured
   - Check if client has valid SVID
   - Ensure client certificate is being sent

4. **Authorization Failed**
   ```
   Error: access denied: No matching policy
   ```
   - Check if user has required roles assigned
   - Verify policy rules are correctly written
   - Check if resource/action mapping is correct
   - Review authorization logs for detailed reasons

5. **Policy Compilation Failed**
   ```
   Error: failed to prepare policy: rego_parse_error
   ```
   - Check Rego syntax in policy rules
   - Verify package declarations
   - Ensure proper import statements

### Debugging

Enable debug logging to troubleshoot issues:

```go
logger := zap.NewDevelopment()
authService, _ := auth.NewSPIFFEAuthService(authConfig, logger)
```

Check SPIRE agent logs:
```bash
docker logs spire-agent
```

Verify SVID status:
```bash
spire-agent api fetch -socketPath /tmp/spire-agent/public/api.sock
```

Debug authorization decisions:
```go
// Enable detailed authorization logging
result, err := authzService.Authorize(ctx, request)
if !result.Allowed {
    logger.Warn("Authorization denied",
        zap.String("resource", request.Resource),
        zap.String("action", request.Action),
        zap.String("reason", result.Reason),
        zap.Any("details", result.Details))
}
```

Test policies in OPA playground:
```bash
# Use OPA CLI to test policies
opa eval -d policies/ -i input.json "data.authz.allow"
```

## Testing

Run the test suite:

```bash
go test ./internal/auth/...
```

For integration tests with actual SPIRE infrastructure:

```bash
# Start SPIRE server and agent
docker-compose up -d spire-server spire-agent

# Run integration tests
go test -tags=integration ./internal/auth/...
```

## References

- [SPIFFE Specification](https://github.com/spiffe/spiffe)
- [SPIRE Documentation](https://spiffe.io/docs/latest/spire/)
- [Go-SPIFFE Library](https://github.com/spiffe/go-spiffe)
- [SPIFFE/SPIRE Examples](https://github.com/spiffe/spire-examples)