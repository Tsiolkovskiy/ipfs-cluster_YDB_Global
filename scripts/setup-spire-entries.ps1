# Setup SPIRE registration entries for Global Data Controller services
# This script should be run after SPIRE server and agent are running

param(
    [string]$SpireServerContainer = "gdc-spire-server",
    [string]$TrustDomain = "gdc.local"
)

$ErrorActionPreference = "Stop"

Write-Host "Setting up SPIRE registration entries for GDC services..." -ForegroundColor Green

# Function to create registration entry
function Create-Entry {
    param(
        [string]$SpiffeId,
        [string]$ParentId,
        [string]$Selector,
        [string]$Description
    )
    
    Write-Host "Creating registration entry for $Description ($SpiffeId)..." -ForegroundColor Yellow
    
    try {
        docker exec $SpireServerContainer /opt/spire/bin/spire-server entry create `
            -spiffeID "$SpiffeId" `
            -parentID "$ParentId" `
            -selector "$Selector" `
            -ttl 3600
    }
    catch {
        Write-Host "Entry may already exist: $SpiffeId" -ForegroundColor Orange
    }
}

# Wait for SPIRE server to be ready
Write-Host "Waiting for SPIRE server to be ready..." -ForegroundColor Blue
do {
    try {
        docker exec $SpireServerContainer /opt/spire/bin/spire-server healthcheck -socketPath /tmp/spire-server/private/api.sock
        $serverReady = $true
    }
    catch {
        Write-Host "SPIRE server not ready, waiting..." -ForegroundColor Yellow
        Start-Sleep -Seconds 5
        $serverReady = $false
    }
} while (-not $serverReady)

# Get the agent SPIFFE ID (this will be the parent for workload entries)
$agentListOutput = docker exec $SpireServerContainer /opt/spire/bin/spire-server agent list -socketPath /tmp/spire-server/private/api.sock
$agentId = ($agentListOutput | Select-String "spiffe://$TrustDomain/spire/agent" | Select-Object -First 1).ToString().Split()[1]

if (-not $agentId) {
    Write-Error "No SPIRE agent found. Make sure the agent is running and has joined the server."
    exit 1
}

Write-Host "Using agent ID: $agentId" -ForegroundColor Green

# Create registration entries for GDC services

# Main GDC Server
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/gdc-server" `
    -ParentId $agentId `
    -Selector "docker:label:service:gdc-server" `
    -Description "GDC Main Server"

# Policy Engine
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/policy-engine" `
    -ParentId $agentId `
    -Selector "docker:label:service:policy-engine" `
    -Description "Policy Engine Service"

# Scheduler
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/scheduler" `
    -ParentId $agentId `
    -Selector "docker:label:service:scheduler" `
    -Description "Scheduler Service"

# Orchestrator
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/orchestrator" `
    -ParentId $agentId `
    -Selector "docker:label:service:orchestrator" `
    -Description "Orchestrator Service"

# Cluster Adapter
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/cluster-adapter" `
    -ParentId $agentId `
    -Selector "docker:label:service:cluster-adapter" `
    -Description "Cluster Adapter Service"

# Shard Manager
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/shard-manager" `
    -ParentId $agentId `
    -Selector "docker:label:service:shard-manager" `
    -Description "Shard Manager Service"

# Topology Service
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/topology-service" `
    -ParentId $agentId `
    -Selector "docker:label:service:topology-service" `
    -Description "Topology Service"

# Admin UI
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/admin-ui" `
    -ParentId $agentId `
    -Selector "docker:label:service:admin-ui" `
    -Description "Admin UI Service"

# API Gateway
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/api-gateway" `
    -ParentId $agentId `
    -Selector "docker:label:service:api-gateway" `
    -Description "API Gateway Service"

# Health Service (for health checks)
Create-Entry `
    -SpiffeId "spiffe://$TrustDomain/health-service" `
    -ParentId $agentId `
    -Selector "docker:label:service:health-service" `
    -Description "Health Service"

Write-Host "SPIRE registration entries created successfully!" -ForegroundColor Green

# List all entries to verify
Write-Host "Current registration entries:" -ForegroundColor Blue
docker exec $SpireServerContainer /opt/spire/bin/spire-server entry show -socketPath /tmp/spire-server/private/api.sock

Write-Host "Setup complete! Services can now authenticate using SPIFFE/SPIRE." -ForegroundColor Green