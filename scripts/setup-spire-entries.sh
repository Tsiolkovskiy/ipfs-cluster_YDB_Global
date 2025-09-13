#!/bin/bash

# Setup SPIRE registration entries for Global Data Controller services
# This script should be run after SPIRE server and agent are running

set -e

SPIRE_SERVER_CONTAINER="gdc-spire-server"
TRUST_DOMAIN="gdc.local"

echo "Setting up SPIRE registration entries for GDC services..."

# Function to create registration entry
create_entry() {
    local spiffe_id=$1
    local parent_id=$2
    local selector=$3
    local description=$4
    
    echo "Creating registration entry for $description ($spiffe_id)..."
    
    docker exec $SPIRE_SERVER_CONTAINER /opt/spire/bin/spire-server entry create \
        -spiffeID "$spiffe_id" \
        -parentID "$parent_id" \
        -selector "$selector" \
        -ttl 3600 || echo "Entry may already exist: $spiffe_id"
}

# Wait for SPIRE server to be ready
echo "Waiting for SPIRE server to be ready..."
until docker exec $SPIRE_SERVER_CONTAINER /opt/spire/bin/spire-server healthcheck -socketPath /tmp/spire-server/private/api.sock; do
    echo "SPIRE server not ready, waiting..."
    sleep 5
done

# Get the agent SPIFFE ID (this will be the parent for workload entries)
AGENT_ID=$(docker exec $SPIRE_SERVER_CONTAINER /opt/spire/bin/spire-server agent list -socketPath /tmp/spire-server/private/api.sock | grep "spiffe://$TRUST_DOMAIN/spire/agent" | head -1 | awk '{print $2}')

if [ -z "$AGENT_ID" ]; then
    echo "No SPIRE agent found. Make sure the agent is running and has joined the server."
    exit 1
fi

echo "Using agent ID: $AGENT_ID"

# Create registration entries for GDC services

# Main GDC Server
create_entry \
    "spiffe://$TRUST_DOMAIN/gdc-server" \
    "$AGENT_ID" \
    "docker:label:service:gdc-server" \
    "GDC Main Server"

# Policy Engine
create_entry \
    "spiffe://$TRUST_DOMAIN/policy-engine" \
    "$AGENT_ID" \
    "docker:label:service:policy-engine" \
    "Policy Engine Service"

# Scheduler
create_entry \
    "spiffe://$TRUST_DOMAIN/scheduler" \
    "$AGENT_ID" \
    "docker:label:service:scheduler" \
    "Scheduler Service"

# Orchestrator
create_entry \
    "spiffe://$TRUST_DOMAIN/orchestrator" \
    "$AGENT_ID" \
    "docker:label:service:orchestrator" \
    "Orchestrator Service"

# Cluster Adapter
create_entry \
    "spiffe://$TRUST_DOMAIN/cluster-adapter" \
    "$AGENT_ID" \
    "docker:label:service:cluster-adapter" \
    "Cluster Adapter Service"

# Shard Manager
create_entry \
    "spiffe://$TRUST_DOMAIN/shard-manager" \
    "$AGENT_ID" \
    "docker:label:service:shard-manager" \
    "Shard Manager Service"

# Topology Service
create_entry \
    "spiffe://$TRUST_DOMAIN/topology-service" \
    "$AGENT_ID" \
    "docker:label:service:topology-service" \
    "Topology Service"

# Admin UI
create_entry \
    "spiffe://$TRUST_DOMAIN/admin-ui" \
    "$AGENT_ID" \
    "docker:label:service:admin-ui" \
    "Admin UI Service"

# API Gateway
create_entry \
    "spiffe://$TRUST_DOMAIN/api-gateway" \
    "$AGENT_ID" \
    "docker:label:service:api-gateway" \
    "API Gateway Service"

# Health Service (for health checks)
create_entry \
    "spiffe://$TRUST_DOMAIN/health-service" \
    "$AGENT_ID" \
    "docker:label:service:health-service" \
    "Health Service"

echo "SPIRE registration entries created successfully!"

# List all entries to verify
echo "Current registration entries:"
docker exec $SPIRE_SERVER_CONTAINER /opt/spire/bin/spire-server entry show -socketPath /tmp/spire-server/private/api.sock

echo "Setup complete! Services can now authenticate using SPIFFE/SPIRE."