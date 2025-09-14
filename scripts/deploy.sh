#!/bin/bash

set -euo pipefail

# IPFS Cluster ZFS Integration Deployment Script
# This script automates the deployment of IPFS Cluster with ZFS integration

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default configuration
DEPLOYMENT_TYPE="single-node"
CONFIG_FILE=""
SKIP_ZFS_SETUP=false
SKIP_BUILD=false
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Deploy IPFS Cluster with ZFS integration

OPTIONS:
    -t, --type TYPE         Deployment type: single-node, multi-node, docker (default: single-node)
    -c, --config FILE       Configuration file path
    -s, --skip-zfs          Skip ZFS setup (assume already configured)
    -b, --skip-build        Skip building binaries (assume already built)
    -v, --verbose           Enable verbose output
    -h, --help              Show this help message

EXAMPLES:
    $0 --type single-node
    $0 --type multi-node --config cluster.conf
    $0 --type docker
    
EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -t|--type)
                DEPLOYMENT_TYPE="$2"
                shift 2
                ;;
            -c|--config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            -s|--skip-zfs)
                SKIP_ZFS_SETUP=true
                shift
                ;;
            -b|--skip-build)
                SKIP_BUILD=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if running as root for ZFS operations
    if [[ $EUID -ne 0 ]] && [[ "$SKIP_ZFS_SETUP" == false ]]; then
        log_error "This script must be run as root for ZFS setup"
        exit 1
    fi
    
    # Check required commands
    local required_commands=("go" "make" "git")
    if [[ "$SKIP_ZFS_SETUP" == false ]]; then
        required_commands+=("zpool" "zfs")
    fi
    
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "Required command not found: $cmd"
            exit 1
        fi
    done
    
    log_success "Prerequisites check passed"
}

# Setup ZFS pools and datasets
setup_zfs() {
    if [[ "$SKIP_ZFS_SETUP" == true ]]; then
        log_info "Skipping ZFS setup as requested"
        return
    fi
    
    log_info "Setting up ZFS pools and datasets..."
    
    # Check if pools already exist
    local pools=("hot-tier" "warm-tier" "cold-tier")
    for pool in "${pools[@]}"; do
        if zpool list "$pool" &> /dev/null; then
            log_warning "ZFS pool '$pool' already exists, skipping creation"
            continue
        fi
        
        log_info "Creating ZFS pool: $pool"
        case $pool in
            "hot-tier")
                # Create with NVMe devices (adjust device paths as needed)
                if [[ -b /dev/nvme0n1 ]] && [[ -b /dev/nvme1n1 ]]; then
                    zpool create -f "$pool" \
                        -o ashift=12 \
                        -O compression=lz4 \
                        -O recordsize=128K \
                        -O atime=off \
                        /dev/nvme0n1 /dev/nvme1n1
                else
                    log_warning "NVMe devices not found, creating test pool with loop devices"
                    create_test_pool "$pool" "10G" "lz4" "128K"
                fi
                ;;
            "warm-tier")
                # Create with SATA SSDs
                if [[ -b /dev/sda ]] && [[ -b /dev/sdb ]]; then
                    zpool create -f "$pool" \
                        -o ashift=12 \
                        -O compression=gzip-6 \
                        -O recordsize=1M \
                        -O atime=off \
                        /dev/sda /dev/sdb /dev/sdc /dev/sdd
                else
                    log_warning "SATA SSD devices not found, creating test pool with loop devices"
                    create_test_pool "$pool" "50G" "gzip-6" "1M"
                fi
                ;;
            "cold-tier")
                # Create with HDDs
                if [[ -b /dev/sde ]] && [[ -b /dev/sdf ]]; then
                    zpool create -f "$pool" \
                        -o ashift=12 \
                        -O compression=zstd \
                        -O recordsize=1M \
                        -O atime=off \
                        /dev/sde /dev/sdf /dev/sdg /dev/sdh
                else
                    log_warning "HDD devices not found, creating test pool with loop devices"
                    create_test_pool "$pool" "100G" "zstd" "1M"
                fi
                ;;
        esac
    done
    
    # Create datasets
    log_info "Creating ZFS datasets..."
    local datasets=(
        "hot-tier/ipfs-cluster"
        "hot-tier/ipfs-cluster/metadata"
        "hot-tier/ipfs-cluster/hot-shards"
        "warm-tier/ipfs-cluster"
        "warm-tier/ipfs-cluster/warm-shards"
        "cold-tier/ipfs-cluster"
        "cold-tier/ipfs-cluster/cold-shards"
        "cold-tier/ipfs-cluster/archive"
    )
    
    for dataset in "${datasets[@]}"; do
        if zfs list "$dataset" &> /dev/null; then
            log_warning "Dataset '$dataset' already exists, skipping"
            continue
        fi
        
        zfs create "$dataset"
        log_success "Created dataset: $dataset"
    done
    
    log_success "ZFS setup completed"
}

# Create test pool with loop devices
create_test_pool() {
    local pool_name="$1"
    local size="$2"
    local compression="$3"
    local recordsize="$4"
    
    local loop_file="/tmp/${pool_name}.img"
    
    log_info "Creating test pool '$pool_name' with loop device"
    
    # Create sparse file
    truncate -s "$size" "$loop_file"
    
    # Setup loop device
    local loop_device
    loop_device=$(losetup -f --show "$loop_file")
    
    # Create ZFS pool
    zpool create -f "$pool_name" \
        -o ashift=12 \
        -O compression="$compression" \
        -O recordsize="$recordsize" \
        -O atime=off \
        "$loop_device"
    
    log_success "Created test pool '$pool_name' on $loop_device"
}

# Build IPFS Cluster
build_cluster() {
    if [[ "$SKIP_BUILD" == true ]]; then
        log_info "Skipping build as requested"
        return
    fi
    
    log_info "Building IPFS Cluster..."
    
    cd "$PROJECT_ROOT"
    
    # Clean previous builds
    make clean || true
    
    # Build binaries
    make build
    
    # Install binaries
    sudo cp ipfs-cluster-service /usr/local/bin/
    sudo cp ipfs-cluster-ctl /usr/local/bin/
    sudo cp ipfs-cluster-follow /usr/local/bin/
    
    sudo chmod +x /usr/local/bin/ipfs-cluster-*
    
    log_success "IPFS Cluster build completed"
}

# Deploy single node
deploy_single_node() {
    log_info "Deploying single node configuration..."
    
    # Create user and directories
    if ! id -u ipfs-cluster &> /dev/null; then
        sudo useradd -r -s /bin/bash -d /var/lib/ipfs-cluster ipfs-cluster
    fi
    
    sudo mkdir -p /var/lib/ipfs-cluster /var/log/ipfs-cluster
    sudo chown ipfs-cluster:ipfs-cluster /var/lib/ipfs-cluster /var/log/ipfs-cluster
    
    # Initialize IPFS Cluster
    sudo -u ipfs-cluster ipfs-cluster-service init --config-dir /var/lib/ipfs-cluster
    
    # Configure ZFS integration
    local config_file="/var/lib/ipfs-cluster/service.json"
    sudo -u ipfs-cluster cp "$PROJECT_ROOT/docs/deployment/configs/single-node.json" "$config_file"
    
    # Create systemd service
    create_systemd_service
    
    # Start services
    sudo systemctl daemon-reload
    sudo systemctl enable ipfs-cluster
    sudo systemctl start ipfs-cluster
    
    log_success "Single node deployment completed"
}

# Deploy multi-node cluster
deploy_multi_node() {
    log_info "Deploying multi-node cluster..."
    
    if [[ -z "$CONFIG_FILE" ]]; then
        log_error "Multi-node deployment requires a configuration file (-c option)"
        exit 1
    fi
    
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
    
    # Use Ansible for multi-node deployment
    if command -v ansible-playbook &> /dev/null; then
        log_info "Using Ansible for multi-node deployment"
        ansible-playbook -i "$CONFIG_FILE" "$PROJECT_ROOT/ansible/deploy-cluster.yml"
    else
        log_error "Ansible not found. Please install Ansible or use manual deployment"
        exit 1
    fi
    
    log_success "Multi-node deployment completed"
}

# Deploy with Docker
deploy_docker() {
    log_info "Deploying with Docker..."
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "docker-compose not found. Please install Docker Compose"
        exit 1
    fi
    
    cd "$PROJECT_ROOT"
    
    # Generate cluster secret
    export CLUSTER_SECRET=$(openssl rand -hex 32)
    
    # Start the cluster
    docker-compose -f docker/docker-compose.zfs.yml up -d
    
    log_success "Docker deployment completed"
    log_info "Cluster secret: $CLUSTER_SECRET"
}

# Create systemd service
create_systemd_service() {
    log_info "Creating systemd service..."
    
    cat > /tmp/ipfs-cluster.service << 'EOF'
[Unit]
Description=IPFS Cluster Service
After=network.target

[Service]
Type=simple
User=ipfs-cluster
Group=ipfs-cluster
ExecStart=/usr/local/bin/ipfs-cluster-service daemon --config-dir /var/lib/ipfs-cluster
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=ipfs-cluster

[Install]
WantedBy=multi-user.target
EOF
    
    sudo mv /tmp/ipfs-cluster.service /etc/systemd/system/
    
    log_success "Systemd service created"
}

# Verify deployment
verify_deployment() {
    log_info "Verifying deployment..."
    
    case $DEPLOYMENT_TYPE in
        "single-node")
            # Check if service is running
            if sudo systemctl is-active --quiet ipfs-cluster; then
                log_success "IPFS Cluster service is running"
            else
                log_error "IPFS Cluster service is not running"
                return 1
            fi
            
            # Check API endpoint
            if curl -s http://localhost:9094/api/v0/id > /dev/null; then
                log_success "API endpoint is responding"
            else
                log_error "API endpoint is not responding"
                return 1
            fi
            ;;
        "docker")
            # Check Docker containers
            if docker-compose -f "$PROJECT_ROOT/docker/docker-compose.zfs.yml" ps | grep -q "Up"; then
                log_success "Docker containers are running"
            else
                log_error "Docker containers are not running properly"
                return 1
            fi
            ;;
    esac
    
    # Check ZFS pools
    if [[ "$SKIP_ZFS_SETUP" == false ]]; then
        local pools=("hot-tier" "warm-tier" "cold-tier")
        for pool in "${pools[@]}"; do
            if zpool list "$pool" &> /dev/null; then
                log_success "ZFS pool '$pool' is available"
            else
                log_error "ZFS pool '$pool' is not available"
                return 1
            fi
        done
    fi
    
    log_success "Deployment verification completed"
}

# Main function
main() {
    parse_args "$@"
    
    log_info "Starting IPFS Cluster ZFS Integration deployment"
    log_info "Deployment type: $DEPLOYMENT_TYPE"
    
    check_prerequisites
    
    case $DEPLOYMENT_TYPE in
        "single-node")
            setup_zfs
            build_cluster
            deploy_single_node
            ;;
        "multi-node")
            deploy_multi_node
            ;;
        "docker")
            deploy_docker
            ;;
        *)
            log_error "Unknown deployment type: $DEPLOYMENT_TYPE"
            usage
            exit 1
            ;;
    esac
    
    verify_deployment
    
    log_success "Deployment completed successfully!"
    
    # Print next steps
    cat << EOF

Next steps:
1. Check cluster status: ipfs-cluster-ctl status
2. Review logs: journalctl -u ipfs-cluster -f
3. Monitor ZFS pools: zpool status
4. Access API: http://localhost:9094/api/v0/id

For more information, see the documentation at:
- Deployment Guide: docs/deployment/README.md
- Operations Runbook: docs/operations/README.md

EOF
}

# Run main function
main "$@"