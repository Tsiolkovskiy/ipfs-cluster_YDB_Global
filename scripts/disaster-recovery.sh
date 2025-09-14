#!/bin/bash

# IPFS Cluster ZFS Integration Disaster Recovery Script
# Handles backup, restore, and emergency procedures

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="/backup/ipfs-cluster"
LOG_DIR="/var/log/ipfs-cluster"
RETENTION_DAYS=30

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: $1" >> "$LOG_DIR/disaster-recovery.log"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') SUCCESS: $1" >> "$LOG_DIR/disaster-recovery.log"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') WARNING: $1" >> "$LOG_DIR/disaster-recovery.log"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR: $1" >> "$LOG_DIR/disaster-recovery.log"
}

# Create backup
create_backup() {
    local backup_type="$1"
    local backup_date
    backup_date=$(date +%Y%m%d-%H%M%S)
    local backup_path="$BACKUP_DIR/$backup_date"
    
    log_info "Creating $backup_type backup: $backup_path"
    
    mkdir -p "$backup_path"
    
    case $backup_type in
        "full")
            create_full_backup "$backup_path"
            ;;
        "incremental")
            create_incremental_backup "$backup_path"
            ;;
        "emergency")
            create_emergency_backup "$backup_path"
            ;;
        *)
            log_error "Unknown backup type: $backup_type"
            exit 1
            ;;
    esac
    
    # Create backup manifest
    create_backup_manifest "$backup_path" "$backup_type"
    
    log_success "Backup completed: $backup_path"
}

# Create full backup
create_full_backup() {
    local backup_path="$1"
    
    log_info "Creating full system backup..."
    
    # Stop services for consistent backup
    log_info "Stopping services for consistent backup..."
    systemctl stop ipfs-cluster || true
    sleep 5
    
    # Export cluster state
    log_info "Exporting cluster state..."
    if [[ -f /var/lib/ipfs-cluster/service.json ]]; then
        ipfs-cluster-service state export "$backup_path/cluster-state.json" --config-dir /var/lib/ipfs-cluster || true
    fi
    
    # Backup configuration
    log_info "Backing up configuration..."
    cp -r /var/lib/ipfs-cluster "$backup_path/config" 2>/dev/null || true
    cp -r /etc/ipfs-cluster "$backup_path/system-config" 2>/dev/null || true
    
    # Create ZFS snapshots
    log_info "Creating ZFS snapshots..."
    local pools=("hot-tier" "warm-tier" "cold-tier")
    
    for pool in "${pools[@]}"; do
        if zpool list "$pool" > /dev/null 2>&1; then
            log_info "Processing pool: $pool"
            
            for dataset in $(zfs list -H -o name | grep "$pool/ipfs-cluster" || true); do
                local snapshot_name="${dataset}@backup-$backup_date"
                
                if zfs snapshot "$snapshot_name"; then
                    log_success "Created snapshot: $snapshot_name"
                    
                    # Export snapshot metadata
                    zfs get all "$snapshot_name" > "$backup_path/snapshot-${dataset//\//-}-metadata.txt"
                    
                    # If backup pool exists, send snapshot there
                    if zpool list backup-pool > /dev/null 2>&1; then
                        local backup_dataset="backup-pool/${dataset##*/}-$(date +%Y%m%d)"
                        
                        if zfs send "$snapshot_name" | zfs receive "$backup_dataset"; then
                            log_success "Sent snapshot to backup pool: $backup_dataset"
                        else
                            log_warning "Failed to send snapshot to backup pool"
                        fi
                    fi
                else
                    log_error "Failed to create snapshot: $snapshot_name"
                fi
            done
        else
            log_warning "Pool $pool not found, skipping"
        fi
    done
    
    # Backup system information
    log_info "Collecting system information..."
    {
        echo "=== System Information ==="
        uname -a
        echo
        echo "=== ZFS Version ==="
        zfs version
        echo
        echo "=== Pool Status ==="
        zpool status
        echo
        echo "=== Dataset List ==="
        zfs list
        echo
        echo "=== Network Configuration ==="
        ip addr show
        echo
        echo "=== Disk Information ==="
        lsblk
        echo
        echo "=== Memory Information ==="
        free -h
        echo
        echo "=== CPU Information ==="
        lscpu
    } > "$backup_path/system-info.txt"
    
    # Restart services
    log_info "Restarting services..."
    systemctl start ipfs-cluster || true
    
    log_success "Full backup completed"
}

# Create incremental backup
create_incremental_backup() {
    local backup_path="$1"
    
    log_info "Creating incremental backup..."
    
    # Find last full backup
    local last_backup
    last_backup=$(find "$BACKUP_DIR" -name "manifest.json" -exec grep -l '"type": "full"' {} \; | sort | tail -1)
    
    if [[ -z "$last_backup" ]]; then
        log_warning "No previous full backup found, creating full backup instead"
        create_full_backup "$backup_path"
        return
    fi
    
    local last_backup_dir
    last_backup_dir=$(dirname "$last_backup")
    local last_backup_date
    last_backup_date=$(jq -r '.date' "$last_backup")
    
    log_info "Last full backup: $last_backup_dir ($last_backup_date)"
    
    # Export current cluster state
    log_info "Exporting current cluster state..."
    if [[ -f /var/lib/ipfs-cluster/service.json ]]; then
        ipfs-cluster-service state export "$backup_path/cluster-state.json" --config-dir /var/lib/ipfs-cluster || true
    fi
    
    # Create incremental ZFS snapshots
    log_info "Creating incremental ZFS snapshots..."
    local pools=("hot-tier" "warm-tier" "cold-tier")
    
    for pool in "${pools[@]}"; do
        if zpool list "$pool" > /dev/null 2>&1; then
            for dataset in $(zfs list -H -o name | grep "$pool/ipfs-cluster" || true); do
                local snapshot_name="${dataset}@incremental-$(date +%Y%m%d-%H%M%S)"
                local last_snapshot
                last_snapshot=$(zfs list -t snapshot -H -o name | grep "$dataset@backup-" | sort | tail -1 || true)
                
                if zfs snapshot "$snapshot_name"; then
                    log_success "Created incremental snapshot: $snapshot_name"
                    
                    # Send incremental if we have a base snapshot
                    if [[ -n "$last_snapshot" ]] && zpool list backup-pool > /dev/null 2>&1; then
                        local backup_dataset="backup-pool/${dataset##*/}-incremental-$(date +%Y%m%d)"
                        
                        if zfs send -i "$last_snapshot" "$snapshot_name" | zfs receive "$backup_dataset"; then
                            log_success "Sent incremental snapshot to backup pool"
                        else
                            log_warning "Failed to send incremental snapshot"
                        fi
                    fi
                fi
            done
        fi
    done
    
    log_success "Incremental backup completed"
}

# Create emergency backup
create_emergency_backup() {
    local backup_path="$1"
    
    log_info "Creating emergency backup (minimal, fast)..."
    
    # Export cluster state only
    if [[ -f /var/lib/ipfs-cluster/service.json ]]; then
        ipfs-cluster-service state export "$backup_path/cluster-state.json" --config-dir /var/lib/ipfs-cluster || true
    fi
    
    # Copy critical configuration
    cp -r /var/lib/ipfs-cluster "$backup_path/config" 2>/dev/null || true
    
    # Create emergency snapshots (no send/receive)
    local pools=("hot-tier" "warm-tier" "cold-tier")
    
    for pool in "${pools[@]}"; do
        if zpool list "$pool" > /dev/null 2>&1; then
            for dataset in $(zfs list -H -o name | grep "$pool/ipfs-cluster" | head -3 || true); do  # Limit to first 3 datasets
                local snapshot_name="${dataset}@emergency-$(date +%Y%m%d-%H%M%S)"
                zfs snapshot "$snapshot_name" || true
            done
        fi
    done
    
    log_success "Emergency backup completed"
}

# Create backup manifest
create_backup_manifest() {
    local backup_path="$1"
    local backup_type="$2"
    
    local manifest
    manifest=$(jq -n \
        --arg type "$backup_type" \
        --arg date "$(date -Iseconds)" \
        --arg path "$backup_path" \
        --arg hostname "$(hostname)" \
        '{
            type: $type,
            date: $date,
            path: $path,
            hostname: $hostname,
            files: [],
            snapshots: []
        }')
    
    # List backup files
    if [[ -d "$backup_path" ]]; then
        local files
        files=$(find "$backup_path" -type f -printf '%P\n' | jq -R . | jq -s .)
        manifest=$(echo "$manifest" | jq --argjson files "$files" '.files = $files')
    fi
    
    # List snapshots created
    local snapshots
    snapshots=$(zfs list -t snapshot -H -o name | grep "@.*-$(date +%Y%m%d)" | jq -R . | jq -s . || echo "[]")
    manifest=$(echo "$manifest" | jq --argjson snapshots "$snapshots" '.snapshots = $snapshots')
    
    echo "$manifest" > "$backup_path/manifest.json"
}

# Restore from backup
restore_backup() {
    local backup_path="$1"
    local restore_type="${2:-full}"
    
    if [[ ! -d "$backup_path" ]]; then
        log_error "Backup path not found: $backup_path"
        exit 1
    fi
    
    if [[ ! -f "$backup_path/manifest.json" ]]; then
        log_error "Backup manifest not found: $backup_path/manifest.json"
        exit 1
    fi
    
    log_info "Starting restore from: $backup_path"
    
    # Confirm restore operation
    echo -e "${YELLOW}WARNING: This will overwrite current system state!${NC}"
    read -p "Are you sure you want to continue? (yes/no): " confirm
    
    if [[ "$confirm" != "yes" ]]; then
        log_info "Restore cancelled by user"
        exit 0
    fi
    
    case $restore_type in
        "full")
            restore_full_backup "$backup_path"
            ;;
        "config-only")
            restore_config_only "$backup_path"
            ;;
        "state-only")
            restore_state_only "$backup_path"
            ;;
        *)
            log_error "Unknown restore type: $restore_type"
            exit 1
            ;;
    esac
    
    log_success "Restore completed from: $backup_path"
}

# Restore full backup
restore_full_backup() {
    local backup_path="$1"
    
    log_info "Performing full system restore..."
    
    # Stop services
    log_info "Stopping services..."
    systemctl stop ipfs-cluster || true
    systemctl stop ipfs || true
    
    # Restore configuration
    if [[ -d "$backup_path/config" ]]; then
        log_info "Restoring configuration..."
        rm -rf /var/lib/ipfs-cluster
        cp -r "$backup_path/config" /var/lib/ipfs-cluster
        chown -R ipfs-cluster:ipfs-cluster /var/lib/ipfs-cluster 2>/dev/null || true
    fi
    
    # Restore ZFS datasets
    log_info "Restoring ZFS datasets..."
    
    # Read snapshots from manifest
    local snapshots
    snapshots=$(jq -r '.snapshots[]?' "$backup_path/manifest.json" 2>/dev/null || true)
    
    if [[ -n "$snapshots" ]]; then
        while IFS= read -r snapshot; do
            if [[ -n "$snapshot" ]]; then
                log_info "Rolling back to snapshot: $snapshot"
                
                # Extract dataset name
                local dataset
                dataset=$(echo "$snapshot" | cut -d'@' -f1)
                
                if zfs list "$dataset" > /dev/null 2>&1; then
                    if zfs rollback -r "$snapshot"; then
                        log_success "Rolled back dataset: $dataset"
                    else
                        log_error "Failed to rollback dataset: $dataset"
                    fi
                else
                    log_warning "Dataset not found: $dataset"
                fi
            fi
        done <<< "$snapshots"
    fi
    
    # Import cluster state
    if [[ -f "$backup_path/cluster-state.json" ]]; then
        log_info "Importing cluster state..."
        
        # Clean existing state
        ipfs-cluster-service state clean --config-dir /var/lib/ipfs-cluster || true
        
        # Import backup state
        if ipfs-cluster-service state import "$backup_path/cluster-state.json" --config-dir /var/lib/ipfs-cluster; then
            log_success "Cluster state imported successfully"
        else
            log_error "Failed to import cluster state"
        fi
    fi
    
    # Start services
    log_info "Starting services..."
    systemctl start ipfs || true
    sleep 10
    systemctl start ipfs-cluster || true
    
    # Verify restore
    sleep 30
    if ipfs-cluster-ctl status > /dev/null 2>&1; then
        log_success "Cluster is responding after restore"
    else
        log_warning "Cluster may need manual intervention"
    fi
}

# Restore configuration only
restore_config_only() {
    local backup_path="$1"
    
    log_info "Restoring configuration only..."
    
    if [[ -d "$backup_path/config" ]]; then
        systemctl stop ipfs-cluster || true
        
        # Backup current config
        cp -r /var/lib/ipfs-cluster "/var/lib/ipfs-cluster.backup.$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true
        
        # Restore config
        rm -rf /var/lib/ipfs-cluster
        cp -r "$backup_path/config" /var/lib/ipfs-cluster
        chown -R ipfs-cluster:ipfs-cluster /var/lib/ipfs-cluster 2>/dev/null || true
        
        systemctl start ipfs-cluster || true
        
        log_success "Configuration restored"
    else
        log_error "Configuration backup not found"
        exit 1
    fi
}

# Restore state only
restore_state_only() {
    local backup_path="$1"
    
    log_info "Restoring cluster state only..."
    
    if [[ -f "$backup_path/cluster-state.json" ]]; then
        systemctl stop ipfs-cluster || true
        
        # Clean and import state
        ipfs-cluster-service state clean --config-dir /var/lib/ipfs-cluster || true
        ipfs-cluster-service state import "$backup_path/cluster-state.json" --config-dir /var/lib/ipfs-cluster
        
        systemctl start ipfs-cluster || true
        
        log_success "Cluster state restored"
    else
        log_error "Cluster state backup not found"
        exit 1
    fi
}

# Emergency shutdown
emergency_shutdown() {
    log_info "EMERGENCY SHUTDOWN INITIATED"
    
    # Stop services with timeout
    log_info "Stopping IPFS Cluster..."
    timeout 60 systemctl stop ipfs-cluster || systemctl kill ipfs-cluster
    
    log_info "Stopping IPFS..."
    timeout 30 systemctl stop ipfs || systemctl kill ipfs
    
    # Export pools safely
    log_info "Exporting ZFS pools..."
    local pools=("hot-tier" "warm-tier" "cold-tier")
    
    for pool in "${pools[@]}"; do
        if zpool list "$pool" > /dev/null 2>&1; then
            if zpool export "$pool"; then
                log_success "Exported pool: $pool"
            else
                log_error "Failed to export pool: $pool"
            fi
        fi
    done
    
    log_info "Emergency shutdown completed"
}

# List available backups
list_backups() {
    log_info "Available backups:"
    
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log_info "No backup directory found: $BACKUP_DIR"
        return
    fi
    
    local backups
    backups=$(find "$BACKUP_DIR" -name "manifest.json" | sort)
    
    if [[ -z "$backups" ]]; then
        log_info "No backups found"
        return
    fi
    
    printf "%-20s %-12s %-20s %-15s\n" "DATE" "TYPE" "PATH" "SIZE"
    printf "%-20s %-12s %-20s %-15s\n" "----" "----" "----" "----"
    
    while IFS= read -r manifest; do
        local backup_dir
        backup_dir=$(dirname "$manifest")
        local backup_date
        backup_date=$(jq -r '.date' "$manifest" | cut -d'T' -f1)
        local backup_type
        backup_type=$(jq -r '.type' "$manifest")
        local backup_size
        backup_size=$(du -sh "$backup_dir" | cut -f1)
        
        printf "%-20s %-12s %-20s %-15s\n" "$backup_date" "$backup_type" "$(basename "$backup_dir")" "$backup_size"
    done <<< "$backups"
}

# Cleanup old backups
cleanup_backups() {
    log_info "Cleaning up backups older than $RETENTION_DAYS days..."
    
    if [[ ! -d "$BACKUP_DIR" ]]; then
        log_info "No backup directory found"
        return
    fi
    
    local deleted_count=0
    
    while IFS= read -r backup_dir; do
        if [[ -n "$backup_dir" ]]; then
            log_info "Removing old backup: $(basename "$backup_dir")"
            rm -rf "$backup_dir"
            ((deleted_count++))
        fi
    done < <(find "$BACKUP_DIR" -maxdepth 1 -type d -mtime +$RETENTION_DAYS)
    
    log_success "Cleaned up $deleted_count old backups"
}

# Usage
usage() {
    cat << EOF
Usage: $0 COMMAND [OPTIONS]

Disaster recovery operations for IPFS Cluster ZFS Integration

COMMANDS:
    backup TYPE             Create backup (full|incremental|emergency)
    restore PATH [TYPE]     Restore from backup (full|config-only|state-only)
    list                    List available backups
    cleanup                 Remove old backups
    emergency-shutdown      Emergency system shutdown
    
OPTIONS:
    --backup-dir DIR        Set backup directory (default: $BACKUP_DIR)
    --retention-days N      Set backup retention days (default: $RETENTION_DAYS)
    -h, --help              Show this help message

EXAMPLES:
    $0 backup full                          # Create full backup
    $0 backup incremental                   # Create incremental backup
    $0 restore /backup/ipfs-cluster/20231201-120000 full
    $0 list                                 # List all backups
    $0 cleanup                              # Remove old backups
    
EOF
}

# Parse arguments
COMMAND=""

while [[ $# -gt 0 ]]; do
    case $1 in
        backup|restore|list|cleanup|emergency-shutdown)
            COMMAND="$1"
            shift
            break
            ;;
        --backup-dir)
            BACKUP_DIR="$2"
            shift 2
            ;;
        --retention-days)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Execute command
case $COMMAND in
    "backup")
        if [[ $# -lt 1 ]]; then
            echo "Backup type required"
            usage
            exit 1
        fi
        mkdir -p "$LOG_DIR"
        create_backup "$1"
        ;;
    "restore")
        if [[ $# -lt 1 ]]; then
            echo "Backup path required"
            usage
            exit 1
        fi
        mkdir -p "$LOG_DIR"
        restore_backup "$1" "${2:-full}"
        ;;
    "list")
        list_backups
        ;;
    "cleanup")
        mkdir -p "$LOG_DIR"
        cleanup_backups
        ;;
    "emergency-shutdown")
        mkdir -p "$LOG_DIR"
        emergency_shutdown
        ;;
    "")
        echo "Command required"
        usage
        exit 1
        ;;
    *)
        echo "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac