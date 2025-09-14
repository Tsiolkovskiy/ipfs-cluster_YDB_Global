#!/bin/bash

# IPFS Cluster ZFS Integration Health Check Script
# Performs comprehensive health checks and generates reports

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/var/log/ipfs-cluster"
REPORT_FILE="$LOG_DIR/health-check-$(date +%Y%m%d-%H%M%S).json"
VERBOSE=false
ALERT_MODE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
    [[ "$VERBOSE" == true ]] && echo "$(date '+%Y-%m-%d %H:%M:%S') INFO: $1" >> "$LOG_DIR/health-check.log"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    [[ "$VERBOSE" == true ]] && echo "$(date '+%Y-%m-%d %H:%M:%S') SUCCESS: $1" >> "$LOG_DIR/health-check.log"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') WARNING: $1" >> "$LOG_DIR/health-check.log"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ERROR: $1" >> "$LOG_DIR/health-check.log"
}

# Initialize report structure
init_report() {
    mkdir -p "$LOG_DIR"
    cat > "$REPORT_FILE" << 'EOF'
{
  "timestamp": "",
  "overall_status": "unknown",
  "checks": {
    "cluster": {},
    "zfs": {},
    "system": {},
    "performance": {}
  },
  "alerts": [],
  "recommendations": []
}
EOF
    
    # Update timestamp
    jq --arg ts "$(date -Iseconds)" '.timestamp = $ts' "$REPORT_FILE" > "$REPORT_FILE.tmp" && mv "$REPORT_FILE.tmp" "$REPORT_FILE"
}

# Update report with check results
update_report() {
    local section="$1"
    local key="$2"
    local value="$3"
    
    jq --arg sec "$section" --arg k "$key" --argjson v "$value" '.checks[$sec][$k] = $v' "$REPORT_FILE" > "$REPORT_FILE.tmp" && mv "$REPORT_FILE.tmp" "$REPORT_FILE"
}

# Add alert to report
add_alert() {
    local severity="$1"
    local message="$2"
    local component="$3"
    
    local alert=$(jq -n --arg sev "$severity" --arg msg "$message" --arg comp "$component" '{
        severity: $sev,
        message: $msg,
        component: $comp,
        timestamp: now | strftime("%Y-%m-%dT%H:%M:%SZ")
    }')
    
    jq --argjson alert "$alert" '.alerts += [$alert]' "$REPORT_FILE" > "$REPORT_FILE.tmp" && mv "$REPORT_FILE.tmp" "$REPORT_FILE"
}

# Check IPFS Cluster health
check_cluster_health() {
    log_info "Checking IPFS Cluster health..."
    
    local cluster_status="healthy"
    local cluster_data="{}"
    
    # Check if cluster service is running
    if systemctl is-active --quiet ipfs-cluster; then
        log_success "IPFS Cluster service is running"
        cluster_data=$(echo "$cluster_data" | jq '.service_status = "running"')
    else
        log_error "IPFS Cluster service is not running"
        cluster_status="unhealthy"
        cluster_data=$(echo "$cluster_data" | jq '.service_status = "stopped"')
        add_alert "critical" "IPFS Cluster service is not running" "cluster"
    fi
    
    # Check API endpoint
    if curl -s --max-time 10 http://localhost:9094/api/v0/id > /dev/null 2>&1; then
        log_success "Cluster API is responding"
        cluster_data=$(echo "$cluster_data" | jq '.api_status = "responding"')
        
        # Get cluster ID and peer count
        local cluster_info
        cluster_info=$(curl -s http://localhost:9094/api/v0/id)
        local cluster_id
        cluster_id=$(echo "$cluster_info" | jq -r '.id')
        cluster_data=$(echo "$cluster_data" | jq --arg id "$cluster_id" '.cluster_id = $id')
        
        # Check peers
        local peer_count
        if peer_count=$(ipfs-cluster-ctl peers ls 2>/dev/null | wc -l); then
            cluster_data=$(echo "$cluster_data" | jq --argjson count "$peer_count" '.peer_count = $count')
            log_success "Cluster has $peer_count peers"
            
            if [[ $peer_count -eq 0 ]]; then
                add_alert "warning" "No cluster peers found" "cluster"
            fi
        fi
        
        # Check pin status
        local pin_stats
        if pin_stats=$(ipfs-cluster-ctl status 2>/dev/null | grep -E "PINNED|ERROR|UNPINNED" | sort | uniq -c); then
            local total_pins=0
            local error_pins=0
            
            while read -r count status; do
                total_pins=$((total_pins + count))
                if [[ "$status" == "ERROR" ]]; then
                    error_pins=$count
                fi
            done <<< "$pin_stats"
            
            cluster_data=$(echo "$cluster_data" | jq --argjson total "$total_pins" --argjson errors "$error_pins" '.pins = {total: $total, errors: $errors}')
            
            if [[ $error_pins -gt 0 ]]; then
                local error_rate
                error_rate=$(echo "scale=2; $error_pins * 100 / $total_pins" | bc)
                add_alert "warning" "Pin error rate: ${error_rate}%" "cluster"
            fi
        fi
        
    else
        log_error "Cluster API is not responding"
        cluster_status="unhealthy"
        cluster_data=$(echo "$cluster_data" | jq '.api_status = "not_responding"')
        add_alert "critical" "Cluster API is not responding" "cluster"
    fi
    
    cluster_data=$(echo "$cluster_data" | jq --arg status "$cluster_status" '.overall_status = $status')
    update_report "cluster" "health" "$cluster_data"
}

# Check ZFS health
check_zfs_health() {
    log_info "Checking ZFS health..."
    
    local zfs_status="healthy"
    local zfs_data="{}"
    
    # Check ZFS pools
    local pools=("hot-tier" "warm-tier" "cold-tier")
    local pool_data="[]"
    
    for pool in "${pools[@]}"; do
        if zpool list "$pool" > /dev/null 2>&1; then
            local health
            health=$(zpool list -H -o health "$pool")
            local capacity
            capacity=$(zpool list -H -o capacity "$pool" | tr -d '%')
            local size
            size=$(zpool list -H -o size "$pool")
            local free
            free=$(zpool list -H -o free "$pool")
            
            local pool_info
            pool_info=$(jq -n --arg name "$pool" --arg health "$health" --argjson cap "$capacity" --arg size "$size" --arg free "$free" '{
                name: $name,
                health: $health,
                capacity_percent: $cap,
                size: $size,
                free: $free
            }')
            
            if [[ "$health" != "ONLINE" ]]; then
                log_error "Pool $pool health: $health"
                zfs_status="unhealthy"
                add_alert "critical" "ZFS pool $pool is $health" "zfs"
            else
                log_success "Pool $pool: $health (${capacity}% used)"
            fi
            
            if [[ $capacity -gt 90 ]]; then
                add_alert "critical" "Pool $pool is ${capacity}% full" "zfs"
                zfs_status="degraded"
            elif [[ $capacity -gt 80 ]]; then
                add_alert "warning" "Pool $pool is ${capacity}% full" "zfs"
            fi
            
            # Check fragmentation
            local frag
            frag=$(zpool list -H -o frag "$pool" | tr -d '%')
            pool_info=$(echo "$pool_info" | jq --argjson frag "$frag" '.fragmentation_percent = $frag')
            
            if [[ $frag -gt 30 ]]; then
                add_alert "warning" "Pool $pool fragmentation is ${frag}%" "zfs"
            fi
            
            pool_data=$(echo "$pool_data" | jq --argjson pool "$pool_info" '. += [$pool]')
            
        else
            log_error "Pool $pool not found"
            zfs_status="unhealthy"
            add_alert "critical" "ZFS pool $pool not found" "zfs"
        fi
    done
    
    zfs_data=$(echo "$zfs_data" | jq --argjson pools "$pool_data" '.pools = $pools')
    
    # Check ARC statistics
    if [[ -f /proc/spl/kstat/zfs/arcstats ]]; then
        local arc_hits
        arc_hits=$(awk '/^hits/ {print $3}' /proc/spl/kstat/zfs/arcstats)
        local arc_misses
        arc_misses=$(awk '/^misses/ {print $3}' /proc/spl/kstat/zfs/arcstats)
        local arc_size
        arc_size=$(awk '/^size/ {print $3}' /proc/spl/kstat/zfs/arcstats)
        local arc_c_max
        arc_c_max=$(awk '/^c_max/ {print $3}' /proc/spl/kstat/zfs/arcstats)
        
        if [[ $arc_hits -gt 0 ]] && [[ $arc_misses -gt 0 ]]; then
            local hit_ratio
            hit_ratio=$(echo "scale=2; $arc_hits * 100 / ($arc_hits + $arc_misses)" | bc)
            
            local arc_stats
            arc_stats=$(jq -n --argjson hits "$arc_hits" --argjson misses "$arc_misses" --argjson size "$arc_size" --argjson max "$arc_c_max" --argjson ratio "$hit_ratio" '{
                hits: $hits,
                misses: $misses,
                size_bytes: $size,
                max_size_bytes: $max,
                hit_ratio_percent: $ratio
            }')
            
            zfs_data=$(echo "$zfs_data" | jq --argjson arc "$arc_stats" '.arc = $arc')
            
            if (( $(echo "$hit_ratio < 85" | bc -l) )); then
                add_alert "warning" "ARC hit ratio is ${hit_ratio}%" "zfs"
                log_warning "ARC hit ratio: ${hit_ratio}%"
            else
                log_success "ARC hit ratio: ${hit_ratio}%"
            fi
        fi
    fi
    
    zfs_data=$(echo "$zfs_data" | jq --arg status "$zfs_status" '.overall_status = $status')
    update_report "zfs" "health" "$zfs_data"
}

# Check system health
check_system_health() {
    log_info "Checking system health..."
    
    local system_status="healthy"
    local system_data="{}"
    
    # Check disk usage
    local disk_usage
    disk_usage=$(df -h / | awk 'NR==2 {print $5}' | tr -d '%')
    system_data=$(echo "$system_data" | jq --argjson usage "$disk_usage" '.disk_usage_percent = $usage')
    
    if [[ $disk_usage -gt 90 ]]; then
        add_alert "critical" "Root filesystem is ${disk_usage}% full" "system"
        system_status="critical"
    elif [[ $disk_usage -gt 80 ]]; then
        add_alert "warning" "Root filesystem is ${disk_usage}% full" "system"
    fi
    
    # Check memory usage
    local mem_info
    mem_info=$(free -m | awk 'NR==2{printf "%.0f %.0f %.0f", $3*100/$2, $2, $3}')
    read -r mem_percent mem_total mem_used <<< "$mem_info"
    
    system_data=$(echo "$system_data" | jq --argjson percent "$mem_percent" --argjson total "$mem_total" --argjson used "$mem_used" '.memory = {usage_percent: $percent, total_mb: $total, used_mb: $used}')
    
    if [[ $mem_percent -gt 90 ]]; then
        add_alert "warning" "Memory usage is ${mem_percent}%" "system"
    fi
    
    # Check CPU load
    local load_avg
    load_avg=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | tr -d ',')
    local cpu_count
    cpu_count=$(nproc)
    local load_percent
    load_percent=$(echo "scale=0; $load_avg * 100 / $cpu_count" | bc)
    
    system_data=$(echo "$system_data" | jq --argjson load "$load_avg" --argjson percent "$load_percent" --argjson cpus "$cpu_count" '.cpu = {load_average: $load, load_percent: $percent, cpu_count: $cpus}')
    
    if [[ $load_percent -gt 80 ]]; then
        add_alert "warning" "CPU load is ${load_percent}%" "system"
    fi
    
    # Check network connectivity
    if ping -c 1 8.8.8.8 > /dev/null 2>&1; then
        system_data=$(echo "$system_data" | jq '.network_connectivity = true')
        log_success "Network connectivity OK"
    else
        system_data=$(echo "$system_data" | jq '.network_connectivity = false')
        add_alert "warning" "Network connectivity issues detected" "system"
        log_warning "Network connectivity issues"
    fi
    
    system_data=$(echo "$system_data" | jq --arg status "$system_status" '.overall_status = $status')
    update_report "system" "health" "$system_data"
}

# Check performance metrics
check_performance() {
    log_info "Checking performance metrics..."
    
    local perf_data="{}"
    
    # I/O statistics
    if command -v iostat > /dev/null 2>&1; then
        local io_stats
        io_stats=$(iostat -x 1 2 | tail -n +4 | awk 'NF > 0 && !/^avg-cpu/ && !/^Device/ {
            if ($1 != "") {
                printf "%s:%.2f:%.2f:%.2f ", $1, $4, $5, $10
            }
        }')
        
        local io_data="[]"
        for stat in $io_stats; do
            IFS=':' read -r device reads writes util <<< "$stat"
            local device_info
            device_info=$(jq -n --arg dev "$device" --argjson r "$reads" --argjson w "$writes" --argjson u "$util" '{
                device: $dev,
                reads_per_sec: $r,
                writes_per_sec: $w,
                utilization_percent: $u
            }')
            io_data=$(echo "$io_data" | jq --argjson dev "$device_info" '. += [$dev]')
        done
        
        perf_data=$(echo "$perf_data" | jq --argjson io "$io_data" '.io_stats = $io')
    fi
    
    # Network statistics
    local rx_bytes
    rx_bytes=$(cat /sys/class/net/*/statistics/rx_bytes | awk '{sum+=$1} END {print sum}')
    local tx_bytes
    tx_bytes=$(cat /sys/class/net/*/statistics/tx_bytes | awk '{sum+=$1} END {print sum}')
    
    perf_data=$(echo "$perf_data" | jq --argjson rx "$rx_bytes" --argjson tx "$tx_bytes" '.network = {rx_bytes_total: $rx, tx_bytes_total: $tx}')
    
    update_report "performance" "metrics" "$perf_data"
}

# Generate recommendations
generate_recommendations() {
    log_info "Generating recommendations..."
    
    local recommendations="[]"
    
    # Analyze alerts and generate recommendations
    local critical_alerts
    critical_alerts=$(jq -r '.alerts[] | select(.severity == "critical") | .message' "$REPORT_FILE")
    
    if [[ -n "$critical_alerts" ]]; then
        recommendations=$(echo "$recommendations" | jq '. += ["Immediate attention required: Critical alerts detected"]')
    fi
    
    # Check ZFS optimization opportunities
    local pools
    pools=$(jq -r '.checks.zfs.health.pools[]? | select(.fragmentation_percent > 20) | .name' "$REPORT_FILE" 2>/dev/null || true)
    
    if [[ -n "$pools" ]]; then
        recommendations=$(echo "$recommendations" | jq '. += ["Consider running ZFS scrub on fragmented pools"]')
    fi
    
    # Check ARC hit ratio
    local arc_ratio
    arc_ratio=$(jq -r '.checks.zfs.health.arc.hit_ratio_percent // 0' "$REPORT_FILE")
    
    if (( $(echo "$arc_ratio < 85" | bc -l) )); then
        recommendations=$(echo "$recommendations" | jq '. += ["Consider tuning ZFS ARC parameters to improve cache hit ratio"]')
    fi
    
    # Update report with recommendations
    jq --argjson recs "$recommendations" '.recommendations = $recs' "$REPORT_FILE" > "$REPORT_FILE.tmp" && mv "$REPORT_FILE.tmp" "$REPORT_FILE"
}

# Determine overall status
determine_overall_status() {
    local critical_count
    critical_count=$(jq -r '.alerts[] | select(.severity == "critical") | length' "$REPORT_FILE" | wc -l)
    local warning_count
    warning_count=$(jq -r '.alerts[] | select(.severity == "warning") | length' "$REPORT_FILE" | wc -l)
    
    local overall_status="healthy"
    
    if [[ $critical_count -gt 0 ]]; then
        overall_status="critical"
    elif [[ $warning_count -gt 0 ]]; then
        overall_status="warning"
    fi
    
    jq --arg status "$overall_status" '.overall_status = $status' "$REPORT_FILE" > "$REPORT_FILE.tmp" && mv "$REPORT_FILE.tmp" "$REPORT_FILE"
    
    case $overall_status in
        "healthy")
            log_success "Overall system status: HEALTHY"
            ;;
        "warning")
            log_warning "Overall system status: WARNING ($warning_count warnings)"
            ;;
        "critical")
            log_error "Overall system status: CRITICAL ($critical_count critical alerts)"
            ;;
    esac
}

# Print summary
print_summary() {
    echo
    echo "=== Health Check Summary ==="
    
    local overall_status
    overall_status=$(jq -r '.overall_status' "$REPORT_FILE")
    echo "Overall Status: $overall_status"
    
    local alert_count
    alert_count=$(jq -r '.alerts | length' "$REPORT_FILE")
    echo "Total Alerts: $alert_count"
    
    if [[ $alert_count -gt 0 ]]; then
        echo
        echo "Alerts:"
        jq -r '.alerts[] | "  [\(.severity | ascii_upcase)] \(.message)"' "$REPORT_FILE"
    fi
    
    local rec_count
    rec_count=$(jq -r '.recommendations | length' "$REPORT_FILE")
    
    if [[ $rec_count -gt 0 ]]; then
        echo
        echo "Recommendations:"
        jq -r '.recommendations[] | "  - \(.)"' "$REPORT_FILE"
    fi
    
    echo
    echo "Detailed report: $REPORT_FILE"
}

# Usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Perform comprehensive health check of IPFS Cluster ZFS Integration

OPTIONS:
    -v, --verbose       Enable verbose logging
    -a, --alert         Alert mode (exit with non-zero code if issues found)
    -o, --output FILE   Output report to specific file
    -h, --help          Show this help message

EXAMPLES:
    $0                  # Basic health check
    $0 -v               # Verbose health check
    $0 -a               # Alert mode for monitoring systems
    
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -a|--alert)
            ALERT_MODE=true
            shift
            ;;
        -o|--output)
            REPORT_FILE="$2"
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

# Main execution
main() {
    log_info "Starting IPFS Cluster ZFS Integration health check"
    
    init_report
    check_cluster_health
    check_zfs_health
    check_system_health
    check_performance
    generate_recommendations
    determine_overall_status
    print_summary
    
    # Exit with appropriate code for alert mode
    if [[ "$ALERT_MODE" == true ]]; then
        local overall_status
        overall_status=$(jq -r '.overall_status' "$REPORT_FILE")
        case $overall_status in
            "healthy") exit 0 ;;
            "warning") exit 1 ;;
            "critical") exit 2 ;;
        esac
    fi
}

# Check dependencies
if ! command -v jq > /dev/null 2>&1; then
    log_error "jq is required but not installed"
    exit 1
fi

if ! command -v bc > /dev/null 2>&1; then
    log_error "bc is required but not installed"
    exit 1
fi

# Run main function
main