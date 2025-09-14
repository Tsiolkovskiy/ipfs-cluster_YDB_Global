#!/bin/bash

# IPFS Cluster ZFS Integration Training Environment Setup
# This script sets up a complete training environment for students and instructors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRAINING_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$(dirname "$TRAINING_DIR")")"

# Configuration
ROLE=""
STUDENT_COUNT=1
ENVIRONMENT_TYPE="local"
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

Set up IPFS Cluster ZFS Integration training environment

OPTIONS:
    --role ROLE             Role: student, instructor, or admin
    --type TYPE             Environment type: local, docker, or cloud (default: local)
    --students N            Number of student environments to create (instructor only)
    --verbose               Enable verbose output
    -h, --help              Show this help message

EXAMPLES:
    $0 --role student                    # Setup student environment
    $0 --role instructor --students 20   # Setup instructor environment for 20 students
    $0 --role admin --type cloud         # Setup cloud-based admin environment
    
EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --role)
                ROLE="$2"
                shift 2
                ;;
            --type)
                ENVIRONMENT_TYPE="$2"
                shift 2
                ;;
            --students)
                STUDENT_COUNT="$2"
                shift 2
                ;;
            --verbose)
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
    
    if [[ -z "$ROLE" ]]; then
        log_error "Role is required (--role student|instructor|admin)"
        usage
        exit 1
    fi
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check OS
    if [[ ! -f /etc/os-release ]]; then
        log_error "Unsupported operating system"
        exit 1
    fi
    
    source /etc/os-release
    if [[ "$ID" != "ubuntu" ]] && [[ "$ID" != "debian" ]]; then
        log_warning "This script is optimized for Ubuntu/Debian. Other distributions may require manual adjustments."
    fi
    
    # Check required commands
    local required_commands=("curl" "wget" "git" "make" "gcc")
    
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" &> /dev/null; then
            log_error "Required command not found: $cmd"
            exit 1
        fi
    done
    
    # Check if running as root for system setup
    if [[ $EUID -ne 0 ]] && [[ "$ENVIRONMENT_TYPE" == "local" ]]; then
        log_error "This script must be run as root for local environment setup"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Install system dependencies
install_dependencies() {
    log_info "Installing system dependencies..."
    
    # Update package list
    apt update
    
    # Install ZFS
    apt install -y zfsutils-linux zfs-dkms
    
    # Install development tools
    apt install -y build-essential git curl wget
    
    # Install monitoring tools
    apt install -y htop iotop nethogs jq bc
    
    # Install Docker (for containerized environments)
    if [[ "$ENVIRONMENT_TYPE" == "docker" ]]; then
        apt install -y docker.io docker-compose
        systemctl enable docker
        systemctl start docker
    fi
    
    log_success "Dependencies installed"
}

# Install Go
install_go() {
    log_info "Installing Go..."
    
    local go_version="1.19.5"
    local go_archive="go${go_version}.linux-amd64.tar.gz"
    
    # Download Go
    wget -O "/tmp/$go_archive" "https://golang.org/dl/$go_archive"
    
    # Install Go
    tar -C /usr/local -xzf "/tmp/$go_archive"
    
    # Add Go to PATH
    echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/environment
    
    # Create Go workspace
    mkdir -p /opt/go-workspace
    echo 'export GOPATH=/opt/go-workspace' >> /etc/environment
    
    log_success "Go installed"
}

# Install IPFS
install_ipfs() {
    log_info "Installing IPFS..."
    
    local ipfs_version="0.17.0"
    local ipfs_archive="kubo_v${ipfs_version}_linux-amd64.tar.gz"
    
    # Download IPFS
    wget -O "/tmp/$ipfs_archive" "https://github.com/ipfs/kubo/releases/download/v${ipfs_version}/$ipfs_archive"
    
    # Extract and install
    tar -C /tmp -xzf "/tmp/$ipfs_archive"
    cp /tmp/kubo/ipfs /usr/local/bin/
    chmod +x /usr/local/bin/ipfs
    
    log_success "IPFS installed"
}

# Build IPFS Cluster
build_ipfs_cluster() {
    log_info "Building IPFS Cluster..."
    
    # Set Go environment
    export PATH=$PATH:/usr/local/go/bin
    export GOPATH=/opt/go-workspace
    
    # Clone repository
    if [[ ! -d "/opt/ipfs-cluster" ]]; then
        git clone https://github.com/ipfs/ipfs-cluster.git /opt/ipfs-cluster
    fi
    
    cd /opt/ipfs-cluster
    
    # Build binaries
    make build
    
    # Install binaries
    cp ipfs-cluster-service /usr/local/bin/
    cp ipfs-cluster-ctl /usr/local/bin/
    cp ipfs-cluster-follow /usr/local/bin/
    chmod +x /usr/local/bin/ipfs-cluster-*
    
    log_success "IPFS Cluster built and installed"
}

# Setup student environment
setup_student_environment() {
    log_info "Setting up student environment..."
    
    local student_user="student"
    local student_home="/home/$student_user"
    
    # Create student user
    if ! id "$student_user" &>/dev/null; then
        useradd -m -s /bin/bash "$student_user"
        echo "$student_user:training123" | chpasswd
        usermod -aG sudo "$student_user"
    fi
    
    # Create training directories
    mkdir -p "$student_home/training"
    mkdir -p "$student_home/training/labs"
    mkdir -p "$student_home/training/exercises"
    mkdir -p "$student_home/training/scripts"
    
    # Copy training materials
    cp -r "$TRAINING_DIR"/* "$student_home/training/"
    
    # Create virtual disks for ZFS practice
    mkdir -p "$student_home/training/disks"
    
    for i in {1..3}; do
        truncate -s 10G "$student_home/training/disks/disk${i}.img"
    done
    
    # Create lab environment script
    cat > "$student_home/training/scripts/setup-lab.sh" << 'EOF'
#!/bin/bash

# Setup lab environment with loop devices
echo "Setting up lab environment..."

# Create loop devices
sudo losetup /dev/loop1 ~/training/disks/disk1.img 2>/dev/null || true
sudo losetup /dev/loop2 ~/training/disks/disk2.img 2>/dev/null || true
sudo losetup /dev/loop3 ~/training/disks/disk3.img 2>/dev/null || true

echo "Lab environment ready!"
echo "Use /dev/loop1, /dev/loop2, /dev/loop3 for ZFS pools"
EOF
    
    chmod +x "$student_home/training/scripts/setup-lab.sh"
    
    # Set ownership
    chown -R "$student_user:$student_user" "$student_home/training"
    
    # Create desktop shortcuts (if GUI available)
    if command -v gnome-session &>/dev/null; then
        mkdir -p "$student_home/Desktop"
        
        cat > "$student_home/Desktop/Training Materials.desktop" << EOF
[Desktop Entry]
Version=1.0
Type=Link
Name=Training Materials
Comment=IPFS Cluster ZFS Training Materials
URL=file://$student_home/training
Icon=folder
EOF
        
        chown "$student_user:$student_user" "$student_home/Desktop/Training Materials.desktop"
    fi
    
    log_success "Student environment created for user: $student_user"
}

# Setup instructor environment
setup_instructor_environment() {
    log_info "Setting up instructor environment for $STUDENT_COUNT students..."
    
    local instructor_user="instructor"
    local instructor_home="/home/$instructor_user"
    
    # Create instructor user
    if ! id "$instructor_user" &>/dev/null; then
        useradd -m -s /bin/bash "$instructor_user"
        echo "$instructor_user:instructor123" | chpasswd
        usermod -aG sudo "$instructor_user"
    fi
    
    # Create instructor directories
    mkdir -p "$instructor_home/training"
    mkdir -p "$instructor_home/training/instructor-materials"
    mkdir -p "$instructor_home/training/student-environments"
    mkdir -p "$instructor_home/training/assessments"
    
    # Copy all training materials
    cp -r "$TRAINING_DIR"/* "$instructor_home/training/"
    
    # Create instructor-specific materials
    cat > "$instructor_home/training/instructor-materials/class-management.sh" << 'EOF'
#!/bin/bash

# Class management script for instructors

case "$1" in
    "create-students")
        echo "Creating student environments..."
        for i in $(seq 1 $2); do
            student_user="student$(printf "%02d" $i)"
            if ! id "$student_user" &>/dev/null; then
                sudo useradd -m -s /bin/bash "$student_user"
                echo "$student_user:student123" | sudo chpasswd
                sudo usermod -aG sudo "$student_user"
                echo "Created user: $student_user"
            fi
        done
        ;;
    "reset-environments")
        echo "Resetting student environments..."
        for user in $(getent passwd | grep "student[0-9]" | cut -d: -f1); do
            sudo systemctl stop ipfs-cluster@$user 2>/dev/null || true
            sudo systemctl stop ipfs@$user 2>/dev/null || true
            sudo rm -rf /home/$user/.ipfs /home/$user/.ipfs-cluster
            echo "Reset environment for: $user"
        done
        ;;
    "collect-logs")
        echo "Collecting student logs..."
        mkdir -p ~/training/student-logs/$(date +%Y%m%d-%H%M%S)
        for user in $(getent passwd | grep "student[0-9]" | cut -d: -f1); do
            sudo journalctl -u ipfs@$user -u ipfs-cluster@$user --since "1 hour ago" > ~/training/student-logs/$(date +%Y%m%d-%H%M%S)/$user.log 2>/dev/null || true
        done
        ;;
    *)
        echo "Usage: $0 {create-students N|reset-environments|collect-logs}"
        ;;
esac
EOF
    
    chmod +x "$instructor_home/training/instructor-materials/class-management.sh"
    
    # Create assessment tools
    cat > "$instructor_home/training/assessments/grade-lab.sh" << 'EOF'
#!/bin/bash

# Automated lab grading script

student_user="$1"
lab_number="$2"

if [[ -z "$student_user" || -z "$lab_number" ]]; then
    echo "Usage: $0 <student_user> <lab_number>"
    exit 1
fi

echo "Grading Lab $lab_number for $student_user..."

case "$lab_number" in
    "1")
        # Grade Lab 1: Basic Setup
        score=0
        
        # Check ZFS pools (30 points)
        if sudo -u "$student_user" zpool list hot-tier &>/dev/null; then
            score=$((score + 10))
            echo "✓ Hot tier pool exists (10/10)"
        else
            echo "✗ Hot tier pool missing (0/10)"
        fi
        
        if sudo -u "$student_user" zpool list warm-tier &>/dev/null; then
            score=$((score + 10))
            echo "✓ Warm tier pool exists (10/10)"
        else
            echo "✗ Warm tier pool missing (0/10)"
        fi
        
        if sudo -u "$student_user" zpool list cold-tier &>/dev/null; then
            score=$((score + 10))
            echo "✓ Cold tier pool exists (10/10)"
        else
            echo "✗ Cold tier pool missing (0/10)"
        fi
        
        # Check services (40 points)
        if sudo systemctl is-active ipfs@$student_user &>/dev/null; then
            score=$((score + 20))
            echo "✓ IPFS service running (20/20)"
        else
            echo "✗ IPFS service not running (0/20)"
        fi
        
        if sudo systemctl is-active ipfs-cluster@$student_user &>/dev/null; then
            score=$((score + 20))
            echo "✓ IPFS Cluster service running (20/20)"
        else
            echo "✗ IPFS Cluster service not running (0/20)"
        fi
        
        # Check functionality (30 points)
        if sudo -u "$student_user" ipfs-cluster-ctl status &>/dev/null; then
            score=$((score + 30))
            echo "✓ Cluster responding (30/30)"
        else
            echo "✗ Cluster not responding (0/30)"
        fi
        
        echo "Total Score: $score/100"
        ;;
    *)
        echo "Lab $lab_number grading not implemented yet"
        ;;
esac
EOF
    
    chmod +x "$instructor_home/training/assessments/grade-lab.sh"
    
    # Set ownership
    chown -R "$instructor_user:$instructor_user" "$instructor_home/training"
    
    log_success "Instructor environment created"
}

# Setup admin environment
setup_admin_environment() {
    log_info "Setting up admin environment..."
    
    # Create admin directories
    mkdir -p /opt/training-admin
    mkdir -p /opt/training-admin/monitoring
    mkdir -p /opt/training-admin/automation
    mkdir -p /opt/training-admin/backups
    
    # Create monitoring dashboard
    cat > /opt/training-admin/monitoring/dashboard.sh << 'EOF'
#!/bin/bash

# Training environment monitoring dashboard

echo "=== IPFS Cluster ZFS Training Environment Dashboard ==="
echo "Timestamp: $(date)"
echo

# System resources
echo "=== System Resources ==="
echo "CPU Usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
echo "Memory Usage: $(free | grep Mem | awk '{printf "%.1f%%", $3/$2 * 100.0}')"
echo "Disk Usage: $(df -h / | awk 'NR==2{printf "%s", $5}')"
echo

# Active users
echo "=== Active Training Users ==="
who | grep -E "(student|instructor)" | wc -l
echo

# ZFS pools status
echo "=== ZFS Pools Status ==="
zpool list 2>/dev/null | grep -E "(hot-tier|warm-tier|cold-tier)" | wc -l
echo

# Services status
echo "=== Services Status ==="
systemctl list-units --type=service --state=running | grep -E "(ipfs|cluster)" | wc -l
echo

# Recent errors
echo "=== Recent Errors ==="
journalctl --since "1 hour ago" -p err | grep -E "(ipfs|cluster)" | wc -l
EOF
    
    chmod +x /opt/training-admin/monitoring/dashboard.sh
    
    # Create backup script
    cat > /opt/training-admin/backups/backup-training.sh << 'EOF'
#!/bin/bash

# Backup training environments

backup_dir="/opt/training-admin/backups/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$backup_dir"

echo "Creating training environment backup..."

# Backup user home directories
for user in $(getent passwd | grep -E "(student|instructor)" | cut -d: -f1); do
    if [[ -d "/home/$user" ]]; then
        tar -czf "$backup_dir/$user-home.tar.gz" -C "/home/$user" . 2>/dev/null
        echo "Backed up: $user"
    fi
done

# Backup system configurations
cp -r /etc/systemd/system/*ipfs* "$backup_dir/" 2>/dev/null || true

# Backup ZFS configurations
zpool export -a 2>/dev/null || true
zpool import -a 2>/dev/null || true

echo "Backup completed: $backup_dir"
EOF
    
    chmod +x /opt/training-admin/backups/backup-training.sh
    
    # Create cleanup script
    cat > /opt/training-admin/automation/cleanup.sh << 'EOF'
#!/bin/bash

# Cleanup training environment

echo "Cleaning up training environment..."

# Stop all training services
systemctl stop ipfs-cluster@* 2>/dev/null || true
systemctl stop ipfs@* 2>/dev/null || true

# Clean ZFS pools
for pool in $(zpool list -H -o name | grep -E "(hot-tier|warm-tier|cold-tier)"); do
    zpool destroy "$pool" 2>/dev/null || true
done

# Clean loop devices
for loop in /dev/loop*; do
    losetup -d "$loop" 2>/dev/null || true
done

# Clean temporary files
rm -rf /tmp/ipfs-* /tmp/cluster-* 2>/dev/null || true

echo "Cleanup completed"
EOF
    
    chmod +x /opt/training-admin/automation/cleanup.sh
    
    log_success "Admin environment created"
}

# Create environment verification script
create_verification_script() {
    log_info "Creating environment verification script..."
    
    cat > /usr/local/bin/verify-training-env << 'EOF'
#!/bin/bash

# Training environment verification script

echo "=== Training Environment Verification ==="

# Check ZFS
echo "ZFS Status:"
if command -v zfs &>/dev/null; then
    echo "✓ ZFS installed"
    zfs version | head -1
else
    echo "✗ ZFS not installed"
fi

# Check Go
echo -e "\nGo Status:"
if command -v go &>/dev/null; then
    echo "✓ Go installed"
    go version
else
    echo "✗ Go not installed"
fi

# Check IPFS
echo -e "\nIPFS Status:"
if command -v ipfs &>/dev/null; then
    echo "✓ IPFS installed"
    ipfs version
else
    echo "✗ IPFS not installed"
fi

# Check IPFS Cluster
echo -e "\nIPFS Cluster Status:"
if command -v ipfs-cluster-service &>/dev/null; then
    echo "✓ IPFS Cluster installed"
    ipfs-cluster-service --version
else
    echo "✗ IPFS Cluster not installed"
fi

# Check training materials
echo -e "\nTraining Materials:"
if [[ -d "/home/student/training" ]]; then
    echo "✓ Student materials available"
elif [[ -d "/home/instructor/training" ]]; then
    echo "✓ Instructor materials available"
else
    echo "✗ Training materials not found"
fi

echo -e "\n=== Verification Complete ==="
EOF
    
    chmod +x /usr/local/bin/verify-training-env
    
    log_success "Verification script created"
}

# Main function
main() {
    parse_args "$@"
    
    log_info "Setting up IPFS Cluster ZFS Integration training environment"
    log_info "Role: $ROLE, Type: $ENVIRONMENT_TYPE"
    
    check_prerequisites
    install_dependencies
    install_go
    install_ipfs
    build_ipfs_cluster
    
    case $ROLE in
        "student")
            setup_student_environment
            ;;
        "instructor")
            setup_instructor_environment
            ;;
        "admin")
            setup_admin_environment
            ;;
        *)
            log_error "Unknown role: $ROLE"
            exit 1
            ;;
    esac
    
    create_verification_script
    
    log_success "Training environment setup completed!"
    
    # Print next steps
    case $ROLE in
        "student")
            cat << EOF

=== Next Steps for Students ===
1. Log in as 'student' user (password: training123)
2. Run: ~/training/scripts/setup-lab.sh
3. Start with: ~/training/module-1/01-introduction.md
4. Verify setup: verify-training-env

EOF
            ;;
        "instructor")
            cat << EOF

=== Next Steps for Instructors ===
1. Log in as 'instructor' user (password: instructor123)
2. Create student accounts: ~/training/instructor-materials/class-management.sh create-students 20
3. Review materials in: ~/training/
4. Access assessment tools: ~/training/assessments/

EOF
            ;;
        "admin")
            cat << EOF

=== Next Steps for Admins ===
1. Monitor environment: /opt/training-admin/monitoring/dashboard.sh
2. Create backups: /opt/training-admin/backups/backup-training.sh
3. Cleanup when done: /opt/training-admin/automation/cleanup.sh

EOF
            ;;
    esac
}

# Run main function
main "$@"