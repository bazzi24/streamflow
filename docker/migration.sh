#!/bin/bash

set -e

DEFAULT_BACKUP_FOLDER="backup"
VOLUMES=("docker_mysql_data" "docker_kafka_producer_data" "docker_kafka_consumer_data")
BACKUP_FILE=("mysql_backup.tar.gz" "kafka_producer.tar.gz" "kafka_consumer.tar.gz")

# Function display help information
show_help() {
    echo "StreamFlow Data Migration Tool"
    echo ""
    echo "USAGE:"
    echo "  $0 <operation> [backup_folder]"
    echo ""
    echo "OPERATION:"
    echo "  backup  - Create backup of all StreamFlow data volumes"
    echo "  restore - Restore StreamFlow data volumes from backup"
    echo "  help    - Show this help message"
    echo ""
    echo "PARAMETERS:"
    echo "  backup_folder   - Name of backup folder (default: '$DEFAULT_BACKUP_FOLDER')"
    echo ""
    echo "EXAMPLES:"
    echo "  $0 backup               # Backup to './backup' folder"
    echo "  $0 backup my_backup     # Backup to './my_backup' folder"
    echo "  $0 restore              # Restore from './backup' folder"
    echo "  $0 restore_my_backup    # Restore from './my_backup' folder"
    echo ""
    echo "DOCKER VOLUMES"
    echo "  - docker_mysql_data     (MySQL database)"
    echo "  - docker_kafka          (Apache Kafka)"
    echo "  - docker_kafka_ui       (Kafka UI)"
}

# Check docker running
check_docker() {
    if ! docker info >dev/null 2>&1; then
    echo "Error: Docker is not running or not accessible"
    echo "Please start Docker and ty again"
    exit 1
}

# Function check volumes exists
volume_exists(){
    local volume_name=$1
    docker volume inspect "$volume_name" >/dev/null 2>&1
}

# Function check container using volume
check_container_using_volume() {
    echo "Checking for running containers that might be using target volume..."

    local running_containers=$(docker ps --format "{{.Name}}")

    if [ -z "$running_containers" ]; then
        echo "No running containers found"
        return 0
    fi 

    local containers_using_volume=()
    local volume_usage_details=()

    for container in $running_containers; do
        local mounts=$(docker inspect "$container" --format '{{range .Mounts}}{{.Source}}{{"|"}}{{end}}' 2>/dev/null || echo "")
        for volume in "${VOLUMES[@]}"; do 
            if echo "$mounts" | grep -q "$volume"; then 
                containers_using_volume+=("$container")
                volume_usage_details+=("$container -> $volume")
                break
            fi
        done 
    done 

    if [ ${#containers_using_volume[@]} -gt 0 ]; then 
        echo ""
        echo "ERROR: Found running containers using target volumes!!"
        echo ""
        echo "Running containers status:"
        docker ps --format "table {{.Name}}\t{{.Status}}\t{{.Image}}"
        echo ""
        echo "Volume usage details:"
        for detail in "${volume_usage_details[@]}"; do 
            echo "  - $detail"
        done 
        echo ""
        echo "SOLUTION: Stop the containers before performing backup/restore operations:"
        echo "docker compose down"
        echo "After backup/restore, you can restart with:"
        echo "docker compose up -d"
        echo ""
        exit 1
    fi 
    echo "No containers are using target volumes, safe to proceed"
    return 0 
}

# Function to confirm action
confirm_action() {
    local message=$1
    echo -n "$message (y/n): "
    read -r response
    case "$response" in 
        [yY]|[yY][eE][sS]) return 0 ;;
        *) return 1 ;;
    esac
}

# Function to perform backup
