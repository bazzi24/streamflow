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

    
}