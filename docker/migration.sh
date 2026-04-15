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
    echo "Please start Docker and try again"
    exit 1
    fi
}

# Function check volumes exists
volume_exists() {
    local volume_name=$1
    docker volume inspect "$volume_name" >/dev/null 2>&1
}

# Function check container using volume
check_container_using_volumes() {
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
perform_backup() {
    local backup_folder=$1

    echo "Starting StreamFlow backup..."
    echo "Backup folder: $backup_folder"
    echo ""

    # check if any containers are using the volume_name
    check_container_using_volumes

    # create backup folder if it does not exist 
    mkdir -p "$backup_folder"

    # backup each volume 
    for i in "${!VOLUMES[@]}"; do
        local volume="${VOLUMES[$i]}"
        local backup_file="${BACKUP_FILE[$i]}"
        local step=$((i + 1))

        echo "Step $step/4: Backing up $volume..."

        if volume_exists "$volume"; then 
            docker run --rm \
                -v "$volume":/source \
                -v "$(pwd)/$backup_folder":/backup \
                alpine tar czf "/backup/$backup_file" -C /source .
            echo "Successfully backed up $volume to $backup_folder/$backup_file"
        else 
            echo "Warning: Volume $volume does not exist, skipping..."
        fi
        echo ""
    done

    echo "Backup completed successfully!"
    echo "Backup location: $(pwd)/$backup_folder"

    # list backup files with sizes
    echo ""
    echo "Backup file created:"
    for backup_file in "${BACKUP_FILE[@]}"; do 
        if [ -f "$backup_folder/$backup_file" ]; then
            local size=$(ls -lh "$backup_folder/$backup_file" | awk '{print $5}')
            echo "  - $backup_file($size)"
        fi 
    done
}

# function perform restrore 
perform_restore() {
    local backup_folder=$1

    echo "Starting StreamFlow data restore..."
    echo "Backup folder: $backup_folder"
    echo ""

    # check if any container using volume
    check_container_using_volumes

    # check if backup folder exist
    if [ ! -d "$backup_folder" ]; then
        echo "Error: Bacup folder '$backup_folder' does not exist"
        exit 1
    fi

    # check if all backup file exist
    local missing_files=()
    for backup_file in "${BACKUP_FILE[@]}"; do 
        if [ ! -f "$backup_folder/$backup_file" ]; then 
            missing_files+=("$backup_file")
        fi 
    done

    if [ ${#missing_files[@]} -gt 0 ]; then 
        echo "Error: Missing backup files:"
        for file in "${missing_files[@]}"; do 
            echo "  - $file"
        done 
        echo "Please ensure all backup files are present in '$backup_folder'"
        exit 1
    fi 
    
    # check for exsisting volumes and warn user
    local existing_volumes=()
    for volume in "${VOLUMES[@]}"; do 
        if volume_exists "$volume"; then
            existing_volumes+=("$volume")
        fi 
    done 

    if [ ${#existing_volumes[@]} -gt 0 ]; then 
        echo "WARNING: The following Docker volumes already exist:"
        for volume in "${existing_volumes[@]}"; do 
            echo "  - $volume"
        done 
        echo ""
        echo "  IMPORTANT: Restore will OVERWRITE existing data!"
        echo "  Recomendation: Create a backup of your current data first"
        echo "  $0 backup current_backup_$(date +%Y%m%d_%H%M%S)"
        echo ""

        if ! confirm_action "Do you want to continoue with the restore operation ?"; then 
            echo "Restore operation cancelled by user"
            exit 0
        fi 
    fi 

    # create volume and restore data 
    for i in "${!VOLUMES[@]}"; do 
        local volume="${VOLUMES[$i]}"
        local backup_file="${BACKUP_FILE[$i]}"
        local step=$((i + 1))

        echo "Step $step/4: Restoring $volume..."

        # create volume if it does not exist
        if ! volume_exists "$volume"; then 
            echo "Creating Docker volume: $volume"
        else 
            echo "Using existing Docker volume: $volume"
        fi 

        # restore data 
        echo "Restoring data from $backup_file..."
        docker run --rm \
            -v "$volume":/target \
            -v "$(pwd)/$backup_folder/$backup_file" -C /target
        
        echo "Successfully restored $volume"
        echo ""
    done 
    echo "Restore completed successfully!!"
    echo "You can now start your StreamFlow services"
}

# main script 
main() {
    # check if Docker available
    check_docker

    # parse command line arguments
    local operation=${1:-}
    local backup_folder=${2:-$DEFAULT_BACKUP_FOLDER}

    # handle help or no arguments
    if [ -z "$operation" ] || [ "$operation" = "help" ] || ["$operation" = "-h" ] || [ "$operation" = "--help" ]; then 
        show_help
        exit 0
    fi 

    # validate operation
    case "$operation" in 
        backup)
            perform_backup "$backup_folder"
            ;;
        restore)
            perform_restore "$backup_folder"
            ;;
        *)
            echo "Error: Invalid operation '$operation'"
            echo ""
            show_help
            exit 1
            ;;
    esac 
}

# run main
main "$@"
