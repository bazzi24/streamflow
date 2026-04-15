#!/bin/bash

# The function is used to obtain distribution information
get_distro_info() {
    local distro_id=$(lsb_release -i -s 2>/dev/null)
    local distro_version=$(lsb_release -r -s 2>/dev/null)
    local kernel_version=$(uname -r)

    # If lsd_release is not available, try parsing the/etc/* - release file
    if [ -z "$distro_id" ] || [ -z "$distro_version" ]; then
        distro_id=$(grep '^ID=' /etc/*-release | cut -d= -f2 | tr -d '"')
        distro_version=$(grep '^VERSION_ID=' /etc/*-release | cut -d= -f2 | tr -d '"')
    fi

    echo "$distro_id $distro_version (Kernel version: $kernel_version)"
}

# get Git repository name
git_repo_name=''
if git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    git_repo_name=$(basename "$(git rev-parse --show-toplevel)")
    if [ $? -ne 0 ]; then
        git_repo_name="(Can't get repo name)"
    fi
else
    git_repo_name="Not a Git repo"
fi

# get CPU type
cpu_model=$(uname -m)

# get memory size
memory_size=$(free -h | grep Mem | awk '{print $2}')

# get docker version
docker_version=''
if command -v docker &> /dev/null; then
    docker_version=$(docker --version | cut -d ' ' -f3)
else
    docker_version="Docker not installed"
fi

# get python version
python_version=$(python3 --version 2>&1 || python --version 2>&1 || echo "Python not installed")

# get hostname
hostname_info=$(hostname)

# get current user
current_user=$(whoami)

# get uptime
uptime_info=$(uptime -p 2>/dev/null || uptime)

# get disk space (root partition)
disk_usage=$(df -h / | awk 'NR==2 {print $3 " / " $2 " (" $5 " used)"}')

# get local IP address
local_ip=$(hostname -I 2>/dev/null | awk '{print $1}')

# get public IP address
public_ip=$(curl -s --max-time 5 ifconfig.me 2>/dev/null)

# get GPU info
gpu_info=$(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null)
if [ -z "$gpu_info" ]; then
    gpu_info="No NVIDIA GPU detected"
fi

# get Node.js version
node_version=$(node --version 2>/dev/null || echo "Node.js not installed")

# get Java version
java_version=$(java -version 2>&1 | head -1 || echo "Java not installed")

# get shell
shell_info=$(basename "$SHELL")

# get locale
locale_info=$(locale | grep LANG | cut -d= -f2 | tr -d '"')

# get timezone
timezone_info=$(timedatectl show --property=Timezone --value 2>/dev/null || echo "Unknown")

# get desktop environment
desktop_env=${XDG_CURRENT_DESKTOP:-"Not set"}

# Print all information
echo "Current Repository: $git_repo_name"

# get Commit ID
git_version=$(git log -1 --pretty=format:'%h')

if [ -z "$git_version" ]; then
    echo "Commit Id: The current directory is not a Git repository, or the Git command is not installed."
else
    echo "Commit Id: $git_version"
fi

echo "Operating system: $(get_distro_info)"
echo "CPU Type: $cpu_model"
echo "Memory: $memory_size"
echo "Docker Version: $docker_version"
echo "Python Version: $python_version"
echo "Hostname: $hostname_info"
echo "Current User: $current_user"
echo "Uptime: $uptime_info"
echo "Disk Usage: $disk_usage"
echo "Local IP: $local_ip"
echo "Public IP: ${public_ip:-N/A}"
echo "GPU: $gpu_info"
echo "Node.js Version: $node_version"
echo "Java Version: $java_version"
echo "Shell: $shell_info"
echo "Locale: $locale_info"
echo "Timezone: $timezone_info"
echo "Desktop Environment: $desktop_env"
