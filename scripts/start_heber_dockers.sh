#!/bin/bash

# Configuration
MOUNT_PATH="/Volumes/heber/data"
PROJECT_DIR="/Users/jacobmcmillan/Empire/Heber"
MAX_RETRIES=60  # Wait up to 5 minutes (60 * 5s)
RETRY_INTERVAL=5

echo "Starting Heber Docker startup script at $(date)"

# 1. Wait for the external volume to be mounted
echo "Checking for mount at $MOUNT_PATH..."
retry_count=0
while [ ! -d "$MOUNT_PATH" ]; do
    if [ $retry_count -ge $MAX_RETRIES ]; then
        echo "ERROR: Mount $MOUNT_PATH not found after $((MAX_RETRIES * RETRY_INTERVAL)) seconds. Exiting."
        exit 1
    fi
    echo "Wait for mount $MOUNT_PATH (Attempt $((retry_count + 1))/$MAX_RETRIES)..."
    sleep $RETRY_INTERVAL
    retry_count=$((retry_count + 1))
done
echo "Mount $MOUNT_PATH found!"

# 2. Add an extra delay to ensure the filesystem is fully ready
echo "Waiting an additional 10 seconds for filesystem stabilization..."
sleep 10

# 3. Wait for the Docker daemon to be responsive
echo "Checking for Docker daemon..."
retry_count=0
while ! /usr/local/bin/docker info > /dev/null 2>&1; do
    if [ $retry_count -ge $MAX_RETRIES ]; then
        echo "ERROR: Docker daemon not responding after $((MAX_RETRIES * RETRY_INTERVAL)) seconds. Exiting."
        exit 1
    fi
    echo "Waiting for Docker daemon (Attempt $((retry_count + 1))/$MAX_RETRIES)..."
    sleep $RETRY_INTERVAL
    retry_count=$((retry_count + 1))
done
echo "Docker daemon is responsive!"

# 4. Start the Heber containers
echo "Navigating to $PROJECT_DIR to start containers..."
if cd "$PROJECT_DIR"; then
    echo "Starting Docker Compose services..."
    /usr/local/bin/docker compose up -d
    echo "Heber Docker startup script completed at $(date)."
else
    echo "ERROR: Failed to navigate to $PROJECT_DIR."
    exit 1
fi
