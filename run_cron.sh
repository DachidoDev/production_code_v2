#!/bin/bash

# Get the absolute path of the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment (located in home directory)
source /home/daworker/whisper_env/bin/activate

# Load environment variables
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
else
    echo "Error: .env file not found at $SCRIPT_DIR/.env"
    exit 1
fi

# Setup logging
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
LOG_FILE="$LOG_DIR/cron_${TIMESTAMP}.log"
LATEST_LOG="$LOG_DIR/cron_latest.log"

# Run the batch processing and log output
{
    echo "================================================================================"
    echo "BATCH PROCESSING STARTED - $(date)"
    echo "================================================================================"
    echo ""
    
    cd "$SCRIPT_DIR"
    python3 azure_manager.py process
    
    EXIT_CODE=$?
    
    echo ""
    echo "================================================================================"
    echo "BATCH PROCESSING COMPLETED - $(date)"
    echo "================================================================================"
    
    exit $EXIT_CODE
    
} 2>&1 | tee "$LOG_FILE" "$LATEST_LOG"

# Preserve exit code from the subshell
EXIT_CODE=${PIPESTATUS[0]}

# Keep only last 30 log files
cd "$LOG_DIR"
ls -t cron_*.log | tail -n +31 | xargs -r rm --

exit $EXIT_CODE