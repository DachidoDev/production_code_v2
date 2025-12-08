#!/bin/bash

# ==============================================================================
# COMPLETE CRON JOB SETUP - ONE COMMAND
# ==============================================================================
# This script will:
# 1. Create run_batch_cron.sh
# 2. Make it executable
# 3. Add it to crontab (every hour)
# 4. Verify everything
# ==============================================================================

set -e

echo "=============================================================================="
echo "SETTING UP AUTOMATED DAILY BATCH PROCESSING"
echo "=============================================================================="
echo ""

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Script Directory: $SCRIPT_DIR"
echo ""

# ==============================================================================
# VERIFY PREREQUISITES
# ==============================================================================

echo "[0/5] Checking prerequisites..."

# Check if .env file exists
if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "✗ ERROR: .env file not found at $SCRIPT_DIR/.env"
    echo ""
    echo "Please create a .env file with the following variables:"
    echo "  AZURE_STORAGE_CONNECTION_STRING='your_connection_string'"
    echo "  RECORDINGS_CONTAINER='recordings'"
    echo "  TRANSCRIPTIONS_CONTAINER='transcriptions'"
    echo "  PROCESSED_RECORDINGS_CONTAINER='processed-recordings'"
    echo "  BATCH_SIZE='10'"
    echo "  EMAIL_RECIPIENTS='your@email.com'"
    echo "  SMTP_USERNAME='your_smtp_username'"
    echo "  SMTP_PASSWORD='your_smtp_password'"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo "✗ ERROR: Python not found"
    echo "Please install Python 3"
    exit 1
fi

# Check if azure_manager.py exists
if [ ! -f "$SCRIPT_DIR/azure_manager.py" ]; then
    echo "✗ ERROR: azure_manager.py not found at $SCRIPT_DIR/azure_manager.py"
    exit 1
fi

echo "✓ All prerequisites met"
echo ""

# ==============================================================================
# STEP 1: CREATE run_batch_cron.sh
# ==============================================================================

echo "[1/5] Creating run_batch_cron.sh..."

cat > "$SCRIPT_DIR/run_batch_cron.sh" << 'EOF'
#!/bin/bash

# ==============================================================================
# AUTOMATED BATCH PROCESSING RUNNER
# ==============================================================================
# This script is executed by cron to run the Azure batch processing
# ==============================================================================

set -e

# Get the absolute path of the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ==============================================================================
# LOAD ENVIRONMENT VARIABLES
# ==============================================================================

ENV_FILE="$SCRIPT_DIR/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: .env file not found at $ENV_FILE"
    echo "Please ensure .env exists with required Azure credentials"
    exit 1
fi

# Load environment variables
set -a
source "$ENV_FILE"
set +a

# ==============================================================================
# SETUP LOGGING
# ==============================================================================

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# Create timestamped log file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/cron_${TIMESTAMP}.log"

# Also maintain latest log for easy access
LATEST_LOG="$LOG_DIR/cron_latest.log"

# ==============================================================================
# EXECUTE BATCH PROCESSING
# ==============================================================================

{
    echo "=============================================================================="
    echo "AZURE BATCH PROCESSING - CRON JOB"
    echo "=============================================================================="
    echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Script Directory: $SCRIPT_DIR"
    echo "Log File: $LOG_FILE"
    echo "=============================================================================="
    echo ""
    
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Determine Python command
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    elif command -v python &> /dev/null; then
        PYTHON_CMD="python"
    else
        echo "ERROR: Python not found in PATH"
        exit 1
    fi
    
    echo "Using Python: $PYTHON_CMD ($(which $PYTHON_CMD))"
    echo ""
    
    # Run the batch processing
    $PYTHON_CMD azure_manager.py process
    
    EXIT_CODE=$?
    
    echo ""
    echo "=============================================================================="
    echo "CRON JOB FINISHED"
    echo "=============================================================================="
    echo "Finished: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Exit Code: $EXIT_CODE"
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Status: ✓ SUCCESS"
    else
        echo "Status: ✗ FAILED"
    fi
    
    echo "=============================================================================="
    echo ""
    
    # ==============================================================================
    # CLEANUP OLD LOGS (keep last 30 days)
    # ==============================================================================
    
    echo "Cleaning up old logs (keeping last 30 days)..."
    
    DELETED_COUNT=0
    while IFS= read -r old_log; do
        rm -f "$old_log"
        ((DELETED_COUNT++))
    done < <(find "$LOG_DIR" -name "cron_*.log" -type f -mtime +30)
    
    if [ $DELETED_COUNT -gt 0 ]; then
        echo "✓ Deleted $DELETED_COUNT old log file(s)"
    else
        echo "✓ No old logs to delete"
    fi
    
    echo ""
    
    exit $EXIT_CODE
    
} 2>&1 | tee "$LOG_FILE"

# Copy to latest log (do this outside the subshell)
cp "$LOG_FILE" "$LATEST_LOG"

# Preserve exit code
exit ${PIPESTATUS[0]}
EOF

echo "✓ run_batch_cron.sh created at $SCRIPT_DIR/run_batch_cron.sh"
echo ""

# ==============================================================================
# STEP 2: MAKE EXECUTABLE
# ==============================================================================

echo "[2/5] Making run_batch_cron.sh executable..."
chmod +x "$SCRIPT_DIR/run_batch_cron.sh"
echo "✓ Permissions set (chmod +x)"
echo ""

# ==============================================================================
# STEP 3: VERIFY SCRIPT CAN RUN
# ==============================================================================

echo "[3/5] Verifying script syntax..."
if bash -n "$SCRIPT_DIR/run_batch_cron.sh"; then
    echo "✓ Script syntax is valid"
else
    echo "✗ Script has syntax errors"
    exit 1
fi
echo ""

# ==============================================================================
# STEP 4: ADD TO CRONTAB
# ==============================================================================

echo "[4/5] Adding to crontab (every hour)..."

# Create temporary crontab file
TEMP_CRON=$(mktemp)

# Get existing crontab (if any), excluding our script
crontab -l 2>/dev/null | grep -v "run_batch_cron.sh" > "$TEMP_CRON" || true

# Add new entry with full path - runs at the start of every hour
echo "0 * * * * $SCRIPT_DIR/run_batch_cron.sh" >> "$TEMP_CRON"

# Install new crontab
crontab "$TEMP_CRON"

# Cleanup
rm -f "$TEMP_CRON"

echo "✓ Cron job added"
echo ""

# ==============================================================================
# STEP 5: VERIFY INSTALLATION
# ==============================================================================

echo "[5/5] Verifying installation..."
echo ""

ERRORS=0

# Check run_batch_cron.sh
if [ -f "$SCRIPT_DIR/run_batch_cron.sh" ] && [ -x "$SCRIPT_DIR/run_batch_cron.sh" ]; then
    echo "✓ run_batch_cron.sh exists and is executable"
else
    echo "✗ run_batch_cron.sh not found or not executable"
    ((ERRORS++))
fi

# Check .env file
if [ -f "$SCRIPT_DIR/.env" ]; then
    echo "✓ .env file exists"
else
    echo "✗ .env file not found"
    ((ERRORS++))
fi

# Check azure_manager.py
if [ -f "$SCRIPT_DIR/azure_manager.py" ]; then
    echo "✓ azure_manager.py exists"
else
    echo "✗ azure_manager.py not found"
    ((ERRORS++))
fi

# Check crontab entry
if crontab -l 2>/dev/null | grep -q "run_batch_cron.sh"; then
    echo "✓ Cron job is scheduled"
else
    echo "✗ Cron job not found in crontab"
    ((ERRORS++))
fi

# Check logs directory
if [ -d "$SCRIPT_DIR/logs" ]; then
    echo "✓ Logs directory exists"
else
    echo "✓ Logs directory will be created on first run"
fi

echo ""

if [ $ERRORS -gt 0 ]; then
    echo "=============================================================================="
    echo "⚠ SETUP COMPLETED WITH $ERRORS ERROR(S)"
    echo "=============================================================================="
    echo "Please review the errors above before proceeding"
    exit 1
fi

# ==============================================================================
# SUMMARY
# ==============================================================================

echo "=============================================================================="
echo "✅ SETUP COMPLETE!"
echo "=============================================================================="
echo ""
echo "Your automated batch processing is now configured!"
echo ""
echo "📅 Schedule: Every hour at :00 minutes (e.g., 1:00, 2:00, 3:00...)"
echo "📂 Script: $SCRIPT_DIR/run_batch_cron.sh"
echo "📝 Logs: $SCRIPT_DIR/logs/"
echo ""
echo "Current crontab entry:"
echo "------------------------------------------------------------------------------"
crontab -l | grep "run_batch_cron.sh" || echo "(none found - this shouldn't happen)"
echo "=============================================================================="
echo ""
echo "🧪 RECOMMENDED NEXT STEPS:"
echo ""
echo "1. Test the script manually:"
echo "   bash $SCRIPT_DIR/run_batch_cron.sh"
echo ""
echo "2. Check the log output:"
echo "   cat $SCRIPT_DIR/logs/cron_latest.log"
echo ""
echo "3. Verify email notifications (if configured)"
echo ""
echo "4. Monitor the next automatic run (will occur at the top of the next hour):"
echo "   tail -f $SCRIPT_DIR/logs/cron_latest.log"
echo ""
echo "5. View all logs:"
echo "   ls -lh $SCRIPT_DIR/logs/"
echo ""
echo "=============================================================================="
echo ""

# ==============================================================================
# OFFER TO RUN TEST
# ==============================================================================

read -p "Would you like to run a test now? [y/N]: " -n 1 -r TEST_NOW
echo ""

if [[ $TEST_NOW =~ ^[Yy]$ ]]; then
    echo ""
    echo "=============================================================================="
    echo "RUNNING TEST..."
    echo "=============================================================================="
    echo ""
    
    if bash "$SCRIPT_DIR/run_batch_cron.sh"; then
        echo ""
        echo "=============================================================================="
        echo "✅ TEST COMPLETED SUCCESSFULLY!"
        echo "=============================================================================="
        echo ""
        echo "Next steps:"
        echo "  • Check your email for the processing report"
        echo "  • Review the log: cat $SCRIPT_DIR/logs/cron_latest.log"
        echo "  • The system will run automatically every day at 8 PM"
    else
        echo ""
        echo "=============================================================================="
        echo "⚠ TEST COMPLETED WITH ERRORS"
        echo "=============================================================================="
        echo ""
        echo "Please review the output above and check:"
        echo "  • Your .env file configuration"
        echo "  • Azure connection string"
        echo "  • Python dependencies are installed"
        echo ""
        echo "Log file: $SCRIPT_DIR/logs/cron_latest.log"
    fi
else
    echo ""
    echo "Test skipped. You can test later with:"
    echo "  bash $SCRIPT_DIR/run_batch_cron.sh"
fi

echo ""
echo "🎉 Setup complete! Your system will run automatically every hour."
echo ""