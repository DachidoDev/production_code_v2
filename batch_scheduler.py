import os
import sys
import time
import signal
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from azure_manager import AzureManager

# ============================================================================
# CONFIGURATION
# ============================================================================

# Scheduler Configuration
BATCH_INTERVAL_MINUTES = int(os.environ.get('BATCH_INTERVAL_MINUTES', '60'))  # Default: 1 hour
LOG_DIR = Path(os.environ.get('BATCH_LOG_DIR', './logs'))
MAX_LOG_SIZE_MB = 100  # Max size per log file
LOG_BACKUP_COUNT = 10  # Number of backup log files

# Job State Tracking
JOB_STATE_FILE = Path('./batch_job_state.json')

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging() -> logging.Logger:
    """
    Setup comprehensive logging system
    
    FEATURES:
    - Rotating file handler (prevents disk fill)
    - Console output for monitoring
    - Structured format with timestamps
    - Separate error log
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Main log file (INFO and above)
    main_log = LOG_DIR / 'batch_processing.log'
    
    # Error log file (ERROR and above)
    error_log = LOG_DIR / 'batch_errors.log'
    
    # Create logger
    logger = logging.getLogger('BatchScheduler')
    logger.setLevel(logging.DEBUG)
    
    # Rotating file handler (main log)
    main_handler = RotatingFileHandler(
        main_log,
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    main_handler.setLevel(logging.INFO)
    main_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Rotating file handler (errors only)
    error_handler = RotatingFileHandler(
        error_log,
        maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s\n%(pathname)s:%(lineno)d\n',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    ))
    
    # Add handlers
    logger.addHandler(main_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ============================================================================
# JOB STATE MANAGEMENT
# ============================================================================

class JobState:
    """
    Track job execution state to prevent overlapping runs
    
    WHY: Prevent multiple batch jobs running simultaneously
    HOW: Use a simple file-based lock mechanism
    """
    
    @staticmethod
    def is_running() -> bool:
        """Check if a job is currently running"""
        if not JOB_STATE_FILE.exists():
            return False
        
        try:
            import json
            with open(JOB_STATE_FILE, 'r') as f:
                state = json.load(f)
            
            # Check if job started more than 2 hours ago (stuck job)
            start_time = datetime.fromisoformat(state.get('start_time', ''))
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if elapsed > 7200:  # 2 hours
                logger.warning(f"Found stuck job from {start_time}, clearing state")
                JobState.clear()
                return False
            
            return state.get('running', False)
        
        except Exception as e:
            logger.error(f"Error reading job state: {e}")
            return False
    
    @staticmethod
    def mark_running():
        """Mark job as running"""
        import json
        state = {
            'running': True,
            'start_time': datetime.now().isoformat(),
            'pid': os.getpid()
        }
        
        with open(JOB_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    
    @staticmethod
    def mark_complete(success: bool, stats: dict = None):
        """Mark job as complete"""
        import json
        state = {
            'running': False,
            'last_run_time': datetime.now().isoformat(),
            'last_run_success': success,
            'last_run_stats': stats or {}
        }
        
        with open(JOB_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    
    @staticmethod
    def clear():
        """Clear job state"""
        if JOB_STATE_FILE.exists():
            JOB_STATE_FILE.unlink()
    
    @staticmethod
    def get_last_run() -> Optional[dict]:
        """Get information about last run"""
        if not JOB_STATE_FILE.exists():
            return None
        
        try:
            import json
            with open(JOB_STATE_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            return None


# ============================================================================
# BATCH JOB EXECUTOR
# ============================================================================

def run_batch_job():
    """
    Execute batch processing job
    
    WORKFLOW:
    1. Check if job is already running (prevent overlap)
    2. Mark job as running
    3. Initialize Azure Manager
    4. Process batch
    5. Mark job as complete
    6. Log results
    
    ERROR HANDLING:
    - Catches all exceptions
    - Logs errors
    - Always marks job as complete
    - Never crashes scheduler
    """
    logger.info("="*80)
    logger.info("BATCH JOB STARTED")
    logger.info("="*80)
    
    # Prevent overlapping runs
    if JobState.is_running():
        logger.warning("Previous job still running, skipping this run")
        return
    
    try:
        # Mark as running
        JobState.mark_running()
        
        # Initialize manager
        logger.info("Initializing Azure Manager...")
        manager = AzureManager()
        
        # Process batch
        logger.info("Starting batch processing...")
        start_time = time.time()
        
        manager.process_batch()
        
        elapsed = time.time() - start_time
        
        # Get statistics
        stats = manager.get_processing_stats()
        
        logger.info("="*80)
        logger.info("BATCH JOB COMPLETED")
        logger.info("="*80)
        logger.info(f"Processed: {stats['processed']}")
        logger.info(f"Successful: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Skipped (already processed): {stats['skipped']}")
        logger.info(f"Total time: {elapsed:.1f}s")
        logger.info("="*80)
        
        # Mark complete
        JobState.mark_complete(success=True, stats=stats)
        
    except Exception as e:
        logger.error(f"Batch job failed: {e}", exc_info=True)
        JobState.mark_complete(success=False)
    
    finally:
        logger.info("Batch job finished\n")


# ============================================================================
# SCHEDULER
# ============================================================================

class BatchScheduler:
    """
    Scheduled batch processing system
    
    FEATURES:
    - Runs jobs at fixed intervals (default: 1 hour)
    - Graceful shutdown handling
    - Manual trigger support
    """
    
    def __init__(self, interval_minutes: int = BATCH_INTERVAL_MINUTES):
        """
        Initialize scheduler
        
        Args:
            interval_minutes: How often to run batch job (default: 60)
        """
        self.interval_minutes = interval_minutes
        self.scheduler = BlockingScheduler()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Scheduler initialized: {interval_minutes} minute interval")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def start(self, run_immediately: bool = True):
        """
        Start the scheduler
        
        Args:
            run_immediately: Run first job immediately (default: True)
        """
        logger.info("="*80)
        logger.info("BATCH SCHEDULER STARTING")
        logger.info("="*80)
        logger.info(f"Environment: {os.environ.get('ENVIRONMENT', 'local')}")
        logger.info(f"Interval: Every {self.interval_minutes} minutes")
        logger.info(f"Run immediately: {run_immediately}")
        logger.info(f"Logs directory: {LOG_DIR.absolute()}")
        logger.info("="*80)
        
        # Add job to scheduler
        self.scheduler.add_job(
            run_batch_job,
            trigger=IntervalTrigger(minutes=self.interval_minutes),
            id='batch_processing_job',
            name='Batch Processing',
            max_instances=1,  # Prevent overlapping
            replace_existing=True
        )
        
        # Run immediately if requested
        if run_immediately:
            logger.info("Running first batch immediately...")
            run_batch_job()
        
        # Start scheduler
        logger.info(f"Scheduler started. Next run in {self.interval_minutes} minutes")
        logger.info("Press Ctrl+C to stop\n")
        
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.stop()
    
    def stop(self):
        """Stop the scheduler"""
        logger.info("Stopping scheduler...")
        self.scheduler.shutdown(wait=True)
        logger.info("Scheduler stopped")


# ============================================================================
# COMMAND LINE INTERFACE
# ============================================================================

def main():
    """
    Batch Scheduler CLI
    
    COMMANDS:
        start        Start the scheduler (runs every hour)
        run-once     Run batch job once and exit
        status       Show scheduler status
    
    OPTIONS:
        --interval   Interval in minutes (default: 60)
        --no-immediate  Don't run first job immediately
    
    EXAMPLES:
        # Start scheduler (hourly)
        python batch_scheduler.py start
        
        # Start with 30-minute interval
        python batch_scheduler.py start --interval 30
        
        # Run once and exit
        python batch_scheduler.py run-once
        
        # Check status
        python batch_scheduler.py status
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Batch Processing Scheduler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=main.__doc__
    )
    
    parser.add_argument(
        'command',
        choices=['start', 'run-once', 'status'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=BATCH_INTERVAL_MINUTES,
        help=f'Interval in minutes (default: {BATCH_INTERVAL_MINUTES})'
    )
    
    parser.add_argument(
        '--no-immediate',
        action='store_true',
        help="Don't run first job immediately"
    )
    
    args = parser.parse_args()
    
    try:
        if args.command == 'start':
            scheduler = BatchScheduler(interval_minutes=args.interval)
            scheduler.start(run_immediately=not args.no_immediate)
        
        elif args.command == 'run-once':
            logger.info("Running batch job once...")
            run_batch_job()
        
        elif args.command == 'status':
            last_run = JobState.get_last_run()
            
            print("\n" + "="*80)
            print("BATCH SCHEDULER STATUS")
            print("="*80)
            
            if last_run:
                print(f"Running: {last_run.get('running', False)}")
                
                if 'last_run_time' in last_run:
                    print(f"Last run: {last_run['last_run_time']}")
                    print(f"Success: {last_run.get('last_run_success', 'N/A')}")
                    
                    if 'last_run_stats' in last_run:
                        stats = last_run['last_run_stats']
                        print(f"Processed: {stats.get('processed', 'N/A')}")
                        print(f"Successful: {stats.get('successful', 'N/A')}")
                        print(f"Failed: {stats.get('failed', 'N/A')}")
                        print(f"Skipped: {stats.get('skipped', 'N/A')}")
                else:
                    print("No completed runs yet")
            else:
                print("No job state found")
            
            print("="*80)
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()