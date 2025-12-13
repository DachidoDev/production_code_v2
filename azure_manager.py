import os
import json
import traceback
import sys
import argparse
import time
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError


class Config:
    """Environment-based configuration"""
    
    CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING', '')
    RECORDINGS_CONTAINER = os.environ.get('RECORDINGS_CONTAINER', 'recordings')
    TRANSCRIPTIONS_CONTAINER = os.environ.get('TRANSCRIPTIONS_CONTAINER', 'transcriptions')
    PROCESSED_CONTAINER = os.environ.get('PROCESSED_RECORDINGS_CONTAINER', 'processed-recordings')
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))
    
    # Email configuration
    EMAIL_RECIPIENTS = [r.strip() for r in os.environ.get('EMAIL_RECIPIENTS', '').split(',') if r.strip()]
    SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
    SMTP_USERNAME = os.environ.get('SMTP_USERNAME', '')
    SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', '')
    EMAIL_FROM = os.environ.get('EMAIL_FROM', SMTP_USERNAME)
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        if not cls.CONNECTION_STRING:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING is required!\n"
                "Set it in your .env file or environment"
            )
        
        if not cls.SMTP_USERNAME or not cls.SMTP_PASSWORD:
            print("WARNING: Email credentials not configured - notifications disabled")
    
    @classmethod
    def get_blob_client(cls) -> BlobServiceClient:
        """Create Azure Blob Storage client"""
        print("="*80)
        print("AZURE STORAGE CONNECTION")
        print("="*80)
        print(f"Recordings: {cls.RECORDINGS_CONTAINER}")
        print(f"Transcriptions: {cls.TRANSCRIPTIONS_CONTAINER}")
        print(f"Processed: {cls.PROCESSED_CONTAINER}")
        print(f"Batch Size: {cls.BATCH_SIZE}")
        
        try:
            client = BlobServiceClient.from_connection_string(cls.CONNECTION_STRING)
            account_info = client.get_account_information()
            print("✓ Connected successfully")
            print(f"Account: {account_info.get('account_kind', 'Unknown')}")
            print("="*80 + "\n")
            return client
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            print("="*80 + "\n")
            raise


class EmailNotifier:
    """Send email reports for batch processing"""
    
    def __init__(self):
        self.enabled = bool(
            Config.EMAIL_RECIPIENTS and 
            Config.SMTP_USERNAME and 
            Config.SMTP_PASSWORD
        )
    
    def send_report(self, stats: Dict, errors: List[str] = None):
        """Send processing report"""
        if not self.enabled:
            print("Email notifications disabled (missing config)")
            return False
        
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"Whisper Processing Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            msg['From'] = Config.EMAIL_FROM
            msg['To'] = ', '.join(Config.EMAIL_RECIPIENTS)
            
            if stats.get('processed', 0) == 0:
                print(f"No entities were processed - skipping email")
                return True

            # Create both text and HTML versions
            text_body = self._create_text_report(stats, errors)
            html_body = self._create_html_report(stats, errors)
            
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send
            with smtplib.SMTP(Config.SMTP_SERVER, Config.SMTP_PORT) as server:
                server.starttls()
                server.login(Config.SMTP_USERNAME, Config.SMTP_PASSWORD)
                server.send_message(msg)
            
            print(f"Email sent to {len(Config.EMAIL_RECIPIENTS)} recipient(s)")
            return True
            
        except Exception as e:
            print(f"Email failed: {e}")
            return False
    
    def _create_text_report(self, stats: Dict, errors: List[str] = None) -> str:
        """Plain text report"""
        lines = [
            "WHISPER BATCH PROCESSING REPORT",
            "=" * 80,
            f"Time: {stats.get('start_time', 'N/A')}",
            f"Duration: {stats.get('duration_minutes', 0):.1f} minutes",
            "",
            "RESULTS:",
            f"  Processed: {stats.get('processed', 0)}",
            f"  Successful: {stats.get('successful', 0)}",
            f"  Failed: {stats.get('failed', 0)}",
            f"  Moved: {stats.get('moved', 0)}",
            f"  Deleted: {stats.get('deleted', 0)}",
        ]
        
        if stats.get('processed', 0) > 0:
            success_rate = (stats['successful'] / stats['processed']) * 100
            lines.append(f"  Success Rate: {success_rate:.1f}%")
        
        if errors:
            lines.extend(["", "ERRORS:", "-" * 80])
            for err in errors[:10]:
                lines.append(f"  • {err}")
            if len(errors) > 10:
                lines.append(f"  ... and {len(errors)-10} more")
        
        return "\n".join(lines)
    
    def _create_html_report(self, stats: Dict, errors: List[str] = None) -> str:
        """HTML report"""
        success_rate = 0
        if stats.get('processed', 0) > 0:
            success_rate = (stats['successful'] / stats['processed']) * 100
        
        status_color = "#28a745" if success_rate >= 90 else "#ffc107" if success_rate >= 70 else "#dc3545"
        
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <h2 style="color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px;">
                    Whisper Processing Report
                </h2>
                
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <p style="margin: 5px 0;"><strong>Time:</strong> {stats.get('start_time', 'N/A')}</p>
                    <p style="margin: 5px 0;"><strong>Duration:</strong> {stats.get('duration_minutes', 0):.1f} minutes</p>
                </div>
                
                <h3 style="color: #2c3e50; margin-top: 30px;">Results</h3>
                <table style="width: 100%; border-collapse: collapse; margin: 15px 0;">
                    <tr style="background-color: #ecf0f1;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Processed</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">{stats.get('processed', 0)}</td>
                    </tr>
                    <tr style="background-color: #d4edda;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Successful</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; text-align: right; color: #28a745;">{stats.get('successful', 0)}</td>
                    </tr>
                    <tr style="background-color: #f8d7da;">
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Failed</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; text-align: right; color: #dc3545;">{stats.get('failed', 0)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Moved</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">{stats.get('moved', 0)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Deleted</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; text-align: right;">{stats.get('deleted', 0)}</td>
                    </tr>
                    <tr>
                        <td style="padding: 10px; border: 1px solid #ddd;"><strong>Success Rate</strong></td>
                        <td style="padding: 10px; border: 1px solid #ddd; text-align: right; color: {status_color}; font-weight: bold;">{success_rate:.1f}%</td>
                    </tr>
                </table>
        """
        
        if errors:
            html += f"""
                <h3 style="color: #dc3545; margin-top: 30px;">⚠ Errors</h3>
                <div style="background-color: #f8d7da; border-left: 4px solid #dc3545; padding: 15px;">
                    <ul style="margin: 0; padding-left: 20px;">
            """
            for err in errors[:10]:
                html += f"<li style='margin: 5px 0;'>{err}</li>"
            if len(errors) > 10:
                html += f"<li style='font-style: italic;'>... and {len(errors)-10} more</li>"
            html += "</ul></div>"
        
        html += """
            </div>
        </body>
        </html>
        """
        
        return html




class AzureProcessor:
    """
    Stateless Azure audio processor
    No database - uses blob existence to determine processing status
    """
    
    AUDIO_EXTENSIONS = {'.mp3', '.wav', '.m4a', '.flac', '.ogg', 
                       '.MP3', '.WAV', '.M4A', '.FLAC', '.OGG'}
    
    def __init__(self):
        Config.validate()
        self.blob_client = Config.get_blob_client()
        self.email = EmailNotifier()
        
        # Temp directory for downloads
        self.temp_dir = Path('./temp_audio')
        self.temp_dir.mkdir(exist_ok=True)
        
        # Statistics
        self.stats = {
            'start_time': datetime.now().isoformat(),
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'moved': 0,
            'deleted': 0,
            'errors': []
        }
        
        print(f"Temp Directory: {self.temp_dir}")
        print(f"Email: {'ENABLED' if self.email.enabled else 'DISABLED'}")
        if self.email.enabled:
            print(f"  → {', '.join(Config.EMAIL_RECIPIENTS)}")
        print()
    
    def is_processed(self, audio_blob: str) -> bool:
        """Check if transcription already exists"""
        transcription_name = audio_blob.rsplit('.', 1)[0] + '_transcription.json'
        
        try:
            blob = self.blob_client.get_blob_client(
                Config.TRANSCRIPTIONS_CONTAINER,
                transcription_name
            )
            blob.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False
        except Exception as e:
            print(f"  ⚠ Error checking transcription: {e}")
            return False
    
    def find_pending_files(self) -> List[str]:
        """Find all unprocessed audio files"""
        container = self.blob_client.get_container_client(Config.RECORDINGS_CONTAINER)
        
        pending = []
        orphans = []  # Has transcription but not moved
        total = 0
        
        print("Scanning for pending files...")
        
        try:
            for blob in container.list_blobs():
                total += 1
                
                # Check if it's an audio file
                if not any(blob.name.endswith(ext) for ext in self.AUDIO_EXTENSIONS):
                    continue
                
                # Check processing status
                if self.is_processed(blob.name):
                    orphans.append(blob.name)
                else:
                    pending.append(blob.name)
                
                if total % 100 == 0:
                    print(f"  Scanned {total} blobs...")
        
        except Exception as e:
            print(f"Scan failed: {e}")
            raise
        
        print(f"Scan complete:")
        print(f"  Total blobs: {total}")
        print(f"  Pending: {len(pending)}")
        print(f"  Orphans: {len(orphans)}")
        print()
        
        # Clean up orphans
        if orphans:
            self._cleanup_orphans(orphans)
        
        return pending
    
    def _cleanup_orphans(self, orphans: List[str]):
        """Move orphaned files (transcribed but not moved)"""
        print("Cleaning up orphaned files...")
        
        for blob_name in orphans:
            try:
                # Download
                source = self.blob_client.get_blob_client(Config.RECORDINGS_CONTAINER, blob_name)
                data = source.download_blob().readall()
                
                # Upload to processed
                dest = self.blob_client.get_blob_client(Config.PROCESSED_CONTAINER, blob_name)
                dest.upload_blob(data, overwrite=True)
                
                # Verify and delete
                if source.get_blob_properties().size == dest.get_blob_properties().size:
                    source.delete_blob()
                    print(f"  ✓ Moved: {blob_name}")
                else:
                    print(f"  ⚠ Size mismatch: {blob_name}")
                    
            except Exception as e:
                print(f"  ✗ Failed: {blob_name} - {e}")
        
        print()
    
    def process_file(self, blob_name: str, processor) -> Dict:
        """Process single audio file"""
        local_path = self.temp_dir / Path(blob_name).name
        
        result = {
            'blob_name': blob_name,
            'success': False,
            'error': None
        }
        
        try:
            # Download
            print("  [1/3] Downloading...")
            blob = self.blob_client.get_blob_client(Config.RECORDINGS_CONTAINER, blob_name)
            with open(local_path, 'wb') as f:
                f.write(blob.download_blob().readall())
            
            size = local_path.stat().st_size
            print(f"        ✓ Downloaded ({size:,} bytes)")
            
            # Process with Whisper
            print("  [2/3] Processing...")
            whisper_result = processor.process_audio_file(str(local_path))
            
            if whisper_result.get('status') != 'success':
                raise ValueError(whisper_result.get('error', 'Processing failed'))
            
            print(f"        ✓ Processed ({whisper_result.get('word_count', 0)} words)")
            
            # Upload transcription
            print("  [3/3] Uploading transcription...")
            trans_name = blob_name.rsplit('.', 1)[0] + '_transcription.json'
            trans_blob = self.blob_client.get_blob_client(
                Config.TRANSCRIPTIONS_CONTAINER,
                trans_name
            )
            trans_blob.upload_blob(
                json.dumps(whisper_result, indent=2, ensure_ascii=False).encode('utf-8'),
                overwrite=True
            )
            print(f"        ✓ Uploaded")
            
            result['success'] = True
            result['local_path'] = str(local_path)
            
        except Exception as e:
            result['error'] = str(e)
            print(f"  ✗ Failed: {e}")
            self.stats['errors'].append(f"{blob_name}: {e}")
        
        return result
    
    def move_and_delete(self, successful_files: List[Dict]):
        """Batch move to processed and delete from recordings"""
        if not successful_files:
            return
        
        print("\n" + "="*80)
        print("BATCH MOVE & DELETE")
        print("="*80)
        print(f"Files: {len(successful_files)}\n")
        
        # Move to processed
        print("[1/2] Moving to processed...")
        moved = []
        
        for file_info in successful_files:
            blob_name = file_info['blob_name']
            local_path = Path(file_info['local_path'])
            
            try:
                dest = self.blob_client.get_blob_client(Config.PROCESSED_CONTAINER, blob_name)
                with open(local_path, 'rb') as f:
                    dest.upload_blob(f, overwrite=True)
                
                moved.append(blob_name)
                self.stats['moved'] += 1
                print(f"  ✓ {blob_name}")
                
            except Exception as e:
                print(f"  ✗ {blob_name}: {e}")
                self.stats['errors'].append(f"Move failed: {blob_name}")
        
        # Delete from recordings
        print(f"\n[2/2] Deleting from recordings...")
        
        for blob_name in moved:
            try:
                source = self.blob_client.get_blob_client(Config.RECORDINGS_CONTAINER, blob_name)
                dest = self.blob_client.get_blob_client(Config.PROCESSED_CONTAINER, blob_name)
                
                # Verify sizes match
                if source.get_blob_properties().size == dest.get_blob_properties().size:
                    source.delete_blob()
                    self.stats['deleted'] += 1
                    print(f"  ✓ {blob_name}")
                else:
                    print(f"  ⚠ Size mismatch: {blob_name}")
                    
            except Exception as e:
                print(f"  ✗ {blob_name}: {e}")
                self.stats['errors'].append(f"Delete failed: {blob_name}")
        
        # Cleanup temp files
        for file_info in successful_files:
            try:
                Path(file_info['local_path']).unlink()
            except:
                pass
        
        print("="*80 + "\n")
    
    def process_all(self, batch_size: Optional[int] = None):
        """Process all pending files in batches"""
        batch_size = batch_size or Config.BATCH_SIZE
        start_time = time.time()
        
        print("="*80)
        print("BATCH PROCESSING - ALL PENDING FILES")
        print("="*80)
        print(f"Batch size: {batch_size}")
        print("="*80 + "\n")
        
        # Find pending files
        pending = self.find_pending_files()
        
        if not pending:
            print("✓ No pending files\n")
            if self.email.enabled:
                self.stats['duration_minutes'] = (time.time() - start_time) / 60
                self.email.send_report(self.stats)
            return self.stats
        
        total_files = len(pending)
        total_batches = (total_files + batch_size - 1) // batch_size
        
        print(f"PLAN:")
        print(f"  Files: {total_files}")
        print(f"  Batch size: {batch_size}")
        print(f"  Batches: {total_batches}")
        print()
        
        # Initialize Whisper
        try:
            from whisper_processor import WhisperProcessor
            processor = WhisperProcessor()
        except Exception as e:
            print(f"✗ Failed to initialize Whisper: {e}")
            return self.stats
        
        # Process in batches
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_files)
            batch_files = pending[start_idx:end_idx]
            
            print("="*80)
            print(f"BATCH {batch_num + 1}/{total_batches}")
            print("="*80)
            print(f"Files: {len(batch_files)} | Range: {start_idx+1}-{end_idx}/{total_files}")
            print("="*80 + "\n")
            
            successful = []
            
            for idx, blob_name in enumerate(batch_files, 1):
                print(f"[{idx}/{len(batch_files)}] {blob_name}")
                
                result = self.process_file(blob_name, processor)
                self.stats['processed'] += 1
                
                if result['success']:
                    self.stats['successful'] += 1
                    successful.append(result)
                    print("  ✓ SUCCESS\n")
                else:
                    self.stats['failed'] += 1
                    print(f"  ✗ FAILED\n")
            
            # Move and delete successful files
            if successful:
                self.move_and_delete(successful)
        
        # Final summary
        duration = time.time() - start_time
        self.stats['duration_minutes'] = duration / 60
        
        print("\n" + "="*80)
        print("SESSION COMPLETE")
        print("="*80)
        print(f"Duration: {duration/60:.1f} minutes")
        print(f"Processed: {self.stats['processed']}")
        print(f"Successful: {self.stats['successful']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Moved: {self.stats['moved']}")
        print(f"Deleted: {self.stats['deleted']}")
        
        if self.stats['processed'] > 0:
            rate = (self.stats['successful'] / self.stats['processed']) * 100
            print(f"Success Rate: {rate:.1f}%")
        
        print("="*80 + "\n")
        
        # Send email report
        if self.email.enabled:
            self.email.send_report(self.stats, self.stats['errors'])
        
        return self.stats




def main():
    parser = argparse.ArgumentParser(description='Azure Whisper Processor')
    parser.add_argument('command', choices=['process', 'test-email'], help='Command to run')
    parser.add_argument('--batch-size', type=int, help='Override batch size')
    
    args = parser.parse_args()
    
    try:
        processor = AzureProcessor()
        
        if args.command == 'process':
            processor.process_all(batch_size=args.batch_size)
        
        elif args.command == 'test-email':
            print("Testing email...")
            test_stats = {
                'start_time': datetime.now().isoformat(),
                'duration_minutes': 2.5,
                'processed': 10,
                'successful': 9,
                'failed': 1,
                'moved': 9,
                'deleted': 8
            }
            test_errors = ['Test error 1', 'Test error 2']
            processor.email.send_report(test_stats, test_errors)
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
