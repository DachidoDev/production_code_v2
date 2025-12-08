import os
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional
from dataclasses import dataclass, asdict
from faster_whisper import WhisperModel

MODEL_DIR = os.environ.get('WHISPER_MODEL_DIR', '/opt/whisper_models')
MODEL_CONFIG = {
    "name": os.environ.get('WHISPER_MODEL', 'Systran/faster-whisper-large-v3'),
    "device": os.environ.get('WHISPER_DEVICE', 'cuda'),
    "compute_type": os.environ.get('WHISPER_COMPUTE_TYPE', 'float32'),
    "download_root": MODEL_DIR,
    "local_files_only": True,  # since models are pre-downloaded
}

BEAM_SIZE = int(os.environ.get('WHISPER_BEAM_SIZE', '5'))
VAD_FILTER = True
VAD_PARAMS = {"min_silence_duration_ms": 500}

SUPPORTED_LANGUAGES = {"en": "English", "hi": "Hindi", "ta": "Tamil", "te": "Telugu",}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)

def deduplicate_text(text: str) -> str:
    """
    Remove duplicate consecutive sentences or phrases.
    
    Simple deduplication by removing consecutive duplicate sentences.
    """
    if not text:
        return text
    
    sentences = [s.strip() for s in text.split('.') if s.strip()]
    
    if not sentences:
        return text
    
    deduplicated = [sentences[0]]
    for sentence in sentences[1:]:
        if sentence != deduplicated[-1]:
            deduplicated.append(sentence)
    
    return '. '.join(deduplicated) + ('.' if text.rstrip().endswith('.') else '')

@dataclass
class TranslationResult:
    """Translation result - matches Azure Manager expectations"""
    id: str
    filename: str
    audio_duration: float
    detected_language: str
    language_code: str
    language_confidence: float
    translation: str  # English translation
    translation_time: float
    model_name: str
    timestamp: str
    status: str  # 'success' or 'failed'
    error: Optional[str] = None
    word_count: Optional[int] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class WhisperProcessor:
    """
    Stateless Whisper processor
    Audio → English translation only
    """
    
    def __init__(self):
        logger.info("="*70)
        logger.info("WHISPER PROCESSOR - DIRECT TRANSLATION")
        logger.info("="*70)
        logger.info(f"Model: {MODEL_CONFIG['name']}")
        logger.info(f"Device: {MODEL_CONFIG['device']}")
        logger.info(f"Compute: {MODEL_CONFIG['compute_type']}")
        
        try:
            load_start = time.time()
            self.model = WhisperModel(
                MODEL_CONFIG['name'],
                device=MODEL_CONFIG['device'],
                compute_type=MODEL_CONFIG['compute_type'],
                download_root=MODEL_CONFIG['download_root'],
                local_files_only=MODEL_CONFIG['local_files_only']
            )
            load_time = time.time() - load_start
            logger.info(f"✓ Model loaded in {load_time:.2f}s")
            logger.info("✓ Ready")
            logger.info("="*70 + "\n")
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise
    
    def process_audio_file(self, audio_path: str) -> Dict:
        """
        Process audio → English translation
        Returns: Dictionary with translation result
        """
        start_time = time.time()
        filename = Path(audio_path).name
        file_id = hashlib.md5(
            f"{filename}_{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]
        
        logger.info(f"Processing: {filename}")
        
        # Initialize result
        result = TranslationResult(
            id=file_id,
            filename=filename,
            audio_duration=0.0,
            detected_language="unknown",
            language_code="unknown",
            language_confidence=0.0,
            translation="",
            translation_time=0.0,
            model_name=MODEL_CONFIG['name'],
            timestamp=datetime.now().isoformat(),
            status='failed'
        )
        
        try:
            # OPTIMIZED: Single transcribe call for both language detection AND translation
            logger.info("  [1/1] Detecting language + translating to English...")
            translate_start = time.time()
            
            segments, info = self.model.transcribe(
                audio_path,
                task='translate',  # Will auto-detect language and translate
                beam_size=BEAM_SIZE,
                vad_filter=VAD_FILTER,
                vad_parameters=VAD_PARAMS
            )
            
            # Extract language info from the SAME transcription call
            lang_code = info.language
            confidence = info.language_probability
            lang_name = SUPPORTED_LANGUAGES.get(lang_code, lang_code.upper())
            
            logger.info(f"        → Detected: {lang_name} ({lang_code}) - {confidence:.1%}")
            
            result.language_code = lang_code
            result.detected_language = lang_name
            result.language_confidence = confidence
            result.audio_duration = info.duration
            
            # Collect translation segments
            translation_parts = []
            for segment in segments:
                if segment.text.strip():
                    translation_parts.append(segment.text.strip())
            
            raw_translation = ' '.join(translation_parts)
            
            # Deduplicate
            translation = deduplicate_text(raw_translation)
            translation_time = time.time() - translate_start
            
            result.translation = translation
            result.translation_time = translation_time
            
            # Validate
            if not translation or not translation.strip():
                raise ValueError("Translation is empty")
            
            result.word_count = len(translation.split())
            result.status = 'success'
            
            logger.info(f"        ✓ Success - {translation_time:.2f}s")
            logger.info(f"        → {result.word_count} words")
            
        except Exception as e:
            result.status = 'failed'
            result.error = str(e)
            logger.error(f"        ✗ Failed: {e}")
        
        return result.to_dict()
    
    def cleanup(self):
        """Cleanup (for compatibility)"""
        pass


def main():
    """Test processor"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python whisper_processor.py <audio_file>")
        sys.exit(1)
    
    audio_file = sys.argv[1]
    
    if not Path(audio_file).exists():
        print(f"Error: File not found: {audio_file}")
        sys.exit(1)
    
    print("="*70)
    print("WHISPER PROCESSOR TEST")
    print("="*70)
    print(f"File: {audio_file}\n")
    
    try:
        processor = WhisperProcessor()
        result = processor.process_audio_file(audio_file)
        
        print("\n" + "="*70)
        print("RESULT")
        print("="*70)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        if result['status'] == 'success':
            print("\n✓ Success!")
        else:
            print(f"\n✗ Failed: {result.get('error')}")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()