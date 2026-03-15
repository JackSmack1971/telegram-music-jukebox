"""
Download worker using yt-dlp and Redlock for duplicate prevention.
"""

import asyncio
import hashlib
import time
from pathlib import Path
from typing import Optional

from yt_dlp import YoutubeDL

from src.domain.models import Track, DownloadResult
from src.domain.enums import TrackStatus
from src.domain.logging import get_logger, bind_contextvars
from src.state.redis_client import RedisClient
from src.state.keys import RedisKeys
from .cache_manager import CacheManager

log = get_logger(__name__)


class DownloadWorker:
    """
    yt-dlp download worker with Redlock deduplication.
    
    Features:
        - Redlock on SHA-256(url) prevents duplicate downloads
        - yt-dlp Python API for audio extraction
        - Opus codec for Telegram voice compatibility
        - Progress tracking and error handling
    """
    
    LOCK_TTL_MS = 300_000  # 5 minutes
    YDL_BASE_OPTS = {
        "format": "bestaudio[ext=opus]/bestaudio[ext=webm]/bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "extract_flat": False,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "opus",
                "preferredquality": "0",  # VBR best
            },
            {
                "key": "FFmpegMetadata",
                "add_metadata": True,
            },
        ],
        "writethumbnail": False,
        "writeinfojson": False,
    }
    
    def __init__(
        self,
        cache_manager: CacheManager,
        redis_client: RedisClient,
        keys: RedisKeys,
        download_dir: str = "/tmp/jukebox/downloads",
    ):
        self._cache = cache_manager
        self._redis = redis_client
        self._keys = keys
        self._download_dir = Path(download_dir)
        self._download_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def url_to_sha256(url: str) -> str:
        """Compute SHA-256 hash of URL."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
    
    async def _acquire_lock(self, sha256: str) -> bool:
        """
        Acquire Redlock on download lock key.
        
        Returns:
            True if lock acquired, False if already locked
        """
        redis = self._redis.get_connection()
        lock_key = self._keys.download_lock(sha256)
        
        # Simple SET NX PX for single-node lock
        # Production: use Redlock with multiple Redis instances
        result = await redis.set(
            lock_key,
            "locked",
            nx=True,
            px=self.LOCK_TTL_MS,
        )
        
        return result is True
    
    async def _release_lock(self, sha256: str) -> None:
        """Release download lock."""
        redis = self._redis.get_connection()
        lock_key = self._keys.download_lock(sha256)
        await redis.delete(lock_key)
    
    def _extract_info_sync(self, url: str) -> dict:
        """
        Extract metadata without downloading.
        Runs in sync context (blocking).
        """
        opts = {"quiet": True, "no_warnings": True, "noplaylist": True}
        with YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)
    
    def _download_sync(self, url: str, output_path: str) -> dict:
        """
        Download audio using yt-dlp.
        Runs in sync context (blocking).
        """
        opts = self.YDL_BASE_OPTS.copy()
        opts["outtmpl"] = output_path
        
        with YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=True)
    
    async def download(self, url: str, group_id: int) -> DownloadResult:
        """
        Download track with Redlock deduplication.
        
        Args:
            url: Video/audio URL
            group_id: Telegram group ID
        
        Returns:
            DownloadResult with track or error
        """
        bind_contextvars(group_id=group_id)
        
        start_time = time.monotonic()
        sha256 = self.url_to_sha256(url)
        
        log.info("download_started", url=url, sha256=sha256[:16])
        
        # Check cache first
        cached_path = await self._cache.get(sha256)
        if cached_path:
            log.info("cache_hit", sha256=sha256[:16], path=str(cached_path))
            
            # Load metadata from cache
            # TODO: Retrieve full track metadata from cache
            duration_ms = (time.monotonic() - start_time) * 1000
            
            return DownloadResult.success_result(
                track=Track.create(
                    url=url,
                    title="Cached Track",  # Load from metadata
                    duration_seconds=0.0,
                    requested_by=0,
                    sha256=sha256,
                    file_path=cached_path,
                ),
                duration_ms=duration_ms,
            )
        
        # Acquire download lock
        if not await self._acquire_lock(sha256):
            log.warning("download_locked", sha256=sha256[:16])
            duration_ms = (time.monotonic() - start_time) * 1000
            return DownloadResult.error_result(
                error="Download already in progress",
                duration_ms=duration_ms,
            )
        
        try:
            # Extract metadata first (fast)
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(None, self._extract_info_sync, url)
            
            title = info.get("title", "Unknown")
            duration = info.get("duration", 0.0)
            
            log.info(
                "metadata_extracted",
                title=title,
                duration=duration,
                sha256=sha256[:16],
            )
            
            # Download audio (slow)
            output_path = str(self._download_dir / f"{sha256}.%(ext)s")
            info = await loop.run_in_executor(None, self._download_sync, url, output_path)
            
            # Find downloaded file
            file_path = None
            for ext in ["opus", "mp3", "m4a", "webm"]:
                candidate = self._download_dir / f"{sha256}.{ext}"
                if candidate.exists():
                    file_path = candidate
                    break
            
            if not file_path:
                raise FileNotFoundError("Downloaded file not found")
            
            log.info(
                "download_completed",
                file_path=str(file_path),
                size_mb=file_path.stat().st_size / 1024 / 1024,
            )
            
            # Store in cache
            await self._cache.put(
                sha256=sha256,
                file_path=file_path,
                metadata={
                    "title": title,
                    "duration_seconds": duration,
                    "url": url,
                },
            )
            
            duration_ms = (time.monotonic() - start_time) * 1000
            
            return DownloadResult.success_result(
                track=Track.create(
                    url=url,
                    title=title,
                    duration_seconds=duration,
                    requested_by=0,  # Set by caller
                    sha256=sha256,
                    file_path=file_path,
                ),
                duration_ms=duration_ms,
            )
        
        except Exception as e:
            log.error(
                "download_failed",
                error=str(e),
                url=url,
                sha256=sha256[:16],
            )
            
            duration_ms = (time.monotonic() - start_time) * 1000
            
            return DownloadResult.error_result(
                error=str(e),
                duration_ms=duration_ms,
            )
        
        finally:
            await self._release_lock(sha256)
