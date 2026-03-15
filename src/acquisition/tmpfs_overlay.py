"""
tmpfs staging area manager for active streaming files.
Provides RAM-speed I/O for currently playing tracks.
"""

import asyncio
import shutil
from pathlib import Path
from typing import Optional

from src.domain.logging import get_logger
from src.state.redis_client import RedisClient
from src.state.keys import RedisKeys

log = get_logger(__name__)


class TmpfsOverlay:
    """
    Manages tmpfs staging area for actively streamed audio files.
    
    Pattern:
        1. Download completes to persistent cache
        2. activate() symlinks/copies to tmpfs when streaming starts
        3. Stream serves from tmpfs (RAM speed)
        4. deactivate() removes from tmpfs when streaming ends
    
    Environment:
        TMPFS_PATH: Path to tmpfs mount (default: /dev/shm/jukebox)
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        keys: RedisKeys,
        tmpfs_path: str = "/dev/shm/jukebox",
    ):
        self._redis = redis_client
        self._keys = keys
        self.TMPFS_PATH = Path(tmpfs_path)
        self.TMPFS_PATH.mkdir(parents=True, exist_ok=True)
        self._active_key = f"{keys._prefix}:tmpfs:active"
    
    def available_bytes(self) -> int:
        """Check available tmpfs space."""
        usage = shutil.disk_usage(str(self.TMPFS_PATH))
        return usage.free
    
    def has_capacity(self, required_bytes: int) -> bool:
        """Check if tmpfs has enough space."""
        return self.available_bytes() >= required_bytes
    
    def get_active_path(self, sha256: str, ext: str = "opus") -> Path:
        """Get tmpfs path for active streaming file."""
        return self.TMPFS_PATH / f"{sha256}.{ext}"
    
    async def is_active(self, sha256: str) -> bool:
        """Check if file is in tmpfs staging area."""
        redis = self._redis.get_connection()
        return await redis.sismember(self._active_key, sha256)
    
    async def activate(self, sha256: str, source_path: Path) -> Path:
        """
        Activate file for streaming - copy/symlink to tmpfs.
        
        Args:
            sha256: SHA-256 hash
            source_path: Path to cached file on persistent storage
        
        Returns:
            Path to tmpfs file
        """
        redis = self._redis.get_connection()
        
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")
        
        ext = source_path.suffix.lstrip(".")
        tmpfs_path = self.get_active_path(sha256, ext)
        
        # Check if already active
        if tmpfs_path.exists():
            log.debug("already_active", sha256=sha256[:16], group_id="system")
            return tmpfs_path
        
        file_size = source_path.stat().st_size
        
        if not self.has_capacity(file_size):
            log.warning(
                "tmpfs_capacity_exceeded",
                required_mb=file_size / 1024 / 1024,
                available_mb=self.available_bytes() / 1024 / 1024,
                group_id="system",
            )
            # Fallback: serve directly from persistent cache
            return source_path
        
        # Copy to tmpfs (async)
        await asyncio.to_thread(shutil.copy2, source_path, tmpfs_path)
        
        # Mark as active in Redis
        await redis.sadd(self._active_key, sha256)
        
        log.info(
            "tmpfs_activated",
            sha256=sha256[:16],
            size_mb=file_size / 1024 / 1024,
            group_id="system",
        )
        
        return tmpfs_path
    
    async def deactivate(self, sha256: str) -> None:
        """
        Deactivate file - remove from tmpfs when streaming ends.
        
        Args:
            sha256: SHA-256 hash
        """
        redis = self._redis.get_connection()
        
        # Find file in tmpfs
        for ext in ["opus", "mp3", "m4a", "webm"]:
            tmpfs_path = self.get_active_path(sha256, ext)
            if tmpfs_path.exists():
                await asyncio.to_thread(tmpfs_path.unlink)
                log.info("tmpfs_deactivated", sha256=sha256[:16], group_id="system")
                break
        
        # Remove from Redis active set
        await redis.srem(self._active_key, sha256)
    
    async def get_active_files(self) -> list[str]:
        """Get list of all active SHA-256 hashes in tmpfs."""
        redis = self._redis.get_connection()
        members = await redis.smembers(self._active_key)
        return [m.decode("utf-8") for m in members]
