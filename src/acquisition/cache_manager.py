"""
LRU disk cache manager for audio files.
20GB limit with Redis metadata and disk file storage.
"""

import asyncio
import time
from pathlib import Path
from typing import Optional

from src.domain.logging import get_logger
from src.state.redis_client import RedisClient
from src.state.keys import RedisKeys

log = get_logger(__name__)


class CacheManager:
    """
    20GB LRU disk cache for audio files.
    
    Features:
        - Redis metadata storage (cache:meta:{sha256})
        - LRU eviction via Redis sorted set (ZADD with timestamp)
        - 20GB size limit enforcement
        - Async file operations
    """
    
    MAX_CACHE_SIZE_BYTES = 20 * 1024 * 1024 * 1024  # 20 GB
    CACHE_DIR = Path("/var/cache/jukebox/audio")
    
    def __init__(self, redis_client: RedisClient, keys: RedisKeys):
        self._redis = redis_client
        self._keys = keys
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._lru_key = f"{keys._prefix}:cache:lru"
    
    async def get(self, sha256: str) -> Optional[Path]:
        """
        Get cached file path if exists.
        
        Args:
            sha256: SHA-256 hash of URL
        
        Returns:
            Path to cached file or None if not cached
        """
        redis = self._redis.get_connection()
        meta_key = self._keys.cache_meta(sha256)
        
        # Get metadata from Redis
        meta = await redis.hgetall(meta_key)
        
        if not meta:
            return None
        
        file_path_str = meta.get(b"file_path")
        if not file_path_str:
            return None
        
        file_path = Path(file_path_str.decode("utf-8"))
        
        # Verify file exists on disk
        if not file_path.exists():
            log.warning("cache_file_missing", sha256=sha256[:16], path=str(file_path), group_id="system")
            # Clean up stale metadata
            await redis.delete(meta_key)
            await redis.zrem(self._lru_key, sha256)
            return None
        
        # Update LRU timestamp (access time)
        await redis.zadd(self._lru_key, {sha256: time.time()})
        
        log.debug("cache_hit", sha256=sha256[:16], path=str(file_path), group_id="system")
        
        return file_path
    
    async def put(self, sha256: str, file_path: Path, metadata: dict) -> None:
        """
        Store file in cache with metadata.
        
        Args:
            sha256: SHA-256 hash of URL
            file_path: Path to audio file
            metadata: Dict with title, duration_seconds, url
        """
        redis = self._redis.get_connection()
        meta_key = self._keys.cache_meta(sha256)
        
        file_size = file_path.stat().st_size
        
        # Store metadata in Redis hash
        await redis.hset(
            meta_key,
            mapping={
                "file_path": str(file_path),
                "file_size_bytes": file_size,
                "title": metadata.get("title", "Unknown"),
                "duration_seconds": metadata.get("duration_seconds", 0.0),
                "url": metadata.get("url", ""),
                "cached_at": time.time(),
            },
        )
        
        # Add to LRU sorted set
        await redis.zadd(self._lru_key, {sha256: time.time()})
        
        log.info(
            "cache_stored",
            sha256=sha256[:16],
            size_mb=file_size / 1024 / 1024,
            group_id="system",
        )
        
        # Trigger eviction if over limit
        total_size = await self.get_total_size()
        if total_size > self.MAX_CACHE_SIZE_BYTES:
            evicted = await self.evict_lru()
            log.info("cache_eviction_triggered", evicted_count=evicted, group_id="system")
    
    async def evict_lru(self) -> int:
        """
        Evict least-recently-used entries until under size limit.
        
        Returns:
            Number of entries evicted
        """
        redis = self._redis.get_connection()
        evicted_count = 0
        
        while True:
            total_size = await self.get_total_size()
            if total_size <= self.MAX_CACHE_SIZE_BYTES:
                break
            
            # Get oldest entry (lowest score = oldest timestamp)
            oldest = await redis.zrange(self._lru_key, 0, 0, withscores=False)
            
            if not oldest:
                break
            
            sha256 = oldest[0].decode("utf-8")
            
            # Get metadata to find file path
            meta_key = self._keys.cache_meta(sha256)
            meta = await redis.hgetall(meta_key)
            
            if meta:
                file_path_str = meta.get(b"file_path")
                if file_path_str:
                    file_path = Path(file_path_str.decode("utf-8"))
                    # Delete file from disk
                    if file_path.exists():
                        await asyncio.to_thread(file_path.unlink)
                    
                    log.info(
                        "cache_evicted",
                        sha256=sha256[:16],
                        path=str(file_path),
                        group_id="system",
                    )
            
            # Remove from Redis
            await redis.delete(meta_key)
            await redis.zrem(self._lru_key, sha256)
            
            evicted_count += 1
        
        return evicted_count
    
    async def get_total_size(self) -> int:
        """
        Calculate total cached file size from metadata.
        
        Returns:
            Total size in bytes
        """
        redis = self._redis.get_connection()
        
        # Get all cache entries from LRU set
        all_sha256s = await redis.zrange(self._lru_key, 0, -1)
        
        total_size = 0
        
        for sha256_bytes in all_sha256s:
            sha256 = sha256_bytes.decode("utf-8")
            meta_key = self._keys.cache_meta(sha256)
            
            size_bytes = await redis.hget(meta_key, "file_size_bytes")
            if size_bytes:
                total_size += int(size_bytes)
        
        return total_size
