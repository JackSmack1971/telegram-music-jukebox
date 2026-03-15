"""
Global semaphore pool for limiting concurrent FFmpeg streams.
Prevents resource exhaustion by capping max simultaneous streams.
"""

import asyncio
import os
from typing import Optional

from src.domain.logging import get_logger

log = get_logger(__name__)


class SemaphorePool:
    """
    Singleton semaphore pool for FFmpeg stream concurrency control.
    
    Features:
        - Limits max concurrent streams based on CPU count
        - Async context manager for automatic acquire/release
        - Global singleton pattern
    """
    
    _instance: Optional["SemaphorePool"] = None
    _semaphore: Optional[asyncio.Semaphore] = None
    _max_streams: int = 0
    _active_count: int = 0
    
    def __new__(cls):
        """Singleton pattern - only one instance globally."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._max_streams = max(4, os.cpu_count() * 4)
            cls._semaphore = asyncio.Semaphore(cls._max_streams)
            log.info(
                "semaphore_pool_initialized",
                max_streams=cls._max_streams,
                group_id="system",
            )
        return cls._instance
    
    async def acquire(self, group_id: int):
        """
        Acquire semaphore for group.
        
        Returns:
            Async context manager
        """
        log.debug(
            "semaphore_acquire_requested",
            active_count=self._active_count,
            max_streams=self._max_streams,
            group_id=group_id,
        )
        
        # Return context manager
        return _SemaphoreContext(self, group_id)
    
    def _increment(self):
        """Internal: increment active count."""
        self._active_count += 1
    
    def _decrement(self):
        """Internal: decrement active count."""
        self._active_count = max(0, self._active_count - 1)
    
    @property
    def active_count(self) -> int:
        """Get current active stream count."""
        return self._active_count
    
    @property
    def max_streams(self) -> int:
        """Get max stream capacity."""
        return self._max_streams


class _SemaphoreContext:
    """Async context manager for semaphore acquire/release."""
    
    def __init__(self, pool: SemaphorePool, group_id: int):
        self._pool = pool
        self._group_id = group_id
    
    async def __aenter__(self):
        """Acquire semaphore."""
        await self._pool._semaphore.acquire()
        self._pool._increment()
        
        log.debug(
            "semaphore_acquired",
            active_count=self._pool.active_count,
            group_id=self._group_id,
        )
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release semaphore."""
        self._pool._semaphore.release()
        self._pool._decrement()
        
        log.debug(
            "semaphore_released",
            active_count=self._pool.active_count,
            group_id=self._group_id,
        )
