"""
Concrete implementation of QueueManager using Redis and Lua scripts.
Uses LMOVE shadow queue pattern for exactly-once delivery.
"""

from typing import Optional

import redis.asyncio as aioredis

from src.domain.interfaces import QueueManager
from src.domain.models import Track, GroupSettings
from src.domain.logging import get_logger, bind_contextvars
from .redis_client import RedisClient
from .keys import RedisKeys
from .lua_scripts import (
    ENQUEUE_SCRIPT,
    DEQUEUE_SCRIPT,
    ACK_SCRIPT,
    NACK_SCRIPT,
    CLEAR_SCRIPT,
)
from .serialization import Serializer

log = get_logger(__name__)


class ConcreteQueueManager(QueueManager):
    """
    Redis-backed queue manager with Lua atomic operations.
    
    Features:
        - Atomic enqueue with max-size check
        - LMOVE shadow list for exactly-once delivery
        - ACK/NACK for reliable processing
        - msgpack serialization for compact storage
    """
    
    def __init__(self, redis_client: RedisClient, keys: RedisKeys):
        self._redis_client = redis_client
        self._keys = keys
        self._scripts: dict[str, aioredis.client.Script] = {}
    
    async def _ensure_scripts_loaded(self) -> None:
        """Load Lua scripts into Redis. Call once on startup."""
        if self._scripts:
            return
        
        redis = self._redis_client.get_connection()
        
        self._scripts["enqueue"] = redis.register_script(ENQUEUE_SCRIPT)
        self._scripts["dequeue"] = redis.register_script(DEQUEUE_SCRIPT)
        self._scripts["ack"] = redis.register_script(ACK_SCRIPT)
        self._scripts["nack"] = redis.register_script(NACK_SCRIPT)
        self._scripts["clear"] = redis.register_script(CLEAR_SCRIPT)
        
        log.info("lua_scripts_loaded", count=len(self._scripts), group_id="system")
    
    async def _get_max_queue_size(self, group_id: int) -> int:
        """Get max queue size from GroupSettings or default."""
        # TODO: Load from Redis settings hash
        # For now, return default
        return 100
    
    async def enqueue(self, group_id: int, track: Track) -> int:
        """
        Enqueue a track atomically with max-size check.
        
        Raises:
            ValueError: If queue is full
        """
        bind_contextvars(group_id=group_id)
        await self._ensure_scripts_loaded()
        
        queue_key = self._keys.queue(group_id)
        track_bytes = Serializer.pack(track)
        max_size = await self._get_max_queue_size(group_id)
        
        result = await self._scripts["enqueue"](
            keys=[queue_key],
            args=[track_bytes, max_size],
        )
        
        if result == -1:
            log.warning(
                "queue_full",
                group_id=group_id,
                max_size=max_size,
                track_id=str(track.track_id),
            )
            raise ValueError(f"Queue full (max: {max_size})")
        
        log.info(
            "track_enqueued",
            group_id=group_id,
            track_id=str(track.track_id),
            title=track.title,
            queue_length=result,
        )
        
        return int(result)
    
    async def dequeue(self, group_id: int) -> Optional[Track]:
        """
        Dequeue next track atomically using LMOVE to shadow queue.
        
        Returns:
            Track instance or None if queue is empty
        """
        bind_contextvars(group_id=group_id)
        await self._ensure_scripts_loaded()
        
        queue_key = self._keys.queue(group_id)
        shadow_key = self._keys.shadow_queue(group_id)
        
        result = await self._scripts["dequeue"](
            keys=[queue_key, shadow_key],
        )
        
        if result is None:
            log.debug("queue_empty", group_id=group_id)
            return None
        
        track = Serializer.unpack(result, Track)
        
        log.info(
            "track_dequeued",
            group_id=group_id,
            track_id=str(track.track_id),
            title=track.title,
        )
        
        return track
    
    async def peek(self, group_id: int) -> Optional[Track]:
        """Peek at next track without removing it."""
        bind_contextvars(group_id=group_id)
        
        redis = self._redis_client.get_connection()
        queue_key = self._keys.queue(group_id)
        
        # Get first item without removing
        items = await redis.lrange(queue_key, 0, 0)
        
        if not items:
            return None
        
        return Serializer.unpack(items[0], Track)
    
    async def get_queue(self, group_id: int) -> list[Track]:
        """Get all tracks in queue."""
        bind_contextvars(group_id=group_id)
        
        redis = self._redis_client.get_connection()
        queue_key = self._keys.queue(group_id)
        
        items = await redis.lrange(queue_key, 0, -1)
        
        tracks = [Serializer.unpack(item, Track) for item in items]
        
        log.debug("queue_retrieved", group_id=group_id, count=len(tracks))
        
        return tracks
    
    async def clear(self, group_id: int) -> int:
        """Clear entire queue atomically."""
        bind_contextvars(group_id=group_id)
        await self._ensure_scripts_loaded()
        
        queue_key = self._keys.queue(group_id)
        shadow_key = self._keys.shadow_queue(group_id)
        
        result = await self._scripts["clear"](
            keys=[queue_key, shadow_key],
        )
        
        log.info("queue_cleared", group_id=group_id, removed_count=result)
        
        return int(result)
    
    async def queue_length(self, group_id: int) -> int:
        """Return current queue length."""
        redis = self._redis_client.get_connection()
        queue_key = self._keys.queue(group_id)
        
        return await redis.llen(queue_key)
    
    async def ack(self, group_id: int, track_id: str) -> bool:
        """
        Acknowledge successful processing - remove from shadow queue.
        
        Args:
            group_id: Group ID
            track_id: Track UUID string
        
        Returns:
            True if removed, False if not found
        """
        bind_contextvars(group_id=group_id)
        await self._ensure_scripts_loaded()
        
        redis = self._redis_client.get_connection()
        shadow_key = self._keys.shadow_queue(group_id)
        
        # Get shadow queue items to find matching track_id
        items = await redis.lrange(shadow_key, 0, -1)
        
        for item_bytes in items:
            track = Serializer.unpack(item_bytes, Track)
            if str(track.track_id) == track_id:
                # Found it - remove from shadow
                result = await self._scripts["ack"](
                    keys=[shadow_key],
                    args=[item_bytes],
                )
                
                success = result > 0
                
                log.info(
                    "track_acknowledged",
                    group_id=group_id,
                    track_id=track_id,
                    success=success,
                )
                
                return success
        
        log.warning("track_not_in_shadow", group_id=group_id, track_id=track_id)
        return False
    
    async def nack(self, group_id: int, track_id: str) -> bool:
        """
        Negative acknowledge - move failed track back to main queue.
        
        Args:
            group_id: Group ID
            track_id: Track UUID string
        
        Returns:
            True if moved, False if not found
        """
        bind_contextvars(group_id=group_id)
        await self._ensure_scripts_loaded()
        
        redis = self._redis_client.get_connection()
        shadow_key = self._keys.shadow_queue(group_id)
        queue_key = self._keys.queue(group_id)
        
        # Get shadow queue items to find matching track_id
        items = await redis.lrange(shadow_key, 0, -1)
        
        for item_bytes in items:
            track = Serializer.unpack(item_bytes, Track)
            if str(track.track_id) == track_id:
                # Found it - move back to queue
                result = await self._scripts["nack"](
                    keys=[shadow_key, queue_key],
                    args=[item_bytes],
                )
                
                success = result > 0
                
                log.warning(
                    "track_requeued",
                    group_id=group_id,
                    track_id=track_id,
                    success=success,
                )
                
                return success
        
        log.warning("track_not_in_shadow", group_id=group_id, track_id=track_id)
        return False
