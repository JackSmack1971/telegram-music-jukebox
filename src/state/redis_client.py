"""
Redis client wrapper with Sentinel support and connection management.
Supports both Sentinel HA and single-node fallback.
"""

import os
from typing import Optional

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

from src.domain.logging import get_logger

log = get_logger(__name__)


class RedisClient:
    """
    Async Redis client with Sentinel support.
    
    Environment variables:
        REDIS_SENTINEL_HOSTS: Comma-separated list of host:port (e.g., "sentinel1:26379,sentinel2:26379")
        REDIS_MASTER_NAME: Sentinel master service name (e.g., "jukebox-master")
        REDIS_PASSWORD: Redis password
        REDIS_SENTINEL_PASSWORD: Sentinel password (optional)
        REDIS_DB: Database number (default: 0)
        REDIS_URL: Fallback single-node URL (e.g., "redis://localhost:6379/0")
    """
    
    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._sentinel: Optional[Sentinel] = None
        self._is_sentinel = False
    
    async def connect(self) -> aioredis.Redis:
        """Establish Redis connection."""
        if self._redis:
            return self._redis
        
        sentinel_hosts = os.getenv("REDIS_SENTINEL_HOSTS")
        
        if sentinel_hosts:
            self._redis = await self._connect_sentinel()
        else:
            self._redis = await self._connect_single()
        
        log.info(
            "redis_connected",
            mode="sentinel" if self._is_sentinel else "single",
            group_id="system",
        )
        return self._redis
    
    async def _connect_sentinel(self) -> aioredis.Redis:
        """Connect via Redis Sentinel for HA."""
        sentinel_hosts_str = os.getenv("REDIS_SENTINEL_HOSTS", "")
        master_name = os.getenv("REDIS_MASTER_NAME", "jukebox-master")
        password = os.getenv("REDIS_PASSWORD")
        sentinel_password = os.getenv("REDIS_SENTINEL_PASSWORD")
        db = int(os.getenv("REDIS_DB", "0"))
        
        # Parse sentinel hosts: "host1:port1,host2:port2"
        sentinel_nodes = []
        for host_port in sentinel_hosts_str.split(","):
            host, port = host_port.strip().split(":")
            sentinel_nodes.append((host, int(port)))
        
        sentinel_kwargs = {}
        if sentinel_password:
            sentinel_kwargs["password"] = sentinel_password
        
        self._sentinel = Sentinel(
            sentinel_nodes,
            sentinel_kwargs=sentinel_kwargs,
        )
        
        self._is_sentinel = True
        
        # Get async Redis client for master
        master = self._sentinel.master_for(
            master_name,
            redis_class=aioredis.Redis,
            password=password,
            db=db,
            decode_responses=False,  # Keep bytes for msgpack
        )
        
        return master
    
    async def _connect_single(self) -> aioredis.Redis:
        """Connect to single Redis instance."""
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        
        self._is_sentinel = False
        
        return aioredis.from_url(
            redis_url,
            decode_responses=False,  # Keep bytes for msgpack
            encoding="utf-8",
        )
    
    def get_connection(self) -> aioredis.Redis:
        """Get the Redis connection. Must call connect() first."""
        if not self._redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._redis
    
    def get_pipeline(self) -> aioredis.client.Pipeline:
        """Get a Redis pipeline for batched commands."""
        return self.get_connection().pipeline()
    
    async def ping(self) -> bool:
        """Health check - ping Redis server."""
        try:
            return await self.get_connection().ping()
        except Exception as e:
            log.error("redis_ping_failed", error=str(e), group_id="system")
            return False
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            log.info("redis_closed", group_id="system")
