"""
Application settings using Pydantic v2.
All configuration loaded from environment variables.
"""

from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration with environment variable loading.
    """
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="JUKEBOX_",
        case_sensitive=False,
    )
    
    # Telegram Bot
    bot_token: str = Field(
        ...,
        description="Telegram bot token from @BotFather",
    )
    bot_owner_id: int = Field(
        ...,
        description="Telegram user ID of bot owner for admin commands",
    )
    
    # Redis (Sentinel mode)
    redis_sentinel_hosts: str = Field(
        default="redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379",
        description="Comma-separated Sentinel host:port pairs",
    )
    redis_master_name: str = Field(
        default="jukebox-master",
        description="Redis Sentinel master service name",
    )
    redis_password: str = Field(
        ...,
        description="Redis password",
    )
    redis_sentinel_password: Optional[str] = Field(
        default=None,
        description="Separate Sentinel password (optional)",
    )
    redis_db: int = Field(
        default=0,
        description="Redis database number",
    )
    
    # Redis (single-node fallback)
    redis_url: Optional[str] = Field(
        default=None,
        description="Single Redis URL for non-HA deployments (e.g., redis://localhost:6379/0)",
    )
    
    # Environment
    environment: str = Field(
        default="prod",
        description="Environment name: prod, dev, test",
    )
    
    # Cache
    max_cache_size_gb: int = Field(
        default=20,
        description="Max disk cache size in GB",
    )
    cache_dir: Path = Field(
        default=Path("/var/cache/jukebox"),
        description="Disk cache directory path",
    )
    tmpfs_path: Path = Field(
        default=Path("/dev/shm/jukebox"),
        description="tmpfs staging area for active streams",
    )
    
    # Streaming
    max_streams: Optional[int] = Field(
        default=None,
        description="Max concurrent streams (default: cpu_count * 4)",
    )
    
    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL",
    )
    log_format: str = Field(
        default="json",
        description="Log output format: json, console",
    )
    
    # Queue
    max_queue_size: int = Field(
        default=100,
        description="Max tracks per group queue",
    )
    
    # Pyrogram (for pytgcalls)
    pyrogram_api_id: int = Field(
        ...,
        description="Telegram MTProto API ID from https://my.telegram.org",
    )
    pyrogram_api_hash: str = Field(
        ...,
        description="Telegram MTProto API Hash",
    )
    pyrogram_session_string: Optional[str] = Field(
        default=None,
        description="Pyrogram session string (alternative to session file)",
    )
    
    # Health check
    health_check_port: int = Field(
        default=3000,
        description="HTTP health check endpoint port",
    )


# Global settings instance
settings = Settings()
