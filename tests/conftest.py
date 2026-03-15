"""
Pytest configuration and shared fixtures for all test modules.
"""

import asyncio
import pytest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock
from fakeredis import aioredis as fakeredis_aioredis

from src.state.redis_client import RedisClient
from src.state.keys import RedisKeys
from src.state.queue_manager import ConcreteQueueManager
from src.streaming.stream_engine import ConcreteStreamEngine
from src.domain.models import Track
from src.domain.enums import TrackStatus


# pytest-asyncio configuration
pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(scope="function")
async def redis_client():
    """Fake Redis client for testing."""
    fake_redis = fakeredis_aioredis.FakeRedis(decode_responses=False)
    
    # Mock RedisClient
    client = MagicMock(spec=RedisClient)
    client.get_connection = MagicMock(return_value=fake_redis)
    client.ping = AsyncMock(return_value=True)
    
    yield client
    
    # Cleanup
    await fake_redis.flushall()
    await fake_redis.aclose()


@pytest.fixture
def redis_keys():
    """Redis key builder for testing."""
    return RedisKeys(env="test")


@pytest.fixture
async def queue_manager(redis_client, redis_keys):
    """Queue manager with fake Redis."""
    return ConcreteQueueManager(redis_client, redis_keys)


@pytest.fixture
def mock_ffmpeg_proc():
    """Mock FFmpeg subprocess."""
    proc = MagicMock()
    proc.pid = 12345
    proc.returncode = None
    proc.stdout = MagicMock()
    proc.stderr = MagicMock()
    proc.terminate = MagicMock()
    proc.kill = MagicMock()
    proc.wait = AsyncMock(return_value=0)
    proc.send_signal = MagicMock()
    return proc


@pytest.fixture
async def stream_engine(redis_client, queue_manager, redis_keys):
    """Stream engine with mocked pytgcalls."""
    # Mock dependencies
    mock_tmpfs = AsyncMock()
    mock_pytgcalls = AsyncMock()
    
    engine = ConcreteStreamEngine(
        redis_client=redis_client,
        queue_manager=queue_manager,
        tmpfs_overlay=mock_tmpfs,
        pytgcalls_client=mock_pytgcalls,
        keys=redis_keys,
    )
    
    return engine


@pytest.fixture
def sample_track(tmp_path):
    """Sample track with temporary audio file."""
    # Create temporary wav file
    audio_file = tmp_path / "test_track.opus"
    audio_file.write_bytes(b"OPUS_AUDIO_DATA_PLACEHOLDER")
    
    return Track.create(
        url="https://youtube.com/watch?v=dQw4w9WgXcQ",
        title="Test Track",
        duration_seconds=213.0,
        requested_by=123456789,
        sha256="abcdef1234567890",
        file_path=audio_file,
    )


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Temporary cache directory."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return cache_dir
