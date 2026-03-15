# Telegram Jukebox Bot - Phase 1 Foundation

Python 3.11+ implementation of core domain, Redis state engine, and content acquisition.

## Components

### Domain Layer (`src/domain/`)
- **enums.py**: State machine enums (TrackStatus, PlaybackStatus, LoopMode, ErrorSeverity)
- **models.py**: Slotted dataclasses (Track, PlaybackState, GroupSettings, DownloadResult, JukeboxError)
- **interfaces.py**: Abstract Base Classes (QueueManager, StreamEngine, TelegramHandler)
- **logging.py**: structlog configuration with mandatory group_id context

### State Engine (`src/state/`)
- **redis_client.py**: Redis/Sentinel connection manager
- **keys.py**: Centralized key namespace builder (jukebox:{env}:{type}:{id})
- **lua_scripts.py**: Atomic Lua scripts (enqueue, dequeue, ack, nack, clear)
- **serialization.py**: msgpack serializer for domain models
- **queue_manager.py**: Concrete QueueManager with LMOVE shadow queue pattern

### Content Acquisition (`src/acquisition/`)
- **download_worker.py**: yt-dlp downloader with Redlock deduplication
- **cache_manager.py**: 20GB LRU disk cache with Redis metadata
- **tmpfs_overlay.py**: RAM-backed tmpfs staging for active streams

## Key Patterns

### Exactly-Once Delivery (LMOVE Shadow Queue)
LMOVE queue:123 shadow:123 LEFT LEFT → Atomic dequeue to shadow
Process track
LREM shadow:123 1 <track_bytes> → Acknowledge on success
[Reaper] If stalled, LMOVE shadow:123 queue:123 back to main queue

### Redlock Download Deduplication
Compute sha256 = SHA-256(url)
Acquire lock on jukebox:prod🔒dl:{sha256} with 5-min TTL
If acquired: download
If locked: wait or use cache
Release lock after download completes

### LRU Cache Eviction
Redis sorted set: jukebox:prod:cache:lru

ZADD cache:lru {sha256} {timestamp} → Track access time
ZRANGE cache:lru 0 0 → Get oldest entry
Evict until total_size < 20GB

## Environment Variables

```bash
# Redis (Sentinel mode)
REDIS_SENTINEL_HOSTS=sentinel1:26379,sentinel2:26379,sentinel3:26379
REDIS_MASTER_NAME=jukebox-master
REDIS_PASSWORD=secret
REDIS_SENTINEL_PASSWORD=sentinel_secret
REDIS_DB=0

# Redis (single-node fallback)
REDIS_URL=redis://localhost:6379/0

# Environment
JUKEBOX_ENV=prod  # prod, dev, test

# tmpfs
TMPFS_PATH=/dev/shm/jukebox
Usage
Initialize Logging
Copyfrom src.domain.logging import configure_logging, get_logger

configure_logging(log_level="INFO")
log = get_logger(__name__)
Connect to Redis
Copyfrom src.state.redis_client import RedisClient
from src.state.keys import RedisKeys

redis_client = RedisClient()
await redis_client.connect()

keys = RedisKeys(env="prod")
Enqueue Track
Copyfrom src.state.queue_manager import ConcreteQueueManager
from src.domain.models import Track

queue_mgr = ConcreteQueueManager(redis_client, keys)

track = Track.create(
    url="https://youtube.com/watch?v=dQw4w9WgXcQ",
    title="Never Gonna Give You Up",
    duration_seconds=213.0,
    requested_by=123456789,
)

length = await queue_mgr.enqueue(group_id=-1001234567, track=track)
print(f"Queue length: {length}")
Download Track
Copyfrom src.acquisition.download_worker import DownloadWorker
from src.acquisition.cache_manager import CacheManager

cache_mgr = CacheManager(redis_client, keys)
downloader = DownloadWorker(cache_mgr, redis_client, keys)

result = await downloader.download(
    url="https://youtube.com/watch?v=dQw4w9WgXcQ",
    group_id=-1001234567,
)

if result.success:
    print(f"Downloaded: {result.track.title}")
    print(f"File: {result.track.file_path}")
else:
    print(f"Error: {result.error}")
Activate tmpfs for Streaming
Copyfrom src.acquisition.tmpfs_overlay import TmpfsOverlay

tmpfs = TmpfsOverlay(redis_client, keys)

# Activate file for streaming
active_path = await tmpfs.activate(
    sha256=track.sha256,
    source_path=track.file_path,
)

# Stream from active_path (RAM speed)

# Deactivate when streaming ends
await tmpfs.deactivate(sha256=track.sha256)
Testing
Copy# Install dependencies
pip install -r requirements.txt

# Set environment
export REDIS_URL=redis://localhost:6379/0
export JUKEBOX_ENV=dev

# Run tests (Phase 2+)
pytest tests/
Architecture Notes
All dataclasses use __slots__=True for memory efficiency
IntEnum for all status enums - compact msgpack storage
structlog with mandatory group_id - enforced via custom processor
Lua scripts for atomic operations - registered once, executed via EVALSHA
LMOVE shadow queue - exactly-once delivery guarantee
msgpack serialization - 20-40% smaller than JSON
20GB LRU disk cache - persistent audio storage
tmpfs overlay - RAM-speed I/O for active streams
Next Steps (Phase 2+)
Stream Pipeline Engineer: FFmpeg transcoding, Telegram voice chat integration
Gateway Engineer: Telegram bot handlers, command routing
Platform Engineer: Docker, Kubernetes, monitoring
Reliability Engineer: Circuit breakers, retry policies, health checks
