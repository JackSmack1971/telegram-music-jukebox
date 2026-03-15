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
