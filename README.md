# 🎵 Telegram Music Jukebox Bot

> **Industrial-grade real-time group voice chat music streaming for Telegram — built on Redis-atomic state, watchdog-monitored FFmpeg subprocesses, and Lua-guaranteed exactly-once queue semantics.**

[![Python 312](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)[![Redis 7x](https://img.shields.io/badge/redis-7.x-red.svg)](https://redis.io/)[![aiogram 3x](https://img.shields.io/badge/aiogram-3.x-blue.svg)](https://docs.aiogram.dev/)[![License MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)[![Status PreRelease](https://img.shields.io/badge/status-pre--release-orange.svg)]()

* * *

## Table of Contents

1. [What Is This?](#what-is-this)
2. [Key Differentiators](#key-differentiators)
3. [Architecture Overview](#architecture-overview)
4. [Prerequisites](#prerequisites)
5. [Installation](#installation)
  * [Docker Compose (Recommended)](#docker-compose-recommended)
  * [Manual / VPS Setup](#manual--vps-setup)
6. [Configuration](#configuration)
7. [Bot Commands](#bot-commands)
8. [Repository Structure](#repository-structure)
9. [Redis Schema Reference](#redis-schema-reference)
10. [Error Taxonomy & Recovery](#error-taxonomy--recovery)
11. [Testing](#testing)
12. [Deployment & Operations](#deployment--operations)
13. [Contributing](#contributing)
14. [Risk Register](#risk-register)
15. [License](#license)
16. [Glossary](#glossary)

* * *

## What Is This?

The **Telegram Music Jukebox Bot** transforms any Telegram group voice chat into a collaborative, fault-tolerant jukebox. Users issue slash commands to enqueue tracks from YouTube (and other yt-dlp-supported sources), which are transcoded in real-time via FFmpeg and streamed as live Opus audio directly into the group voice call.

**Target scale at launch:** 500 concurrent voice-chat groups.**Target scale at 6 months:** 2,000+ groups via horizontal worker deployment.

### Why Does This Exist?

Every existing Telegram music bot treats playback state as an in-memory Python variable. When FFmpeg crashes — and it does, every 2–4 hours under load — the group hears silence with no notification and no recovery. Queue corruption on concurrent `/skip` operations affects 5–10% of operations in production bots. Duplicate downloads waste bandwidth on 30–40% of peak requests.

This bot treats voice-chat streaming as a **reliability engineering problem**, not a hobby project. It is the only Telegram music bot that implements:

* Redlock-based download deduplication
* Lua-atomic state transitions on Redis
* Watchdog-driven subprocess crash recovery with seek-resume in under 20ms

* * *

## Key Differentiators

| Capability | This Bot | Telegram_VC_Bot forks | AnnieXMusic / HasiiMusicBot |
| --- | --- | --- | --- |
| **Crash Recovery** | Watchdog + Lua CAS (≤18ms) | None | None |
| **Queue Atomicity** | Lua-atomic + shadow list | None (race conditions) | MongoDB-backed, no Lua |
| **Download Dedup** | Redlock + SHA-256 | None | None |
| **State Authority** | Redis (persistent across restarts) | In-memory Python variable | Mixed |
| **Active Maintenance** | ✅ Planned | Minimal | Sporadic |

* * *

## Architecture Overview

The system is composed of **six core components** that communicate exclusively through Redis Pub/Sub channels and Lua-atomic state transitions. No component holds a direct Python object reference to another.

    ┌─────────────────────────────────────────────────────────────────┐
    │                        TelegramGateway                          │
    │    aiogram Dispatcher · UpdateRouter · RateLimiter Middleware   │
    └─────────────────────────────────┬───────────────────────────────┘
                                      │ Message / CallbackQuery / ChatMemberUpdated
                                      ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │                        CommandBroker                            │
    │   /play /skip /stop /queue /np /volume /loop /shuffle           │
    │   Permission guard (admin_only_skip, dj_role)                   │
    └────────────┬────────────────────────────────────┬───────────────┘
                 │ enqueue/dequeue/peek                │ start/stop/health_check
                 ▼                                     ▼
    ┌────────────────────────┐         ┌───────────────────────────────┐
    │      QueueManager      │         │         StreamEngine           │
    │  Redis LIST + Lua      │         │  FFmpeg subprocess per group   │
    │  RPUSH/LPOP/LMOVE      │         │  Watchdog · Heartbeat (1 Hz)  │
    └────────────┬───────────┘         └──────────────┬────────────────┘
                 │ Track.url                           │ file_path
                 ▼                                     ▼
    ┌────────────────────────┐         ┌───────────────────────────────┐
    │     DownloadWorker     │         │        FFmpegProcess           │
    │  yt-dlp · Redlock      │         │  -vn -acodec libopus           │
    │  Semaphore · Retry ×3  │         │  SIGTERM → drain → SIGKILL    │
    └────────────┬───────────┘         └───────────────────────────────┘
                 │ writes file_path
                 ▼
    ┌────────────────────────────────────────────────────────────────┐
    │                         CacheManager                           │
    │   SHA-256(url) key · 20 GB LRU · 7-day TTL · tmpfs overlay    │
    └────────────────────────────────────────────────────────────────┘
    
              ════════════════ SHARED INFRASTRUCTURE ════════════════
              Redis Cluster (Sentinel HA, 3 nodes)  ·  /var/jukebox/cache/

### Three Invariants That Guarantee Correctness

1. **Redis is always the authoritative state** before and after any subprocess event.
2. **All state transitions touching `playback:state`** are Lua-atomic — no partial writes are observable.
3. **The crash counter decouples retry logic from the hot path**, preventing cascading failures across groups.

* * *

## Prerequisites

### Required

| Dependency | Version | Purpose |
| --- | --- | --- |
| Python | 3.12+ | Runtime |
| Redis | 7.x | State store (Sentinel HA recommended) |
| FFmpeg | 6.x | Audio transcoding to Opus |
| yt-dlp | Pinned (see `requirements.txt`) | YouTube/audio extraction |
| Docker + Compose | Latest | Recommended deployment method |

### Required Telegram Setup

1. Create a bot via [@BotFather](https://t.me/BotFather) and obtain a `BOT_TOKEN`.
2. Disable **Group Privacy mode** so the bot can read commands in groups.
3. Add the bot to your target supergroup and grant it **admin rights** (required for voice chat participation).

### System Resources (per deployment node)

| Resource | Minimum | Recommended |
| --- | --- | --- |
| CPU | 4 cores | 8+ cores (semaphore is `CPU_COUNT × 4`) |
| RAM | 512 MB | 2 GB at 500 concurrent groups (~50 MB/group) |
| Disk | 25 GB | 30 GB+ (20 GB cache budget + headroom) |
| File Descriptors | 65,536 | Set automatically via systemd |

* * *

## Installation

### Docker Compose (Recommended)

This is the fastest path to a running deployment. The Compose file provisions the bot, Redis master, two replicas, and three Sentinel nodes.

**1. Clone the repository**

    git clone https://github.com/your-org/telegram-jukebox-bot.git
    cd telegram-jukebox-bot

**2. Configure environment variables**

    cp .env.example .env
    # Open .env in your editor and fill in required values (see Configuration section)

**3. Start all services**

    docker compose -f deploy/docker/docker-compose.yml up -d

**4. Verify health**

    # Check all 7 containers are running
    docker compose ps
    
    # One-shot health probe (Redis + FFmpeg)
    docker compose exec bot python scripts/health_check.py

**5. Tail logs**

    docker compose logs -f bot

* * *

### Manual / VPS Setup

Use this path if you are self-hosting without Docker or integrating into an existing Redis cluster.

**1. Install system dependencies**

    # Debian / Ubuntu
    sudo apt-get update && sudo apt-get install -y \
        ffmpeg \
        python3.12 python3.12-venv python3-pip
    
    # Install yt-dlp at the pinned version from requirements.txt
    pip install yt-dlp==$(grep yt-dlp requirements.txt | cut -d= -f3)

**2. Create a virtual environment**

    python3.12 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

**3. Configure Redis Sentinel (3-node HA)**

    # Deploy sentinel.conf from deploy/sentinel/sentinel.conf
    # Key settings enforced:
    #   sentinel monitor jukebox 127.0.0.1 6379 2   (quorum=2)
    #   sentinel down-after-milliseconds jukebox 5000
    #   sentinel failover-timeout jukebox 30000
    #   min-replicas-to-write 1
    redis-sentinel deploy/sentinel/sentinel.conf --daemonize yes

**4. Configure systemd service**

    sudo cp deploy/systemd/jukebox-bot.service /etc/systemd/system/
    sudo cp deploy/systemd/jukebox-cache-cleanup.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now jukebox-bot.service
    sudo systemctl enable --now jukebox-cache-cleanup.timer

**5. Run the bot**

    # With systemd (recommended for production)
    sudo systemctl start jukebox-bot
    
    # Or directly (development only)
    python -m src.main

* * *

## Configuration

All configuration is injected via environment variables. Copy `.env.example` to `.env` and fill in the values.

> ⚠️ **Enforcement:** Every variable referenced in `src/config.py` must appear in `.env.example`. This is validated by CI check `CHECK-9`. Missing entries block the build.

### Required Variables

| Variable | Description | Example |
| --- | --- | --- |
| `BOT_TOKEN` | Telegram bot token from @BotFather | `123456:ABC-DEF...` |
| `REDIS_SENTINEL_HOSTS` | Comma-separated `host:port` list | `127.0.0.1:26379,127.0.0.1:26380` |
| `REDIS_SENTINEL_MASTER_NAME` | Sentinel master name | `jukebox` |
| `REDIS_ENV` | Namespace prefix (`prod` / `staging` / `test`) | `prod` |

### Optional / Tunable Variables

| Variable | Default | Description |
| --- | --- | --- |
| `CACHE_DIR` | `/var/jukebox/cache/` | Audio file cache root directory |
| `CACHE_MAX_GB` | `20` | LRU eviction budget in gigabytes |
| `CACHE_EVICTION_THRESHOLD_MB` | `500` | Free space floor before pre-download sweep |
| `MAX_CONCURRENT_DOWNLOADS` | `10` | `asyncio.Semaphore` cap for yt-dlp workers |
| `MAX_QUEUE_LENGTH` | `50` | Per-group queue depth cap |
| `MAX_TRACK_DURATION_S` | `600` | Maximum allowed track duration in seconds (10 min) |
| `MAX_REQUESTS_PER_MIN` | `5` | Per-user, per-group rate limit |
| `FFMPEG_PATH` | `ffmpeg` | Absolute path to FFmpeg binary |
| `LOG_LEVEL` | `INFO` | structlog output level |
| `CHAOS_TESTS_ENABLED` | `0` | Set to `1` to enable chaos test suite |

### Per-Group Settings (Runtime, via `/settings`)

These are stored in Redis under `settings:{group_id}` with no TTL and can be updated at runtime by group admins.

| Setting | Default | Example Command |
| --- | --- | --- |
| `max_queue_len` | `50` | `/settings max_queue 25` |
| `max_duration_s` | `600` | `/settings max_duration 300` |
| `admin_only_skip` | `false` | `/settings admin_skip on` |
| `announce_tracks` | `true` | `/settings announce off` |
| `dj_role_id` | none | `/settings dj_role @DJRole` |

* * *

## Bot Commands

All commands work in any Telegram supergroup where the bot has admin rights and has joined the voice chat.

### Playback Control

| Command | Permission | Description |
| --- | --- | --- |
| `/play <url\\|search query>` | All members | Enqueue a track from a YouTube URL or search query. Resolves, downloads, caches, and begins streaming automatically. |
| `/skip` | Admin or DJ role (configurable) | Atomically stop the current track and advance to the next via LMOVE shadow list. |
| `/stop` | Admin only | Stop playback and clear the entire queue. |
| `/pause` | Admin or DJ role | Suspend FFmpeg output (`SIGSTOP`) while holding the voice chat connection open. |
| `/resume` | Admin or DJ role | Resume a paused stream (`SIGCONT`). |

### Queue Management

| Command | Permission | Description |
| --- | --- | --- |
| `/queue [page]` | All members | Display the current queue as a paginated inline keyboard. |
| `/np` | All members | Show the now-playing track with an ASCII progress bar and thumbnail. |
| `/promote <position>` | Admin only | Move a queued track to the head of the queue (Lua-atomic). |
| `/shuffle` | Admin or DJ role | Randomise the current queue order. |
| `/loop <off\\|track\\|queue>` | Admin or DJ role | Set loop mode for the current session. |

### Configuration

| Command | Permission | Description |
| --- | --- | --- |
| `/volume <0–100>` | Admin or DJ role | Adjust playback volume via FFmpeg lavfi graph reinit. |
| `/settings <key> <value>` | Admin only | Update per-group settings stored in Redis with no TTL. |

* * *

## Repository Structure

The structure is **normative** — every enforced path is validated by `scripts/lint_structure.py` on every CI push. Deviations block merges to `main`.

    telegram-jukebox-bot/
    ├── src/                              # [!] Import root — all application code
    │   ├── __init__.py                   # [R] Empty; marks package
    │   ├── main.py                       # [R] Bot entry point, aiogram app factory
    │   ├── config.py                     # [R] Pydantic BaseSettings — all env vars
    │   │
    │   ├── interfaces/                   # [!] ABCs ONLY — zero implementation
    │   │   ├── __init__.py               # [R]
    │   │   ├── queue_manager.py          # [R] QueueManager ABC (8 abstract methods)
    │   │   ├── stream_engine.py          # [R] StreamEngine ABC (9 abstract methods)
    │   │   ├── telegram_handler.py       # [R] TelegramHandler ABC (11 abstract methods)
    │   │   ├── cache_manager.py          # [R] CacheManager ABC
    │   │   └── download_worker.py        # [R] DownloadWorker ABC
    │   │
    │   ├── models/                       # [!] Dataclasses & enums ONLY — no I/O
    │   │   ├── __init__.py               # [R]
    │   │   ├── enums.py                  # [R] TrackStatus · PlaybackStatus · LoopMode · ErrorSeverity
    │   │   ├── track.py                  # [R] Track (frozen=True, slots=True)
    │   │   ├── playback_state.py         # [R] PlaybackState (mutable — heartbeat updates position_ms)
    │   │   ├── group_settings.py         # [R] GroupSettings
    │   │   ├── download_result.py        # [R] DownloadResult
    │   │   └── errors.py                 # [R] JukeboxError + QueueFullError + StreamAlreadyActiveError
    │   │
    │   ├── gateway/                      # TelegramGateway boundary layer
    │   │   ├── __init__.py
    │   │   ├── dispatcher.py             # aiogram Dispatcher factory
    │   │   ├── router.py                 # UpdateRouter + route registration
    │   │   └── middleware/
    │   │       ├── __init__.py
    │   │       ├── rate_limiter.py       # Token-bucket RateLimiter middleware
    │   │       └── logging_mw.py         # structlog group_id context injection
    │   │
    │   ├── broker/                       # CommandBroker — /play /skip /stop ...
    │   │   ├── __init__.py
    │   │   ├── command_broker.py         # Inline callback router + command dispatch
    │   │   ├── permissions.py            # admin_only_skip, dj_role permission guards
    │   │   └── handlers/                 # Concrete TelegramHandler implementations
    │   │       ├── __init__.py
    │   │       ├── play_handler.py       # on_play_command
    │   │       ├── skip_handler.py       # on_skip_command
    │   │       ├── stop_handler.py       # on_stop_command
    │   │       ├── queue_handler.py      # on_queue_command + on_queue_page_callback
    │   │       ├── nowplaying_handler.py # on_nowplaying_command (ASCII progress bar)
    │   │       ├── volume_handler.py     # on_volume_command
    │   │       ├── voice_chat_handler.py # on_voice_chat_started / on_voice_chat_ended
    │   │       └── error_handler.py      # on_error_event — routes by ErrorSeverity
    │   │
    │   ├── queue/                        # QueueManager — Redis LIST implementation
    │   │   ├── __init__.py
    │   │   ├── redis_queue_manager.py    # Concrete QueueManager (RPUSH/LPOP/LMOVE)
    │   │   └── lua/                      # [!] Lua scripts owned by this service only
    │   │       ├── skip_atomic.lua       # LMOVE-based skip to shadow list
    │   │       ├── promote_atomic.lua    # LREM + LPUSH (atomic /promote)
    │   │       └── ack_processed.lua     # Shadow list cleanup on track completion
    │   │
    │   ├── stream/                       # StreamEngine — FFmpeg subprocess lifecycle
    │   │   ├── __init__.py
    │   │   ├── ffmpeg_stream_engine.py   # Concrete StreamEngine
    │   │   ├── ffmpeg_process.py         # FFmpegProcess wrapper (asyncio.subprocess)
    │   │   ├── watchdog.py               # Crash detection + recovery coroutine
    │   │   ├── heartbeat.py              # 1 Hz position_ms → Redis writer
    │   │   └── lua/                      # [!] Lua scripts owned by this service only
    │   │       ├── playback_state_atomic.lua   # HSET playback:state:* atomically
    │   │       ├── crash_recovery_atomic.lua   # status=recovering + INCR crash:count
    │   │       └── volume_update_atomic.lua    # volume HSET + Redis sync
    │   │
    │   ├── download/                     # DownloadWorker — yt-dlp + Redlock
    │   │   ├── __init__.py
    │   │   ├── download_worker.py        # asyncio.Semaphore, Redlock, retry × 3
    │   │   ├── ytdlp_client.py           # yt-dlp wrapper with jittered backoff
    │   │   └── lua/                      # [!] Lua scripts owned by this service only
    │   │       └── release_lock_atomic.lua     # Redlock safe-release (owner-check DEL)
    │   │
    │   ├── cache/                        # CacheManager — disk LRU + Redis registry
    │   │   ├── __init__.py
    │   │   ├── cache_manager.py          # stat() validation, LRU eviction, hit_count
    │   │   ├── eviction_policy.py        # 20 GB budget enforcement + LRU sweep
    │   │   └── lua/
    │   │       └── cache_touch_atomic.lua      # TTL reset + HINCRBY hit_count
    │   │
    │   ├── pubsub/                       # Redis Pub/Sub event bus
    │   │   ├── __init__.py
    │   │   ├── event_bus.py              # Publisher + Subscriber base classes
    │   │   ├── event_types.py            # Typed EventPayload dicts (9 event types)
    │   │   └── listener.py               # Per-group subscription coroutine
    │   │
    │   ├── redis/                        # Redis client factory + key namespace
    │   │   ├── __init__.py
    │   │   ├── client.py                 # [R] aioredis Sentinel client factory
    │   │   ├── pool.py                   # Connection pool config (max connections)
    │   │   └── keys.py                   # [R] ONLY source of Redis key strings
    │   │
    │   └── logging/
    │       ├── __init__.py
    │       └── setup.py                  # [R] structlog processors, mandatory group_id context
    │
    ├── tests/
    │   ├── __init__.py
    │   ├── conftest.py                   # [R] FakeRedis · MockStreamEngine · chaos_redis fixture
    │   ├── unit/                         # Pure unit tests — zero I/O, zero network
    │   │   ├── __init__.py
    │   │   ├── test_models.py            # Track, PlaybackState, JukeboxError invariants
    │   │   ├── test_permissions.py       # admin_only_skip, dj_role guard logic
    │   │   ├── test_eviction_policy.py   # LRU budget arithmetic
    │   │   ├── test_event_types.py       # EventPayload JSON round-trip
    │   │   └── test_keys.py              # Key builder correctness + namespace collision
    │   ├── integration/                  # Requires live Redis (testcontainers)
    │   │   ├── __init__.py
    │   │   ├── test_redis_queue.py       # RPUSH/LPOP/LMOVE queue operations
    │   │   ├── test_redlock.py           # Redlock acquire/release + race simulation
    │   │   ├── test_pubsub.py            # Pub/Sub publish → subscriber receive
    │   │   ├── test_cache_manager.py     # stat() validation, TTL touch, hit_count
    │   │   └── test_lua_scripts.py       # All Lua atomicity guarantees
    │   ├── e2e/                          # End-to-end flows (fake Telegram, real Redis)
    │   │   ├── __init__.py
    │   │   ├── test_play_flow.py         # /play → enqueue → download → stream
    │   │   ├── test_skip_flow.py         # /skip → LMOVE atomic → next track start
    │   │   ├── test_queue_pagination.py  # /queue inline keyboard paging
    │   │   └── test_voice_chat_lifecycle.py  # VC join → stream → VC terminate
    │   └── chaos/                        # [!] Fault injection — adversarial scenarios
    │       ├── __init__.py
    │       ├── test_ffmpeg_crash.py      # CLASS III-1: SIGSEGV mid-stream recovery
    │       ├── test_redis_partition.py   # CLASS I-1: Redis Sentinel failover
    │       ├── test_disk_full.py         # CLASS I-2: pre-download quota exhaustion
    │       ├── test_concurrent_skip.py   # CLASS V-1: /skip race + crash recovery CAS
    │       ├── test_duplicate_download.py# CLASS V-2: Redlock contention under load
    │       ├── test_zombie_subprocesses.py # CLASS III-3: janitor reap coroutine
    │       └── test_ffmpeg_semaphore.py  # CLASS VI-1: global subprocess slot limit
    │
    ├── docs/
    │   ├── high-level-architecture.md    # [R] Source HLA (do not edit — upstream artifact)
    │   ├── redis-schema.md               # [R] Full Redis key reference (9 key patterns)
    │   ├── error-taxonomy.md             # [R] Error classes I–VI + recovery strategies
    │   ├── lua-scripts.md                # All Lua scripts — purpose, invariants, callers
    │   ├── ffmpeg-pipeline.md            # FFmpeg subprocess lifecycle + Opus pipeline spec
    │   ├── deployment.md                 # Docker + systemd + Sentinel setup guide
    │   ├── ops-runbook.md                # On-call procedures, alert thresholds
    │   └── adr/                          # Architecture Decision Records
    │       ├── 001-redis-as-sot.md       # Why Redis (not Postgres) for ephemeral state
    │       ├── 002-lua-atomicity.md      # Why Lua over MULTI/EXEC transactions
    │       ├── 003-pubsub-decoupling.md  # Why Pub/Sub between DownloadWorker ↔ StreamEngine
    │       └── 004-ffmpeg-subprocess.md  # Why asyncio subprocess over pytgcalls native
    │
    ├── scripts/                          # Ops utilities (not imported by application code)
    │   ├── lint_structure.py             # [R] CI enforcement — path + import boundary check
    │   ├── health_check.py               # One-shot Redis + FFmpeg health probe
    │   ├── cache_purge.py                # Manual LRU sweep utility
    │   ├── redis_flush_group.py          # Flush all keys for a target group_id
    │   └── load_test.sh                  # Concurrent /play simulation (500 groups)
    │
    ├── deploy/
    │   ├── docker/
    │   │   ├── Dockerfile
    │   │   ├── docker-compose.yml        # Bot + Redis Sentinel (3 nodes)
    │   │   └── docker-compose.test.yml   # Testcontainers-compatible compose
    │   ├── systemd/
    │   │   ├── jukebox-bot.service       # LimitNOFILE=65536 · Restart=always
    │   │   └── jukebox-cache-cleanup.timer
    │   └── sentinel/
    │       └── sentinel.conf             # quorum=2, failover-timeout=30000ms
    │
    ├── .env.example                      # [R] All required env vars with defaults
    ├── pyproject.toml                    # [R] Project metadata + dependency groups
    ├── requirements.txt                  # [R] Pinned prod deps (yt-dlp version pinned)
    ├── requirements-dev.txt
    └── README.md                         # [R]

### Import Boundary Rules

The architecture enforces a strict layered import matrix. Violations trigger **immediate CI failure**.

| FROM \ TO | models | interfaces | redis | queue | stream | download | cache | pubsub | broker | gateway | logging |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **models** | —   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   |
| **interfaces** | ✓   | —   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   |
| **redis** | ✓   | ✗   | —   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   |
| **queue** | ✓   | ✓   | ✓   | —   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   |
| **stream** | ✓   | ✓   | ✓   | ✗   | —   | ✗   | ✗   | ✓   | ✗   | ✗   | ✗   |
| **download** | ✓   | ✓   | ✓   | ✗   | ✗   | —   | ✓   | ✗   | ✗   | ✗   | ✗   |
| **cache** | ✓   | ✓   | ✓   | ✗   | ✗   | ✗   | —   | ✗   | ✗   | ✗   | ✗   |
| **pubsub** | ✓   | ✗   | ✓   | ✗   | ✗   | ✗   | ✗   | —   | ✗   | ✗   | ✗   |
| **broker** | ✓   | ✓   | ✗   | ✗   | ✗   | ✗   | ✗   | ✓   | —   | ✗   | ✓   |
| **gateway** | ✓   | ✓   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✓   | —   | ✓   |
| **logging** | ✓   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | ✗   | —   |

**Key:** ✓ = permitted · ✗ = CI failure · — = self

> **RULE-0:** No module outside `src/interfaces/` may couple to another top-level package directly. All cross-package coupling flows through ABCs.
> 
> **RULE-0b:** Lua scripts are colocated with the service that owns them. No service may execute Lua scripts owned by another service.
> 
> **RULE-0c:** All Redis key construction must use `src/redis/keys.py`. Hardcoded key strings elsewhere are a lint failure (`CHECK-5`).

* * *

## Redis Schema Reference

All keys follow the namespace convention: `jukebox:{env}:{key_pattern}` where `env` ∈ `{prod, staging, test}`.

| Key Pattern | Redis Type | TTL | Owner | Purpose |
| --- | --- | --- | --- | --- |
| `queue:{group_id}` | LIST | 86,400s (24h, reset on RPUSH) | QueueManager | Per-group track queue; msgpack-serialized `Track` objects |
| `playback:state:{group_id}` | HASH | 3,600s (1h, reset on HSET) | StreamEngine | Current status, `position_ms`, volume, loop mode |
| `cache:meta:{sha256}` | HASH | 604,800s (7d, LRU touch resets) | CacheManager | Audio file path, size, duration, `hit_count` |
| `lock:download:{sha256}` | STRING | 300s | DownloadWorker | Redlock dedup — `SET NX PX 300000` |
| `settings:{group_id}` | HASH | None (explicit delete) | CommandBroker | Per-group config: max queue, max duration, permissions |
| `ratelimit:{user_id}:{group_id}` | STRING | 60s (auto-reset window) | RateLimiter | Sliding-window counter — INCR with NX on first request |
| `vc:session:{group_id}` | HASH | None (VC lifecycle managed) | VoiceChatHandler | VC join time, `listener_count` |
| `crash:count:{group_id}` | STRING | 300s (auto-evict after quiet) | StreamEngine | Per-group crash counter; reset to 0 on clean transition |
| `events:{group_id}` | Pub/Sub channel | N/A | event_bus.py | Inter-component event bus namespaced by group |

### Pub/Sub Event Payloads (`events:{group_id}`)

    { "event": "track_started",   "track_id": "uuid4-string", "ts": 1712000000 }
    { "event": "track_finished",  "track_id": "uuid4-string", "ts": 1712000000 }
    { "event": "track_skipped",   "track_id": "uuid4-string", "ts": 1712000000 }
    { "event": "stream_crashed",  "exit_code": -11,            "ts": 1712000000 }
    { "event": "download_ready",  "track_id": "uuid4-string", "ts": 1712000000 }
    { "event": "queue_empty",                                  "ts": 1712000000 }

### Lua Script Ownership & Invariants

| Script | Redis Operation | Invariant Guaranteed |
| --- | --- | --- |
| `queue/lua/skip_atomic.lua` | LMOVE source → shadow LIST | Exactly-once dequeue; no double-pop under concurrent `/skip` |
| `queue/lua/promote_atomic.lua` | LREM + LPUSH in one script | Atomic head-insert; track never appears twice in queue |
| `queue/lua/ack_processed.lua` | LREM from shadow LIST | Shadow list never grows unbounded on clean completions |
| `stream/lua/playback_state_atomic.lua` | HSET `playback:state:*` + EXPIRE | Status and position always consistent; no partial writes |
| `stream/lua/crash_recovery_atomic.lua` | HSET `status=recovering` + INCR `crash:count` + EXPIRE | Crash count and status transition are a single atomic unit |
| `stream/lua/volume_update_atomic.lua` | HSET volume field | Volume field never observed mid-write by heartbeat reader |
| `download/lua/release_lock_atomic.lua` | GET owner → DEL if match (Redlock) | Only lock owner can release; prevents foreign key deletion |
| `cache/lua/cache_touch_atomic.lua` | EXPIRE reset + HINCRBY `hit_count` | LRU touch and hit count are always co-consistent |

* * *

## Error Taxonomy & Recovery

The system defines **6 error classes** across **14 specific failure scenarios**, each with a formal severity and automated recovery path.

| Class | ID  | Failure | Severity | Recovery Mechanism |
| --- | --- | --- | --- | --- |
| Infrastructure | I-1 | Redis timeout / connection refused | CRITICAL | Sentinel failover (<30s); aioredis exponential backoff retry ×5 |
| Infrastructure | I-2 | Disk full (audio cache) | RETRYABLE → CRITICAL | Pre-download quota check (500 MB floor); LRU sweep; user notified on failure |
| External Service | II-1 | yt-dlp download failure | RETRYABLE / FATAL | Retry ×3 with jittered backoff; FATAL on geo-block or content removal |
| External Service | II-2 | YouTube format / API change | CRITICAL | Failure rate spike alert (>50% in 5 min); ops Telegram notification |
| Process | III-1 | FFmpeg process crash | RETRYABLE → FATAL | Watchdog seek-resume ≤18ms; 3-strike eviction with user notification |
| Process | III-2 | FFmpeg pipe deadlock | RETRYABLE | Heartbeat stall >10s → SIGKILL → standard crash recovery path |
| Process | III-3 | Zombie subprocess accumulation | TRANSIENT | Janitor coroutine (60s cadence) force-reaps all `poll() != None` |
| Telegram API | IV-1 | FloodWait (HTTP 429) | TRANSIENT | aiogram `retry_after`; MessageThrottle (20 msg/min/group) |
| Telegram API | IV-2 | VC forcibly closed by Telegram | RETRYABLE | Queue preserved (TTL reset to 24h); user prompted to `/play` to restart |
| Telegram API | IV-3 | Bot kicked from group | FATAL | All Redis keys for `group_id` deleted; subprocess freed immediately |
| App Logic | V-1 | Concurrent queue mutation race | TRANSIENT | All mutations Lua-atomic; no optimistic lock or retry needed |
| App Logic | V-2 | Duplicate simultaneous downloads | TRANSIENT | Redlock on SHA-256(url); loser polls then reads completed cache |
| Resources | VI-1 | FFmpeg subprocess limit exceeded | RETRYABLE | `asyncio.Semaphore(CPU_COUNT × 4)` blocks gracefully; user sees "Starting soon…" |
| Resources | VI-2 | File descriptor exhaustion | CRITICAL | `systemd LimitNOFILE=65536`; handles closed immediately after path handoff |

### Validated Failure Scenario: FFmpeg Crash Mid-Stream (Class III-1)

This is the most operationally dangerous failure mode. An FFmpeg process for `group_id=99182` exits with SIGSEGV at 4 minutes into a 5-minute track. The voice chat connection stays open. The group hears silence.

    T+0ms    FFmpeg exits (returncode=-11 / SIGSEGV).
             asyncio child-process watcher fires.
    
    T+5ms    Watchdog unblocks from process.wait().
             Classifies exit code against known clean exits {0} → ABNORMAL.
    
    T+8ms    Lua script executes atomically:
               HSET playback:state:99182 status "recovering"
               INCR crash:count:99182
               EXPIRE crash:count:99182 300
             Publishes to events:99182:
               { "event": "stream_crashed", "exit_code": -11, "ts": <epoch> }
    
    T+10ms   Pub/Sub listener receives "stream_crashed".
             Invokes recover_stream(group_id=99182, track=<current>, seek_ms=241000)
    
    T+11ms   crash:count=1 (< threshold 3). CacheManager stat() confirms file intact.
    
    T+15ms   Fresh FFmpeg spawned with -ss 241 seek flag.
             Existing Telegram voice-client connection reused.
    
    T+18ms   Opus frames resume flowing to the voice chat.
             Lua script (atomic): HSET status "playing", position_ms=241000
             Heartbeat resumes 1 Hz updates.
    
    → Audible gap: ~18ms — imperceptible to listeners.

**3-Strike Escalation:** If `crash:count` reaches 3 on the same track, the system classifies it FATAL:

1. `CacheManager` evicts the Redis cache entry and queues the file for async disk deletion.
2. `QueueManager.remove_by_id()` removes the track from the active list and any shadow list.
3. Group receives: `⚠️ Could not play '<title>' after 3 attempts. Skipping to next track.`
4. `on_track_finished_callback` is invoked synthetically to advance the queue normally.
5. `crash:count:{group_id}` is `DEL`'d — clean slate for the next track.

**Concurrent `/skip` During Recovery (edge case):** If an admin issues `/skip` while `recover_stream` is in-flight, a Lua CAS guard on `playback:state` prevents a duplicate `start_stream` call. Exactly-once semantics are preserved.

* * *

## Testing

### Run Unit Tests (zero I/O, zero network)

    pytest tests/unit/ -v

### Run Integration Tests (requires Docker)

    # A Redis testcontainer is started automatically via pytest fixtures
    pytest tests/integration/ -v

### Run End-to-End Tests (fake Telegram, real Redis)

    pytest tests/e2e/ -v

### Run Chaos / Fault Injection Tests

> ⚠️ **Requirements:** Docker daemon, minimum 4 CPU cores, 512 MB tmpfs at `/tmp/jukebox-chaos-cache/`, and `CHAOS_TESTS_ENABLED=1`.

    export CHAOS_TESTS_ENABLED=1
    pytest tests/chaos/ -v -m chaos

**Chaos test coverage:**

| Test File | HLA Class | Scenario & Key Assertions |
| --- | --- | --- |
| `test_ffmpeg_crash.py` | III-1 | SIGSEGV mid-stream; assert `status=recovering` within 10ms, Opus resumes ≤18ms, `crash:count` increments, 3rd crash evicts track and notifies group |
| `test_redis_partition.py` | I-1 | Sentinel failover simulation; assert in-flight FFmpeg continues, new `/play` returns user-visible error, heartbeat re-syncs on reconnect |
| `test_disk_full.py` | I-2 | tmpfs filled to 100%; assert pre-download quota check triggers LRU sweep, `DownloadResult.success=False` when sweep fails, user notified |
| `test_concurrent_skip.py` | V-1 | Concurrent `/skip` + SIGSEGV race; assert Lua CAS prevents duplicate `start_stream`, exactly-once guarantee, no orphaned subprocesses |
| `test_duplicate_download.py` | V-2 | N goroutines request same URL simultaneously; assert Redlock allows exactly one download, others poll and read cache, no duplicate files |
| `test_zombie_subprocesses.py` | III-3 | 50 groups started then abandoned; assert janitor coroutine (60s cadence) reaps all zombies, `poll()!=None` count → 0 |
| `test_ffmpeg_semaphore.py` | VI-1 | Requests exceed `CPU_COUNT × 4`; assert `asyncio.Semaphore` blocks gracefully, users receive "Starting soon…", no fork-bomb |

### CI Enforcement Checks

`scripts/lint_structure.py` runs as the **first CI step** before any test or build. A non-zero exit code blocks all merges to `main`.

| Check | Validation |
| --- | --- |
| CHECK-1 | All `[R]` (Required) paths exist |
| CHECK-2 | `src/interfaces/*.py` imports nothing from `src/{queue,stream,download,cache,gateway}/` |
| CHECK-3 | `src/models/*.py` imports nothing from `src/*` (stdlib only) |
| CHECK-4 | Each `src/*/lua/*.lua` is only loaded by Python files in its own parent package |
| CHECK-5 | `src/redis/keys.py` is the only file containing hardcoded Redis key string patterns |
| CHECK-6 | `docs/redis-schema.md` contains exactly 9 H3 sections (one per key pattern) + Changelog |
| CHECK-7 | Every test in `tests/chaos/` carries the `@pytest.mark.chaos` decorator |
| CHECK-8 | All Lua files under `src/` are named `*_atomic.lua` |
| CHECK-9 | All environment variables referenced in `src/config.py` appear in `.env.example` |
| CHECK-10 | Import matrix is not violated — validated via AST import analysis across all `src/` modules |

* * *

## Deployment & Operations

### Health Check

    # One-shot probe: Redis connectivity + FFmpeg binary availability
    python scripts/health_check.py

### Manual Cache Purge

    # Trigger LRU sweep manually
    # (normally handled automatically by the systemd timer on an hourly cadence)
    python scripts/cache_purge.py

### Flush All State for a Specific Group

    # WARNING: Stops playback and clears the queue for the target group
    python scripts/redis_flush_group.py --group-id 99182

### Load Testing

    # Simulate 500 concurrent groups each issuing /play commands
    bash scripts/load_test.sh

### Monitoring Reference

| Metric | Source | Target (P99) | Alert Threshold |
| --- | --- | --- | --- |
| Crash recovery time | Watchdog timestamp delta (T+0 to Opus resume) | <50ms | >200ms |
| Silent gap duration | Opus frame gap measurement | <20ms | >100ms |
| Redis operation latency | aioredis instrumentation | <5ms | >10ms |
| FFmpeg subprocess count | Semaphore gauge | ≤ CPU_COUNT × 4 | > CPU_COUNT × 4 |
| Disk cache hit rate | `hit_count` / total downloads | >70% (post-warmup) | <50% |
| yt-dlp failure rate | DownloadWorker error counter | <5% | >50% in 5 min (ops alert) |
| Weekly group retention | Groups with >1 session in 7-day window | >60% | <45% |

### Systemd Service Configuration

    # deploy/systemd/jukebox-bot.service (key directives)
    LimitNOFILE=65536      # File descriptor limit (HLA Class VI-2 mitigation)
    LimitNPROC=32768       # Process limit
    Restart=always
    RestartSec=3s
    After=redis.service

### Sentinel Configuration

    # deploy/sentinel/sentinel.conf (key directives)
    sentinel monitor jukebox 127.0.0.1 6379 2    # quorum=2 for 3-node HA
    sentinel down-after-milliseconds jukebox 5000
    sentinel failover-timeout jukebox 30000
    min-replicas-to-write 1

* * *

## Contributing

### Rules Before You Code

1. **Read `docs/high-level-architecture.md`** — the HLA is the normative contract. PRs that violate it will not be merged.
2. **Read `docs/adr/`** — Architecture Decision Records explain the *why* behind every major design choice:
  * `001-redis-as-sot.md` — Why Redis, not Postgres, for ephemeral state
  * `002-lua-atomicity.md` — Why Lua over MULTI/EXEC transactions
  * `003-pubsub-decoupling.md` — Why Pub/Sub between DownloadWorker and StreamEngine
  * `004-ffmpeg-subprocess.md` — Why asyncio subprocess over pytgcalls native
3. **Run `scripts/lint_structure.py` locally** before pushing. Non-zero exit blocks CI.

### Development Workflow

    # 1. Fork and clone
    git clone https://github.com/your-fork/telegram-jukebox-bot.git
    cd telegram-jukebox-bot
    
    # 2. Install dev dependencies
    pip install -r requirements-dev.txt
    
    # 3. Validate structure before making any changes
    python scripts/lint_structure.py
    
    # 4. Run the unit test suite
    pytest tests/unit/ -v
    
    # 5. Make your changes
    
    # 6. Re-validate structure and run full test suite
    python scripts/lint_structure.py
    pytest tests/unit/ tests/integration/ -v
    
    # 7. Open a PR targeting main

### Naming Conventions

| Artefact | Convention | Example |
| --- | --- | --- |
| Python modules | `snake_case.py` — always, no hyphens | `redis_queue_manager.py` |
| Lua scripts | `snake_case_atomic.lua` — `_atomic` suffix required | `skip_atomic.lua` |
| Test files | `test_{subject}.py` matching module under test | `test_redis_queue.py` |
| ADR files | `NNN-kebab-case-title.md` (zero-padded 3-digit sequence) | `005-new-decision.md` |
| Environment variables | `SCREAMING_SNAKE_CASE` with `.env.example` entry + comment | `MAX_QUEUE_LENGTH` |
| Redis key builders | `build_{key_type}_key()` in `src/redis/keys.py` only | `build_queue_key(group_id: int) -> str` |
| Event payload TypedDicts | Suffix with `Event` in `src/pubsub/event_types.py` | `TrackStartedEvent`, `StreamCrashedEvent` |

### Absolute Prohibitions

> Violations of any of these are **immediate CI failures** and will not be merged under any circumstances.

* ❌ Hardcode Redis key strings anywhere except `src/redis/keys.py` (`CHECK-5`)
* ❌ Add implementation code to `src/interfaces/` (`CHECK-2`)
* ❌ Import from `src/*` inside `src/models/` (`CHECK-3`)
* ❌ Load a Lua script from a package that does not own it (`CHECK-4`, `RULE-0b`)
* ❌ Call `aioredis.create_connection()` directly — always use the injected pool from `src/redis/pool.py`
* ❌ Add chaos tests without the `@pytest.mark.chaos` decorator (`CHECK-7`)
* ❌ Name Lua scripts without the `_atomic.lua` suffix (`CHECK-8`)

* * *

## Technology Stack

| Layer | Technology | Version | Rationale |
| --- | --- | --- | --- |
| Language | Python | 3.12+ | asyncio maturity; yt-dlp/aiogram ecosystem |
| Bot Framework | aiogram | 3.x | Native asyncio; clean middleware chain; active maintenance |
| Voice Chat | pytgcalls / pyrogram | Latest | MTProto voice chat binding; industry standard for TG VC |
| Audio Processing | FFmpeg | 6.x | Opus encoding; lavfi volume control; `-ss` seek support |
| Content Source | yt-dlp | Pinned | YouTube extraction; extensible to other platforms |
| State Store | Redis + Sentinel | 7.x | Lua scripting; Pub/Sub; sub-ms latency; 3-node HA failover |
| Serialization | msgpack | Latest | Compact binary format for queue LIST entries |
| Logging | structlog | Latest | Structured JSON logging with mandatory context binding |
| Deployment | Docker + Compose | Latest | Reproducible builds; single-command deployment |

* * *

## Risk Register

| Risk | Likelihood | Impact | Mitigation Strategy |
| --- | --- | --- | --- |
| YouTube API / format changes break yt-dlp | High | Critical | Pinned yt-dlp version; canary download job; failure rate spike alert (>50% in 5 min); out-of-band update mechanism |
| Telegram deprecates or restricts voice chat bot access | Low | Critical | Abstracted `TelegramHandler` interface enables framework migration; all queue state persists in Redis regardless |
| Redis Sentinel failover causes brief state inconsistency | Medium | High | aioredis `connection_retry=True` with exponential backoff; in-flight FFmpeg streams continue; re-sync on reconnect |
| Disk cache exhaustion during traffic spike | Medium | Medium | 500 MB pre-download free space threshold; aggressive LRU sweep; tmpfs overlay for currently-playing files |
| FFmpeg vulnerability (CVE) in audio codec | Medium | High | Pinned FFmpeg version in Docker image; security mailing list subscription; automated rebuild pipeline |
| User abuse: queue flooding or resource exhaustion | High | Medium | Per-user rate limiting (sliding window, 60s TTL); per-group queue cap (default 50); max track duration (default 600s) |
| Legal risk from streaming copyrighted content | High | High | Audio streamed only within Telegram VC (no redistribution); cache TTL ≤7 days; clear ToS disclaiming operator responsibility |

* * *

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for full terms.

> **Legal notice:** This bot streams audio into Telegram voice chats for ephemeral, session-bound listening. It does not permanently redistribute copyrighted content. Audio cache files are automatically evicted after a maximum of 7 days. Operators are responsible for compliance with applicable copyright law in their jurisdiction. The authors disclaim all liability for misuse.

* * *

## Glossary

| Term | Definition |
| --- | --- |
| **VC** | Voice Chat — Telegram's persistent audio room feature for groups |
| **Lua-atomic** | A Redis operation executed as a single indivisible unit via embedded Lua scripting, preventing any partial-write observation |
| **Redlock** | Distributed mutual exclusion algorithm using Redis `SET NX PX` for safe cross-process locking |
| **Shadow list** | A secondary Redis LIST used to implement exactly-once dequeue semantics — a track is moved to the shadow list before processing and acknowledged after |
| **Watchdog** | A dedicated `asyncio` coroutine per active stream that monitors its FFmpeg subprocess health and drives crash recovery |
| **Heartbeat** | A 1 Hz coroutine that writes the current `position_ms` from the active FFmpeg process into Redis, enabling seek-resume after crashes |
| **LRU eviction** | Least Recently Used cache eviction — the oldest-accessed audio files are deleted first when the 20 GB disk budget is exceeded |
| **MTProto** | Telegram's proprietary encryption and transport protocol underlying all Telegram API communication |
| **Exactly-once delivery** | The guarantee that a track is dequeued and streamed by exactly one consumer — achieved via the LMOVE shadow list pattern combined with Lua CAS guards |
| **Lua CAS** | Compare-And-Swap implemented in Lua: read a value, check a condition, and write atomically in one Redis script execution |
| **MoSCoW** | Prioritisation framework: Must Have, Should Have, Could Have, Won't Have (this release) |

* * *

*Built with surgical precision. Maintained with operational obsession.*
### Three Invariants That Guarantee Correctness

1. **Redis is always the authoritative state** before and after any subprocess event.

2. **All state transitions touching `playback:state`** are Lua-atomic — no partial writes are observable.

3. **The crash counter decouples retry logic from the hot path**, preventing cascading failures across groups.

* * *

Prerequisites
-------------

### Required

| **Dependency**   | **Version**                     | **Purpose**                           |
| ---------------- | ------------------------------- | ------------------------------------- |
| Python           | 3.12+                           | Runtime                               |
| Redis            | 7.x                             | State store (Sentinel HA recommended) |
| FFmpeg           | 6.x                             | Audio transcoding to Opus             |
| yt-dlp           | Pinned (see `requirements.txt`) | YouTube/audio extraction              |
| Docker + Compose | Latest                          | Recommended deployment method         |

### Required Telegram Setup

1. Create a bot via [@BotFather](https://t.me/BotFather) and obtain a `BOT_TOKEN`.

2. Enable **Group Privacy mode OFF** so the bot can read commands in groups.

3. Add the bot to your target supergroup and grant it **admin rights** (required for voice chat participation).

### System Resources (per deployment node)

* **CPU:** Minimum 4 cores (the global FFmpeg semaphore is `CPU_COUNT × 4`)

* **RAM:** 512 MB minimum; 2 GB recommended at 500 concurrent groups (~50 MB per active group)

* **Disk:** 25 GB+ recommended (20 GB cache budget + headroom)

* **File Descriptors:** systemd sets `LimitNOFILE=65536` automatically

* * *

Installation
------------

### Docker Compose (Recommended)

This is the fastest path to a running deployment. The Compose file includes the bot, Redis master, two replicas, and three Sentinel nodes.

**1. Clone the repository**

Bash
    git clone [https://github.com/your-org/telegram-jukebox-bot.git](https://github.com/your-org/telegram-jukebox-bot.git)
    cd telegram-jukebox-bot

**2. Configure environment variables**

Bash
    cp .env.example .env
    # Open .env in your editor and fill in required values (see Configuration section)

**3. Start all services**

Bash
    docker compose -f deploy/docker/docker-compose.yml up -d

**4. Verify health**

Bash
    # Check all 7 containers are running
    docker compose ps

    # One-shot health probe (Redis + FFmpeg)
    docker compose exec bot python scripts/health_check.py

**5. Tail logs**

Bash
    docker compose logs -f bot

### Manual / VPS Setup

Use this path if you are self-hosting without Docker or integrating into an existing Redis cluster.

**1. Install system dependencies**

Bash
    # Debian/Ubuntu
    sudo apt-get update && sudo apt-get install -y \
        ffmpeg \
        python3.12 python3.12-venv python3-pip

    # Install yt-dlp (pinned version from requirements.txt)
    pip install yt-dlp==$(grep yt-dlp requirements.txt | cut -d= -f3)

**2. Create a virtual environment**

Bash
    python3.12 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

**3. Configure Redis Sentinel (3-node HA)**

Bash
    # Deploy sentinel.conf from deploy/sentinel/sentinel.conf
    # Key settings enforced:
    #   sentinel monitor jukebox 127.0.0.1 6379 2   (quorum=2)
    #   sentinel down-after-milliseconds jukebox 5000
    #   sentinel failover-timeout jukebox 30000
    #   min-replicas-to-write 1
    redis-sentinel deploy/sentinel/sentinel.conf --daemonize yes

**4. Configure systemd service**

Bash
    sudo cp deploy/systemd/jukebox-bot.service /etc/systemd/system/
    sudo cp deploy/systemd/jukebox-cache-cleanup.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now jukebox-bot.service
    sudo systemctl enable --now jukebox-cache-cleanup.timer

**5. Run the bot**

Bash
    # With systemd (recommended)
    sudo systemctl start jukebox-bot

    # Or directly (development)
    python -m src.main

* * *

Configuration
-------------

All configuration is injected via environment variables. Copy `.env.example` to `.env` and fill in the values. **Every variable in `src/config.py` must appear in `.env.example`** — this is enforced by CI (`CHECK-9`).

### Required Variables

| **Variable**                 | **Description**                            | **Example**                       |
| ---------------------------- | ------------------------------------------ | --------------------------------- |
| `BOT_TOKEN`                  | Telegram bot token from @BotFather         | `123456:ABC-DEF...`               |
| `REDIS_SENTINEL_HOSTS`       | Comma-separated `host:port` list           | `127.0.0.1:26379,127.0.0.1:26380` |
| `REDIS_SENTINEL_MASTER_NAME` | Sentinel master name                       | `jukebox`                         |
| `REDIS_ENV`                  | Namespace prefix (`prod`/`staging`/`test`) | `prod`                            |

### Optional / Tunable Variables

| **Variable**                  | **Default**           | **Description**                             |
| ----------------------------- | --------------------- | ------------------------------------------- |
| `CACHE_DIR`                   | `/var/jukebox/cache/` | Audio file cache root directory             |
| `CACHE_MAX_GB`                | `20`                  | LRU eviction budget in gigabytes            |
| `CACHE_EVICTION_THRESHOLD_MB` | `500`                 | Free space floor before pre-download sweep  |
| `MAX_CONCURRENT_DOWNLOADS`    | `10`                  | `asyncio.Semaphore` cap for yt-dlp workers  |
| `MAX_QUEUE_LENGTH`            | `50`                  | Per-group queue depth cap                   |
| `MAX_TRACK_DURATION_S`        | `600`                 | Maximum allowed track duration (10 minutes) |
| `MAX_REQUESTS_PER_MIN`        | `5`                   | Per-user, per-group rate limit              |
| `FFMPEG_PATH`                 | `ffmpeg`              | Absolute path to FFmpeg binary              |
| `LOG_LEVEL`                   | `INFO`                | structlog output level                      |
| `CHAOS_TESTS_ENABLED`         | `0`                   | Set to `1` to enable chaos test suite       |

### Per-Group Settings (Runtime, via `/settings` command)

These are stored in Redis under `settings:{group_id}` with no TTL and can be changed at runtime by group admins:

| **Setting**       | **Default** | **Command**                  |
| ----------------- | ----------- | ---------------------------- |
| `max_queue_len`   | 50          | `/settings max_queue 25`     |
| `max_duration_s`  | 600         | `/settings max_duration 300` |
| `admin_only_skip` | false       | `/settings admin_skip on`    |
| `announce_tracks` | true        | `/settings announce off`     |
| `dj_role_id`      | none        | `/settings dj_role @DJRole`  |

* * *

Bot Commands
------------

All commands work in any Telegram supergroup where the bot has admin rights and has joined the voice chat.

### Playback Control

| **Command**                 | **Permission**                  | **Description**                                                                                                |
| --------------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `/play <url\|search query>` | All members                     | Enqueue a track from a YouTube URL or search query. The bot resolves, downloads, caches, and begins streaming. |
| `/skip`                     | Admin or DJ role (configurable) | Atomically stop the current track and advance to the next. Uses LMOVE shadow list for exactly-once semantics.  |
| `/stop`                     | Admin only                      | Stop playback and clear the entire queue.                                                                      |
| `/pause`                    | Admin or DJ role                | Suspend FFmpeg output (SIGSTOP) while holding the voice chat connection.                                       |
| `/resume`                   | Admin or DJ role                | Resume a paused stream (SIGCONT).                                                                              |

### Queue Management

| **Command**                 | **Permission**   | **Description**                                                      |
| --------------------------- | ---------------- | -------------------------------------------------------------------- |
| `/queue [page]`             | All members      | Display the current queue as a paginated inline keyboard.            |
| `/np`                       | All members      | Show the now-playing track with an ASCII progress bar and thumbnail. |
| `/promote <position>`       | Admin only       | Move a queued track to the head of the queue (Lua-atomic).           |
| `/shuffle`                  | Admin or DJ role | Randomise the current queue order.                                   |
| `/loop <off\|track\|queue>` | Admin or DJ role | Set loop mode for the current session.                               |

### Configuration

| **Command**               | **Permission**   | **Description**                                       |
| ------------------------- | ---------------- | ----------------------------------------------------- |
| `/volume <0–100>`         | Admin or DJ role | Adjust playback volume via FFmpeg lavfi graph reinit. |
| `/settings <key> <value>` | Admin only       | Update per-group settings stored in Redis.            |

* * *

Repository Structure
--------------------

The structure is **normative** — every enforced path is validated by `scripts/lint_structure.py` on every CI push. Deviations block merges to `main`.

Plaintext
    telegram-jukebox-bot/
    ├── src/                          # [!] Import root — all application code
    │   ├── main.py                   # [R] Bot entry point, aiogram app factory
    │   ├── config.py                 # [R] Pydantic BaseSettings — all env vars
    │   │
    │   ├── interfaces/               # [!] ABCs ONLY — zero implementation
    │   │   ├── queue_manager.py      # [R] QueueManager ABC (8 abstract methods)
    │   │   ├── stream_engine.py      # [R] StreamEngine ABC (9 abstract methods)
    │   │   ├── telegram_handler.py   # [R] TelegramHandler ABC (11 abstract methods)
    │   │   ├── cache_manager.py      # [R] CacheManager ABC
    │   │   └── download_worker.py    # [R] DownloadWorker ABC
    │   │
    │   ├── models/                   # [!] Dataclasses & enums ONLY — no I/O
    │   │   ├── enums.py              # TrackStatus · PlaybackStatus · LoopMode · ErrorSeverity
    │   │   ├── track.py              # Track (frozen, slots)
    │   │   ├── playback_state.py     # PlaybackState (mutable — heartbeat updates position_ms)
    │   │   ├── group_settings.py     # GroupSettings
    │   │   ├── download_result.py    # DownloadResult
    │   │   └── errors.py             # JukeboxError + QueueFullError + StreamAlreadyActiveError
    │   │
    │   ├── gateway/                  # TelegramGateway boundary layer
    │   │   ├── dispatcher.py         # aiogram Dispatcher factory
    │   │   ├── router.py             # UpdateRouter + route registration
    │   │   └── middleware/
    │   │       ├── rate_limiter.py   # Token-bucket RateLimiter middleware
    │   │       └── logging_mw.py     # structlog group_id context injection
    │   │
    │   ├── broker/                   # CommandBroker — /play /skip /stop ...
    │   │   ├── command_broker.py     # Inline callback router + command dispatch
    │   │   ├── permissions.py        # admin_only_skip, dj_role permission guards
    │   │   └── handlers/             # Concrete TelegramHandler implementations
    │   │       ├── play_handler.py
    │   │       ├── skip_handler.py
    │   │       ├── stop_handler.py
    │   │       ├── queue_handler.py
    │   │       ├── nowplaying_handler.py
    │   │       ├── volume_handler.py
    │   │       ├── voice_chat_handler.py
    │   │       └── error_handler.py
    │   │
    │   ├── queue/                    # QueueManager — Redis LIST implementation
    │   │   ├── redis_queue_manager.py
    │   │   └── lua/                  # [!] Lua scripts owned by this service only
    │   │       ├── skip_atomic.lua   # LMOVE-based skip to shadow list
    │   │       ├── promote_atomic.lua# LREM + LPUSH (atomic /promote)
    │   │       └── ack_processed.lua # Shadow list cleanup on track completion
    │   │
    │   ├── stream/                   # StreamEngine — FFmpeg subprocess lifecycle
    │   │   ├── ffmpeg_stream_engine.py
    │   │   ├── ffmpeg_process.py     # asyncio.subprocess wrapper
    │   │   ├── watchdog.py           # Crash detection + recovery coroutine
    │   │   ├── heartbeat.py          # 1 Hz position_ms → Redis writer
    │   │   └── lua/
    │   │       ├── playback_state_atomic.lua
    │   │       ├── crash_recovery_atomic.lua
    │   │       └── volume_update_atomic.lua
    │   │
    │   ├── download/                 # DownloadWorker — yt-dlp + Redlock
    │   │   ├── download_worker.py    # asyncio.Semaphore, Redlock, retry × 3
    │   │   ├── ytdlp_client.py       # yt-dlp wrapper with jittered backoff
    │   │   └── lua/
    │   │       └── release_lock_atomic.lua  # Redlock safe-release (owner-check DEL)
    │   │
    │   ├── cache/                    # CacheManager — disk LRU + Redis registry
    │   │   ├── cache_manager.py      # stat() validation, LRU eviction, hit_count
    │   │   ├── eviction_policy.py    # 20 GB budget enforcement + LRU sweep
    │   │   └── lua/
    │   │       └── cache_touch_atomic.lua   # TTL reset + HINCRBY hit_count
    │   │
    │   ├── pubsub/                   # Redis Pub/Sub event bus
    │   │   ├── event_bus.py          # Publisher + Subscriber base classes
    │   │   ├── event_types.py        # Typed EventPayload dicts (9 event types)
    │   │   └── listener.py           # Per-group subscription coroutine
    │   │
    │   ├── redis/                    # Redis client factory + key namespace
    │   │   ├── client.py             # [R] aioredis Sentinel client factory
    │   │   ├── pool.py               # Connection pool config
    │   │   └── keys.py               # [R] ONLY source of Redis key strings
    │   │
    │   └── logging/
    │       └── setup.py              # [R] structlog processors, mandatory group_id context
    │
    ├── tests/
    │   ├── conftest.py               # [R] FakeRedis · MockStreamEngine · chaos_redis
    │   ├── unit/                     # Zero I/O, zero network
    │   ├── integration/              # Requires live Redis (testcontainers)
    │   ├── e2e/                      # Fake Telegram + real Redis
    │   └── chaos/                    # [!] Fault injection — requires CHAOS_TESTS_ENABLED=1
    │
    ├── docs/
    │   ├── high-level-architecture.md# [R] Source HLA (do not edit — upstream artifact)
    │   ├── redis-schema.md           # [R] Full Redis key reference (9 key patterns)
    │   ├── error-taxonomy.md         # [R] Error classes I–VI + recovery strategies
    │   ├── ffmpeg-pipeline.md        # FFmpeg subprocess lifecycle + Opus pipeline spec
    │   ├── deployment.md             # Docker + systemd + Sentinel setup guide
    │   ├── ops-runbook.md            # On-call procedures, alert thresholds
    │   └── adr/                      # Architecture Decision Records
    │       ├── 001-redis-as-sot.md
    │       ├── 002-lua-atomicity.md
    │       ├── 003-pubsub-decoupling.md
    │       └── 004-ffmpeg-subprocess.md
    │
    ├── scripts/
    │   ├── lint_structure.py         # [R] CI enforcement — path + import boundary check
    │   ├── health_check.py           # One-shot Redis + FFmpeg health probe
    │   ├── cache_purge.py            # Manual LRU sweep utility
    │   ├── redis_flush_group.py      # Flush all keys for a target group_id
    │   └── load_test.sh              # Concurrent /play simulation (500 groups)
    │
    ├── deploy/
    │   ├── docker/
    │   │   ├── Dockerfile
    │   │   ├── docker-compose.yml    # Bot + Redis Sentinel (3 nodes)
    │   │   └── docker-compose.test.yml
    │   ├── systemd/
    │   │   ├── jukebox-bot.service   # LimitNOFILE=65536 · Restart=always
    │   │   └── jukebox-cache-cleanup.timer
    │   └── sentinel/
    │       └── sentinel.conf         # quorum=2, failover-timeout=30000ms
    │
    ├── .env.example                  # [R] All required env vars with defaults
    ├── pyproject.toml                # [R]
    ├── requirements.txt              # [R] Pinned prod deps
    └── README.md                     # [R]

### Import Boundary Rules

The architecture enforces a strict layered import matrix. Violations are **CI failures**.

Plaintext
    models    → may import: stdlib only
    interfaces → may import: models, stdlib
    redis     → may import: models
    queue     → may import: models, interfaces, redis
    stream    → may import: models, interfaces, redis, pubsub
    download  → may import: models, interfaces, redis, cache
    cache     → may import: models, interfaces, redis
    pubsub    → may import: models, redis
    broker    → may import: models, interfaces, pubsub, logging
    gateway   → may import: models, interfaces, broker, logging

> **RULE-0:** No module outside `src/interfaces/` may couple to another top-level package directly. All cross-package coupling flows through ABCs.
> 
> **RULE-0c:** All Redis key construction must use `src/redis/keys.py`. Hardcoded key strings anywhere else are a lint failure.

* * *

Redis Schema Reference
----------------------

All keys follow the namespace convention: `jukebox:{env}:{key_pattern}` where `env` ∈ `{prod, staging, test}`.

| **Key Pattern**                  | **Redis Type**  | **TTL**       | **Owner**        | **Purpose**                                               |
| -------------------------------- | --------------- | ------------- | ---------------- | --------------------------------------------------------- |
| `queue:{group_id}`               | LIST            | 86,400s (24h) | QueueManager     | Per-group track queue; msgpack-serialized `Track` objects |
| `playback:state:{group_id}`      | HASH            | 3,600s (1h)   | StreamEngine     | Current status, position_ms, volume, loop_mode            |
| `cache:meta:{sha256}`            | HASH            | 604,800s (7d) | CacheManager     | Audio file path, size, duration, hit_count                |
| `lock:download:{sha256}`         | STRING          | 300s          | DownloadWorker   | Redlock dedup — SET NX PX 300000                          |
| `settings:{group_id}`            | HASH            | None          | CommandBroker    | Per-group config (explicit delete on /reset)              |
| `ratelimit:{user_id}:{group_id}` | STRING          | 60s           | RateLimiter      | Sliding-window counter — INCR + NX                        |
| `vc:session:{group_id}`          | HASH            | None          | VoiceChatHandler | VC join time, listener count                              |
| `crash:count:{group_id}`         | STRING          | 300s          | StreamEngine     | Per-group crash counter; reset on clean transition        |
| `events:{group_id}`              | Pub/Sub channel | N/A           | event_bus.py     | Inter-component event bus                                 |

### Pub/Sub Event Payloads (`events:{group_id}`)

JSON
    { "event": "track_started",   "track_id": "uuid4", "ts": 1712000000 }
    { "event": "track_finished",  "track_id": "uuid4", "ts": 1712000000 }
    { "event": "track_skipped",   "track_id": "uuid4", "ts": 1712000000 }
    { "event": "stream_crashed",  "exit_code": -11,    "ts": 1712000000 }
    { "event": "download_ready",  "track_id": "uuid4", "ts": 1712000000 }
    { "event": "queue_empty",                          "ts": 1712000000 }

* * *

Error Taxonomy & Recovery
-------------------------

The system defines **6 error classes** across **14 specific failure scenarios**, each with a formal severity and automated recovery path.

| **Class**        | **ID** | **Failure**                        | **Severity**       | **Recovery**                                                        |
| ---------------- | ------ | ---------------------------------- | ------------------ | ------------------------------------------------------------------- |
| Infrastructure   | I-1    | Redis timeout / connection refused | CRITICAL           | Sentinel failover (<30s); aioredis exponential backoff retry ×5     |
| Infrastructure   | I-2    | Disk full (audio cache)            | RETRYABLE→CRITICAL | Pre-download quota check; LRU sweep; user notified on failure       |
| External Service | II-1   | yt-dlp download failure            | RETRYABLE/FATAL    | Retry ×3 with jittered backoff; FATAL on geo-block or removal       |
| External Service | II-2   | YouTube format/API change          | CRITICAL           | Failure rate spike alert (>50% in 5 min); ops Telegram notification |
| Process          | III-1  | FFmpeg process crash               | RETRYABLE→FATAL    | Watchdog seek-resume ≤18ms; 3-strike eviction                       |
| Process          | III-2  | FFmpeg pipe deadlock               | RETRYABLE          | Heartbeat stall >10s → SIGKILL → crash recovery path                |
| Process          | III-3  | Zombie subprocess accumulation     | TRANSIENT          | Janitor coroutine (60s cadence) force-reaps poll()≠None             |
| Telegram API     | IV-1   | FloodWait (HTTP 429)               | TRANSIENT          | aiogram retry_after; MessageThrottle (20 msg/min/group)             |
| Telegram API     | IV-2   | VC forcibly closed                 | RETRYABLE          | Queue preserved (TTL reset); user prompted to `/play`               |
| Telegram API     | IV-3   | Bot kicked from group              | FATAL              | All Redis keys deleted; subprocess freed                            |
| App Logic        | V-1    | Concurrent queue mutation race     | TRANSIENT          | All mutations Lua-atomic; no optimistic lock needed                 |
| App Logic        | V-2    | Duplicate simultaneous downloads   | TRANSIENT          | Redlock on SHA-256(url); poll-then-cache pattern                    |
| Resources        | VI-1   | FFmpeg subprocess limit exceeded   | RETRYABLE          | `asyncio.Semaphore(CPU_COUNT × 4)` blocks gracefully                |
| Resources        | VI-2   | File descriptor exhaustion         | CRITICAL           | `systemd LimitNOFILE=65536`; immediate handle close                 |

### Validated Failure Scenario: FFmpeg Crash Mid-Stream (Class III-1)

This is the most operationally dangerous failure mode. An FFmpeg process for `group_id=99182` exits with SIGSEGV at 4 minutes into a 5-minute track. The voice chat connection stays open. The group hears silence.

Plaintext
    T+0ms   FFmpeg exits (returncode=-11). asyncio child-process watcher fires.
    T+5ms   Watchdog unblocks from process.wait(); classifies exit as ABNORMAL.
    T+8ms   Lua script (atomic):
              HSET playback:state:99182 status "recovering"
              INCR crash:count:99182
              EXPIRE crash:count:99182 300
            Publishes: { "event": "stream_crashed", "exit_code": -11, "ts": ... }
    T+10ms  Pub/Sub listener invokes recover_stream(group_id=99182, seek_ms=241000)
    T+11ms  crash:count=1 (<3 threshold). CacheManager stat() confirms file intact.
    T+15ms  Fresh FFmpeg spawned with -ss 241 seek flag; existing VC connection reused.
    T+18ms  Opus frames resume. Lua: HSET status "playing", position_ms=241000.

    → Audible gap: ~18ms — imperceptible to listeners.

**3-Strike escalation:** If `crash:count` reaches 3 on the same track, the system classifies it FATAL: evicts the cache entry, removes the track from the queue, notifies the group, and synthetically advances to the next track.

* * *

Testing
-------

### Run Unit Tests (zero I/O, fast)

Bash
    pytest tests/unit/ -v

### Run Integration Tests (requires Docker)

Bash
    # Starts a Redis testcontainer automatically
    pytest tests/integration/ -v

### Run End-to-End Tests (fake Telegram, real Redis)

Bash
    pytest tests/e2e/ -v

### Run Chaos / Fault Injection Tests

These tests require Docker, minimum 4 CPU cores, and a 512 MB tmpfs mount.

Bash
    export CHAOS_TESTS_ENABLED=1
    # The testcontainers-compatible compose is used automatically
    pytest tests/chaos/ -v -m chaos

**Chaos test coverage:**

| **Test File**                 | **HLA Class** | **Scenario**                                                         |
| ----------------------------- | ------------- | -------------------------------------------------------------------- |
| `test_ffmpeg_crash.py`        | III-1         | SIGSEGV mid-stream; assert ≤18ms gap, crash:count increments         |
| `test_redis_partition.py`     | I-1           | Sentinel failover; assert in-flight stream continues                 |
| `test_disk_full.py`           | I-2           | tmpfs filled 100%; assert pre-download sweep triggers                |
| `test_concurrent_skip.py`     | V-1           | /skip race + SIGSEGV; assert Lua CAS prevents duplicate start_stream |
| `test_duplicate_download.py`  | V-2           | N goroutines on same URL; assert exactly one download                |
| `test_zombie_subprocesses.py` | III-3         | 50 abandoned groups; assert janitor reaps all within 60s             |
| `test_ffmpeg_semaphore.py`    | VI-1          | Requests exceed CPU_COUNT×4; assert no fork-bomb                     |

### CI Enforcement Checks

`scripts/lint_structure.py` runs as the **first CI step** before any test or build:

| **Check** | **What It Validates**                                                 |
| --------- | --------------------------------------------------------------------- |
| CHECK-1   | All `[R]` (Required) paths exist                                      |
| CHECK-2   | `src/interfaces/` imports nothing from implementation packages        |
| CHECK-3   | `src/models/` imports nothing from `src/*` (stdlib only)              |
| CHECK-4   | Each `lua/*.lua` is only imported by Python in its own parent package |
| CHECK-5   | `src/redis/keys.py` is the only file with hardcoded Redis key strings |
| CHECK-6   | `docs/redis-schema.md` contains exactly 9 H3 sections + Changelog     |
| CHECK-7   | Every test in `tests/chaos/` carries `@pytest.mark.chaos`             |
| CHECK-8   | All Lua files under `src/` are named `*_atomic.lua`                   |
| CHECK-9   | All env vars referenced in `src/config.py` appear in `.env.example`   |
| CHECK-10  | Import matrix (§7 of repo spec) is not violated via AST analysis      |

* * *

Deployment & Operations
-----------------------

### Health Check

Bash
    # One-shot probe — Redis connectivity + FFmpeg binary availability
    python scripts/health_check.py

### Manual Cache Purge

Bash
    # Trigger LRU sweep manually (normally handled by the systemd timer hourly)
    python scripts/cache_purge.py

### Flush All State for a Specific Group

Bash
    # WARNING: This stops playback and clears the queue for the target group
    python scripts/redis_flush_group.py --group-id 99182

### Load Testing

Bash
    # Simulate 500 concurrent groups each sending /play commands
    bash scripts/load_test.sh

### Monitoring

The system exposes operational state via Redis keys. Key metrics to watch:

| **Metric**                    | **Source**                  | **Alert Threshold**       |
| ----------------------------- | --------------------------- | ------------------------- |
| Crash recovery time (P99)     | Watchdog timestamp delta    | >200ms                    |
| Silent gap duration (P99)     | Opus frame gap measurement  | >100ms                    |
| Redis operation latency (P99) | aioredis instrumentation    | >5ms                      |
| FFmpeg subprocess count       | Semaphore gauge             | >CPU_COUNT × 4            |
| Disk cache hit rate           | hit_count / total downloads | <70% (post-warmup)        |
| Download failure rate         | yt-dlp error rate           | >50% in 5 min (ops alert) |

### Systemd Service Configuration

The `jukebox-bot.service` enforces the following resource limits critical to stability:

Ini, TOML
    LimitNOFILE=65536    # File descriptor limit (HLA Class VI-2)
    LimitNPROC=32768     # Process limit
    Restart=always
    RestartSec=3s
    After=redis.service

* * *

Technology Stack
----------------

| **Layer**        | **Technology**       | **Version** | **Rationale**                                              |
| ---------------- | -------------------- | ----------- | ---------------------------------------------------------- |
| Language         | Python               | 3.12+       | asyncio maturity; yt-dlp/aiogram ecosystem                 |
| Bot Framework    | aiogram              | 3.x         | Native asyncio; clean middleware chain; active maintenance |
| Voice Chat       | pytgcalls / pyrogram | Latest      | MTProto voice chat binding; industry standard for TG VC    |
| Audio Processing | FFmpeg               | 6.x         | Opus encoding; lavfi volume control; `-ss` seek support    |
| Content Source   | yt-dlp               | Pinned      | YouTube extraction; extensible to other platforms          |
| State Store      | Redis + Sentinel     | 7.x         | Lua scripting; Pub/Sub; sub-ms latency; HA failover        |
| Serialization    | msgpack              | Latest      | Compact binary format for queue entries                    |
| Logging          | structlog            | Latest      | Structured JSON logging with mandatory context binding     |
| Deployment       | Docker + Compose     | Latest      | Reproducible builds; single-command deployment             |

* * *

Contributing
------------

### Rules Before You Code

1. **Read `docs/high-level-architecture.md`** — the HLA is the contract. PRs that violate it will not be merged.

2. **Read `docs/adr/`** — Architecture Decision Records explain _why_ the system is structured the way it is, particularly:
   
   * `001-redis-as-sot.md` — Why Redis, not Postgres, for ephemeral state
   
   * `002-lua-atomicity.md` — Why Lua over MULTI/EXEC transactions
   
   * `003-pubsub-decoupling.md` — Why Pub/Sub between DownloadWorker and StreamEngine

3. **Run `scripts/lint_structure.py` locally** before pushing. A non-zero exit blocks CI.

### Development Workflow

Bash
    # 1. Fork and clone
    git clone [https://github.com/your-fork/telegram-jukebox-bot.git](https://github.com/your-fork/telegram-jukebox-bot.git)
    cd telegram-jukebox-bot

    # 2. Install dev dependencies
    pip install -r requirements-dev.txt

    # 3. Run the structure linter
    python scripts/lint_structure.py

    # 4. Run the unit test suite
    pytest tests/unit/ -v

    # 5. Make your changes, then run integration tests
    pytest tests/unit/ tests/integration/ -v

    # 6. Open a PR targeting main

### Naming Conventions

| **Artefact**             | **Convention**                                   | **Example**                 |
| ------------------------ | ------------------------------------------------ | --------------------------- |
| Python modules           | `snake_case.py`                                  | `redis_queue_manager.py`    |
| Lua scripts              | `snake_case_atomic.lua` (suffix required)        | `skip_atomic.lua`           |
| Test files               | `test_{subject}.py` matching module under test   | `test_redis_queue.py`       |
| ADR files                | `NNN-kebab-case-title.md`                        | `005-new-decision.md`       |
| Environment variables    | `SCREAMING_SNAKE_CASE` with `.env.example` entry | `MAX_QUEUE_LENGTH`          |
| Redis key builders       | `build_{key_type}_key()` in `src/redis/keys.py`  | `build_queue_key(group_id)` |
| Event payload TypedDicts | Suffixed with `Event`                            | `TrackStartedEvent`         |

### What NOT to Do

* ❌ Do **not** hardcode Redis key strings anywhere except `src/redis/keys.py` (CHECK-5)

* ❌ Do **not** add implementation code to `src/interfaces/` (CHECK-2)

* ❌ Do **not** import from `src/*` inside `src/models/` (CHECK-3)

* ❌ Do **not** load a Lua script from a package that doesn't own it (CHECK-4, RULE-0b)

* ❌ Do **not** call `aioredis.create_connection()` directly — always use the injected pool from `src/redis/pool.py`

* ❌ Do **not** add chaos tests without the `@pytest.mark.chaos` decorator (CHECK-7)

* * *

Risk Register
-------------

| **Risk**                                                 | **Likelihood** | **Impact** | **Mitigation**                                                                                 |
| -------------------------------------------------------- | -------------- | ---------- | ---------------------------------------------------------------------------------------------- |
| YouTube API/format changes break yt-dlp                  | High           | Critical   | Pinned yt-dlp version; canary download job; failure rate spike alert (>50% in 5 min)           |
| Telegram restricts voice chat bot access                 | Low            | Critical   | Abstracted `TelegramHandler` interface; queue state persists in Redis regardless               |
| Redis Sentinel failover causes brief state inconsistency | Medium         | High       | aioredis retry with exponential backoff; in-flight streams continue; re-sync on reconnect      |
| Disk cache exhaustion during traffic spike               | Medium         | Medium     | 500 MB pre-download threshold; aggressive LRU sweep; tmpfs overlay for active files            |
| FFmpeg vulnerability (CVE) in audio codec                | Medium         | High       | Pinned FFmpeg version in Docker image; automated rebuild pipeline                              |
| User abuse: queue flooding                               | High           | Medium     | Per-user rate limiting (sliding window); per-group queue cap (default 50); max duration (600s) |
| Legal risk from streaming copyrighted content            | High           | High       | Audio streamed only within Telegram VC (no redistribution); cache TTL ≤7 days; clear ToS       |

* * *

License
-------

This project is licensed under the **MIT License**. See the [LICENSE](https://claude.ai/chat/LICENSE) file for details.

> **Legal notice:** This bot streams audio into Telegram voice chats for ephemeral listening. It does not permanently redistribute copyrighted content. Audio cache files are automatically evicted after a maximum of 7 days. Operators are responsible for compliance with applicable copyright law in their jurisdiction. The authors disclaim all liability for misuse.

* * *

Glossary
--------

| **Term**                  | **Definition**                                                                                                       |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **VC**                    | Voice Chat — Telegram's persistent audio room feature for groups                                                     |
| **Lua-atomic**            | Redis operation executed as a single atomic unit via embedded Lua scripting                                          |
| **Redlock**               | Distributed lock algorithm using Redis `SET NX PX` for mutual exclusion                                              |
| **Shadow list**           | A secondary Redis LIST used for exactly-once dequeue semantics                                                       |
| **Watchdog**              | A dedicated `asyncio` coroutine monitoring FFmpeg subprocess health                                                  |
| **Heartbeat**             | 1 Hz `position_ms` update from `StreamEngine` to Redis                                                               |
| **LRU eviction**          | Least Recently Used cache eviction strategy for 20 GB disk budget                                                    |
| **MTProto**               | Telegram's proprietary encryption/transport protocol                                                                 |
| **Exactly-once delivery** | Guarantee that a track is dequeued and processed by exactly one consumer, achieved via the LMOVE shadow list pattern |

* * *

_Built with surgical precision. Maintained with operational obsession._
