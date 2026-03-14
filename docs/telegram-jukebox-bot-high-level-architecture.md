* * *

Telegram Music Jukebox Bot — High-Level Architecture
====================================================

> **SoT Pre-analysis (internal, summarised):**  
> At 500 concurrent voice-chat users the dominant bottlenecks are (1) one FFmpeg subprocess per active voice-chat group, (2) yt-dlp download bandwidth and deduplication across groups requesting the same track, (3) Redis connection pool exhaustion if every coroutine opens its own connection, and (4) Telegram’s MTProto voice-chat sessions being stateful and non-restartable without re-joining. Persistence strategy: Redis is the single source of truth for all ephemeral state; the filesystem is the source of truth for audio blobs; no relational DB is warranted at this scale. All cross-process coordination is done via Redis Pub/Sub + Lua-atomic writes.

* * *

Section 1 — Component Diagram (Text UML)
----------------------------------------

     ╔══════════════════════════════════════════════════════════════════════════╗
     ║                         TELEGRAM JUKEBOX BOT                            ║
     ╚══════════════════════════════════════════════════════════════════════════╝
    
     ┌────────────────────────────────────────────────────────────────────────┐
     │                        <<boundary>>                                    │
     │                      TelegramGateway                                   │
     │  ┌─────────────────────────────────────────────────────────────────┐  │
     │  │  aiogram Dispatcher  │  UpdateRouter  │  RateLimiter Middleware │  │
     │  └──────────────────────┴────────────────┴────────────────────────┘  │
     └──────────────────────────────┬─────────────────────────────────────────┘
                                    │  aiogram.types.{Message, CallbackQuery,
                                    │  ChatMemberUpdated}
                                    ▼
     ┌──────────────────────────────────────────────────────────────────────┐
     │                        <<control>>                                   │
     │                       CommandBroker                                  │
     │   /play  /skip  /stop  /queue  /np  /volume  /loop  /shuffle        │
     │   Inline-keyboard callback router                                    │
     │   Permission guard (admin_only_skip, dj_role)                        │
     └────────────┬──────────────────────────────────────┬──────────────────┘
                  │ enqueue / dequeue / peek              │ start / stop /
                  │ clear / promote_to_front              │ health_check
                  ▼                                       ▼
     ┌────────────────────────────┐         ┌─────────────────────────────────┐
     │      <<control>>           │         │         <<service>>              │
     │      QueueManager          │         │         StreamEngine             │
     │                            │         │                                  │
     │  • RPUSH / LPOP on Redis   │         │  • group_id → asyncio.subprocess │
     │  • Atomic LMOVE for skip   │         │    mapping (in-process dict)     │
     │  • Sorted-set priority     │         │  • FFmpeg watchdog coroutine     │
     │    lane for /promote       │         │  • position_ms heartbeat (1 Hz)  │
     │  • TTL refresh on activity │         │    written to Redis              │
     └────────────┬───────────────┘         │  • Opus/PCM pipeline to TgVC     │
                  │                         └──────────┬──────────────────────┘
                  │ Track.url                          │ file_path: str
                  ▼                                    ▼
     ┌────────────────────────────┐         ┌─────────────────────────────────┐
     │      <<service>>           │         │        <<process>>               │
     │      DownloadWorker        │         │        FFmpegProcess             │
     │                            │         │                                  │
     │  • asyncio semaphore       │         │  • One subprocess per group      │
     │    (max N concurrent DLs)  │         │  • stdin pipe: raw audio bytes   │
     │  • Redlock on URL hash     │         │  • -vn -acodec libopus           │
     │    (prevents duplicate DL) │         │    -af volume={v}                │
     │  • yt-dlp best-audio fmt   │         │  • SIGTERM on graceful stop      │
     │  • Retry × 3 w/ backoff    │         │  • SIGKILL after 5s timeout      │
     └────────────┬───────────────┘         └──────────────────────────────────┘
                  │ writes file_path
                  ▼
     ┌────────────────────────────┐
     │      <<service>>           │
     │      CacheManager          │
     │                            │
     │  • Key: SHA-256(url)       │
     │  • LRU eviction (20 GB     │
     │    disk budget)            │
     │  • cache:meta:{hash} HASH  │
     │    in Redis (7-day TTL)    │
     │  • tmpfs mount for active  │
     │    stream files            │
     └────────────┬───────────────┘
                  │
                  ▼
     ┌──────────────────────────────────────────────────────────────────────┐
     │                     <<infrastructure>> (shared)                      │
     │                                                                      │
     │   ┌──────────────────────────────┐   ┌──────────────────────────┐  │
     │   │     Redis Cluster            │   │   Filesystem Cache       │  │
     │   │  • Sentinel HA (3 nodes)     │   │   /var/jukebox/cache/    │  │
     │   │  • Queues, playback state,   │   │   max 20 GB, LRU         │  │
     │   │    locks, rate-limits,       │   │   tmpfs overlay for      │  │
     │   │    crash counters            │   │   currently-playing file │  │
     │   │  • Pub/Sub: events:{gid}     │   └──────────────────────────┘  │
     │   └──────────────────────────────┘                                  │
     └──────────────────────────────────────────────────────────────────────┘
    
     ═══════════════════════ CROSS-CUTTING CONCERNS ════════════════════════
      • All Redis writes touching playback state use Lua scripts for atomicity.
      • DownloadWorker ←→ StreamEngine communicate exclusively via Redis
        Pub/Sub channel  events:{group_id}  (no direct Python object refs).
      • A global asyncio.Semaphore caps total FFmpeg subprocesses to
        min(active_voice_chats, CPU_COUNT × 4) to prevent fork-bomb.
      • Structured logging (structlog) with group_id as mandatory context key.

* * *

Section 2 — Redis Schema
------------------------

    ══════════════════════════════════════════════════════════════════════════
     NAMESPACE PREFIX CONVENTION:  jukebox:{env}:{key_pattern}
     All keys below omit the prefix for readability.
    ══════════════════════════════════════════════════════════════════════════
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 1. PER-GROUP TRACK QUEUE                                            │
    │                                                                     │
    │  Key   : queue:{group_id}                                           │
    │  Type  : LIST                                                       │
    │  TTL   : 86 400 s  (24 h; reset on every RPUSH)                    │
    │  Value : Each element = msgpack-serialised Track                    │
    │  Ops   : RPUSH (enqueue tail)  LPOP (dequeue head)                 │
    │          LRANGE 0 N-1 (snapshot)  LLEN (depth)                     │
    │  Notes : LMOVE source dest LEFT RIGHT used for atomic /skip        │
    │          to a "processing" shadow list, preventing double-dequeue   │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 2. CURRENT PLAYBACK STATE                                           │
    │                                                                     │
    │  Key    : playback:state:{group_id}                                 │
    │  Type   : HASH                                                      │
    │  TTL    : 3 600 s  (reset on every HSET)                           │
    │  Fields :                                                           │
    │    status        → "idle" | "loading" | "playing" |                │
    │                    "paused" | "stopped" | "recovering"              │
    │    track_id      → str  (UUID4)                                     │
    │    title         → str                                              │
    │    url           → str                                              │
    │    duration_s    → int                                              │
    │    position_ms   → int  (updated by watchdog heartbeat, 1 Hz)      │
    │    started_at    → int  (Unix epoch, ms precision)                  │
    │    volume        → str  (float 0.00–1.00, stored as string)         │
    │    loop_mode     → "none" | "track" | "queue"                       │
    │    requested_by  → int  (Telegram user_id)                          │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 3. AUDIO FILE CACHE REGISTRY                                        │
    │                                                                     │
    │  Key    : cache:meta:{sha256_of_canonical_url}                      │
    │  Type   : HASH                                                      │
    │  TTL    : 604 800 s  (7 days; LRU touch resets TTL)                │
    │  Fields :                                                           │
    │    file_path      → str  (absolute path on disk)                    │
    │    file_size_b    → int                                             │
    │    duration_s     → int                                             │
    │    title          → str                                             │
    │    thumbnail_url  → str | ""                                        │
    │    cached_at      → int  (epoch)                                    │
    │    hit_count      → int  (HINCRBY on each cache hit)                │
    │  Notes  : Existence of key does NOT guarantee file is on disk;      │
    │           CacheManager stat()s the path before trusting this key.  │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 4. DOWNLOAD DEDUPLICATION LOCK  (Redlock pattern)                   │
    │                                                                     │
    │  Key    : lock:download:{sha256_of_canonical_url}                   │
    │  Type   : STRING  (value = worker_uuid)                             │
    │  TTL    : 300 s  (hard ceiling on any download)                     │
    │  Ops    : SET NX PX 300000  (atomic acquire)                        │
    │           DEL with Lua owner-check (safe release)                   │
    │  Notes  : Any racing coroutine that fails SET NX polls key with     │
    │           WAIT 100ms until lock disappears, then checks cache.     │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 5. PER-GROUP SETTINGS  (semi-permanent)                             │
    │                                                                     │
    │  Key    : settings:{group_id}                                       │
    │  Type   : HASH                                                      │
    │  TTL    : none  (must be deleted explicitly on /reset)              │
    │  Fields :                                                           │
    │    max_queue_len       → int   (default: 50)                        │
    │    max_duration_s      → int   (default: 600)                       │
    │    admin_only_skip     → "0" | "1"                                  │
    │    announce_tracks     → "0" | "1"                                  │
    │    dj_role_id          → int | ""                                   │
    │    shuffle_seed        → int | ""  (for reproducible shuffle)       │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 6. USER RATE LIMITING  (sliding-window, per group)                  │
    │                                                                     │
    │  Key    : ratelimit:{user_id}:{group_id}                            │
    │  Type   : STRING  (counter, INCR)                                   │
    │  TTL    : 60 s  (auto-resets window)                                │
    │  Logic  : INCR → if result > settings.max_requests_per_min → deny  │
    │           Uses SET PX only on first request (NX flag)               │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 7. VOICE-CHAT SESSION REGISTRY                                      │
    │                                                                     │
    │  Key    : vc:session:{group_id}                                     │
    │  Type   : HASH                                                      │
    │  TTL    : none  (managed on VC join/leave events)                   │
    │  Fields :                                                           │
    │    chat_id          → int                                           │
    │    voice_chat_id    → int                                           │
    │    bot_joined_at    → int  (epoch)                                  │
    │    listener_count   → int  (updated via ChatMemberUpdated)          │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 8. CRASH / RETRY COUNTER  (per-group, per-track)                    │
    │                                                                     │
    │  Key    : crash:count:{group_id}                                    │
    │  Type   : STRING  (counter, INCR)                                   │
    │  TTL    : 300 s  (auto-evicts after 5 min quiet period)             │
    │  Logic  : Incremented by StreamEngine watchdog on abnormal exit.   │
    │           Reset to 0 by watchdog on clean track transition.         │
    └─────────────────────────────────────────────────────────────────────┘
    
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 9. INTER-COMPONENT EVENT BUS  (Redis Pub/Sub)                       │
    │                                                                     │
    │  Channel   : events:{group_id}                                      │
    │  Publisher : StreamEngine, DownloadWorker, CacheManager             │
    │  Subscriber: CommandBroker (for next-track advancement)             │
    │  Payloads  (JSON):                                                  │
    │    { "event": "track_started",   "track_id": str, "ts": int }      │
    │    { "event": "track_finished",  "track_id": str, "ts": int }      │
    │    { "event": "track_skipped",   "track_id": str, "ts": int }      │
    │    { "event": "stream_crashed",  "exit_code": int, "ts": int }     │
    │    { "event": "download_ready",  "track_id": str, "ts": int }      │
    │    { "event": "queue_empty",     "ts": int }                        │
    └─────────────────────────────────────────────────────────────────────┘

* * *

Section 3 — Core Python Dataclasses and Abstract Signatures
-----------------------------------------------------------

    # ──────────────────────────────────────────────────────────────────────
    #  ENUMERATIONS
    # ──────────────────────────────────────────────────────────────────────
    
    from enum import StrEnum
    
    
    class TrackStatus(StrEnum):
        QUEUED       = "queued"
        DOWNLOADING  = "downloading"
        READY        = "ready"
        PLAYING      = "playing"
        FAILED       = "failed"
        SKIPPED      = "skipped"
    
    
    class PlaybackStatus(StrEnum):
        IDLE        = "idle"
        LOADING     = "loading"
        PLAYING     = "playing"
        PAUSED      = "paused"
        STOPPED     = "stopped"
        RECOVERING  = "recovering"   # transient: crash detected, retrying
    
    
    class LoopMode(StrEnum):
        NONE   = "none"
        TRACK  = "track"
        QUEUE  = "queue"
    
    
    class ErrorSeverity(StrEnum):
        TRANSIENT    = "transient"   # retry immediately
        RETRYABLE    = "retryable"   # retry with backoff
        FATAL        = "fatal"       # skip track, notify user
        CRITICAL     = "critical"    # halt group playback entirely
    
    
    # ──────────────────────────────────────────────────────────────────────
    #  CORE DOMAIN DATACLASSES
    # ──────────────────────────────────────────────────────────────────────
    
    from __future__ import annotations
    from dataclasses import dataclass, field
    from datetime import datetime
    
    
    @dataclass(frozen=True, slots=True)
    class Track:
        track_id:      str                  # UUID4, generated at enqueue time
        url:           str                  # canonical source URL
        title:         str
        duration_s:    int                  # 0 if unknown before download
        file_path:     str | None           # None until download completes
        requested_by:  int                  # Telegram user_id
        group_id:      int                  # Telegram supergroup chat_id
        added_at:      datetime
        status:        TrackStatus = TrackStatus.QUEUED
        thumbnail_url: str | None  = None
    
    
    @dataclass(slots=True)
    class PlaybackState:
        group_id:      int
        status:        PlaybackStatus
        current_track: Track | None
        position_ms:   int                  # last known playhead position
        volume:        float                # 0.0–1.0
        loop_mode:     LoopMode
        updated_at:    datetime = field(default_factory=datetime.utcnow)
    
    
    @dataclass(frozen=True, slots=True)
    class GroupSettings:
        group_id:            int
        max_queue_length:    int       = 50
        max_track_duration_s: int      = 600
        admin_only_skip:     bool      = False
        announce_tracks:     bool      = True
        dj_role_id:          int | None = None
        max_requests_per_min: int      = 5
    
    
    @dataclass(slots=True)
    class DownloadResult:
        track_id:        str
        success:         bool
        file_path:       str | None
        error_message:   str | None
        duration_s:      int | None
        file_size_bytes: int | None
        cache_hit:       bool = False
    
    
    @dataclass(frozen=True, slots=True)
    class JukeboxError:
        code:      str            # e.g. "FFMPEG_CRASH", "REDIS_TIMEOUT"
        severity:  ErrorSeverity
        group_id:  int | None
        track_id:  str | None
        detail:    str
        raised_at: datetime = field(default_factory=datetime.utcnow)
    
    
    # ──────────────────────────────────────────────────────────────────────
    #  ABSTRACT INTERFACE: QueueManager
    #  Justification: decouples business logic from Redis topology;
    #  a test double can implement this against a plain dict.
    # ──────────────────────────────────────────────────────────────────────
    
    from abc import ABC, abstractmethod
    
    
    class QueueManager(ABC):
    
        @abstractmethod
        async def enqueue(
            self,
            group_id: int,
            track: Track,
        ) -> int:
            """
            Append `track` to the tail of the group queue.
            Returns the new queue length after insertion.
            Raises QueueFullError if len >= GroupSettings.max_queue_length.
            """
            ...
    
        @abstractmethod
        async def dequeue(
            self,
            group_id: int,
        ) -> Track | None:
            """
            Atomically pop and return the head track.
            Uses LMOVE to a shadow 'processing' list before returning,
            ensuring exactly-once delivery even under concurrent callers.
            Returns None if the queue is empty.
            """
            ...
    
        @abstractmethod
        async def peek_next(
            self,
            group_id: int,
        ) -> Track | None:
            """
            Return the head track without removing it.
            Used by the now-playing announcement pre-fetch.
            """
            ...
    
        @abstractmethod
        async def get_snapshot(
            self,
            group_id: int,
            limit: int = 20,
            offset: int = 0,
        ) -> list[Track]:
            """
            Return up to `limit` tracks starting at `offset` (non-destructive).
            Used for /queue pagination.
            """
            ...
    
        @abstractmethod
        async def remove_by_id(
            self,
            group_id: int,
            track_id: str,
        ) -> bool:
            """
            Locate and remove a specific track by its UUID.
            Returns True on success, False if track_id not found.
            Must be O(N) scan; acceptable given max_queue_length = 50.
            """
            ...
    
        @abstractmethod
        async def promote_to_front(
            self,
            group_id: int,
            track_id: str,
        ) -> bool:
            """
            Move a queued track to the head position (admin /promote command).
            Implemented as remove_by_id + LPUSH in a Lua atomic script.
            Returns False if track not found.
            """
            ...
    
        @abstractmethod
        async def clear(
            self,
            group_id: int,
        ) -> int:
            """
            Flush entire queue. Returns the count of removed tracks.
            """
            ...
    
        @abstractmethod
        async def length(
            self,
            group_id: int,
        ) -> int:
            """
            Return current queue depth via Redis LLEN (O(1)).
            Does NOT deserialise any Track objects.
            """
            ...
    
        @abstractmethod
        async def ack_processed(
            self,
            group_id: int,
            track_id: str,
        ) -> None:
            """
            Confirm successful processing of a track previously dequeued
            into the shadow list.  Removes it from the shadow list in Redis.
            Must be called by StreamEngine after clean track completion.
            """
            ...
    
    
    # ──────────────────────────────────────────────────────────────────────
    #  ABSTRACT INTERFACE: StreamEngine
    #  Justification: hides FFmpeg subprocess lifecycle and Telegram
    #  voice-client API behind a stable contract; enables unit-testing
    #  of CommandBroker with a mock engine.
    # ──────────────────────────────────────────────────────────────────────
    
    class StreamEngine(ABC):
    
        @abstractmethod
        async def start_stream(
            self,
            group_id: int,
            track: Track,
        ) -> None:
            """
            Spawn an FFmpeg subprocess for `group_id` and begin piping
            Opus frames to the group's Telegram voice chat.
            Raises StreamAlreadyActiveError if a stream is in progress.
            """
            ...
    
        @abstractmethod
        async def stop_stream(
            self,
            group_id: int,
            *,
            graceful: bool = True,
        ) -> None:
            """
            Terminate the active stream.
            graceful=True → SIGTERM then drain stdout before killing.
            graceful=False → SIGKILL immediately (used on VC termination).
            """
            ...
    
        @abstractmethod
        async def pause_stream(
            self,
            group_id: int,
        ) -> None:
            """
            Send SIGSTOP to the FFmpeg process to freeze output
            while holding the voice-chat connection open.
            Writes status=PAUSED to Redis atomically.
            """
            ...
    
        @abstractmethod
        async def resume_stream(
            self,
            group_id: int,
        ) -> None:
            """
            Send SIGCONT to resume a SIGSTOP-paused FFmpeg process.
            Writes status=PLAYING to Redis atomically.
            """
            ...
    
        @abstractmethod
        async def get_playback_state(
            self,
            group_id: int,
        ) -> PlaybackState:
            """
            Return the current PlaybackState by reading from Redis HASH.
            Does NOT inspect the subprocess directly (decoupled by heartbeat).
            """
            ...
    
        @abstractmethod
        async def set_volume(
            self,
            group_id: int,
            volume: float,
        ) -> None:
            """
            Adjust output volume.
            Implemented by sending a newline-delimited JSON command to
            FFmpeg's stdin (using -af volume=N via dynamic lavfi graph
            reinit) and updating the Redis HASH field atomically.
            """
            ...
    
        @abstractmethod
        async def health_check(
            self,
            group_id: int,
        ) -> bool:
            """
            Return True iff the FFmpeg subprocess for `group_id` exists
            and its OS process is in a running state (poll() is None).
            """
            ...
    
        @abstractmethod
        async def get_active_group_ids(
            self,
        ) -> frozenset[int]:
            """
            Return the set of group_ids with a currently active subprocess.
            Used by the global semaphore enforcer and health-check scheduler.
            """
            ...
    
        @abstractmethod
        async def recover_stream(
            self,
            group_id: int,
            track: Track,
            seek_ms: int,
        ) -> None:
            """
            Restart a crashed stream, seeking to `seek_ms` to minimise
            audible gap.  Called exclusively by the watchdog coroutine.
            Increments crash:count:{group_id} in Redis before attempting.
            """
            ...
    
    
    # ──────────────────────────────────────────────────────────────────────
    #  ABSTRACT INTERFACE: TelegramHandler
    #  Justification: isolates aiogram-specific types from domain logic;
    #  a future migration to another framework touches only this layer.
    # ──────────────────────────────────────────────────────────────────────
    
    from aiogram.types import CallbackQuery, ChatMemberUpdated, Message
    
    
    class TelegramHandler(ABC):
    
        @abstractmethod
        async def on_play_command(
            self,
            message: Message,
            query: str,
        ) -> None:
            """
            Handle /play <url|search-query>.
            1. Rate-limit check.  2. Resolve URL or search via yt-dlp.
            3. Validate duration vs. GroupSettings.max_track_duration_s.
            4. Enqueue Track.  5. If idle, trigger StreamEngine.start_stream.
            """
            ...
    
        @abstractmethod
        async def on_skip_command(
            self,
            message: Message,
        ) -> None:
            """
            Handle /skip.
            Permission-gate (admin_only_skip / dj_role).
            Calls StreamEngine.stop_stream(graceful=False) then dequeues next.
            """
            ...
    
        @abstractmethod
        async def on_stop_command(
            self,
            message: Message,
        ) -> None:
            """
            Handle /stop.  Admin-only.
            Stops stream, clears queue, publishes queue_empty event.
            """
            ...
    
        @abstractmethod
        async def on_queue_command(
            self,
            message: Message,
        ) -> None:
            """
            Handle /queue [page].
            Renders paginated inline-keyboard view of QueueManager.get_snapshot.
            """
            ...
    
        @abstractmethod
        async def on_nowplaying_command(
            self,
            message: Message,
        ) -> None:
            """
            Handle /np.
            Reads PlaybackState from StreamEngine.get_playback_state,
            renders ASCII progress bar, attaches thumbnail if available.
            """
            ...
    
        @abstractmethod
        async def on_volume_command(
            self,
            message: Message,
            level: int,
        ) -> None:
            """
            Handle /volume <0–100>.
            Validates range, converts to 0.0–1.0, calls StreamEngine.set_volume.
            """
            ...
    
        @abstractmethod
        async def on_queue_page_callback(
            self,
            query: CallbackQuery,
            page: int,
        ) -> None:
            """
            Handle inline-keyboard page-turn callback for /queue.
            Edits the existing message in-place to avoid spam.
            """
            ...
    
        @abstractmethod
        async def on_voice_chat_started(
            self,
            update: ChatMemberUpdated,
        ) -> None:
            """
            Fired when a group voice chat is activated.
            Bot joins via pyrogram/pytgcalls, restores vc:session in Redis,
            and resumes playback if queue is non-empty.
            """
            ...
    
        @abstractmethod
        async def on_voice_chat_ended(
            self,
            update: ChatMemberUpdated,
        ) -> None:
            """
            Fired when the voice chat is terminated by Telegram.
            Calls StreamEngine.stop_stream(graceful=False),
            preserves queue in Redis (TTL reset), removes vc:session key.
            """
            ...
    
        @abstractmethod
        async def on_track_finished_callback(
            self,
            group_id: int,
            finished_track: Track,
        ) -> None:
            """
            Internal event hub entry-point; called by the Redis Pub/Sub
            listener when  events:{group_id}  publishes 'track_finished'.
            Calls QueueManager.ack_processed, then dequeues and starts next.
            Publishes 'queue_empty' if queue is now empty.
            """
            ...
    
        @abstractmethod
        async def on_error_event(
            self,
            error: JukeboxError,
        ) -> None:
            """
            Central error dispatcher.
            Routes JukeboxError by severity to the correct recovery path
            and optionally notifies the group chat.
            """
            ...

* * *

Section 4 — Error Taxonomy and Recovery Strategies
--------------------------------------------------

### 4.1 Error Taxonomy

    ┌──────────────────────────────────────────────────────────────────────────┐
    │ CLASS I — INFRASTRUCTURE FAILURES                                        │
    │                                                                          │
    │  I-1  Redis Timeout / Connection Refused                                 │
    │       Severity  : CRITICAL                                               │
    │       Cause     : Redis node failover, network partition, OOM kill.      │
    │       Impact    : All queue reads/writes fail; state diverges from       │
    │                   in-memory subprocess map.                              │
    │       Recovery  : Sentinel auto-promotes replica (< 30 s). Bot's        │
    │                   aioredis client has connection_retry=True with         │
    │                   exponential backoff (max 5 attempts). During           │
    │                   outage, in-flight streams continue (FFmpeg subprocess  │
    │                   is unaffected) but new /play commands return a user-   │
    │                   visible error. On reconnect, StreamEngine re-syncs     │
    │                   position_ms heartbeat back to Redis.                   │
    │                                                                          │
    │  I-2  Disk Full (Audio Cache)                                            │
    │       Severity  : RETRYABLE → CRITICAL                                   │
    │       Cause     : Cache eviction lag; simultaneous large downloads.      │
    │       Recovery  : CacheManager runs a pre-download quota check; if      │
    │                   free space < 500 MB, triggers LRU eviction sweep       │
    │                   before writing. If sweep fails (all files active),     │
    │                   DownloadResult.success=False, track status→FAILED,     │
    │                   user notified.                                         │
    └──────────────────────────────────────────────────────────────────────────┘
    
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ CLASS II — EXTERNAL SERVICE FAILURES                                     │
    │                                                                          │
    │  II-1  yt-dlp Download Failure                                           │
    │        Severity  : RETRYABLE (HTTP 429, timeout) or FATAL (geo-block,   │
    │                    removed video, format unavailable).                   │
    │        Recovery  : Retry × 3 with jittered backoff for RETRYABLE codes. │
    │                    On FATAL: set TrackStatus.FAILED, remove from queue,  │
    │                    notify user with specific error reason.               │
    │                                                                          │
    │  II-2  YouTube Format / API Change                                       │
    │        Severity  : CRITICAL (affects all groups simultaneously).         │
    │        Recovery  : yt-dlp version pinned in requirements; an out-of-    │
    │                    band update mechanism (auto-update job, canary check) │
    │                    detects download failure rate spike (> 50% in 5 min)  │
    │                    and alerts ops via Telegram monitoring group.         │
    └──────────────────────────────────────────────────────────────────────────┘
    
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ CLASS III — PROCESS FAILURES                                             │
    │                                                                          │
    │  III-1  FFmpeg Process Crash  ← TESTED BELOW                            │
    │         Severity  : RETRYABLE (exit 1, SIGSEGV) or FATAL (corrupt file) │
    │                                                                          │
    │  III-2  FFmpeg Pipe Deadlock  (stdout buffer fills, process hangs)       │
    │         Severity  : RETRYABLE                                            │
    │         Recovery  : Health watchdog monitors position_ms heartbeat;      │
    │                     if delta > 10 s with status=PLAYING, watchdog        │
    │                     treats this as a stall and issues SIGKILL, then      │
    │                     triggers the same crash recovery path as III-1.      │
    │                                                                          │
    │  III-3  Zombie Subprocess Accumulation                                   │
    │         Severity  : TRANSIENT                                            │
    │         Recovery  : All subprocess objects stored in a dict keyed by    │
    │                     group_id; StreamEngine.stop_stream always calls      │
    │                     process.wait() to reap. A periodic janitor           │
    │                     coroutine (every 60 s) iterates get_active_group_ids │
    │                     and force-reaps any process with poll() != None.     │
    └──────────────────────────────────────────────────────────────────────────┘
    
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ CLASS IV — TELEGRAM API FAILURES                                         │
    │                                                                          │
    │  IV-1  FloodWait (HTTP 429)                                              │
    │        Severity  : TRANSIENT                                             │
    │        Recovery  : aiogram's built-in retry_after handler; all          │
    │                    announcement messages are queued through an async     │
    │                    MessageThrottle (token-bucket, 20 msg/min per group). │
    │                                                                          │
    │  IV-2  Voice Chat Forcibly Closed by Telegram                            │
    │        Severity  : RETRYABLE                                             │
    │        Recovery  : ChatMemberUpdated event triggers on_voice_chat_ended. │
    │                    Queue is preserved in Redis (TTL reset to 24 h).      │
    │                    Bot does NOT auto-rejoin (would annoy users);         │
    │                    sends a group message: "Voice chat ended — use        │
    │                    /play to restart."                                    │
    │                                                                          │
    │  IV-3  Bot Kicked from Group                                             │
    │        Severity  : FATAL (for that group)                                │
    │        Recovery  : my_chat_member update handler calls                  │
    │                    StreamEngine.stop_stream(graceful=False), deletes     │
    │                    all Redis keys for group_id, frees subprocess.        │
    └──────────────────────────────────────────────────────────────────────────┘
    
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ CLASS V — APPLICATION LOGIC FAILURES                                     │
    │                                                                          │
    │  V-1  Concurrent Queue Mutation Race                                     │
    │       Severity  : TRANSIENT                                              │
    │       Recovery  : All multi-step queue mutations (dequeue + start,      │
    │                   promote + push) are wrapped in Lua scripts executed    │
    │                   atomically on Redis. No optimistic-lock retry needed.  │
    │                                                                          │
    │  V-2  Duplicate Simultaneous Downloads of the Same URL                  │
    │       Severity  : TRANSIENT                                              │
    │       Recovery  : Redlock on SHA-256(url) prevents dual download;       │
    │                   second requester polls until lock releases, then       │
    │                   reads the completed cache:meta key directly.           │
    └──────────────────────────────────────────────────────────────────────────┘
    
    ┌──────────────────────────────────────────────────────────────────────────┐
    │ CLASS VI — RESOURCE EXHAUSTION                                           │
    │                                                                          │
    │  VI-1  FFmpeg Subprocess Limit Exceeded                                  │
    │        Severity  : RETRYABLE                                             │
    │        Recovery  : Global asyncio.Semaphore(value=CPU_COUNT × 4) blocks │
    │                    start_stream until a slot is free. Waiting /play      │
    │                    commands emit "Starting soon…" to the user.           │
    │                                                                          │
    │  VI-2  File Descriptor Exhaustion                                        │
    │        Severity  : CRITICAL                                              │
    │        Recovery  : Process ulimit set via systemd LimitNOFILE=65536.    │
    │                    CacheManager closes file handles immediately after    │
    │                    handing path to FFmpeg (FFmpeg reopens independently).│
    └──────────────────────────────────────────────────────────────────────────┘

* * *

### 4.2 Tested Failure Scenario — FFmpeg Process Crash Mid-Stream

**Selected failure:** `CLASS III-1` — An active FFmpeg subprocess for `group_id = 99182` exits unexpectedly with a non-zero code (e.g., SIGSEGV from a malformed Opus frame or an OOM kill from the kernel) while 4 minutes into a 5-minute track. The voice chat connection remains open. The group hears silence. Redis still holds `status = "playing"` and `position_ms = 241 000`.

* * *

**Why this scenario is the most operationally dangerous:**  
Unlike a Redis timeout (external, recoverable by reconnect) or a yt-dlp failure (pre-stream, isolatable per track), an FFmpeg crash is silent, group-specific, leaves dangling state, and occurs at a point where the user investment in the current track is highest. It exercises the watchdog, Redis atomicity, crash counter, and cache integrity in one path.

* * *

    FAILURE TIMELINE AND RECOVERY WALK-THROUGH
    ═══════════════════════════════════════════
    
    T+0 ms  │ FFmpeg subprocess for group 99182 exits (returncode = -11 / SIGSEGV).
            │ asyncio's child-process watcher fires the process callback.
    
    T+5 ms  │ StreamEngine internal watchdog task (awaiting process.wait()) unblocks.
            │ It reads exit_code = -11, compares against known clean exit codes
            │ {0} → determines this is ABNORMAL.
    
    T+8 ms  │ Watchdog atomically writes via Lua script:
            │   HSET playback:state:99182 status "recovering"
            │   INCR crash:count:99182  (result: 1)
            │   EXPIRE crash:count:99182 300
            │ Publishes to Redis channel events:99182:
            │   { "event": "stream_crashed", "exit_code": -11, "ts": <epoch> }
    
    T+10 ms │ The group's Pub/Sub listener coroutine receives "stream_crashed".
            │ It invokes StreamEngine.recover_stream(group_id=99182,
            │   track=<current_track>, seek_ms=241000).
    
    T+11 ms │ recover_stream reads crash:count:99182 → 1 (< threshold 3).
            │ Calls CacheManager: stat() the file at Track.file_path.
            │   → File exists and is intact.  Cache hit confirmed.
    
    T+15 ms │ A fresh FFmpeg subprocess is spawned with -ss 241 seek flag,
            │   identical audio pipeline, and the existing Telegram voice-client
            │   connection (which remained open throughout).
            │ Watchdog re-attaches to the new subprocess's process.wait().
    
    T+18 ms │ Opus frames resume flowing to the voice chat.
            │ Redis updated atomically:
            │   HSET playback:state:99182 status "playing"
            │                              position_ms 241000
            │                              started_at <new_epoch>
            │ Heartbeat task resumes 1 Hz position_ms updates.
    
            │ AUDIBLE GAP: ~18 ms of silence — imperceptible to users.
    
    ──── ALTERNATIVE BRANCH: crash:count reaches threshold 3 ────────────────
    
    T+0 ms  │ Third crash on same track (crash:count = 3).
    
    T+8 ms  │ Watchdog reads crash:count:99182 → 3 (>= threshold).
            │ Classifies track as FATAL for this group.
    
    T+9 ms  │ CacheManager evicts cache:meta:{sha256} from Redis and
            │   queues the file for deletion from disk (async deletion
            │   deferred to avoid blocking the event loop).
    
    T+10 ms │ QueueManager.remove_by_id(99182, current_track.track_id) removes
            │   the offending track from both the active-list and any shadow list.
    
    T+11 ms │ TelegramHandler.on_error_event receives a JukeboxError(
            │   code="FFMPEG_CRASH_FATAL", severity=FATAL, group_id=99182).
            │   Sends group message: "⚠️ Could not play '<title>' after 3 attempts.
            │   Skipping to next track."
    
    T+13 ms │ on_track_finished_callback is invoked synthetically.
            │ QueueManager.dequeue(99182) fetches the next track.
            │ StreamEngine.start_stream(99182, next_track) is called normally.
            │ crash:count:99182 is DEL'd (clean slate for the new track).
    
    ──── CONCURRENT /skip COMMAND COLLISION (edge case within scenario) ─────
    
    T+7 ms  │ A group admin sends /skip while recover_stream is in-flight.
            │ on_skip_command calls StreamEngine.stop_stream — which checks
            │   the playback state Lua read:
            │   IF status == "recovering" THEN return "skip already queued"
            │ The /skip command returns "⏭ Skipping after recovery…" to the user.
            │ recover_stream's final HSET is guarded by a Lua CAS check:
            │   IF status == "recovering" THEN HSET status "playing"
            │   ELSE no-op (skip has since changed it to "stopped")
            │ No duplicate start_stream call is made.  Exactly-once guaranteed.

* * *

> **Architecture verdict against this scenario:** The design holds. The three key invariants — (1) Redis is always the authoritative state before and after a subprocess event, (2) all state transitions touching `playback:state` are Lua-atomic, and (3) the crash counter decouples retry logic from the hot path — ensure correct behaviour even when a `/skip` command races a mid-crash recovery. The only information lost across a crash is at most one position-heartbeat interval (≤ 1 second of seek precision), which is acceptable for a music jukebox.
