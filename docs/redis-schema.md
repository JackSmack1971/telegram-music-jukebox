# Redis Schema Reference
## Telegram Jukebox Bot

> **Normative status:** This document is the authoritative reference for all Redis key patterns, types, TTLs, field contracts, and atomicity guarantees. `src/redis/keys.py` must expose exactly the key-builder functions listed here — no more, no less. CI check `CHECK-6` validates section count; `CHECK-5` validates key-builder coverage.
>
> **Regeneration:** Run `scripts/gen_redis_docs.py` after any schema change. Do not hand-edit key patterns in `src/redis/keys.py` without a corresponding update to this file and a Changelog entry.

---

## Table of Contents

- [Namespace Convention](#namespace-convention)
- [Key Index](#key-index)
  - [Key 1 — Per-Group Track Queue](#key-1--per-group-track-queue)
  - [Key 2 — Current Playback State](#key-2--current-playback-state)
  - [Key 3 — Audio File Cache Registry](#key-3--audio-file-cache-registry)
  - [Key 4 — Download Deduplication Lock](#key-4--download-deduplication-lock)
  - [Key 5 — Per-Group Settings](#key-5--per-group-settings)
  - [Key 6 — User Rate Limiting](#key-6--user-rate-limiting)
  - [Key 7 — Voice-Chat Session Registry](#key-7--voice-chat-session-registry)
  - [Key 8 — Crash / Retry Counter](#key-8--crash--retry-counter)
  - [Key 9 — Inter-Component Event Bus](#key-9--inter-component-event-bus)
- [TTL Policy](#ttl-policy)
- [Eviction Policy](#eviction-policy)
- [Pub/Sub Channel Catalogue](#pubsub-channel-catalogue)
- [Key Lifecycle Diagrams](#key-lifecycle-diagrams)
- [Changelog](#changelog)

---

## Namespace Convention

All keys in this document omit the full prefix for readability. Every key constructed by `src/redis/keys.py` is prefixed as follows:

```
jukebox:{env}:{key_pattern}
```

| Segment | Values | Source |
|---------|--------|--------|
| `jukebox` | Literal string — never changes | Hardcoded in `keys.py` |
| `{env}` | `prod` \| `staging` \| `test` | `REDIS_KEY_PREFIX` env var → `src/config.py` |
| `{key_pattern}` | One of the 9 patterns defined below | `keys.py` builder functions |

**Enforcement:** No module outside `src/redis/keys.py` may construct a Redis key string directly. Hardcoded key patterns elsewhere are a `CHECK-5` lint failure.

**Key-builder function naming convention:** `build_{key_type}_key()` — e.g. `build_queue_key(group_id: int) -> str`.

**Example:**

```python
# src/redis/keys.py
def build_queue_key(group_id: int) -> str:
    return f"{settings.redis_key_prefix}queue:{group_id}"
# resolves to: jukebox:prod:queue:99182
```

---

## Key Index

### Key 1 — Per-Group Track Queue

```
Key pattern : queue:{group_id}
Redis type  : LIST
TTL         : 86 400 s  (24 h — reset on every RPUSH)
Owner       : src/queue/redis_queue_manager.py
Key builder : build_queue_key(group_id: int) -> str
```

#### Fields / Value Format

Each LIST element is a **msgpack-serialised `Track` dataclass** (`src/models/track.py`). No JSON — msgpack reduces per-element overhead at queue depths ≤ 50.

| Dataclass Field | Type | Notes |
|----------------|------|-------|
| `track_id` | `str` (UUID4) | Generated at enqueue time |
| `url` | `str` | Canonical source URL |
| `title` | `str` | |
| `duration_s` | `int` | `0` if unknown before download |
| `file_path` | `str \| None` | `None` until download completes |
| `requested_by` | `int` | Telegram `user_id` |
| `group_id` | `int` | Telegram supergroup `chat_id` |
| `added_at` | `datetime` | ISO 8601 epoch ms |
| `status` | `TrackStatus` | `QUEUED \| DOWNLOADING \| READY \| PLAYING \| FAILED \| SKIPPED` |
| `thumbnail_url` | `str \| None` | Optional |

#### Permitted Operations

| Operation | Command | Notes |
|-----------|---------|-------|
| Enqueue (tail) | `RPUSH queue:{gid} <msgpack>` | Raises `QueueFullError` if `LLEN >= GroupSettings.max_queue_length` |
| Dequeue (head, atomic) | `LMOVE queue:{gid} queue:processing:{gid} LEFT RIGHT` | Via `skip_atomic.lua` — never raw `LPOP` in application code |
| Snapshot (non-destructive) | `LRANGE queue:{gid} 0 N-1` | Used by `/queue` pagination |
| Depth check | `LLEN queue:{gid}` | O(1); does not deserialise any Track |
| Remove by ID | `LREM` scan inside `promote_atomic.lua` | O(N); acceptable at max depth 50 |
| Promote to front | `promote_atomic.lua` | Atomic `LREM` + `LPUSH` in single script |
| Clear | `DEL queue:{gid}` | Returns count of removed elements |
| TTL refresh | `EXPIRE queue:{gid} 86400` | On every `RPUSH` |

#### Atomicity Guarantees

| Operation | Guarantee | Script |
|-----------|-----------|--------|
| Dequeue for processing | Exactly-once delivery; track moves to shadow list atomically. No double-pop under concurrent `/skip`. | `src/queue/lua/skip_atomic.lua` |
| Promote to front | Track never appears twice in queue. Remove and re-insert are one Redis operation. | `src/queue/lua/promote_atomic.lua` |
| Shadow list cleanup | Shadow list `queue:processing:{gid}` never grows unbounded on clean completions. | `src/queue/lua/ack_processed.lua` |

#### Shadow List Pattern

```
queue:{group_id}            ← active queue (RPUSH / LRANGE)
queue:processing:{group_id} ← in-flight track (between dequeue and ack)
```

`LMOVE` atomically transfers the head track to the shadow list before `StreamEngine` begins playback. `QueueManager.ack_processed()` removes it from the shadow list on clean track completion. If the process crashes before `ack_processed`, the track remains in the shadow list and is re-queued on startup reconciliation.

#### Related Keys

- `playback:state:{group_id}` — updated atomically alongside dequeue in crash recovery path
- `crash:count:{group_id}` — FATAL tracks are removed from this queue before the counter is deleted

---

### Key 2 — Current Playback State

```
Key pattern : playback:state:{group_id}
Redis type  : HASH
TTL         : 3 600 s  (1 h — reset on every HSET)
Owner       : src/stream/ffmpeg_stream_engine.py
Key builder : build_playback_state_key(group_id: int) -> str
```

#### Fields

| Field | Type | Valid Values | Updated By |
|-------|------|-------------|------------|
| `status` | `str` | `idle \| loading \| playing \| paused \| stopped \| recovering` | `playback_state_atomic.lua`, `crash_recovery_atomic.lua` |
| `track_id` | `str` | UUID4 or `""` | `playback_state_atomic.lua` |
| `title` | `str` | | `playback_state_atomic.lua` |
| `url` | `str` | | `playback_state_atomic.lua` |
| `duration_s` | `int` | | `playback_state_atomic.lua` |
| `position_ms` | `int` | Milliseconds | `src/stream/heartbeat.py` — 1 Hz write |
| `started_at` | `int` | Unix epoch ms | `playback_state_atomic.lua` |
| `volume` | `str` | Float `0.00–1.00` stored as string | `volume_update_atomic.lua` |
| `loop_mode` | `str` | `none \| track \| queue` | `playback_state_atomic.lua` |
| `requested_by` | `int` | Telegram `user_id` | `playback_state_atomic.lua` |

> **Why `volume` is stored as a string:** Redis HASH values are byte strings. Storing as float-formatted string (`"0.80"`) avoids float precision drift on INCR operations and is directly passable to FFmpeg's `-af volume=` flag without conversion.

#### Permitted Operations

| Operation | Command | Notes |
|-----------|---------|-------|
| Full state write | `HSET playback:state:{gid} status ... track_id ...` | Always via Lua — never raw `HSET` in application code |
| Position heartbeat | `HSET playback:state:{gid} position_ms {ms}` | Direct from `heartbeat.py` — only field allowed raw `HSET` |
| Volume update | `HSET playback:state:{gid} volume {v}` | Via `volume_update_atomic.lua` |
| Full state read | `HGETALL playback:state:{gid}` | Used by `StreamEngine.get_playback_state()` |
| Single field read | `HGET playback:state:{gid} status` | Used by watchdog CAS checks |

#### Status Transition Graph

```
                 ┌──────────────────────────────┐
                 │              idle             │◄──── bot joins VC / queue emptied
                 └──────────────┬───────────────┘
                                │ /play enqueued, download starts
                                ▼
                 ┌──────────────────────────────┐
                 │            loading            │
                 └──────────────┬───────────────┘
                                │ download complete, FFmpeg spawned
                                ▼
          ┌──────────────────────────────────────────┐
          │                  playing                  │◄─── recover_stream() success
          └──┬─────────────┬────────────┬────────────┘
             │             │            │
           /pause       /skip       crash detected (watchdog)
             │             │            │
             ▼             ▼            ▼
          paused        stopped     recovering
             │                         │
           /resume                     ├─ crash_count < 3 → playing (seek resumed)
             │                         └─ crash_count ≥ 3 → stopped (track evicted)
             └──────────► playing
```

#### Atomicity Guarantees

| Operation | Guarantee | Script |
|-----------|-----------|--------|
| Status + field co-write | `status` and all related fields (`track_id`, `started_at`, `position_ms`) are always written together. No observer sees a partial state. | `src/stream/lua/playback_state_atomic.lua` |
| Crash recovery transition | `status=recovering` and `INCR crash:count` are a single atomic unit. A reader never sees `recovering` without a corresponding crash counter increment. | `src/stream/lua/crash_recovery_atomic.lua` |
| Skip-race CAS | If `status == "recovering"`, a concurrent `/skip` returns without triggering a new `start_stream`. The CAS guard is evaluated and the conditional write are atomic. | `src/stream/lua/crash_recovery_atomic.lua` |
| Volume update | `volume` field and FFmpeg `-af volume` reinit are co-consistent. | `src/stream/lua/volume_update_atomic.lua` |

#### Related Keys

- `queue:{group_id}` — dequeued track's metadata copied into this HASH on stream start
- `crash:count:{group_id}` — incremented atomically alongside `status=recovering`
- `vc:session:{group_id}` — voice-chat session must exist before this key is written

---

### Key 3 — Audio File Cache Registry

```
Key pattern : cache:meta:{sha256_of_canonical_url}
Redis type  : HASH
TTL         : 604 800 s  (7 days — LRU touch resets TTL)
Owner       : src/cache/cache_manager.py
Key builder : build_cache_meta_key(url: str) -> str  (hashes URL internally)
```

#### Hash Function

```python
# src/redis/keys.py
import hashlib

def build_cache_meta_key(url: str) -> str:
    sha = hashlib.sha256(url.encode()).hexdigest()
    return f"{settings.redis_key_prefix}cache:meta:{sha}"
```

The canonical URL is normalised before hashing (query parameter sort, scheme lowercase) to maximise deduplication hit rate across slightly different URL forms pointing to the same resource.

#### Fields

| Field | Type | Notes |
|-------|------|-------|
| `file_path` | `str` | Absolute path on disk — must be `stat()`-verified before use |
| `file_size_b` | `int` | Bytes |
| `duration_s` | `int` | Total track duration |
| `title` | `str` | Extracted by yt-dlp |
| `thumbnail_url` | `str \| ""` | Empty string if unavailable |
| `cached_at` | `int` | Unix epoch ms — write timestamp |
| `hit_count` | `int` | Incremented by `HINCRBY` on each cache hit |

#### Critical Invariant

> **INV-009:** Key existence does NOT guarantee the file is on disk. `CacheManager.stat()` MUST be called on `file_path` before trusting this key. A cache hit without a `stat()` confirmation is a bug.

```python
# Correct cache-hit pattern (src/cache/cache_manager.py)
meta = await redis.hgetall(build_cache_meta_key(url))
if meta and os.path.exists(meta["file_path"]):
    await _touch(meta)          # reset TTL + increment hit_count
    return meta["file_path"]
else:
    await redis.delete(key)     # evict stale registry entry
    return None                 # triggers download
```

#### Permitted Operations

| Operation | Command | Notes |
|-----------|---------|-------|
| Register | `HSET cache:meta:{sha} field value [...]` | On download completion |
| Cache hit touch | `EXPIRE cache:meta:{sha} 604800` + `HINCRBY hit_count 1` | Via `cache_touch_atomic.lua` — atomic TTL reset + counter |
| Evict (manual) | `DEL cache:meta:{sha}` + async file delete | On `crash_count ≥ 3` (fatal track) or manual `scripts/cache_purge.py` |
| Existence check | `EXISTS cache:meta:{sha}` | Before enqueuing a download |

#### Atomicity Guarantees

| Operation | Guarantee | Script |
|-----------|-----------|--------|
| LRU touch | TTL reset and `hit_count` increment are co-consistent. A reader never observes a reset TTL without a corresponding counter increment. | `src/cache/lua/cache_touch_atomic.lua` |

#### LRU Eviction Logic

The Redis key TTL is a soft eviction mechanism. Hard eviction is managed by `src/cache/eviction_policy.py`:

1. Pre-download: if `CACHE_DIR` free space < `CACHE_EVICTION_THRESHOLD_MB` (default 500 MB), a synchronous LRU sweep runs before writing any new file.
2. LRU order is approximated by `cached_at` ascending — files not accessed recently have earlier timestamps.
3. Active files (currently playing, `file_path` in any `playback:state:*` HASH) are exempt from eviction.
4. If sweep cannot free enough space (all files active), `DownloadResult.success=False`, track set to `TrackStatus.FAILED`.

#### Related Keys

- `lock:download:{sha256}` — Redlock on same hash prevents duplicate concurrent downloads
- `playback:state:{group_id}` — `file_path` from this key is passed to `StreamEngine`

---

### Key 4 — Download Deduplication Lock

```
Key pattern : lock:download:{sha256_of_canonical_url}
Redis type  : STRING  (value = worker_uuid)
TTL         : 300 s  (hard ceiling — any download exceeding this is killed)
Owner       : src/download/download_worker.py
Key builder : build_download_lock_key(url: str) -> str
```

#### Lock Value

The value stored is a **UUID4 identifying the worker coroutine** that acquired the lock. This is the Redlock owner-check mechanism — only the coroutine that set the key may delete it.

```
Value format : <worker_uuid>          e.g. "f47ac10b-58cc-4372-a567-0e02b2c3d479"
```

#### Permitted Operations

| Operation | Command | Notes |
|-----------|---------|-------|
| Acquire | `SET lock:download:{sha} {uuid} NX PX 300000` | `NX` = only set if not exists; `PX` = TTL in ms |
| Release (safe) | Lua owner-check: `GET` → if value matches `{uuid}` → `DEL` | Via `src/download/lua/release_lock_atomic.lua` |
| Poll (racing coroutine) | `EXISTS lock:download:{sha}` with 100 ms sleep | Racing coroutine waits, then reads `cache:meta:{sha}` directly |

#### Redlock Protocol (Single-Instance Simplified)

This implementation uses single-instance Redlock (not distributed multi-instance) because the Redis Sentinel cluster presents a single logical master. Full Redlock requires N independent Redis instances and is not warranted here.

```
Acquire:
  SET lock:download:{sha} {worker_uuid} NX PX 300000
  → returns "OK"  → lock acquired, proceed with download
  → returns nil   → lock held by another worker

  Racing worker:
    WAIT 100ms, re-check EXISTS
    When key disappears → check cache:meta:{sha}
    If cache hit → return cached file_path (no download needed)
    If no cache hit → this indicates the prior download failed;
                      re-acquire lock and retry

Release:
  -- release_lock_atomic.lua --
  local v = redis.call('GET', KEYS[1])
  if v == ARGV[1] then
    return redis.call('DEL', KEYS[1])
  end
  return 0
```

#### Atomicity Guarantees

| Operation | Guarantee | Script |
|-----------|-----------|--------|
| Safe release | Only the owning worker can release the lock. A foreign `DEL` (e.g. from a crashed worker's recovery path) is a no-op. | `src/download/lua/release_lock_atomic.lua` |

> **Why the TTL is 300 s:** Matches `REDLOCK_TTL_S` in `.env.example`. This is the hard ceiling on any download — if a worker hangs, the lock auto-expires and the next requester can retry. Raising above 300 s risks queue starvation when a download permanently hangs.

#### Related Keys

- `cache:meta:{sha256}` — written by the lock holder on download completion; read by racing coroutines after lock release

---

### Key 5 — Per-Group Settings

```
Key pattern : settings:{group_id}
Redis type  : HASH
TTL         : none  (persistent — deleted explicitly on /reset or bot kick)
Owner       : src/broker/handlers/  (read), src/queue/redis_queue_manager.py (enforced)
Key builder : build_settings_key(group_id: int) -> str
```

#### Fields

| Field | Type | Default | Set By |
|-------|------|---------|--------|
| `max_queue_len` | `int` | `50` | `/settings` admin command |
| `max_duration_s` | `int` | `600` | `/settings` admin command |
| `admin_only_skip` | `"0" \| "1"` | `"0"` | `/settings` admin command |
| `announce_tracks` | `"0" \| "1"` | `"1"` | `/settings` admin command |
| `dj_role_id` | `int \| ""` | `""` | `/settings` admin command |
| `shuffle_seed` | `int \| ""` | `""` | `/shuffle` command (reproducible shuffle) |
| `max_requests_per_min` | `int` | `5` | `/settings` admin command |

> **Boolean storage:** Redis HASH values are byte strings. Booleans are stored as `"0"` / `"1"`, not Python `True`/`False` or JSON `true`/`false`. `src/models/group_settings.py` handles deserialisation via `bool(int(v))`.

#### Key Lifecycle

```
Created  : On first /play command in a group, if key does not exist.
           Defaults are written by src/queue/redis_queue_manager.py
           using HSETNX (set-if-not-exists per field).
Updated  : By admin /settings command.
Deleted  : On /reset (admin), or on IV-3 (bot kicked) by
           src/broker/handlers/voice_chat_handler.py.
TTL      : None — must be deleted explicitly. No TTL prevents
           settings being silently wiped by Redis memory pressure.
```

#### No TTL Rationale

> Settings represent a group administrator's deliberate configuration. Auto-expiring them after inactivity would silently reset `admin_only_skip`, `dj_role_id`, and queue limits — breaking security invariants without any user-visible event. See `docs/adr/001-redis-as-sot.md` for full rationale.

#### Related Keys

- `queue:{group_id}` — `max_queue_len` enforced on every `RPUSH`
- `ratelimit:{user_id}:{group_id}` — `max_requests_per_min` read on every `/play`
- `vc:session:{group_id}` — deleted alongside settings on bot kick (CLASS IV-3)

---

### Key 6 — User Rate Limiting

```
Key pattern : ratelimit:{user_id}:{group_id}
Redis type  : STRING  (counter, INCR)
TTL         : 60 s  (auto-resets sliding window)
Owner       : src/gateway/middleware/rate_limiter.py
Key builder : build_ratelimit_key(user_id: int, group_id: int) -> str
```

#### Logic

```
On every /play command by user_id in group_id:

  1. SET ratelimit:{uid}:{gid} 1 NX PX 60000
     → "OK"  → first request in window; counter initialised to 1
     → nil   → window already open; proceed to INCR

  2. INCR ratelimit:{uid}:{gid}
     → result ≤ settings.max_requests_per_min → allow
     → result > settings.max_requests_per_min → deny
       Reply: "⏳ Slow down — max {n} tracks/min."
       Do NOT reset TTL on deny (prevents window extension attack).
```

> **Why `SET NX` before `INCR`:** Setting with `NX PX` on the first request attaches the 60 s TTL atomically. A bare `INCR` on a non-existent key creates the key without a TTL — the counter would never expire and permanently block the user. `SET NX` + conditional `INCR` is the correct sliding-window pattern.

#### Permitted Operations

| Operation | Command | Notes |
|-----------|---------|-------|
| Initialise window | `SET ratelimit:{uid}:{gid} 1 NX PX 60000` | First request per window |
| Increment | `INCR ratelimit:{uid}:{gid}` | Subsequent requests in same window |
| Read (health check) | `GET ratelimit:{uid}:{gid}` | Monitoring only |

#### No Lua Required

All rate-limit operations are single-command (no multi-key atomicity needed). The `SET NX` + `INCR` sequence is safe under concurrent access because:
- If two coroutines race on `SET NX`, exactly one receives `"OK"` and one receives `nil`.
- The `nil` responder then `INCR`s a key that already has a TTL set.
- No two-command sequence creates a window inconsistency.

#### Related Keys

- `settings:{group_id}` — `max_requests_per_min` field read to determine the deny threshold

---

### Key 7 — Voice-Chat Session Registry

```
Key pattern : vc:session:{group_id}
Redis type  : HASH
TTL         : none  (managed on VC join/leave events)
Owner       : src/broker/handlers/voice_chat_handler.py
Key builder : build_vc_session_key(group_id: int) -> str
```

#### Fields

| Field | Type | Notes |
|-------|------|-------|
| `chat_id` | `int` | Telegram supergroup `chat_id` |
| `voice_chat_id` | `int` | Internal Telegram VC session identifier |
| `bot_joined_at` | `int` | Unix epoch ms |
| `listener_count` | `int` | Updated on `ChatMemberUpdated` events |

#### Key Lifecycle

```
Created  : on_voice_chat_started — bot joins VC via pyrogram/pytgcalls.
           HSET vc:session:{gid} chat_id ... voice_chat_id ... bot_joined_at ... listener_count 0

Updated  : on ChatMemberUpdated → HSET vc:session:{gid} listener_count {n}

Deleted  : on_voice_chat_ended  → DEL vc:session:{gid}
           on_bot_kicked (IV-3) → DEL vc:session:{gid} (alongside all other group keys)

TTL      : None — the VC session exists exactly as long as the bot is in the voice chat.
           Auto-expiry would cause the bot to lose session state while still connected.
```

#### Session Existence Check

`StreamEngine.start_stream()` checks for `vc:session:{group_id}` existence before spawning an FFmpeg subprocess. Starting a stream without an active VC session is a no-op with a log warning, not an error — the VC may have just been forcibly closed by Telegram (CLASS IV-2).

#### Related Keys

- `playback:state:{group_id}` — cleared to `status=stopped` on VC termination
- `queue:{group_id}` — TTL reset to 24 h on VC termination (queue preserved for when VC resumes)
- `settings:{group_id}` — deleted on bot kick (IV-3) but preserved on VC end (IV-2)

---

### Key 8 — Crash / Retry Counter

```
Key pattern : crash:count:{group_id}
Redis type  : STRING  (counter, INCR)
TTL         : 300 s  (auto-evicts after 5 min quiet period — reset on INCR)
Owner       : src/stream/watchdog.py
Key builder : build_crash_count_key(group_id: int) -> str
```

#### Logic

```
On abnormal FFmpeg exit (watchdog detects returncode ∉ {0}):

  1. INCR crash:count:{gid}          → new count N
  2. EXPIRE crash:count:{gid} 300    → reset 5-min eviction window
     (both ops in crash_recovery_atomic.lua alongside HSET status=recovering)

  if N < CRASH_COUNT_THRESHOLD (default 3):
    → RETRYABLE path: spawn fresh FFmpeg with -ss {position_ms} seek
    → status transitions: recovering → playing

  if N >= CRASH_COUNT_THRESHOLD:
    → FATAL path:
        DEL cache:meta:{sha256}       (evict from Redis registry)
        async delete file from disk   (deferred, non-blocking)
        QueueManager.remove_by_id()   (remove from active + shadow list)
        notify group: "⚠️ Could not play '<title>' after 3 attempts."
        DEL crash:count:{gid}         (clean slate for next track)
        QueueManager.dequeue()        (advance to next track)

On clean track completion (ack_processed):
    DEL crash:count:{gid}             (clean slate)
```

#### Why 300 s TTL

The 5-minute TTL ensures the counter does not persist across unrelated tracks. If a group plays Track A (crashes once), then Track B (completes cleanly), the counter for the group evicts naturally within 5 minutes. The `DEL` on clean completion is belt-and-suspenders.

#### Atomicity Guarantees

| Operation | Guarantee | Script |
|-----------|-----------|--------|
| Crash counter + status transition | `INCR crash:count` and `HSET status=recovering` are a single atomic unit. No observer sees `recovering` status without a matching counter increment. | `src/stream/lua/crash_recovery_atomic.lua` |

#### Related Keys

- `playback:state:{group_id}` — `status=recovering` written atomically with this counter increment
- `cache:meta:{sha256}` — evicted when counter reaches threshold
- `queue:{group_id}` — FATAL track removed via `QueueManager.remove_by_id()`

---

### Key 9 — Inter-Component Event Bus

```
Key pattern : events:{group_id}           ← per-group channel
Redis type  : Pub/Sub CHANNEL  (not a persistent key)
TTL         : N/A  (Pub/Sub channels have no TTL or persistence)
Owner       : src/pubsub/event_bus.py
Key builder : build_event_channel(group_id: int) -> str
```

> **This is not a stored key.** Pub/Sub channels exist only while there are active subscribers. No data is persisted in Redis — if no subscriber is listening when a message is published, the message is lost. This is by design; missed events are recovered via `playback:state` polling on reconnect.

#### Publishers

| Component | Module | Events Published |
|-----------|--------|-----------------|
| `StreamEngine` | `src/stream/ffmpeg_stream_engine.py` | `track_started`, `track_finished`, `track_skipped`, `stream_crashed` |
| `DownloadWorker` | `src/download/download_worker.py` | `download_ready` |
| `QueueManager` | `src/queue/redis_queue_manager.py` | `queue_empty` |

#### Subscribers

| Component | Module | Events Consumed |
|-----------|--------|----------------|
| `CommandBroker` | `src/pubsub/listener.py` | All — routes to `TelegramHandler` callbacks |

#### Payload Schemas

All payloads are **JSON-encoded strings**. Subscribers must handle `json.loads()` failures gracefully (log and discard malformed messages).

```json
{ "event": "track_started",  "track_id": "<uuid4>",  "ts": 1718000000000 }
{ "event": "track_finished", "track_id": "<uuid4>",  "ts": 1718000000000 }
{ "event": "track_skipped",  "track_id": "<uuid4>",  "ts": 1718000000000 }
{ "event": "stream_crashed", "exit_code": -11,       "ts": 1718000000000 }
{ "event": "download_ready", "track_id": "<uuid4>",  "ts": 1718000000000 }
{ "event": "queue_empty",                            "ts": 1718000000000 }
```

| Field | Type | Notes |
|-------|------|-------|
| `event` | `str` | One of the 6 event names above |
| `track_id` | `str` (UUID4) | Present on all track-level events; absent on `queue_empty` |
| `exit_code` | `int` | Present only on `stream_crashed`; OS signal code (e.g. `-11` = SIGSEGV) |
| `ts` | `int` | Unix epoch milliseconds — publisher timestamp |

#### Decoupling Contract

`DownloadWorker` and `StreamEngine` communicate **exclusively** via this channel. No direct Python object references, shared state, or import paths between these two packages are permitted. This is enforced by `CHECK-10` (import matrix) in `scripts/lint_structure.py`. See `docs/adr/003-pubsub-decoupling.md`.

---

## TTL Policy

| Key Pattern | TTL | Reset Trigger | Rationale |
|-------------|-----|---------------|-----------|
| `queue:{gid}` | 86 400 s (24 h) | Every `RPUSH` | Queue survives VC termination for 24 h; user can `/play` to resume |
| `playback:state:{gid}` | 3 600 s (1 h) | Every `HSET` | Short TTL — state is meaningless after 1 h of inactivity |
| `cache:meta:{sha}` | 604 800 s (7 d) | `cache_touch_atomic.lua` on every hit | LRU approximation; 7-day window balances hit rate vs. disk pressure |
| `lock:download:{sha}` | 300 s | Not reset — hard ceiling | Any download exceeding 5 min is treated as a hung worker |
| `settings:{gid}` | **none** | Explicit `DEL` only | Admin config must not auto-expire |
| `ratelimit:{uid}:{gid}` | 60 s | Not reset on deny | Sliding-window counter; extension attack prevention |
| `vc:session:{gid}` | **none** | Explicit `DEL` on VC end / bot kick | Session lifetime tied to VC lifecycle, not wall-clock time |
| `crash:count:{gid}` | 300 s | Every `INCR` | Auto-evicts after 5 min quiet period between crash events |
| `events:{gid}` | N/A (Pub/Sub) | N/A | Not a stored key; no TTL applies |

### Keys With No TTL — Explicit Deletion Contract

Two keys carry no TTL and must be deleted explicitly:

**`settings:{group_id}`** — deleted by:
- `on_reset_command` (admin `/reset`)
- `on_bot_kicked` (CLASS IV-3) in `voice_chat_handler.py`

**`vc:session:{group_id}`** — deleted by:
- `on_voice_chat_ended` (CLASS IV-2)
- `on_bot_kicked` (CLASS IV-3)

If either key leaks (process crash before deletion), `scripts/redis_flush_group.py` provides manual cleanup.

---

## Eviction Policy

### Redis `maxmemory-policy` Setting

```
maxmemory-policy: allkeys-lru
```

**Rationale:** `allkeys-lru` allows Redis to evict any key under memory pressure, ordered by least-recently-used. This is preferable to `volatile-lru` (only TTL-bearing keys) because it ensures Redis never OOMs even if `settings:{gid}` or `vc:session:{gid}` (TTL-less keys) accumulate.

**Risk mitigation:** The two TTL-less key types (`settings`, `vc:session`) are small HASHes (≤ 7 fields, ≤ 256 bytes each). At 500 concurrent groups, total memory for these keys is < 256 KB — negligible relative to queue and playback state. LRU eviction of a `settings:{gid}` key would silently reset group configuration; this is acceptable as a last-resort safety valve under severe memory pressure, and would trigger a structured log alarm.

### Disk Cache Eviction

Managed by `src/cache/eviction_policy.py` independently of Redis:

```
Budget       : CACHE_MAX_SIZE_GB (default 20 GB)
Sweep trigger: free_space < CACHE_EVICTION_THRESHOLD_MB (default 500 MB)
LRU order    : cache:meta:{sha}.cached_at ascending (oldest-written first)
Exempt files : any file_path present in playback:state:*.file_path
               (currently-playing files are never evicted mid-stream)
```

---

## Pub/Sub Channel Catalogue

| Channel | Publisher(s) | Subscriber(s) | Message Count (max/group) |
|---------|-------------|---------------|--------------------------|
| `events:{group_id}` | `StreamEngine`, `DownloadWorker`, `QueueManager` | `src/pubsub/listener.py` (one per group) | 6 distinct event types |

### Subscription Lifecycle

```
Subscribe  : on_voice_chat_started → listener.py creates per-group subscription coroutine
Unsubscribe: on_voice_chat_ended  → coroutine cancelled, UNSUBSCRIBE sent
Reconnect  : On Redis reconnect (CLASS I-1 recovery), all per-group subscriptions
             are re-established. Missed events during outage are recovered by
             reading playback:state:{gid} directly (HGETALL) on reconnect.
```

### Message Loss Policy

Pub/Sub is fire-and-forget. A message published while no subscriber is listening is **permanently lost**. This is acceptable because:

1. `track_finished` → if missed, the heartbeat stall detector (CLASS III-2) triggers recovery after `STREAM_STALL_TIMEOUT_S`.
2. `stream_crashed` → if missed, the same heartbeat stall path handles it.
3. `download_ready` → if missed, `StreamEngine` polls `cache:meta:{sha}` on next dequeue.
4. `queue_empty` → purely informational; no recovery action required.

---

## Key Lifecycle Diagrams

### Per-Group Key Flow — Normal Play Cycle

```
User sends /play <url>
        │
        ├─► build_ratelimit_key(uid, gid)
        │     SET NX PX 60000 → INCR → allow/deny
        │
        ├─► build_settings_key(gid)          [read] max_duration_s, max_queue_len
        │
        ├─► build_queue_key(gid)
        │     RPUSH <msgpack Track>          → queue depth +1
        │     EXPIRE 86400
        │
        ├─► build_download_lock_key(url)
        │     SET NX PX 300000 {worker_uuid} → lock acquired
        │     [yt-dlp download runs]
        │     release_lock_atomic.lua        → DEL if owner
        │
        ├─► build_cache_meta_key(url)
        │     HSET file_path, size, duration, title, cached_at, hit_count=1
        │     EXPIRE 604800
        │
        ├─► events:{gid} PUBLISH download_ready
        │
        ├─► build_queue_key(gid)
        │     skip_atomic.lua               → LMOVE queue → queue:processing
        │
        ├─► build_playback_state_key(gid)
        │     playback_state_atomic.lua     → HSET status=playing, track_id, ...
        │     EXPIRE 3600
        │
        │   [FFmpeg subprocess running]
        │   heartbeat.py (1 Hz):
        │     HSET playback:state:{gid} position_ms {ms}
        │
        ├─► events:{gid} PUBLISH track_finished
        │
        ├─► build_queue_key(gid)
        │     ack_processed.lua             → LREM queue:processing
        │
        └─► [repeat from RPUSH for next track, or HSET status=idle]
```

### Per-Group Key Flow — FFmpeg Crash Recovery (CLASS III-1)

```
FFmpeg exits with returncode = -11 (SIGSEGV)
        │
        ├─► crash_recovery_atomic.lua
        │     HSET playback:state:{gid} status=recovering
        │     INCR crash:count:{gid}        → N
        │     EXPIRE crash:count:{gid} 300
        │     PUBLISH events:{gid} stream_crashed
        │
        ├─► [if N < 3] RETRYABLE PATH
        │     build_cache_meta_key(url)     → stat() file_path → cache hit confirmed
        │     [spawn fresh FFmpeg -ss {position_ms}]
        │     playback_state_atomic.lua     → HSET status=playing, started_at=now
        │     [heartbeat resumes]
        │
        └─► [if N >= 3] FATAL PATH
              DEL cache:meta:{sha}
              [async file delete]
              QueueManager.remove_by_id()   → LREM queue:{gid}
              DEL crash:count:{gid}
              PUBLISH events:{gid} track_skipped
              [advance to next track]
```

### Key Cleanup — Bot Kicked (CLASS IV-3)

```
my_chat_member update → new_status = "kicked"
        │
        ├─► StreamEngine.stop_stream(graceful=False)  → SIGKILL FFmpeg
        │
        ├─► DEL playback:state:{gid}
        ├─► DEL queue:{gid}
        ├─► DEL queue:processing:{gid}
        ├─► DEL vc:session:{gid}
        ├─► DEL settings:{gid}
        └─► DEL crash:count:{gid}   (if present)
            [cache:meta keys are NOT deleted — audio files remain for other groups]
```

---

## Changelog

> Append-only. One entry per schema change. Do not edit existing entries.

| Version | Date | Author | Change |
|---------|------|--------|--------|
| 1.0.0 | <!-- YYYY-MM-DD --> | Architect | Initial schema — 9 key patterns, all fields, TTL policy, eviction policy, lifecycle diagrams |
