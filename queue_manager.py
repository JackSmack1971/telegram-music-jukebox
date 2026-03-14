from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

import msgpack
import redis.asyncio as redis


"""Implementation Reasoning
- A per-group Redis lock guards multi-step queue mutations so length, duplicate, and per-user checks are consistent.
- The lock uses SET NX PX with a token and a Lua owner-check release to prevent accidental unlocks.
- Duplicate detection scans both the active queue and the processing shadow list to block re-queues of the same URL.
- LMOVE handles dequeue atomically while TTL refresh keeps queues alive after every successful mutation.
"""


class TrackStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    READY = "ready"
    PLAYING = "playing"
    FAILED = "failed"
    SKIPPED = "skipped"


class PlaybackStatus(StrEnum):
    IDLE = "idle"
    LOADING = "loading"
    PLAYING = "playing"
    PAUSED = "paused"
    STOPPED = "stopped"
    RECOVERING = "recovering"


class LoopMode(StrEnum):
    NONE = "none"
    TRACK = "track"
    QUEUE = "queue"


class ErrorSeverity(StrEnum):
    TRANSIENT = "transient"
    RETRYABLE = "retryable"
    FATAL = "fatal"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class Track:
    track_id: str
    url: str
    title: str
    duration_s: int
    file_path: str | None
    requested_by: int
    group_id: int
    added_at: datetime
    status: TrackStatus = TrackStatus.QUEUED
    thumbnail_url: str | None = None


@dataclass(slots=True)
class PlaybackState:
    group_id: int
    status: PlaybackStatus
    current_track: Track | None
    position_ms: int
    volume: float
    loop_mode: LoopMode
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True, slots=True)
class GroupSettings:
    group_id: int
    max_queue_length: int = 50
    max_track_duration_s: int = 600
    admin_only_skip: bool = False
    announce_tracks: bool = True
    dj_role_id: int | None = None
    max_requests_per_min: int = 5


@dataclass(slots=True)
class DownloadResult:
    track_id: str
    success: bool
    file_path: str | None
    error_message: str | None
    duration_s: int | None
    file_size_bytes: int | None
    cache_hit: bool = False


@dataclass(frozen=True, slots=True)
class JukeboxError:
    code: str
    severity: ErrorSeverity
    group_id: int | None
    track_id: str | None
    detail: str
    raised_at: datetime = field(default_factory=datetime.utcnow)


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


class QueueFullError(Exception):
    pass


class DuplicateTrackError(Exception):
    pass


class UserQueueLimitError(Exception):
    pass


class QueueLockTimeoutError(Exception):
    pass


class AsyncRedisLock:
    def __init__(
        self,
        client: redis.Redis,
        key: str,
        *,
        ttl_ms: int,
        wait_timeout_s: float,
        poll_interval_s: float = 0.1,
    ) -> None:
        self._client = client
        self._key = key
        self._ttl_ms = ttl_ms
        self._wait_timeout_s = wait_timeout_s
        self._poll_interval_s = poll_interval_s
        self._token: str | None = None

    async def __aenter__(self) -> str:
        token = uuid.uuid4().hex
        deadline = asyncio.get_running_loop().time() + self._wait_timeout_s
        while True:
            acquired = await self._client.set(self._key, token, nx=True, px=self._ttl_ms)
            if acquired:
                self._token = token
                return token
            if asyncio.get_running_loop().time() >= deadline:
                raise QueueLockTimeoutError(f"Timed out acquiring lock {self._key}")
            await asyncio.sleep(self._poll_interval_s)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not self._token:
            return
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
            "return redis.call('DEL', KEYS[1]) "
            "else return 0 end"
        )
        await self._client.eval(script, 1, self._key, self._token)
        self._token = None


class RedisQueueManager(QueueManager):
    def __init__(
        self,
        client: redis.Redis,
        *,
        namespace: str,
        queue_ttl_s: int = 86_400,
        max_queue_length: int = 20,
        max_per_user: int = 3,
        lock_ttl_ms: int = 3_000,
        lock_wait_s: float = 5.0,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._queue_ttl_s = queue_ttl_s
        self._max_queue_length = max_queue_length
        self._max_per_user = max_per_user
        self._lock_ttl_ms = lock_ttl_ms
        self._lock_wait_s = lock_wait_s

    async def enqueue(self, group_id: int, track: Track) -> int:
        queue_key = self._queue_key(group_id)
        processing_key = self._processing_key(group_id)
        async with self._lock(group_id):
            current_length = await self._client.llen(queue_key)
            if current_length >= self._max_queue_length:
                raise QueueFullError("Queue length limit reached")

            queue_items = await self._client.lrange(queue_key, 0, -1)
            processing_items = await self._client.lrange(processing_key, 0, -1)
            all_items = queue_items + processing_items

            user_count = 0
            for raw in all_items:
                existing = self._unpack_track(raw)
                if existing.url == track.url or existing.track_id == track.track_id:
                    raise DuplicateTrackError("Track already queued")
                if existing.requested_by == track.requested_by:
                    user_count += 1

            if user_count >= self._max_per_user:
                raise UserQueueLimitError("User queue limit reached")

            packed = self._pack_track(track)
            pipeline = self._client.pipeline()
            pipeline.rpush(queue_key, packed)
            pipeline.expire(queue_key, self._queue_ttl_s)
            pipeline.expire(processing_key, self._queue_ttl_s)
            new_length, _, _ = await pipeline.execute()
            return int(new_length)

    async def dequeue(self, group_id: int) -> Track | None:
        queue_key = self._queue_key(group_id)
        processing_key = self._processing_key(group_id)
        packed = await self._client.lmove(queue_key, processing_key, "LEFT", "RIGHT")
        if packed is None:
            return None
        pipeline = self._client.pipeline()
        pipeline.expire(queue_key, self._queue_ttl_s)
        pipeline.expire(processing_key, self._queue_ttl_s)
        await pipeline.execute()
        return self._unpack_track(packed)

    async def peek_next(self, group_id: int) -> Track | None:
        packed = await self._client.lindex(self._queue_key(group_id), 0)
        if packed is None:
            return None
        return self._unpack_track(packed)

    async def get_snapshot(self, group_id: int, limit: int = 20, offset: int = 0) -> list[Track]:
        if limit <= 0:
            return []
        end = offset + limit - 1
        packed_items = await self._client.lrange(self._queue_key(group_id), offset, end)
        return [self._unpack_track(item) for item in packed_items]

    async def remove_by_id(self, group_id: int, track_id: str) -> bool:
        queue_key = self._queue_key(group_id)
        async with self._lock(group_id):
            items = await self._client.lrange(queue_key, 0, -1)
            for raw in items:
                track = self._unpack_track(raw)
                if track.track_id == track_id:
                    await self._client.lrem(queue_key, 1, raw)
                    await self._client.expire(queue_key, self._queue_ttl_s)
                    return True
        return False

    async def promote_to_front(self, group_id: int, track_id: str) -> bool:
        queue_key = self._queue_key(group_id)
        script = (
            "local list_key = KEYS[1] "
            "local target_id = ARGV[1] "
            "local items = redis.call('LRANGE', list_key, 0, -1) "
            "for i = 1, #items do "
            "local data = cmsgpack.unpack(items[i]) "
            "if data['track_id'] == target_id then "
            "redis.call('LREM', list_key, 1, items[i]) "
            "redis.call('LPUSH', list_key, items[i]) "
            "return 1 "
            "end "
            "end "
            "return 0"
        )
        result = await self._client.eval(script, 1, queue_key, track_id)
        if result:
            await self._client.expire(queue_key, self._queue_ttl_s)
        return bool(result)

    async def clear(self, group_id: int) -> int:
        queue_key = self._queue_key(group_id)
        processing_key = self._processing_key(group_id)
        pipeline = self._client.pipeline()
        pipeline.llen(queue_key)
        pipeline.delete(queue_key)
        pipeline.delete(processing_key)
        length, _, _ = await pipeline.execute()
        return int(length)

    async def length(self, group_id: int) -> int:
        return int(await self._client.llen(self._queue_key(group_id)))

    async def ack_processed(self, group_id: int, track_id: str) -> None:
        processing_key = self._processing_key(group_id)
        async with self._lock(group_id):
            items = await self._client.lrange(processing_key, 0, -1)
            for raw in items:
                track = self._unpack_track(raw)
                if track.track_id == track_id:
                    await self._client.lrem(processing_key, 1, raw)
                    await self._client.expire(processing_key, self._queue_ttl_s)
                    return

    def _queue_key(self, group_id: int) -> str:
        return f"{self._namespace}:queue:{group_id}"

    def _processing_key(self, group_id: int) -> str:
        return f"{self._namespace}:queue:processing:{group_id}"

    def _lock_key(self, group_id: int) -> str:
        return f"{self._namespace}:lock:queue:{group_id}"

    def _lock(self, group_id: int) -> AsyncRedisLock:
        return AsyncRedisLock(
            self._client,
            self._lock_key(group_id),
            ttl_ms=self._lock_ttl_ms,
            wait_timeout_s=self._lock_wait_s,
        )

    def _pack_track(self, track: Track) -> bytes:
        return msgpack.packb(self._track_to_dict(track), use_bin_type=True)

    def _unpack_track(self, packed: bytes) -> Track:
        data = msgpack.unpackb(packed, raw=False)
        return self._track_from_dict(data)

    def _track_to_dict(self, track: Track) -> dict[str, object]:
        return {
            "track_id": track.track_id,
            "url": track.url,
            "title": track.title,
            "duration_s": track.duration_s,
            "file_path": track.file_path,
            "requested_by": track.requested_by,
            "group_id": track.group_id,
            "added_at": track.added_at.isoformat(),
            "status": track.status.value,
            "thumbnail_url": track.thumbnail_url,
        }

    def _track_from_dict(self, data: dict[str, object]) -> Track:
        return Track(
            track_id=str(data["track_id"]),
            url=str(data["url"]),
            title=str(data["title"]),
            duration_s=int(data["duration_s"]),
            file_path=data.get("file_path") if data.get("file_path") is not None else None,
            requested_by=int(data["requested_by"]),
            group_id=int(data["group_id"]),
            added_at=datetime.fromisoformat(str(data["added_at"])),
            status=TrackStatus(str(data["status"])),
            thumbnail_url=data.get("thumbnail_url") if data.get("thumbnail_url") is not None else None,
        )
