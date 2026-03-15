"""
Concrete implementation of StreamEngine using FFmpeg, pytgcalls, and watchdog.
Manages the complete audio streaming pipeline to Telegram voice chats.
"""

import asyncio
import time
from pathlib import Path
from typing import Optional

from pytgcalls import PyTgCalls
from pytgcalls.types.input_stream import InputAudioStream
from pytgcalls.types.stream import Stream

from src.domain.interfaces import StreamEngine
from src.domain.models import Track, PlaybackState
from src.domain.enums import PlaybackStatus, LoopMode
from src.domain.logging import get_logger, bind_contextvars
from src.state.redis_client import RedisClient
from src.state.queue_manager import ConcreteQueueManager
from src.state.keys import RedisKeys
from src.state.serialization import Serializer
from src.acquisition.tmpfs_overlay import TmpfsOverlay
from .semaphore_pool import SemaphorePool
from .watchdog import StreamWatchdog
from .recovery import SeekResumeRecovery

log = get_logger(__name__)


class ConcreteStreamEngine(StreamEngine):
    """
    Concrete streaming engine implementation.
    
    Features:
        - FFmpeg Opus transcoding pipeline
        - pytgcalls integration for Telegram voice chat
        - Semaphore-based resource limiting
        - Watchdog monitoring with sub-20ms recovery
        - Position tracking for seek-resume
    """
    
    def __init__(
        self,
        redis_client: RedisClient,
        queue_manager: ConcreteQueueManager,
        tmpfs_overlay: TmpfsOverlay,
        pytgcalls_client: PyTgCalls,
        keys: RedisKeys,
    ):
        self._redis = redis_client
        self._queue = queue_manager
        self._tmpfs = tmpfs_overlay
        self._pytgcalls = pytgcalls_client
        self._keys = keys
        
        # Active streams registry: group_id -> stream state
        self._active_streams: dict[int, dict] = {}
        
        # Watchdog and recovery
        self._watchdog = StreamWatchdog()
        self._recovery = SeekResumeRecovery()
        
        # Semaphore pool for resource limiting
        self._semaphore_pool = SemaphorePool()
    
    async def start_stream(self, group_id: int, track: Track, chat_id: int) -> bool:
        """
        Start streaming track to voice chat.
        
        Returns:
            True if started successfully
        """
        bind_contextvars(group_id=group_id)
        
        try:
            # Acquire semaphore for resource limiting
            async with self._semaphore_pool.acquire(group_id):
                log.info(
                    "stream_start_requested",
                    track_id=str(track.track_id),
                    title=track.title,
                    chat_id=chat_id,
                )
                
                # Activate tmpfs overlay for fast I/O
                if track.file_path and track.sha256:
                    active_path = await self._tmpfs.activate(
                        sha256=track.sha256,
                        source_path=track.file_path,
                    )
                else:
                    log.error("track_missing_file", track_id=str(track.track_id))
                    return False
                
                # Build FFmpeg command for Opus output
                # FFmpeg 7+ requires: -f data -map 0:a for raw Opus
                ffmpeg_cmd = [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel", "error",
                    "-re",  # Real-time rate
                    "-i", str(active_path),
                    "-ac", "2",  # Stereo
                    "-ar", "48000",  # 48kHz
                    "-acodec", "libopus",
                    "-b:a", "128k",  # Bitrate
                    "-f", "data",  # Raw output (FFmpeg 7+)
                    "-map", "0:a",  # Audio stream only
                    "pipe:1",
                ]
                
                # Launch FFmpeg subprocess
                ffmpeg_proc = await asyncio.create_subprocess_exec(
                    *ffmpeg_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                
                log.info(
                    "ffmpeg_started",
                    pid=ffmpeg_proc.pid,
                    file_path=str(active_path),
                )
                
                # Create InputAudioStream for pytgcalls v2.2.11 API
                # Pass FFmpeg stdout pipe directly
                audio_stream = InputAudioStream(
                    ffmpeg_proc.stdout,
                    parameters={
                        "sample_rate": 48000,
                        "channels": 2,
                        "bitrate": 128000,
                    }
                )
                
                # Start playback with pytgcalls
                # app.play() is the modern API (replaces join_group_call + AudioPiped)
                await self._pytgcalls.play(chat_id, audio_stream)
                
                # Create PlaybackState
                playback_state = PlaybackState(
                    group_id=group_id,
                    current_track=track,
                    status=PlaybackStatus.PLAYING,
                    loop_mode=LoopMode.NONE,
                    volume=100,
                    position_seconds=0.0,
                    started_at=None,  # Set in update_state
                )
                
                # Store stream state
                self._active_streams[group_id] = {
                    "ffmpeg_proc": ffmpeg_proc,
                    "chat_id": chat_id,
                    "track": track,
                    "start_time": time.monotonic(),
                    "seek_offset": 0.0,
                    "paused_at": None,
                }
                
                # Persist state to Redis
                await self._update_state(group_id, playback_state)
                
                # Start watchdog monitoring
                asyncio.create_task(
                    self._watchdog.watch(group_id, ffmpeg_proc, self)
                )
                
                log.info(
                    "stream_started",
                    track_id=str(track.track_id),
                    chat_id=chat_id,
                )
                
                return True
        
        except Exception as e:
            log.error(
                "stream_start_failed",
                error=str(e),
                track_id=str(track.track_id),
            )
            return False
    
    async def stop_stream(self, group_id: int) -> None:
        """Stop current stream immediately."""
        bind_contextvars(group_id=group_id)
        
        stream_state = self._active_streams.get(group_id)
        if not stream_state:
            log.warning("no_active_stream")
            return
        
        ffmpeg_proc = stream_state["ffmpeg_proc"]
        chat_id = stream_state["chat_id"]
        track = stream_state["track"]
        
        # Terminate FFmpeg gracefully
        ffmpeg_proc.terminate()
        try:
            await asyncio.wait_for(ffmpeg_proc.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            ffmpeg_proc.kill()
            await ffmpeg_proc.wait()
        
        log.info("ffmpeg_terminated", pid=ffmpeg_proc.pid)
        
        # Leave voice chat
        await self._pytgcalls.leave_group_call(chat_id)
        
        # Stop watchdog
        await self._watchdog.stop(group_id)
        
        # Deactivate tmpfs
        if track.sha256:
            await self._tmpfs.deactivate(track.sha256)
        
        # Update state to IDLE
        idle_state = PlaybackState.idle(group_id)
        await self._update_state(group_id, idle_state)
        
        # Remove from active streams
        del self._active_streams[group_id]
        
        log.info("stream_stopped", track_id=str(track.track_id))
    
    async def pause_stream(self, group_id: int) -> bool:
        """Pause current stream."""
        bind_contextvars(group_id=group_id)
        
        stream_state = self._active_streams.get(group_id)
        if not stream_state:
            log.warning("no_active_stream")
            return False
        
        chat_id = stream_state["chat_id"]
        
        # Pause via pytgcalls
        await self._pytgcalls.pause_stream(chat_id)
        
        # Record pause time for position tracking
        stream_state["paused_at"] = time.monotonic()
        
        # Update Redis state
        state = await self.get_state(group_id)
        state.status = PlaybackStatus.PAUSED
        await self._update_state(group_id, state)
        
        log.info("stream_paused")
        return True
    
    async def resume_stream(self, group_id: int) -> bool:
        """Resume paused stream."""
        bind_contextvars(group_id=group_id)
        
        stream_state = self._active_streams.get(group_id)
        if not stream_state:
            log.warning("no_active_stream")
            return False
        
        chat_id = stream_state["chat_id"]
        paused_at = stream_state.get("paused_at")
        
        # Resume via pytgcalls
        await self._pytgcalls.resume_stream(chat_id)
        
        # Adjust start_time to account for pause duration
        if paused_at:
            pause_duration = time.monotonic() - paused_at
            stream_state["start_time"] += pause_duration
            stream_state["paused_at"] = None
        
        # Update Redis state
        state = await self.get_state(group_id)
        state.status = PlaybackStatus.PLAYING
        await self._update_state(group_id, state)
        
        log.info("stream_resumed")
        return True
    
    async def set_volume(self, group_id: int, volume: int) -> bool:
        """
        Set playback volume (0-200).
        
        Note: pytgcalls v2.2.11 may not support volume control directly.
        Store in state for future implementation.
        """
        bind_contextvars(group_id=group_id)
        
        if not 0 <= volume <= 200:
            log.warning("invalid_volume", volume=volume)
            return False
        
        # Update state
        state = await self.get_state(group_id)
        state.volume = volume
        await self._update_state(group_id, state)
        
        log.info("volume_set", volume=volume)
        return True
    
    async def get_state(self, group_id: int) -> PlaybackState:
        """Get current playback state for group."""
        bind_contextvars(group_id=group_id)
        
        redis = self._redis.get_connection()
        state_key = self._keys.playback_state(group_id)
        
        state_bytes = await redis.get(state_key)
        
        if not state_bytes:
            # Return idle state if not found
            return PlaybackState.idle(group_id)
        
        state = Serializer.unpack(state_bytes, PlaybackState)
        
        # Update position from in-memory tracking
        stream_state = self._active_streams.get(group_id)
        if stream_state:
            state.position_seconds = self._get_current_position(group_id)
        
        return state
    
    def _get_current_position(self, group_id: int) -> float:
        """Compute current position for active stream."""
        stream_state = self._active_streams.get(group_id)
        if not stream_state:
            return 0.0
        
        start_time = stream_state["start_time"]
        seek_offset = stream_state["seek_offset"]
        paused_at = stream_state.get("paused_at")
        
        if paused_at:
            # Paused - return position at pause time
            return (paused_at - start_time) + seek_offset
        
        # Playing - return current elapsed time
        return (time.monotonic() - start_time) + seek_offset
    
    async def _update_state(self, group_id: int, state: PlaybackState) -> None:
        """Persist PlaybackState to Redis."""
        redis = self._redis.get_connection()
        state_key = self._keys.playback_state(group_id)
        
        state_bytes = Serializer.pack(state)
        await redis.set(state_key, state_bytes)
    
    async def _on_track_end(self, group_id: int) -> None:
        """
        Callback when track finishes normally.
        Dequeues next track and starts playback.
        """
        bind_contextvars(group_id=group_id)
        
        log.info("track_ended")
        
        # Get current stream state
        stream_state = self._active_streams.get(group_id)
        if not stream_state:
            return
        
        current_track = stream_state["track"]
        chat_id = stream_state["chat_id"]
        
        # Check loop mode
        state = await self.get_state(group_id)
        loop_mode = state.loop_mode
        
        if loop_mode == LoopMode.TRACK:
            # Repeat current track
            log.info("loop_track", track_id=str(current_track.track_id))
            await self.stop_stream(group_id)
            await self.start_stream(group_id, current_track, chat_id)
            return
        
        # Dequeue next track
        next_track = await self._queue.dequeue(group_id)
        
        if next_track:
            log.info("auto_play_next", track_id=str(next_track.track_id))
            await self.stop_stream(group_id)
            await self.start_stream(group_id, next_track, chat_id)
        else:
            # Queue empty
            log.info("queue_empty")
            
            if loop_mode == LoopMode.QUEUE:
                # Re-enqueue current track for loop
                await self._queue.enqueue(group_id, current_track)
                log.info("requeued_for_loop", track_id=str(current_track.track_id))
            
            await self.stop_stream(group_id)
