"""
Abstract Base Classes defining core system interfaces.
All methods are async for asyncio-based Telegram bot.
"""

from abc import ABC, abstractmethod
from typing import Optional

from .models import Track, PlaybackState, GroupSettings


class QueueManager(ABC):
    """Abstract interface for Redis-backed queue operations."""
    
    @abstractmethod
    async def enqueue(self, group_id: int, track: Track) -> int:
        """
        Enqueue a track. Returns new queue length.
        Raises ValueError if queue is full.
        """
        ...
    
    @abstractmethod
    async def dequeue(self, group_id: int) -> Optional[Track]:
        """
        Dequeue next track atomically using shadow list pattern.
        Returns None if queue is empty.
        """
        ...
    
    @abstractmethod
    async def peek(self, group_id: int) -> Optional[Track]:
        """Peek at next track without removing it."""
        ...
    
    @abstractmethod
    async def get_queue(self, group_id: int) -> list[Track]:
        """Get all tracks in queue."""
        ...
    
    @abstractmethod
    async def clear(self, group_id: int) -> int:
        """Clear entire queue. Returns number of tracks removed."""
        ...
    
    @abstractmethod
    async def queue_length(self, group_id: int) -> int:
        """Return current queue length."""
        ...
    
    @abstractmethod
    async def ack(self, group_id: int, track_id: str) -> bool:
        """
        Acknowledge successful processing of dequeued track.
        Removes track from shadow queue.
        """
        ...
    
    @abstractmethod
    async def nack(self, group_id: int, track_id: str) -> bool:
        """
        Negative acknowledge - move failed track back to main queue.
        """
        ...


class StreamEngine(ABC):
    """Abstract interface for audio streaming pipeline."""
    
    @abstractmethod
    async def start_stream(self, group_id: int, track: Track, chat_id: int) -> bool:
        """
        Start streaming track to voice chat.
        Returns True if started successfully.
        """
        ...
    
    @abstractmethod
    async def stop_stream(self, group_id: int) -> None:
        """Stop current stream immediately."""
        ...
    
    @abstractmethod
    async def pause_stream(self, group_id: int) -> bool:
        """Pause current stream. Returns success."""
        ...
    
    @abstractmethod
    async def resume_stream(self, group_id: int) -> bool:
        """Resume paused stream. Returns success."""
        ...
    
    @abstractmethod
    async def set_volume(self, group_id: int, volume: int) -> bool:
        """Set playback volume (0-100). Returns success."""
        ...
    
    @abstractmethod
    async def get_state(self, group_id: int) -> PlaybackState:
        """Get current playback state for group."""
        ...


class TelegramHandler(ABC):
    """Abstract interface for Telegram command handlers."""
    
    @abstractmethod
    async def handle_play(self, message: object) -> None:
        """Handle /play <url> command."""
        ...
    
    @abstractmethod
    async def handle_skip(self, message: object) -> None:
        """Handle /skip command."""
        ...
    
    @abstractmethod
    async def handle_stop(self, message: object) -> None:
        """Handle /stop command."""
        ...
    
    @abstractmethod
    async def handle_queue(self, message: object) -> None:
        """Handle /queue command."""
        ...
    
    @abstractmethod
    async def handle_nowplaying(self, message: object) -> None:
        """Handle /nowplaying command."""
        ...
