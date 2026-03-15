"""
Core domain models using Python 3.11+ dataclass with __slots__.
All models use slots=True for memory efficiency and performance.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from .enums import TrackStatus, PlaybackStatus, LoopMode, ErrorSeverity


@dataclass(slots=True)
class Track:
    """Track domain model with __slots__ for memory efficiency."""
    
    track_id: UUID
    url: str
    title: str
    duration_seconds: float
    file_path: Optional[Path]
    sha256: Optional[str]
    status: TrackStatus
    requested_by: int
    requested_at: datetime
    metadata: dict = field(default_factory=dict)
    
    @classmethod
    def create(
        cls,
        url: str,
        title: str,
        duration_seconds: float,
        requested_by: int,
        sha256: Optional[str] = None,
        file_path: Optional[Path] = None,
    ) -> "Track":
        """Factory method for creating new tracks."""
        return cls(
            track_id=uuid4(),
            url=url,
            title=title,
            duration_seconds=duration_seconds,
            file_path=file_path,
            sha256=sha256,
            status=TrackStatus.PENDING,
            requested_by=requested_by,
            requested_at=datetime.now(),
            metadata={},
        )


@dataclass(slots=True)
class PlaybackState:
    """Playback state model for a group."""
    
    group_id: int
    current_track: Optional[Track]
    status: PlaybackStatus
    loop_mode: LoopMode
    volume: int
    position_seconds: float
    started_at: Optional[datetime]
    
    @classmethod
    def idle(cls, group_id: int) -> "PlaybackState":
        """Create idle playback state."""
        return cls(
            group_id=group_id,
            current_track=None,
            status=PlaybackStatus.IDLE,
            loop_mode=LoopMode.NONE,
            volume=100,
            position_seconds=0.0,
            started_at=None,
        )


@dataclass(slots=True)
class GroupSettings:
    """Group configuration settings."""
    
    group_id: int
    dj_role_id: Optional[int]
    admin_only_skip: bool
    max_queue_size: int
    announce_tracks: bool
    
    @classmethod
    def defaults(cls, group_id: int) -> "GroupSettings":
        """Create default group settings."""
        return cls(
            group_id=group_id,
            dj_role_id=None,
            admin_only_skip=False,
            max_queue_size=100,
            announce_tracks=True,
        )


@dataclass(slots=True)
class DownloadResult:
    """Result of a track download operation."""
    
    success: bool
    track: Optional[Track]
    error: Optional[str]
    duration_ms: float
    
    @classmethod
    def success_result(cls, track: Track, duration_ms: float) -> "DownloadResult":
        """Create successful download result."""
        return cls(
            success=True,
            track=track,
            error=None,
            duration_ms=duration_ms,
        )
    
    @classmethod
    def error_result(cls, error: str, duration_ms: float) -> "DownloadResult":
        """Create error download result."""
        return cls(
            success=False,
            track=None,
            error=error,
            duration_ms=duration_ms,
        )


@dataclass(slots=True)
class JukeboxError:
    """Structured error with severity and context."""
    
    code: str
    message: str
    severity: ErrorSeverity
    group_id: Optional[int]
    context: dict
    
    @classmethod
    def create(
        cls,
        code: str,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        group_id: Optional[int] = None,
        **context,
    ) -> "JukeboxError":
        """Factory method for creating errors."""
        return cls(
            code=code,
            message=message,
            severity=severity,
            group_id=group_id,
            context=context,
        )
