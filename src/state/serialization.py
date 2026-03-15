"""
msgpack serialization layer for domain models.
Handles conversion between dataclasses and binary format.
"""

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar
from uuid import UUID

import msgpack

from src.domain.models import Track, PlaybackState, GroupSettings
from src.domain.enums import TrackStatus, PlaybackStatus, LoopMode

T = TypeVar("T", Track, PlaybackState, GroupSettings)


class Serializer:
    """msgpack serializer for domain models."""
    
    @staticmethod
    def _encode_default(obj: Any) -> Any:
        """Custom encoder for non-msgpack-native types."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return str(obj)
        elif isinstance(obj, UUID):
            return str(obj)
        elif isinstance(obj, (TrackStatus, PlaybackStatus, LoopMode)):
            return obj.value
        elif obj is None:
            return None
        raise TypeError(f"Cannot serialize type {type(obj)}")
    
    @staticmethod
    def _decode_datetime(s: str) -> datetime:
        """Decode ISO datetime string."""
        return datetime.fromisoformat(s)
    
    @classmethod
    def pack(cls, obj: Track | PlaybackState | GroupSettings) -> bytes:
        """
        Serialize dataclass to msgpack bytes.
        
        Args:
            obj: Domain model instance
        
        Returns:
            Compact msgpack binary data
        """
        data = asdict(obj)
        return msgpack.packb(data, default=cls._encode_default, use_bin_type=True)
    
    @classmethod
    def unpack(cls, data: bytes, model_cls: type[T]) -> T:
        """
        Deserialize msgpack bytes to dataclass.
        
        Args:
            data: msgpack binary data
            model_cls: Target dataclass type (Track, PlaybackState, GroupSettings)
        
        Returns:
            Reconstructed domain model instance
        """
        d = msgpack.unpackb(data, raw=False, strict_map_key=True)
        
        # Reconstruct specific types based on model_cls
        if model_cls == Track:
            return cls._unpack_track(d)
        elif model_cls == PlaybackState:
            return cls._unpack_playback_state(d)
        elif model_cls == GroupSettings:
            return cls._unpack_group_settings(d)
        else:
            raise ValueError(f"Unsupported model class: {model_cls}")
    
    @classmethod
    def _unpack_track(cls, d: dict) -> Track:
        """Reconstruct Track from dict."""
        d["track_id"] = UUID(d["track_id"])
        d["status"] = TrackStatus(d["status"])
        d["requested_at"] = cls._decode_datetime(d["requested_at"])
        d["file_path"] = Path(d["file_path"]) if d["file_path"] else None
        return Track(**d)
    
    @classmethod
    def _unpack_playback_state(cls, d: dict) -> PlaybackState:
        """Reconstruct PlaybackState from dict."""
        d["status"] = PlaybackStatus(d["status"])
        d["loop_mode"] = LoopMode(d["loop_mode"])
        d["started_at"] = cls._decode_datetime(d["started_at"]) if d["started_at"] else None
        if d["current_track"]:
            d["current_track"] = cls._unpack_track(d["current_track"])
        return PlaybackState(**d)
    
    @classmethod
    def _unpack_group_settings(cls, d: dict) -> GroupSettings:
        """Reconstruct GroupSettings from dict."""
        return GroupSettings(**d)
