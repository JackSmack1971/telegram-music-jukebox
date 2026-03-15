"""
/np (now playing) command with ASCII progress bar.
"""

from aiogram.types import Message

from src.domain.interfaces import StreamEngine, QueueManager
from src.domain.enums import PlaybackStatus, LoopMode
from src.domain.logging import get_logger, bind_contextvars

log = get_logger(__name__)


class NowPlayingCommand:
    """
    Handler for /np and /nowplaying commands.
    
    Features:
        - Current track display
        - ASCII progress bar (▓ filled, ░ empty)
        - Loop mode and volume display
        - Position and duration
    """
    
    def __init__(self, stream_engine: StreamEngine, queue_manager: QueueManager):
        self._stream = stream_engine
        self._queue = queue_manager
    
    async def handle(self, message: Message) -> None:
        """Handle /np command."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        log.info("nowplaying_requested")
        
        state = await self._stream.get_state(group_id)
        
        if state.status == PlaybackStatus.IDLE or not state.current_track:
            await message.reply("⚠️ Nothing playing. Use /play to start!")
            return
        
        track = state.current_track
        position = state.position_seconds
        duration = track.duration_seconds
        
        # Build progress bar
        progress_bar = self._build_progress_bar(position, duration, width=20)
        
        # Format times
        pos_str = self._format_time(position)
        dur_str = self._format_time(duration)
        
        # Loop mode emoji
        loop_emoji = {
            LoopMode.NONE: "🔁 Off",
            LoopMode.TRACK: "🔂 Track",
            LoopMode.QUEUE: "🔁 Queue",
        }
        
        # Status emoji
        status_emoji = {
            PlaybackStatus.PLAYING: "▶️",
            PlaybackStatus.PAUSED: "⏸",
            PlaybackStatus.IDLE: "⏹",
        }
        
        # Build message
        text = (
            f"{status_emoji[state.status]} <b>Now Playing</b>\n\n"
            f"🎵 <b>{track.title}</b>\n"
            f"{progress_bar}\n"
            f"⏱ {pos_str} / {dur_str}\n\n"
            f"{loop_emoji[state.loop_mode]} | 🔊 Volume: {state.volume}%"
        )
        
        await message.reply(text, parse_mode="HTML")
    
    @staticmethod
    def _build_progress_bar(position: float, duration: float, width: int = 20) -> str:
        """
        Build ASCII progress bar.
        
        Args:
            position: Current position in seconds
            duration: Total duration in seconds
            width: Bar width in characters
        
        Returns:
            Progress bar string like: ▓▓▓▓▓▓▓░░░░░░░
        """
        if duration <= 0:
            return "░" * width
        
        progress = position / duration
        filled = int(progress * width)
        
        return "▓" * filled + "░" * (width - filled)
    
    @staticmethod
    def _format_time(seconds: float) -> str:
        """Format time as MM:SS."""
        m, s = divmod(int(seconds), 60)
        return f"{m}:{s:02d}"
