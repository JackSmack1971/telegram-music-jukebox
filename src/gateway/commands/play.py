"""
/play command handler for adding tracks to queue and starting playback.
"""

import re
from aiogram.types import Message

from src.domain.interfaces import StreamEngine, QueueManager
from src.domain.models import Track
from src.domain.enums import PlaybackStatus
from src.domain.logging import get_logger, bind_contextvars
from src.acquisition.download_worker import DownloadWorker

log = get_logger(__name__)


class PlayCommand:
    """
    Handler for /play <url_or_search_query> command.
    
    Features:
        - URL detection and validation
        - yt-dlp search integration for search queries
        - Automatic playback start if idle
        - Queue enqueue if already playing
    """
    
    URL_PATTERN = re.compile(
        r"https?://(?:www\.)?"
        r"(?:youtube\.com|youtu\.be|soundcloud\.com|spotify\.com|music\.youtube\.com)/"
    )
    
    def __init__(
        self,
        stream_engine: StreamEngine,
        queue_manager: QueueManager,
        download_worker: DownloadWorker = None,
    ):
        self._stream = stream_engine
        self._queue = queue_manager
        self._downloader = download_worker  # Injected dependency
    
    async def handle(self, message: Message) -> None:
        """Handle /play command."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        # Parse arguments
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.reply(
                "⚠️ Usage: /play <url_or_search_query>\n"
                "Example: /play https://youtu.be/dQw4w9WgXcQ\n"
                "Example: /play never gonna give you up"
            )
            return
        
        query = args[1].strip()
        
        # Detect URL vs search query
        if self.URL_PATTERN.match(query):
            url = query
            log.info("play_url_requested", url=url)
        else:
            # Use yt-dlp search: ytsearch1: prefix
            url = f"ytsearch1:{query}"
            log.info("play_search_requested", query=query)
        
        # Reply with download status
        status_msg = await message.reply("⏳ Downloading...")
        
        try:
            # Download track
            if not self._downloader:
                await status_msg.edit_text("❌ Download service unavailable")
                return
            
            result = await self._downloader.download(url, group_id)
            
            if not result.success:
                await status_msg.edit_text(f"❌ Download failed: {result.error}")
                return
            
            track = result.track
            
            # Set requested_by
            track.requested_by = message.from_user.id
            
            log.info(
                "track_downloaded",
                track_id=str(track.track_id),
                title=track.title,
                duration_ms=result.duration_ms,
            )
            
            # Check playback state
            state = await self._stream.get_state(group_id)
            
            if state.status == PlaybackStatus.IDLE:
                # Start streaming immediately
                success = await self._stream.start_stream(
                    group_id=group_id,
                    track=track,
                    chat_id=message.chat.id,
                )
                
                if success:
                    await status_msg.edit_text(
                        f"▶️ Now playing: <b>{track.title}</b>\n"
                        f"⏱ Duration: {self._format_duration(track.duration_seconds)}",
                        parse_mode="HTML",
                    )
                else:
                    await status_msg.edit_text("❌ Failed to start stream")
            
            else:
                # Enqueue track
                position = await self._queue.enqueue(group_id, track)
                
                await status_msg.edit_text(
                    f"✅ Added to queue: <b>{track.title}</b>\n"
                    f"📋 Position: {position}\n"
                    f"⏱ Duration: {self._format_duration(track.duration_seconds)}",
                    parse_mode="HTML",
                )
        
        except Exception as e:
            log.error("play_command_error", error=str(e))
            await status_msg.edit_text(f"❌ Error: {str(e)}")
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration as MM:SS."""
        m, s = divmod(int(seconds), 60)
        return f"{m}:{s:02d}"
