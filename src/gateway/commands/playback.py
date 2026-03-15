"""
Playback control commands: skip, stop, pause, resume, volume, loop.
"""

from aiogram.types import Message, ChatMemberAdministrator, ChatMemberOwner

from src.domain.interfaces import StreamEngine, QueueManager
from src.domain.enums import PlaybackStatus, LoopMode
from src.domain.logging import get_logger, bind_contextvars

log = get_logger(__name__)


class PlaybackCommands:
    """
    Playback control command handlers.
    
    Commands:
        - /skip: Skip to next track
        - /stop: Stop playback and clear queue
        - /pause: Pause playback
        - /resume: Resume playback
        - /volume <0-200>: Set volume
        - /loop: Cycle loop mode (NONE -> TRACK -> QUEUE -> NONE)
    """
    
    def __init__(self, stream_engine: StreamEngine, queue_manager: QueueManager):
        self._stream = stream_engine
        self._queue = queue_manager
    
    async def skip(self, message: Message) -> None:
        """Skip current track."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        # Check permissions (admin or DJ role)
        if not await self._check_skip_permission(message):
            await message.reply("⚠️ Only admins can skip tracks")
            return
        
        state = await self._stream.get_state(group_id)
        
        if state.status == PlaybackStatus.IDLE:
            await message.reply("⚠️ Nothing is playing")
            return
        
        current_track = state.current_track
        
        log.info("skip_requested", track_id=str(current_track.track_id))
        
        # Stop current stream
        await self._stream.stop_stream(group_id)
        
        # Dequeue next track
        next_track = await self._queue.dequeue(group_id)
        
        if next_track:
            # Start next track
            await self._stream.start_stream(group_id, next_track, message.chat.id)
            await message.reply(
                f"⏭ Skipped! Now playing: <b>{next_track.title}</b>",
                parse_mode="HTML",
            )
        else:
            await message.reply("⏭ Skipped. Queue is empty.")
    
    async def stop(self, message: Message) -> None:
        """Stop playback and clear queue."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        # Admin only
        if not await self._is_admin(message):
            await message.reply("⚠️ Only admins can stop playback")
            return
        
        log.info("stop_requested")
        
        # Clear queue
        removed = await self._queue.clear(group_id)
        
        # Stop stream
        await self._stream.stop_stream(group_id)
        
        await message.reply(f"⏹ Stopped playback. Cleared {removed} tracks from queue.")
    
    async def pause(self, message: Message) -> None:
        """Pause playback."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        state = await self._stream.get_state(group_id)
        
        if state.status != PlaybackStatus.PLAYING:
            await message.reply("⚠️ Nothing is playing")
            return
        
        log.info("pause_requested")
        
        success = await self._stream.pause_stream(group_id)
        
        if success:
            await message.reply("⏸ Paused")
        else:
            await message.reply("❌ Failed to pause")
    
    async def resume(self, message: Message) -> None:
        """Resume playback."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        state = await self._stream.get_state(group_id)
        
        if state.status != PlaybackStatus.PAUSED:
            await message.reply("⚠️ Playback is not paused")
            return
        
        log.info("resume_requested")
        
        success = await self._stream.resume_stream(group_id)
        
        if success:
            await message.reply("▶️ Resumed")
        else:
            await message.reply("❌ Failed to resume")
    
    async def volume(self, message: Message) -> None:
        """Set volume (0-200)."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        # Parse volume argument
        args = message.text.split()
        if len(args) < 2:
            await message.reply("⚠️ Usage: /volume <0-200>")
            return
        
        try:
            vol = int(args[1])
        except ValueError:
            await message.reply("⚠️ Volume must be a number (0-200)")
            return
        
        if not 0 <= vol <= 200:
            await message.reply("⚠️ Volume must be between 0 and 200")
            return
        
        log.info("volume_requested", volume=vol)
        
        success = await self._stream.set_volume(group_id, vol)
        
        if success:
            await message.reply(f"🔊 Volume set to {vol}%")
        else:
            await message.reply("❌ Failed to set volume")
    
    async def loop(self, message: Message) -> None:
        """Cycle loop mode."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        state = await self._stream.get_state(group_id)
        
        # Cycle: NONE -> TRACK -> QUEUE -> NONE
        if state.loop_mode == LoopMode.NONE:
            new_mode = LoopMode.TRACK
        elif state.loop_mode == LoopMode.TRACK:
            new_mode = LoopMode.QUEUE
        else:
            new_mode = LoopMode.NONE
        
        state.loop_mode = new_mode
        
        # Update state (simplified - should use proper method)
        # In production: stream_engine should have set_loop_mode method
        
        log.info("loop_mode_changed", mode=new_mode.name)
        
        mode_names = {
            LoopMode.NONE: "Off",
            LoopMode.TRACK: "Track",
            LoopMode.QUEUE: "Queue",
        }
        
        await message.reply(f"🔁 Loop mode: {mode_names[new_mode]}")
    
    async def _is_admin(self, message: Message) -> bool:
        """Check if user is admin."""
        member = await message.bot.get_chat_member(
            message.chat.id,
            message.from_user.id,
        )
        return isinstance(member, (ChatMemberAdministrator, ChatMemberOwner))
    
    async def _check_skip_permission(self, message: Message) -> bool:
        """
        Check if user has skip permission.
        
        TODO: Check admin_only_skip from GroupSettings
        TODO: Check DJ role
        """
        return await self._is_admin(message)
