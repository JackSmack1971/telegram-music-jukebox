"""
Command broker using aiogram 3.x Router for command dispatching.
Registers all command handlers and middleware.
"""

from aiogram import Router
from aiogram.filters import Command

from src.domain.interfaces import StreamEngine, QueueManager
from .commands.play import PlayCommand
from .commands.playback import PlaybackCommands
from .commands.queue import QueueCommand
from .commands.nowplaying import NowPlayingCommand
from .throttle import MessageThrottle
from .permissions import PermissionMiddleware


class CommandBroker:
    """
    Centralized command routing and handler registration.
    
    Features:
        - Router-based command mapping
        - Middleware pipeline (throttle, permissions, context)
        - Dependency injection for handlers
    """
    
    def __init__(self, stream_engine: StreamEngine, queue_manager: QueueManager):
        self.router = Router(name="jukebox_commands")
        
        # Create command handler instances
        self.play_command = PlayCommand(stream_engine, queue_manager)
        self.playback_commands = PlaybackCommands(stream_engine, queue_manager)
        self.queue_command = QueueCommand(queue_manager)
        self.nowplaying_command = NowPlayingCommand(stream_engine, queue_manager)
        
        # Register middleware (outer = before filters)
        self.router.message.outer_middleware(MessageThrottle())
        self.router.message.outer_middleware(PermissionMiddleware())
        
        # Register command handlers
        self._register_handlers()
    
    def _register_handlers(self) -> None:
        """Register all command handlers to router."""
        
        # /play <url>
        @self.router.message(Command("play"))
        async def play_handler(message):
            await self.play_command.handle(message)
        
        # /skip
        @self.router.message(Command("skip"))
        async def skip_handler(message):
            await self.playback_commands.skip(message)
        
        # /stop
        @self.router.message(Command("stop"))
        async def stop_handler(message):
            await self.playback_commands.stop(message)
        
        # /pause
        @self.router.message(Command("pause"))
        async def pause_handler(message):
            await self.playback_commands.pause(message)
        
        # /resume
        @self.router.message(Command("resume"))
        async def resume_handler(message):
            await self.playback_commands.resume(message)
        
        # /volume <0-200>
        @self.router.message(Command("volume"))
        async def volume_handler(message):
            await self.playback_commands.volume(message)
        
        # /loop
        @self.router.message(Command("loop"))
        async def loop_handler(message):
            await self.playback_commands.loop(message)
        
        # /queue
        @self.router.message(Command("queue"))
        async def queue_handler(message):
            await self.queue_command.handle(message)
        
        # /np (now playing)
        @self.router.message(Command("np", "nowplaying"))
        async def nowplaying_handler(message):
            await self.nowplaying_command.handle(message)
