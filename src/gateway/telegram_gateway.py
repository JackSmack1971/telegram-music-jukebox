"""
Concrete implementation of TelegramHandler using aiogram 3.x.
Routes commands to specialized command handlers.
"""

from aiogram import Bot, Dispatcher
from aiogram.types import Message

from src.domain.interfaces import TelegramHandler, StreamEngine, QueueManager
from src.domain.logging import get_logger
from .command_broker import CommandBroker

log = get_logger(__name__)


class TelegramGateway(TelegramHandler):
    """
    Telegram gateway implementation using aiogram 3.x.
    
    Features:
        - Router-based command dispatching
        - Middleware integration (throttle, permissions, context)
        - Global error handling
        - Graceful start/stop
    """
    
    def __init__(
        self,
        bot: Bot,
        dp: Dispatcher,
        stream_engine: StreamEngine,
        queue_manager: QueueManager,
    ):
        self._bot = bot
        self._dp = dp
        self._stream_engine = stream_engine
        self._queue_manager = queue_manager
        
        # Create command broker
        self._broker = CommandBroker(stream_engine, queue_manager)
    
    async def start(self) -> None:
        """Start Telegram bot polling."""
        # Include command router
        self._dp.include_router(self._broker.router)
        
        log.info("telegram_gateway_starting", group_id="system")
        
        # Start polling
        await self._dp.start_polling(self._bot, allowed_updates=self._dp.resolve_used_update_types())
    
    async def stop(self) -> None:
        """Stop Telegram bot polling."""
        log.info("telegram_gateway_stopping", group_id="system")
        await self._dp.stop_polling()
    
    # Implement TelegramHandler ABC methods by delegating to broker
    
    async def handle_play(self, message: Message) -> None:
        """Delegate to PlayCommand."""
        await self._broker.play_command.handle(message)
    
    async def handle_skip(self, message: Message) -> None:
        """Delegate to PlaybackCommands.skip."""
        await self._broker.playback_commands.skip(message)
    
    async def handle_stop(self, message: Message) -> None:
        """Delegate to PlaybackCommands.stop."""
        await self._broker.playback_commands.stop(message)
    
    async def handle_queue(self, message: Message) -> None:
        """Delegate to QueueCommand."""
        await self._broker.queue_command.handle(message)
    
    async def handle_nowplaying(self, message: Message) -> None:
        """Delegate to NowPlayingCommand."""
        await self._broker.nowplaying_command.handle(message)
