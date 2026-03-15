"""
/queue command handler with paginated inline keyboard.
"""

from aiogram.types import Message, CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters.callback_data import CallbackData

from src.domain.interfaces import QueueManager
from src.domain.logging import get_logger, bind_contextvars

log = get_logger(__name__)


class Pagination(CallbackData, prefix="pag"):
    """Callback data for pagination buttons."""
    action: str  # 'prev' or 'next'
    page: int


class QueueCommand:
    """
    Handler for /queue command with pagination.
    
    Features:
        - Paginated queue display (10 tracks per page)
        - Inline keyboard with Prev/Next buttons
        - Callback query handling for page navigation
    """
    
    PAGE_SIZE = 10
    
    def __init__(self, queue_manager: QueueManager):
        self._queue = queue_manager
    
    async def handle(self, message: Message) -> None:
        """Handle /queue command."""
        group_id = message.chat.id
        bind_contextvars(group_id=group_id)
        
        log.info("queue_requested")
        
        # Get queue
        tracks = await self._queue.get_queue(group_id)
        
        if not tracks:
            await message.reply("📋 Queue is empty. Use /play to add songs!")
            return
        
        # Build paginated message
        markup = self._build_keyboard(tracks, page=0)
        text = self._format_page(tracks, page=0)
        
        await message.reply(text, reply_markup=markup, parse_mode="HTML")
    
    async def handle_page_callback(self, callback: CallbackQuery, callback_data: Pagination) -> None:
        """Handle pagination callback."""
        group_id = callback.message.chat.id
        bind_contextvars(group_id=group_id)
        
        page = callback_data.page
        
        if callback_data.action == "next":
            page += 1
        elif callback_data.action == "prev" and page > 0:
            page -= 1
        
        # Get queue
        tracks = await self._queue.get_queue(group_id)
        
        # Build updated message
        markup = self._build_keyboard(tracks, page=page)
        text = self._format_page(tracks, page=page)
        
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        await callback.answer()
    
    def _format_page(self, tracks: list, page: int) -> str:
        """Format queue page as text."""
        start = page * self.PAGE_SIZE
        end = min(start + self.PAGE_SIZE, len(tracks))
        
        lines = [f"📋 <b>Queue</b> (Total: {len(tracks)} tracks)\n"]
        
        for i in range(start, end):
            track = tracks[i]
            duration = self._format_duration(track.duration_seconds)
            lines.append(
                f"{i+1}. <b>{track.title}</b> [{duration}]"
            )
        
        return "\n".join(lines)
    
    def _build_keyboard(self, tracks: list, page: int):
        """Build inline keyboard with pagination."""
        builder = InlineKeyboardBuilder()
        
        start = page * self.PAGE_SIZE
        end = min(start + self.PAGE_SIZE, len(tracks))
        
        # Navigation buttons
        nav_buttons = []
        
        if page > 0:
            nav_buttons.append(
                InlineKeyboardButton(
                    text="⬅️ Prev",
                    callback_data=Pagination(action="prev", page=page).pack(),
                )
            )
        
        if end < len(tracks):
            nav_buttons.append(
                InlineKeyboardButton(
                    text="Next ➡️",
                    callback_data=Pagination(action="next", page=page).pack(),
                )
            )
        
        if nav_buttons:
            builder.row(*nav_buttons)
        
        return builder.as_markup()
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """Format duration as MM:SS."""
        m, s = divmod(int(seconds), 60)
        return f"{m}:{s:02d}"
