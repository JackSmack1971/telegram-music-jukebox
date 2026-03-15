"""
Permission system for command access control.
"""

from aiogram import BaseMiddleware
from aiogram.types import Message, ChatMemberAdministrator, ChatMemberOwner
from typing import Any, Callable, Dict, Awaitable

from src.domain.logging import get_logger

log = get_logger(__name__)


class PermissionSystem:
    """
    Permission checking utilities.
    
    Features:
        - Admin check via get_chat_member
        - DJ role validation
        - Skip permission logic (admin_only_skip + dj_role)
    """
    
    @staticmethod
    async def is_admin(message: Message) -> bool:
        """Check if user is admin or owner."""
        try:
            member = await message.bot.get_chat_member(
                chat_id=message.chat.id,
                user_id=message.from_user.id,
            )
            return isinstance(member, (ChatMemberAdministrator, ChatMemberOwner))
        except Exception as e:
            log.error("permission_check_failed", error=str(e), group_id=message.chat.id)
            return False
    
    @staticmethod
    async def has_dj_role(message: Message, dj_role_id: int = None) -> bool:
        """
        Check if user has DJ role.
        
        TODO: Implement role checking via custom_title or external role system
        """
        # Placeholder - implement role checking logic
        return False
    
    @staticmethod
    async def check_skip_permission(message: Message, admin_only_skip: bool = False) -> bool:
        """
        Check if user has skip permission.
        
        Args:
            message: Message instance
            admin_only_skip: If True, only admins can skip
        
        Returns:
            True if user can skip
        """
        if not admin_only_skip:
            # Anyone can skip
            return True
        
        # Check admin status
        return await PermissionSystem.is_admin(message)
    
    @staticmethod
    async def can_manage_voice_chat(message: Message) -> bool:
        """Check if user has voice chat management rights."""
        try:
            member = await message.bot.get_chat_member(
                chat_id=message.chat.id,
                user_id=message.from_user.id,
            )
            
            if isinstance(member, ChatMemberOwner):
                return True
            
            if isinstance(member, ChatMemberAdministrator):
                return member.can_manage_video_chats
            
            return False
        
        except Exception as e:
            log.error("permission_check_failed", error=str(e), group_id=message.chat.id)
            return False


class PermissionMiddleware(BaseMiddleware):
    """
    Middleware to inject permission utilities into handler context.
    
    Note: This is a simple context injector. For stricter enforcement,
    use Filter classes with @router.message(AdminFilter()).
    """
    
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        """Inject permission system into data dict."""
        data["permissions"] = PermissionSystem
        data["group_id"] = event.chat.id
        
        return await handler(event, data)
