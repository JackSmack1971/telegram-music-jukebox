"""
Message throttle middleware using token-bucket rate limiting.
Prevents flood by limiting commands per group.
"""

import asyncio
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Awaitable

from aiogram import BaseMiddleware
from aiogram.types import Message

from src.domain.logging import get_logger

log = get_logger(__name__)


class AsyncTokenBucket:
    """
    Async-safe token bucket for rate limiting.
    
    Algorithm:
        - Tokens refill at constant rate
        - Capacity caps max burst
        - Consume 1 token per request
    """
    
    def __init__(self, capacity: float, refill_rate: float):
        """
        Initialize token bucket.
        
        Args:
            capacity: Max tokens (burst size)
            refill_rate: Tokens added per second
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        tokens_to_add = elapsed * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_refill = now
    
    async def consume(self, tokens: float = 1.0) -> tuple[bool, float]:
        """
        Try to consume tokens.
        
        Returns:
            (allowed, wait_time_seconds)
        """
        async with self.lock:
            self._refill()
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True, 0.0
            else:
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.refill_rate
                return False, wait_time


class MessageThrottle(BaseMiddleware):
    """
    Per-group command rate limiting middleware.
    
    Settings:
        - Rate: 20 commands per minute per group
        - Burst capacity: 5 commands
    
    Behavior:
        - Silently drop rate-limited messages
        - Log warning with group_id
    """
    
    TOKEN_BUCKET_RATE = 20.0  # messages per minute
    TOKEN_BUCKET_CAPACITY = 5.0  # burst capacity
    
    def __init__(self):
        # Per-group buckets
        self._buckets: Dict[int, AsyncTokenBucket] = defaultdict(
            lambda: AsyncTokenBucket(
                capacity=self.TOKEN_BUCKET_CAPACITY,
                refill_rate=self.TOKEN_BUCKET_RATE / 60.0,  # per second
            )
        )
    
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        """Throttle messages per group."""
        chat_id = event.chat.id
        
        bucket = self._buckets[chat_id]
        allowed, wait_time = await bucket.consume()
        
        if not allowed:
            log.warning(
                "message_throttled",
                group_id=chat_id,
                user_id=event.from_user.id,
                wait_time_sec=wait_time,
            )
            # Silently drop (do not call handler)
            return
        
        # Pass through
        return await handler(event, data)
