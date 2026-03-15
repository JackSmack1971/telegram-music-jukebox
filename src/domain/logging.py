"""
Structured logging configuration using structlog.
Enforces mandatory group_id context key in all log records.
"""

import logging
from typing import Any

import structlog
from structlog.contextvars import (
    bind_contextvars,
    clear_contextvars,
    merge_contextvars,
)
from structlog.types import EventDict, Processor


def require_group_id(
    logger: Any, method_name: str, event_dict: EventDict
) -> EventDict:
    """
    Processor that enforces group_id is always present.
    In production, injects sentinel rather than raising to avoid log storms.
    """
    if "group_id" not in event_dict:
        event_dict["group_id"] = "MISSING"
    return event_dict


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structlog globally. Call once at application startup.
    
    Args:
        log_level: Python logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    shared_processors: list[Processor] = [
        merge_contextvars,  # Must be first - merges contextvars into event_dict
        require_group_id,  # Enforce mandatory group_id key
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    
    structlog.configure(
        processors=shared_processors + [
            structlog.processors.JSONRenderer(),  # JSON output for production
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,  # Performance optimization
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a bound logger instance.
    
    Usage:
        log = get_logger(__name__)
        log.info("event", key=value)
    
    Note: Always bind group_id at handler entry point:
        bind_contextvars(group_id=group_id)
    """
    return structlog.get_logger(name)


class GroupContextFilter:
    """
    Optional filter for stdlib logging integration.
    Ensures group_id is present in log records.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add group_id to record if missing."""
        if not hasattr(record, "group_id"):
            record.group_id = "MISSING"  # type: ignore
        return True


# Export public API
__all__ = [
    "configure_logging",
    "get_logger",
    "bind_contextvars",
    "clear_contextvars",
    "GroupContextFilter",
]
