"""
Stream watchdog for monitoring FFmpeg process health.
Polls process status at 1Hz heartbeat and triggers recovery on crash.
"""

import asyncio
from typing import TYPE_CHECKING

from src.domain.logging import get_logger, bind_contextvars

if TYPE_CHECKING:
    from .stream_engine import ConcreteStreamEngine

log = get_logger(__name__)


class StreamWatchdog:
    """
    FFmpeg process watchdog with heartbeat monitoring.
    
    Features:
        - 1Hz polling loop
        - Automatic crash detection
        - Recovery triggering
        - Graceful cancellation
    """
    
    HEARTBEAT_INTERVAL = 1.0  # seconds
    
    def __init__(self):
        # Active watch tasks: group_id -> asyncio.Task
        self._tasks: dict[int, asyncio.Task] = {}
    
    async def watch(
        self,
        group_id: int,
        ffmpeg_proc: asyncio.subprocess.Process,
        stream_engine: "ConcreteStreamEngine",
    ) -> None:
        """
        Start monitoring FFmpeg process.
        
        Args:
            group_id: Group ID
            ffmpeg_proc: FFmpeg subprocess to monitor
            stream_engine: StreamEngine instance for recovery callback
        """
        bind_contextvars(group_id=group_id)
        
        log.info("watchdog_started", pid=ffmpeg_proc.pid)
        
        # Create task and store reference
        task = asyncio.create_task(
            self._monitor_loop(group_id, ffmpeg_proc, stream_engine)
        )
        self._tasks[group_id] = task
    
    async def _monitor_loop(
        self,
        group_id: int,
        ffmpeg_proc: asyncio.subprocess.Process,
        stream_engine: "ConcreteStreamEngine",
    ) -> None:
        """Internal monitoring loop."""
        bind_contextvars(group_id=group_id)
        
        try:
            while True:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                
                # Check process returncode
                returncode = ffmpeg_proc.returncode
                
                if returncode is not None:
                    # Process exited
                    if returncode == 0:
                        # Normal exit
                        log.info("ffmpeg_exited_normally", pid=ffmpeg_proc.pid)
                        await stream_engine._on_track_end(group_id)
                    else:
                        # Crash detected
                        log.error(
                            "ffmpeg_crashed",
                            pid=ffmpeg_proc.pid,
                            returncode=returncode,
                        )
                        
                        # Trigger recovery
                        success = await stream_engine._recovery.attempt_recovery(
                            group_id=group_id,
                            failed_proc=ffmpeg_proc,
                            stream_engine=stream_engine,
                        )
                        
                        if not success:
                            log.error("recovery_failed")
                            await stream_engine.stop_stream(group_id)
                    
                    # Exit monitoring loop
                    break
                
                # Heartbeat log at DEBUG level
                log.debug("watchdog_heartbeat", pid=ffmpeg_proc.pid)
        
        except asyncio.CancelledError:
            log.info("watchdog_cancelled")
            raise
        
        except Exception as e:
            log.error("watchdog_error", error=str(e))
        
        finally:
            # Clean up task reference
            if group_id in self._tasks:
                del self._tasks[group_id]
    
    async def stop(self, group_id: int) -> None:
        """Stop watchdog for group."""
        task = self._tasks.get(group_id)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        log.info("watchdog_stopped", group_id=group_id)
