""" Domain enumerations for Jukebox state machines. Uses IntEnum for compact Redis storage with msgpack. """
from enum import IntEnum

class TrackStatus(IntEnum): 
    """Track lifecycle status enum.""" 
    PENDING = 0 
    DOWNLOADING = 1 
    READY = 2 
    PLAYING = 3 
    PAUSED = 4 
    DONE = 5 
    ERROR = 6

class PlaybackStatus(IntEnum): 
    """Playback engine status enum.""" 
    IDLE = 0 
    PLAYING = 1 
    PAUSED = 2 
    STOPPED = 3
```
```python
class LoopMode(IntEnum): 
    """Queue loop behavior enum.""" 
    NONE = 0 
    TRACK = 1 
    QUEUE = 2

class ErrorSeverity(IntEnum): 
    """Error severity levels matching Python logging constants.""" 
    LOW = 10 
    MEDIUM = 20 
    HIGH = 30 
    CRITICAL = 40 
