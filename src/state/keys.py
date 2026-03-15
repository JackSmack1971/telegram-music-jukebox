"""
Redis key namespace builder.
Convention: jukebox:{env}:{key_type}:{identifier}
"""

import os


class RedisKeys:
    """
    Centralized Redis key builder with environment prefix.
    
    Key families:
        1. queue:{group_id} - Main FIFO queue list
        2. shadow:{group_id} - Shadow processing list (LMOVE target)
        3. state:{group_id} - PlaybackState hash
        4. settings:{group_id} - GroupSettings hash
        5. cache:meta:{sha256} - Cache metadata hash
        6. lock:dl:{sha256} - Download lock (Redlock)
        7. track:{track_id} - Track metadata
        8. active:{group_id} - Active group marker with TTL
        9. ratelimit:{group_id} - Rate limiting sorted set
    """
    
    def __init__(self, env: str = ""):
        """
        Initialize key builder.
        
        Args:
            env: Environment prefix (prod, dev, test). Defaults to JUKEBOX_ENV env var.
        """
        self.env = env or os.getenv("JUKEBOX_ENV", "prod")
        self._prefix = f"jukebox:{self.env}"
    
    def queue(self, group_id: int) -> str:
        """Main queue list key."""
        return f"{self._prefix}:queue:{group_id}"
    
    def shadow_queue(self, group_id: int) -> str:
        """Shadow processing list key (LMOVE target)."""
        return f"{self._prefix}:shadow:{group_id}"
    
    def playback_state(self, group_id: int) -> str:
        """PlaybackState hash key."""
        return f"{self._prefix}:state:{group_id}"
    
    def group_settings(self, group_id: int) -> str:
        """GroupSettings hash key."""
        return f"{self._prefix}:settings:{group_id}"
    
    def cache_meta(self, sha256: str) -> str:
        """Cache metadata hash key."""
        return f"{self._prefix}:cache:meta:{sha256}"
    
    def download_lock(self, sha256: str) -> str:
        """Download lock key for Redlock."""
        return f"{self._prefix}:lock:dl:{sha256}"
    
    def track_meta(self, track_id: str) -> str:
        """Track metadata hash key."""
        return f"{self._prefix}:track:{track_id}"
    
    def group_active(self, group_id: int) -> str:
        """Active group marker with TTL."""
        return f"{self._prefix}:active:{group_id}"
    
    def rate_limit(self, group_id: int) -> str:
        """Rate limiting sorted set key."""
        return f"{self._prefix}:ratelimit:{group_id}"
