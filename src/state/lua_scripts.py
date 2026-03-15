"""
Lua scripts for atomic Redis operations.
All scripts are loaded once and executed via EVALSHA.
"""

# Atomic enqueue with max-length check
# KEYS[1]: queue key
# ARGV[1]: serialized track (msgpack bytes)
# ARGV[2]: max_queue_length
# Returns: -1 if queue full, else new length
ENQUEUE_SCRIPT = """
local current_len = redis.call('LLEN', KEYS[1])
local max_len = tonumber(ARGV[2])
if current_len >= max_len then
    return -1
end
redis.call('RPUSH', KEYS[1], ARGV[1])
return redis.call('LLEN', KEYS[1])
"""

# Atomic dequeue using LMOVE to shadow processing list
# KEYS[1]: queue key
# KEYS[2]: shadow processing queue key
# Returns: msgpack bytes of track or nil if empty
DEQUEUE_SCRIPT = """
local item = redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'LEFT')
if item == false then
    return nil
end
return item
"""

# Acknowledge processed message - remove from shadow list
# KEYS[1]: shadow processing queue key
# ARGV[1]: serialized track bytes (exact match)
# Returns: number of items removed (1 or 0)
ACK_SCRIPT = """
return redis.call('LREM', KEYS[1], 1, ARGV[1])
"""

# Negative acknowledge - move from shadow back to head of main queue
# KEYS[1]: shadow processing queue key
# KEYS[2]: main queue key
# ARGV[1]: serialized track bytes
# Returns: 1 if moved, 0 if not found
NACK_SCRIPT = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed == 0 then
    return 0
end
redis.call('LPUSH', KEYS[2], ARGV[1])
return 1
"""

# Promote track to front of queue
# KEYS[1]: queue key
# ARGV[1]: track_id to promote (string UUID)
# Returns: 1 if promoted, 0 if not found
# Note: This script deserializes msgpack in Lua (complex) - better to do in Python
PROMOTE_SCRIPT = """
local items = redis.call('LRANGE', KEYS[1], 0, -1)
local track_id = ARGV[1]
for i, item in ipairs(items) do
    -- Simple substring search for track_id in msgpack blob
    -- Production: deserialize in Python, use LINSERT instead
    if string.find(item, track_id) then
        redis.call('LREM', KEYS[1], 1, item)
        redis.call('LPUSH', KEYS[1], item)
        return 1
    end
end
return 0
"""

# Clear entire queue atomically
# KEYS[1]: queue key
# KEYS[2]: shadow queue key
# Returns: total number of items removed
CLEAR_SCRIPT = """
local main_len = redis.call('LLEN', KEYS[1])
local shadow_len = redis.call('LLEN', KEYS[2])
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
return main_len + shadow_len
"""
