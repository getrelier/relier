# ========================================================================
# ADMISSION LUA
# ========================================================================

# Redis admission-control script.
# - Atomic fixed-window counter implemented entirely inside Redis to avoid
# - race conditions under concurrent request spikes.
# Return shape:
#   [is_admitted (1|0), current_count, retry_after_seconds]

ADMISSION_LUA = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
local limit = tonumber(ARGV[1])
if current > limit then
    return {0, current, redis.call('TTL', KEYS[1])}
end
return {1, current, 0}
"""

# =============================================================================
# Idempotency
# =============================================================================

# Atomically resolve a cached result or claim execution ownership.
ACQUIRE_LUA = """
local existing = redis.call('GET', KEYS[1])
if existing then
    return {1, existing}
end
redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
return {0, false}
"""

# Release ownership only if the stored lock ID matches ours.
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


# =========================================================================
# PHOENIX LUA
# =========================================================================

# Atomic resurrection claim.
# Guarantees:
# - Only ONE resurrector acquires the lease.
# - Fence token is updated atomically with lease acquisition.
# Returns:
#   1 -> Lease acquired successfully
#   0 -> Lease already exists
RESURRECT_LUA = """
    local lease_key = KEYS[1]
    local fence_key = KEYS[2]

    local token = ARGV[1]
    local lease_ttl = tonumber(ARGV[2])
    local fence_ttl = tonumber(ARGV[3])

    -- Another resurrector already owns the lease
    if redis.call("EXISTS", lease_key) == 1 then
        return 0
    end

    -- Acquire lease
    redis.call("SET", lease_key, token, "EX", lease_ttl)

    -- Publish latest fence token
    redis.call("SET", fence_key, token, "EX", fence_ttl)

    return 1
    """

# Worker validation script.
# Guarantees:
# - Worker still owns lease.
# - Fence token is still current.
# Returns:
#   1 -> valid
#   0 -> invalid lease
#   2 -> stale fence
VALIDATE_LUA = """
    local lease_key = KEYS[1]
    local fence_key = KEYS[2]

    local expected = ARGV[1]

    local lease = redis.call("GET", lease_key)
    if lease ~= expected then
        return 0
    end

    local fence = redis.call("GET", fence_key)
    if fence ~= expected then
        return 2
    end

    return 1
    """


# Commit validation script.
# Used BEFORE writing results.
# Prevents zombie workers from committing stale data.
# Returns:
#   1 -> still current
#   0 -> stale worker
COMMIT_CHECK_LUA = """
    local fence_key = KEYS[1]
    local expected = ARGV[1]

    local current = redis.call("GET", fence_key)

    if current ~= expected then
        return 0
    end

    return 1
    """

# Cleanup script.
# Removes lease ONLY if caller still owns it.
CLEANUP_LUA = """
    local lease_key = KEYS[1]
    local expected = ARGV[1]

    local current = redis.call("GET", lease_key)

    if current == expected then
        redis.call("DEL", lease_key)
        return 1
    end

    return 0
    """
