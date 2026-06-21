"""
Lua scripts executed atomically inside Redis.

Why Lua instead of a distributed lock:
Redis executes a Lua script as a single uninterruptible unit (it is
single-threaded for command/script execution). That gives us the same
atomicity a distributed lock would give us, but in ONE network round
trip instead of 4+ (SET lock / GET / DECRBY / DEL lock), and with no
lock-expiry or stale-lock failure modes to reason about.

Key layout (Redis Hash), one hash per (org, feature):
    quota:{org_id}:{feature}
        limit   -> int, the monthly quota ceiling
        used    -> int, units consumed so far this period
        period  -> str, "YYYY-MM" the `used` counter applies to

Reset semantics live INSIDE the script: if the stored period does not
match the current period, `used` is reset to 0 and `period` is updated
before the check runs. This makes "is it a new month" and "deduct"
ONE atomic operation, so two requests racing across a month boundary
cannot both see "needs reset" and double-reset.
"""

# KEYS[1] = quota hash key, e.g. "quota:org_123:container-tracking"
# ARGV[1] = amount requested (int)
# ARGV[2] = current period string, e.g. "2026-06"
# ARGV[3] = default limit to use if this hash doesn't exist yet
#           (fallback only; normally limit is set by provisioning)
#
# Returns: {allowed (0/1), remaining_after, used, limit, period}
CHECK_AND_DEDUCT = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])
local current_period = ARGV[2]
local default_limit = tonumber(ARGV[3])

local data = redis.call('HMGET', key, 'limit', 'used', 'period')
local limit = tonumber(data[1])
local used = tonumber(data[2])
local period = data[3]

-- First time we've ever seen this org+feature: seed it.
if limit == nil then
    limit = default_limit
    used = 0
    period = current_period
    redis.call('HSET', key, 'limit', limit, 'used', used, 'period', period)
end

-- Roll over to a new period if the stored period is stale.
-- This check-and-reset is inside the same atomic script as the
-- deduction below, so no two concurrent callers can race the reset.
if period ~= current_period then
    used = 0
    period = current_period
    redis.call('HSET', key, 'used', used, 'period', period)
end

local remaining = limit - used

if remaining < amount then
    -- Denied. All-or-nothing: no partial deduction on insufficient quota.
    return {0, remaining, used, limit, period}
end

used = used + amount
redis.call('HSET', key, 'used', used)
remaining = limit - used

return {1, remaining, used, limit, period}
"""

# KEYS[1] = quota hash key
# ARGV[1] = amount to refund (int)
# Used to undo a deduction when the downstream operation fails, or
# when reconciling a crashed/abandoned idempotency record.
# Floors at 0 so a buggy double-refund can't manufacture quota.
REFUND = """
local key = KEYS[1]
local amount = tonumber(ARGV[1])

local used = tonumber(redis.call('HGET', key, 'used') or '0')
used = used - amount
if used < 0 then
    used = 0
end
redis.call('HSET', key, 'used', used)
return used
"""

# KEYS[1] = quota hash key
# ARGV[1] = current period string
# Read-only usage lookup for the reporting endpoint. Applies the same
# rollover logic (without mutating) so a stale period reports 0 used
# rather than last month's leftover count.
GET_USAGE = """
local key = KEYS[1]
local current_period = ARGV[1]

local data = redis.call('HMGET', key, 'limit', 'used', 'period')
local limit = tonumber(data[1])
local used = tonumber(data[2])
local period = data[3]

if limit == nil then
    return {0, 0, nil}
end

if period ~= current_period then
    used = 0
end

return {limit, used, period}
"""
