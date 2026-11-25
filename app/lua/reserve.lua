-- reserve.lua: all-or-nothing budget reservation across levels (run ⊂ agent ⊂ team).
--
-- KEYS[1]          reservation index  am:resv (ZSET: id -> expiry, unix ms, Redis clock)
-- KEYS[2]          reservation hash   am:r:{id} (amount + level keys)
-- KEYS[3..n+2]     level hashes       am:b:run:..., am:b:agent:..., am:b:team:...
-- ARGV[1]          reservation id
-- ARGV[2]          estimate, integer micro-USD
-- ARGV[3]          reservation TTL, milliseconds
-- ARGV[4+3(i-1)]   level i limit, integer micro-USD
-- ARGV[5+3(i-1)]   level i absolute expiry, unix seconds (EXPIREAT)
-- ARGV[6+3(i-1)]   level i "1" = limit is fixed at first write (HSETNX), "0" = always HSET
--
-- Returns {1} on success, {0, i, spent, reserved, limit} for the first level i that
-- can't fit the estimate (nothing written), or {-1} if the reservation id already exists.
--
-- Money arrives as decimal strings and is written back as those strings, so values are
-- never formatted through a Lua float. Comparisons use Lua numbers (doubles), which are
-- exact for integers up to 2^53 micro-USD (about 9 billion USD).
--
-- The expiry score uses Redis TIME, so every proxy replica shares one clock for it.
-- (Redis 7 replicates script effects, so TIME before writes is allowed.)

local index_key = KEYS[1]
local resv_key = KEYS[2]
local resv_id = ARGV[1]
local estimate_s = ARGV[2]
local estimate = tonumber(estimate_s)
local ttl_ms = tonumber(ARGV[3])
local n = #KEYS - 2

if redis.call('EXISTS', resv_key) == 1 then
  return {-1}
end

local function level_args(i)
  local base = 4 + (i - 1) * 3
  return ARGV[base], ARGV[base + 1], ARGV[base + 2] == '1'
end

-- Pass 1: check every level. Nothing is written unless all of them fit.
for i = 1, n do
  local key = KEYS[i + 2]
  local limit_s, _, fixed = level_args(i)
  local limit = tonumber(limit_s)
  local stored = redis.call('HMGET', key, 'limit', 'spent', 'reserved')
  if fixed and stored[1] then
    limit = tonumber(stored[1])
  end
  local spent = tonumber(stored[2] or '0')
  local reserved = tonumber(stored[3] or '0')
  if spent + reserved + estimate > limit then
    return {0, i, spent, reserved, limit}
  end
end

-- Pass 2: reserve at every level.
local keys = {}
for i = 1, n do
  local key = KEYS[i + 2]
  local limit_s, expire_at, fixed = level_args(i)
  if fixed then
    redis.call('HSETNX', key, 'limit', limit_s)
  else
    redis.call('HSET', key, 'limit', limit_s)
  end
  redis.call('HINCRBY', key, 'reserved', estimate_s)
  redis.call('EXPIREAT', key, expire_at)
  keys[i] = key
end

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('HSET', resv_key, 'amount', estimate_s, 'keys', table.concat(keys, '\n'))
redis.call('ZADD', index_key, string.format('%.0f', now_ms + ttl_ms), resv_id)
return {1}
