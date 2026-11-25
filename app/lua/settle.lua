-- settle.lua: turn a reservation into actual spend, exactly once.
--
-- KEYS[1]          reservation index  am:resv (ZSET)
-- KEYS[2]          reservation hash   am:r:{id}
-- KEYS[3..n+2]     the same level hashes the reservation was made on
-- ARGV[1]          reservation id
-- ARGV[2]          actual cost, integer micro-USD
-- ARGV[3]          tombstone TTL, milliseconds
-- ARGV[4..n+3]     level i absolute expiry, unix seconds (re-applied if a key lost its TTL)
--
-- Returns
--   1  reservation found: reserved -= amount and spent += actual at each level
--   0  reservation gone (swept): spent += actual only; the sweep already released it
--   2  already settled: nothing changes
--
-- Afterwards the hash is replaced by a tombstone {settled=1} that lives for the TTL, so a
-- repeated settle can neither release twice nor count the spend twice. The id leaves the
-- index, so a later sweep never sees it.

local index_key = KEYS[1]
local resv_key = KEYS[2]
local resv_id = ARGV[1]
local actual_s = ARGV[2]
local tombstone_ms = ARGV[3]
local n = #KEYS - 2

local stored = redis.call('HMGET', resv_key, 'amount', 'settled')
local amount, settled = stored[1], stored[2]
if settled then
  return 2
end

for i = 1, n do
  local key = KEYS[i + 2]
  if amount and amount ~= '0' then
    -- Negate as a string: Lua 5.1 would format a large number as 1e+14.
    local reserved = redis.call('HINCRBY', key, 'reserved', '-' .. amount)
    if reserved < 0 then
      -- Only possible if the level key expired and was re-created mid-call.
      redis.call('HSET', key, 'reserved', 0)
    end
  end
  redis.call('HINCRBY', key, 'spent', actual_s)
  if redis.call('TTL', key) == -1 then
    redis.call('EXPIREAT', key, ARGV[i + 3])
  end
end

redis.call('ZREM', index_key, resv_id)
redis.call('DEL', resv_key)
redis.call('HSET', resv_key, 'settled', '1')
redis.call('PEXPIRE', resv_key, tombstone_ms)
if amount then
  return 1
end
return 0
