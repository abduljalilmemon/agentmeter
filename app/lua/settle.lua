-- settle.lua: turn a reservation into actual spend.
--
-- KEYS[1]          reservation key  am:resv:{id}
-- KEYS[2..n+1]     the same level hashes the reservation was made on
-- ARGV[1]          actual cost, integer micro-USD
-- ARGV[2..n+1]     level i absolute expiry, unix seconds (re-applied if a key lost its TTL)
--
-- Reservation present: reserved -= amount and spent += actual at each level, then the
-- reservation is deleted. Reservation gone (expired or swept): spent += actual only.
-- Returns 1 if the reservation was found, else 0.
--
-- Not idempotent by itself: the caller settles each reservation id once.

local resv_key = KEYS[1]
local actual_s = ARGV[1]
local n = #KEYS - 1

local amount = redis.call('HGET', resv_key, 'amount')

for i = 1, n do
  local key = KEYS[i + 1]
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
    redis.call('EXPIREAT', key, ARGV[i + 1])
  end
end

if amount then
  redis.call('DEL', resv_key)
  return 1
end
return 0
