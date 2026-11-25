-- sweep.lua: reclaim reservations whose expiry passed without a settle (crashed calls).
--
-- KEYS[1]  reservation index  am:resv (ZSET: id -> expiry, unix ms)
-- ARGV[1]  batch size (max ids taken per call)
-- ARGV[2]  reservation hash key prefix, "am:r:"
--
-- For each expired id: reserved -= amount at every level key the reservation names,
-- then the hash and the index entry are deleted. Returns {released, examined}.
--
-- Runs atomically, so any number of replicas can sweep at once and race with settle.
-- Whichever runs first owns the release: settle removes the id from the index, and a
-- sweep deletes the hash so a late settle adds spend only. The reservation and level
-- keys are derived from the index, not passed in KEYS: fine on a single Redis node
-- (the deployment this project supports), not on Redis Cluster.

local index_key = KEYS[1]
local limit = tonumber(ARGV[1])
local prefix = ARGV[2]

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', index_key, '-inf', string.format('%.0f', now_ms),
  'LIMIT', 0, limit)

local released = 0
for _, id in ipairs(ids) do
  local resv_key = prefix .. id
  local stored = redis.call('HMGET', resv_key, 'amount', 'keys')
  local amount, keys = stored[1], stored[2]
  if amount then
    if amount ~= '0' and keys then
      for key in string.gmatch(keys, '[^\n]+') do
        -- A level key that already expired has nothing left to release.
        if redis.call('EXISTS', key) == 1 then
          local reserved = redis.call('HINCRBY', key, 'reserved', '-' .. amount)
          if reserved < 0 then
            redis.call('HSET', key, 'reserved', 0)
          end
        end
      end
    end
    redis.call('DEL', resv_key)
    released = released + 1
  end
  redis.call('ZREM', index_key, id)
end
return {released, #ids}
