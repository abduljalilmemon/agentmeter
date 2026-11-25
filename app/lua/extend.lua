-- extend.lua: keep a live reservation from being swept (heartbeat for long streams).
--
-- KEYS[1]  reservation index  am:resv (ZSET)
-- KEYS[2]  reservation hash   am:r:{id}
-- ARGV[1]  reservation id
-- ARGV[2]  new TTL from now, milliseconds
--
-- Returns 1 if the expiry moved to now + TTL (never earlier than it was), 0 if the
-- reservation is gone (settled or swept). Uses Redis TIME, like reserve.lua.

local index_key = KEYS[1]
local resv_key = KEYS[2]
local resv_id = ARGV[1]

if not redis.call('ZSCORE', index_key, resv_id) then
  return 0
end
if not redis.call('HGET', resv_key, 'amount') then
  return 0
end

local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local score = string.format('%.0f', now_ms + tonumber(ARGV[2]))
redis.call('ZADD', index_key, 'XX', 'GT', score, resv_id)
return 1
