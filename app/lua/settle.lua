-- settle.lua: turn a reservation into actual spend, exactly once, and emit its events.
--
-- KEYS[1]                 reservation index  am:resv (ZSET)
-- KEYS[2]                 reservation hash   am:r:{id}
-- KEYS[3]                 event stream       am:events
-- KEYS[3+i]   i = 1..n    the level hashes the reservation was made on
-- KEYS[3+n+(i-1)*m+j]     threshold flag for level i, threshold j  (am:alert:...:{bp})
-- KEYS[#KEYS]             run report stats  am:run:{id}:stats (hash; expires with run)
--
-- ARGV[1]  reservation id         ARGV[2]  actual cost, integer micro-USD
-- ARGV[3]  tombstone TTL, ms      ARGV[4]  stream MAXLEN (approximate)
-- ARGV[5]  n (levels)             ARGV[6]  m (thresholds)
-- ARGV[6+j]                 threshold j in basis points, increasing
-- ARGV[7+m+2(i-1)]          level i name (run | agent | team)
-- ARGV[8+m+2(i-1)]          level i absolute expiry, unix seconds
-- ARGV[7+m+2n ..]           run, agent, team, then call_settled field/value pairs
--                           (prompt_tokens, completion_tokens and estimate_micro
--                           are also summed into the run's stats)
--
-- Returns
--   1  reservation found: reserved -= amount and spent += actual at each level
--   0  reservation gone (swept): spent += actual only; the sweep already released it
--   2  already settled: nothing changes, no events
--
-- Thresholds are measured on `spent` (not spent + reserved), after this settle's spend:
-- level i crosses threshold bp when spent * 10000 >= limit * bp. It is evaluated exactly
-- with integers only: with limit = q * 10000 + r, that is spent >= q * bp + ceil(r * bp
-- / 10000), and no intermediate value exceeds the limit (so stays below 2^53). A crossing
-- is emitted only if SET NX of its flag succeeds, so each (level key, threshold) fires
-- once, however many replicas settle at the same time.

local index_key, resv_key, stream = KEYS[1], KEYS[2], KEYS[3]
local resv_id = ARGV[1]
local actual_s = ARGV[2]
local tombstone_ms = ARGV[3]
local maxlen = ARGV[4]
local n = tonumber(ARGV[5])
local m = tonumber(ARGV[6])
local base = 7 + m + 2 * n
local run_id, agent, team = ARGV[base], ARGV[base + 1], ARGV[base + 2]

local stored = redis.call('HMGET', resv_key, 'amount', 'settled')
local amount, settled = stored[1], stored[2]
if settled then
  return 2
end

local function int_s(x)
  return string.format('%.0f', x)
end

-- Smallest integer spent with spent * 10000 >= limit * bp.
local function needed(limit, bp)
  local q = math.floor(limit / 10000)
  local r = limit - q * 10000
  if r < 0 then q = q - 1; r = r + 10000 elseif r >= 10000 then q = q + 1; r = r - 10000 end
  return q * bp + math.floor((r * bp + 9999) / 10000)
end

for i = 1, n do
  local key = KEYS[3 + i]
  local name = ARGV[7 + m + 2 * (i - 1)]
  local expire_at = ARGV[8 + m + 2 * (i - 1)]
  if amount and amount ~= '0' then
    -- Negate as a string: Lua 5.1 would format a large number as 1e+14.
    local reserved = redis.call('HINCRBY', key, 'reserved', '-' .. amount)
    if reserved < 0 then
      -- Only possible if the level key expired and was re-created mid-call.
      redis.call('HSET', key, 'reserved', 0)
    end
  end
  local spent = redis.call('HINCRBY', key, 'spent', actual_s)
  if redis.call('TTL', key) == -1 then
    redis.call('EXPIREAT', key, expire_at)
  end

  local limit = tonumber(redis.call('HGET', key, 'limit') or '0')
  if limit > 0 then
    for j = 1, m do
      local bp = tonumber(ARGV[6 + j])
      if spent < needed(limit, bp) then
        break  -- thresholds are increasing
      end
      local flag = KEYS[3 + n + (i - 1) * m + j]
      if redis.call('SET', flag, '1', 'NX', 'EXAT', expire_at) then
        redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*',
          'type', 'threshold_crossed', 'level', name, 'key', key,
          'threshold_bp', ARGV[6 + j], 'spent', int_s(spent), 'limit', int_s(limit),
          'run', run_id, 'agent', agent, 'team', team)
      else
        -- Already fired: keep the flag alive as long as its (sliding) budget key.
        redis.call('EXPIREAT', flag, expire_at, 'GT')
      end
    end
  end
end

redis.call('ZREM', index_key, resv_id)
redis.call('DEL', resv_key)
redis.call('HSET', resv_key, 'settled', '1')
redis.call('PEXPIRE', resv_key, tombstone_ms)

-- Run report (GET /runs/{id}): running totals, kept here rather than by scanning the
-- stream. Level 1 is always the run, so its expiry is the run key's.
local stats_key = KEYS[#KEYS]
redis.call('HINCRBY', stats_key, 'calls', 1)
redis.call('HINCRBY', stats_key, 'cost_micro', actual_s)
for k = base + 3, #ARGV - 1, 2 do
  local field = ARGV[k]
  if field == 'prompt_tokens' or field == 'completion_tokens' or field == 'estimate_micro' then
    redis.call('HINCRBY', stats_key, field, ARGV[k + 1])
  end
end
if agent ~= '' then redis.call('HSET', stats_key, 'agent', agent) end
if team ~= '' then redis.call('HSET', stats_key, 'team', team) end
if n >= 1 then redis.call('EXPIREAT', stats_key, ARGV[8 + m]) end

local event = {'type', 'call_settled', 'run', run_id, 'agent', agent, 'team', team,
  'cost_micro', actual_s, 'reservation', resv_id,
  'reservation_state', amount and 'released' or 'gone'}
for k = base + 3, #ARGV do
  event[#event + 1] = ARGV[k]
end
redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*', unpack(event))

if amount then
  return 1
end
return 0
