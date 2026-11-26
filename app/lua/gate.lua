-- gate.lua: the per-run circuit breaker, step cap and request-loop check (PLAN §3.2, §3.3).
-- Runs once per request, before reserve.lua. A refused call reserves nothing.
--
-- KEYS[1]  breaker      am:brk:{run}    hash; present = OPEN (reason, source, steps, opened_ms)
-- KEYS[2]  step count   am:steps:{run}  integer
-- KEYS[3]  fingerprints am:fp:{run}     hash: fingerprint -> times seen
-- KEYS[4]  run budget   am:b:run:{run}  read only (spent, for the 429 body)
-- KEYS[5]  event stream am:events       breaker_open is XADDed when it opens
-- KEYS[6]  history      am:run:{run}:breaker  list of breaker events (last 20)
-- ARGV[1]  step cap
-- ARGV[2]  repeat threshold
-- ARGV[3]  request fingerprint, or "" (fewer than 2 messages, or loop-exempt)
-- ARGV[4]  absolute expiry, unix seconds (the run key's: these keys share its TTL)
-- ARGV[5]  stream MAXLEN (approximate)
-- ARGV[6..8]  run id, agent, team (for the event; "" when absent)
--
-- Returns
--   {1, steps, repeats}                      allowed
--   {0, reason, spent, steps, repeats, new}  refused; new = 1 if this call opened it
--
-- The call that opens the breaker is refused too.

local brk_key, steps_key, fp_key, run_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local stream, history = KEYS[5], KEYS[6]
local step_cap = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local fingerprint = ARGV[3]
local expire_at = ARGV[4]
local maxlen = ARGV[5]
local run_id, agent, team = ARGV[6], ARGV[7], ARGV[8]

local function spent()
  return redis.call('HGET', run_key, 'spent') or '0'
end

local function open(reason, source, steps)
  local t = redis.call('TIME')
  local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  redis.call('HSET', brk_key, 'reason', reason, 'source', source, 'steps', steps,
    'opened_ms', string.format('%.0f', now_ms))
  redis.call('EXPIREAT', brk_key, expire_at)
  redis.call('XADD', stream, 'MAXLEN', '~', maxlen, '*',
    'type', 'breaker_open', 'run', run_id, 'agent', agent, 'team', team,
    'reason', reason, 'source', source, 'steps', steps, 'spent', spent())
  redis.call('RPUSH', history, cjson.encode({event = 'open', reason = reason,
    source = source, steps = steps, at_ms = string.format('%.0f', now_ms)}))
  redis.call('LTRIM', history, -20, -1)
  redis.call('EXPIREAT', history, expire_at)
end

local reason = redis.call('HGET', brk_key, 'reason')
if reason then
  local steps = tonumber(redis.call('GET', steps_key) or '0')
  return {0, reason, spent(), steps, 0, 0}
end

local steps = redis.call('INCR', steps_key)
redis.call('EXPIREAT', steps_key, expire_at)
if steps > step_cap then
  open('step_cap', 'step_cap', steps)
  return {0, 'step_cap', spent(), steps, 0, 1}
end

local repeats = 0
if fingerprint ~= '' then
  repeats = redis.call('HINCRBY', fp_key, fingerprint, 1)
  redis.call('EXPIREAT', fp_key, expire_at)
  if repeats >= threshold then
    open('loop', 'request_repeats', steps)
    return {0, 'loop', spent(), steps, repeats, 1}
  end
end

return {1, steps, repeats}
