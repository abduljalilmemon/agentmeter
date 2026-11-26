-- toolcall.lua: response-side loop check (PLAN §3.2). Runs after a successful response.
--
-- KEYS[1]  streak   am:tc:{run}   hash: fp (last response tool call), count (in a row)
-- KEYS[2]  breaker  am:brk:{run}
-- KEYS[3]  stream   am:events       breaker_open is XADDed when it opens
-- KEYS[4]  run budget am:b:run:{run} read only (spent, for the event)
-- KEYS[5]  history  am:run:{run}:breaker  list of breaker events (last 20)
-- ARGV[1]  fingerprint of the response's first tool call, or "" (no tool call)
-- ARGV[2]  K, consecutive identical tool calls that count as a loop
-- ARGV[3]  absolute expiry, unix seconds (the run key's)
-- ARGV[4]  stream MAXLEN (approximate)
-- ARGV[5..7]  run id, agent, team (for the event)
--
-- Returns {count, opened}. At count >= K the breaker opens with reason "loop", so the
-- run's next call is refused (this response has already been delivered).

local tc_key, brk_key = KEYS[1], KEYS[2]
local fingerprint = ARGV[1]
local k = tonumber(ARGV[2])
local expire_at = ARGV[3]

if fingerprint == '' then
  redis.call('DEL', tc_key)  -- a response without a tool call breaks the streak
  return {0, 0}
end

local count
if redis.call('HGET', tc_key, 'fp') == fingerprint then
  count = redis.call('HINCRBY', tc_key, 'count', 1)
else
  redis.call('HSET', tc_key, 'fp', fingerprint, 'count', 1)
  count = 1
end
redis.call('EXPIREAT', tc_key, expire_at)

if count >= k and redis.call('EXISTS', brk_key) == 0 then
  local t = redis.call('TIME')
  local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  redis.call('HSET', brk_key, 'reason', 'loop', 'source', 'response_tool_calls',
    'opened_ms', string.format('%.0f', now_ms))
  redis.call('EXPIREAT', brk_key, expire_at)
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[4], '*',
    'type', 'breaker_open', 'run', ARGV[5], 'agent', ARGV[6], 'team', ARGV[7],
    'reason', 'loop', 'source', 'response_tool_calls', 'count', count,
    'spent', redis.call('HGET', KEYS[4], 'spent') or '0')
  redis.call('RPUSH', KEYS[5], cjson.encode({event = 'open', reason = 'loop',
    source = 'response_tool_calls', count = count, at_ms = string.format('%.0f', now_ms)}))
  redis.call('LTRIM', KEYS[5], -20, -1)
  redis.call('EXPIREAT', KEYS[5], expire_at)
  return {count, 1}
end
return {count, 0}
