-- Token bucket + monthly spend cap, checked atomically in one round trip.
--
-- KEYS[1] = bucket hash (fields: tokens, ts)
-- KEYS[2] = monthly spend counter, in nano-USD (integer)
-- ARGV[1] = refill rate, tokens per second
-- ARGV[2] = burst (bucket capacity)
-- ARGV[3] = monthly cap in nano-USD (0 = no cap)
--
-- Returns {code, retry_after_ms}: code 1 = allowed, 0 = rate limited,
-- -1 = over spend cap.
--
-- Why Lua: read-refill-decrement must be atomic. With separate GET/SET calls,
-- two gateway replicas can both read "1 token left" and both allow. Redis runs
-- a script without interleaving other commands, so no locks are needed.
--
-- Why Redis TIME instead of a timestamp from the gateway: clocks on different
-- gateway replicas drift. One shared clock (Redis) keeps refill math
-- consistent no matter which replica handles the request.

local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cap   = tonumber(ARGV[3])

if cap > 0 then
  local spent = tonumber(redis.call('GET', KEYS[2]) or '0')
  if spent >= cap then
    return {-1, 0}
  end
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil then
  tokens = burst
  ts = now
end

tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)

local code, retry = 0, 0
if tokens >= 1 then
  tokens = tokens - 1
  code = 1
else
  retry = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
-- Expire idle buckets once they would be full again, so inactive keys don't
-- accumulate in Redis forever.
redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000 / rate) + 1000)
return {code, retry}
