import { Request, Response, NextFunction } from 'express';
import { redisClient } from '../config/redis.config';

/**
 * Distributed Token Bucket Rate Limiter.
 *
 * Why token bucket over a fixed window counter:
 * - Fixed windows allow a burst of 2x the limit at window boundaries (e.g. 10 requests
 *   at 0.99s + 10 requests at 1.01s = 20 requests in ~20ms). Token bucket enforces a
 *   *smooth* sustained rate while still tolerating short bursts up to the bucket capacity.
 * - State (tokens remaining + last refill timestamp) is stored in Redis, not in
 *   process memory, so the limit holds correctly across every horizontally-scaled
 *   Node instance sitting behind Nginx — a limiter with in-memory state would let
 *   each replica grant its own separate quota to the same client IP.
 *
 * Atomicity:
 * The read-refill-decide-write cycle MUST be a single atomic operation, otherwise
 * two concurrent requests from the same client can race the read step and both
 * be admitted. This is executed as a Lua script via EVAL, which Redis guarantees
 * runs atomically (single-threaded execution, no other command interleaves).
 */

// KEYS[1] = bucket key
// ARGV[1] = bucket capacity (max burst size)
// ARGV[2] = refill rate in tokens/second
// ARGV[3] = current time in milliseconds
// ARGV[4] = tokens requested for this call (always 1 per HTTP request here)
// ARGV[5] = key TTL in seconds (bucket is discarded if the client goes idle)
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local bucket = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(bucket[1])
local last_ts = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  last_ts = now
end

-- Refill proportionally to elapsed time since last touch, capped at capacity.
local elapsed_ms = math.max(0, now - last_ts)
local refill_amount = elapsed_ms * (refill_rate / 1000.0)
tokens = math.min(capacity, tokens + refill_amount)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call("HMSET", key, "tokens", tostring(tokens), "ts", tostring(now))
redis.call("EXPIRE", key, ttl)

return { allowed, tostring(tokens) }
`;

export interface TokenBucketOptions {
  /** Max requests admitted in an instantaneous burst. */
  capacity: number;
  /** Sustained requests/second allowed thereafter. */
  refillRatePerSecond: number;
  /** Redis key namespace, so different routes can keep independent buckets. */
  keyPrefix: string;
}

/**
 * Resolves the client identity the bucket is keyed on. Trusts X-Forwarded-For's
 * first hop because this service sits behind the Nginx reverse proxy defined in
 * docker-compose.yml (`proxy_set_header X-Forwarded-For`); falls back to the
 * socket address for direct/local access.
 */
function resolveClientKey(req: Request): string {
  const forwardedFor = req.headers['x-forwarded-for'];
  if (typeof forwardedFor === 'string' && forwardedFor.length > 0) {
    return forwardedFor.split(',')[0].trim();
  }
  return req.socket.remoteAddress || 'unknown';
}

export function createTokenBucketLimiter(options: TokenBucketOptions) {
  const { capacity, refillRatePerSecond, keyPrefix } = options;
  // Idle buckets expire after 2x the time it would take to fully refill from empty,
  // so abandoned client keys don't accumulate in Redis forever.
  const ttlSeconds = Math.max(60, Math.ceil((capacity / refillRatePerSecond) * 2));

  return async function tokenBucketRateLimiter(req: Request, res: Response, next: NextFunction): Promise<void> {
    const clientKey = resolveClientKey(req);
    const redisKey = `ratelimit:${keyPrefix}:${clientKey}`;

    try {
      const result = (await redisClient.eval(
        TOKEN_BUCKET_LUA,
        1,
        redisKey,
        capacity,
        refillRatePerSecond,
        Date.now(),
        1,
        ttlSeconds
      )) as [number, string];

      const [allowed, tokensRemaining] = result;

      res.setHeader('X-RateLimit-Limit', capacity.toString());
      res.setHeader('X-RateLimit-Remaining', Math.floor(parseFloat(tokensRemaining)).toString());

      if (allowed === 1) {
        next();
        return;
      }

      // Time until at least one token is available again.
      const retryAfterSeconds = Math.max(1, Math.ceil((1 - parseFloat(tokensRemaining)) / refillRatePerSecond));
      res.setHeader('Retry-After', retryAfterSeconds.toString());
      res.status(429).json({
        error: 'Rate limit exceeded. Too many requests submitted in this interval.',
        retryAfterSeconds,
      });
    } catch (error) {
      // Fail-open: if Redis is unreachable, the rate limiter must not become a
      // single point of failure that takes the entire ingress path down with it.
      console.error('[Rate Limiter Failure] Redis unavailable, failing open for this request:', error);
      next();
    }
  };
}
