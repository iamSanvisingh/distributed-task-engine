import { ConnectionOptions } from 'bullmq';
import Redis from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Enterprise Redis Connection Configuration.
 * Conforms strictly to BullMQ requirements regarding blocking commands.
 */
export const redisConnectionOptions: ConnectionOptions = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  /**
   * CRITICAL ARCHITECTURAL REQUIREMENT:
   * BullMQ workers utilize blocking commands (e.g., BRPOPLPUSH/BLMOVE) to poll for work.
   * Setting maxRetriesPerRequest to null prevents the underlying ioredis driver from
   * throwing a connection timeout exception when a worker is waiting for a job.
   */
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

/**
 * Shared, general-purpose ioredis client for cross-cutting infrastructure concerns
 * that are NOT BullMQ's blocking job-polling connections: the token-bucket rate
 * limiter (EVALSHA/Lua) and the idempotency result cache (GET/SET NX).
 *
 * Deliberately kept separate from BullMQ's internal connections — BullMQ manages
 * its own connection lifecycle per Queue/Worker instance, and mixing blocking
 * commands with ad-hoc GET/SET traffic on the same client is an anti-pattern
 * that can starve job polling under load.
 */
export const redisClient = new Redis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379', 10),
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

redisClient.on('error', (err) => {
  console.error('[Redis Client Error] Shared infrastructure client encountered an error:', err.message);
});
