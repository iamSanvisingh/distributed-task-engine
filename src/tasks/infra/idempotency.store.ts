import { redisClient } from '../../shared/config/redis.config';

/**
 * Idempotency Cache.
 *
 * Guarantees "exactly-once effect" semantics for POST /submit-task under
 * at-least-once network conditions (client retries after a timeout, a proxy
 * replays a request, a double-click, etc.) by keying a result cache off a
 * client-supplied idempotency key.
 *
 * State machine per key, stored as a single Redis STRING (JSON-encoded):
 *   (absent) --SETNX--> PROCESSING --worker completes/fails--> COMPLETED | FAILED
 *
 * The SETNX (SET ... NX) on claim() is the linchpin: it is atomic, so if two
 * requests race with the same key, exactly one of them wins the claim and
 * proceeds to enqueue a job; the loser is told to look at the winner's outcome.
 */

export type IdempotencyStatus = 'PROCESSING' | 'COMPLETED' | 'FAILED';

export interface IdempotencyRecord {
  status: IdempotencyStatus;
  jobId: string;
  result?: Record<string, any>;
  error?: string;
  createdAt: number;
}

const KEY_PREFIX = 'idempotency:';
// Cached long enough to absorb realistic client retry/backoff windows without
// keeping every historical task result in Redis indefinitely.
const RECORD_TTL_SECONDS = 24 * 60 * 60;

function keyFor(idempotencyKey: string): string {
  return `${KEY_PREFIX}${idempotencyKey}`;
}

export class IdempotencyStore {
  /**
   * Attempts to atomically claim an idempotency key for a brand-new job.
   * Returns { claimed: true } if this caller owns the key and should proceed
   * to enqueue work. Returns { claimed: false, record } if the key already
   * exists — the caller must NOT re-enqueue and should instead branch on the
   * existing record's status.
   */
  public async claim(idempotencyKey: string, jobId: string): Promise<
    { claimed: true } | { claimed: false; record: IdempotencyRecord }
  > {
    const record: IdempotencyRecord = {
      status: 'PROCESSING',
      jobId,
      createdAt: Date.now(),
    };

    // NX = only set if not already present. This is the atomic compare-and-set
    // that prevents duplicate submissions from racing into two separate jobs.
    const setResult = await redisClient.set(
      keyFor(idempotencyKey),
      JSON.stringify(record),
      'EX',
      RECORD_TTL_SECONDS,
      'NX'
    );

    if (setResult === 'OK') {
      return { claimed: true };
    }

    const existing = await this.get(idempotencyKey);
    // Extremely narrow race: key expired between the failed NX and this GET.
    // Treat as claimable rather than surfacing a false "in progress" state.
    if (!existing) {
      return { claimed: true };
    }
    return { claimed: false, record: existing };
  }

  /**
   * Overwrites the PROCESSING record's placeholder jobId with the real BullMQ
   * job id once it's known. Unlike claim(), this is a plain SET (no NX) since
   * the caller already legitimately owns this key from a prior successful claim.
   */
  public async updateJobId(idempotencyKey: string, jobId: string): Promise<void> {
    const existing = await this.get(idempotencyKey);
    if (!existing) return; // Key expired/vanished between claim and this call — nothing to update.
    existing.jobId = jobId;
    await redisClient.set(keyFor(idempotencyKey), JSON.stringify(existing), 'EX', RECORD_TTL_SECONDS, 'XX');
  }

  public async get(idempotencyKey: string): Promise<IdempotencyRecord | null> {
    const raw = await redisClient.get(keyFor(idempotencyKey));
    if (!raw) return null;
    try {
      return JSON.parse(raw) as IdempotencyRecord;
    } catch {
      return null;
    }
  }

  /**
   * Called by the worker once a job reaches a terminal state, so subsequent
   * duplicate submissions of the same key short-circuit to the cached outcome
   * instead of re-running the heavy computation.
   */
  public async markCompleted(idempotencyKey: string, jobId: string, result: Record<string, any>): Promise<void> {
    const record: IdempotencyRecord = {
      status: 'COMPLETED',
      jobId,
      result,
      createdAt: Date.now(),
    };
    await redisClient.set(keyFor(idempotencyKey), JSON.stringify(record), 'EX', RECORD_TTL_SECONDS);
  }

  public async markFailed(idempotencyKey: string, jobId: string, error: string): Promise<void> {
    const record: IdempotencyRecord = {
      status: 'FAILED',
      jobId,
      error,
      createdAt: Date.now(),
    };
    // Failed records are cached for a much shorter window: a permanent
    // downstream failure shouldn't hard-block a client from retrying with the
    // same key forever once the underlying issue is fixed.
    await redisClient.set(keyFor(idempotencyKey), JSON.stringify(record), 'EX', 600);
  }
}

export const idempotencyStore = new IdempotencyStore();
