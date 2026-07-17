import { Queue, Job } from 'bullmq';
import { redisConnectionOptions } from '../../shared/config/redis.config';
import { ITaskIngressPayload, IDeadLetterEntry } from '../domain/task.interfaces';
import { QueueOperationError } from '../../shared/errors/app.errors';
import { bullmqJobCountGauge } from '../../telemetry/metrics.middleware';

export class TaskQueueEngine {
  private queue: Queue;
  private deadLetterQueue: Queue;
  private readonly QUEUE_NAME = 'heavy-computations-queue';
  private readonly DLQ_NAME = 'heavy-computations-dlq';
  private metricsPollHandle?: NodeJS.Timeout;

  constructor() {
    this.queue = new Queue(this.QUEUE_NAME, { connection: redisConnectionOptions });

    // Isolated queue: a separate BullMQ Queue (backed by its own Redis keyspace,
    // not merely a status flag on the original job) so poisoned/failed payloads
    // are physically quarantined from the healthy work queue. This means an
    // operator can inspect, replay, or purge the DLQ without any risk of a
    // corrupt payload being picked back up by a live worker's normal polling.
    this.deadLetterQueue = new Queue(this.DLQ_NAME, { connection: redisConnectionOptions });

    this.startJobCountMetricsPolling();
  }

  /**
   * Marshals the domain payload into an atomic memory entry in the Redis cluster.
   */
  public async addJobToQueue(payload: ITaskIngressPayload): Promise<Job> {
    try {
      return await this.queue.add('process-task', payload, {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000, // Scales by 2^n. Retry 1: 2s, Retry 2: 4s, Retry 3: 8s
        },
        removeOnComplete: { age: 3600 }, // Retention policies prevent memory saturation
        removeOnFail: { age: 86400 },
      });
    } catch (error) {
      console.error(`[Queue Infrastructure Failure] Failed pushing job:`, error);
      throw new QueueOperationError('Failed to safely submit task execution parameter to broker.');
    }
  }

  public async getJobStatus(jobId: string): Promise<Job | null> {
    try {
      const job = await this.queue.getJob(jobId);
      return job ?? null;
    } catch (error) {
      console.error(`[Queue Read Failure] Error fetching status for job ${jobId}:`, error);
      throw new QueueOperationError('Failed to query broker state memory store.');
    }
  }

  /**
   * Routes a permanently-failed job (all retry attempts exhausted) to the DLQ
   * for isolated storage of its payload + full error context. Called from the
   * worker's 'failed' listener — never from the request/response path.
   */
  public async routeToDeadLetterQueue(entry: IDeadLetterEntry): Promise<void> {
    try {
      await this.deadLetterQueue.add('dead-letter', entry, {
        // DLQ entries are terminal by definition — no retries, kept for a long
        // audit/inspection window rather than the short healthy-queue TTL.
        attempts: 1,
        removeOnComplete: false,
        removeOnFail: false,
      });
    } catch (error) {
      // If we can't even reach the DLQ, log loudly — this is the last line of
      // defense before the failure context is lost entirely.
      console.error(`[DLQ Routing Failure] Could not quarantine job ${entry.originalJobId}:`, error);
    }
  }

  public getQueue(): Queue {
    return this.queue;
  }

  public getDeadLetterQueue(): Queue {
    return this.deadLetterQueue;
  }

  /**
   * Periodically samples BullMQ's own job-count accounting (waiting/active/
   * completed/failed/delayed) into the Prometheus gauge, for both the main
   * queue and the DLQ. Polling — rather than incrementing counters on every
   * lifecycle event — is used because BullMQ's counts are already the source
   * of truth and are consistent across every horizontally-scaled replica
   * (they live in Redis), so there's no risk of per-process counters drifting.
   */
  private startJobCountMetricsPolling(): void {
    const sample = async () => {
      try {
        const [mainCounts, dlqCounts] = await Promise.all([
          this.queue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed'),
          this.deadLetterQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed'),
        ]);

        for (const [state, count] of Object.entries(mainCounts)) {
          bullmqJobCountGauge.set({ queue: this.QUEUE_NAME, state }, count as number);
        }
        for (const [state, count] of Object.entries(dlqCounts)) {
          bullmqJobCountGauge.set({ queue: this.DLQ_NAME, state }, count as number);
        }
      } catch (error) {
        console.error('[Metrics Polling Failure] Could not sample BullMQ job counts:', error);
      }
    };

    this.metricsPollHandle = setInterval(sample, 5000);
    // Don't let the polling interval keep the Node process alive on its own.
    this.metricsPollHandle.unref?.();
    void sample();
  }
}
