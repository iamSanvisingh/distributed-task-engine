import { Worker, Job } from 'bullmq';
import { redisConnectionOptions } from '../../shared/config/redis.config';
import { ITaskIngressPayload, IDeadLetterEntry } from '../domain/task.interfaces';
import { TaskQueueEngine } from './task.queue';
import { idempotencyStore } from './idempotency.store';
import { bullmqActiveJobsGauge } from '../../telemetry/metrics.middleware';

export class TaskWorkerEngine {
  private worker: Worker;
  private readonly QUEUE_NAME = 'heavy-computations-queue';
  private queueEngine: TaskQueueEngine;

  constructor() {
    // Reused to reach the DLQ Queue instance — this worker never touches the
    // healthy queue's `add`, only the DLQ's, keeping the failure-isolation
    // path one-directional (main queue -> DLQ, never the reverse).
    this.queueEngine = new TaskQueueEngine();

    this.worker = new Worker(
      this.QUEUE_NAME,
      async (job: Job<ITaskIngressPayload>) => {
        bullmqActiveJobsGauge.inc({ queue: this.QUEUE_NAME });
        try {
          const result = await this.simulateHeavyComputation(job);

          // Idempotency write-back: only now — after the heavy computation has
          // actually finished — do we persist the result under the client's
          // idempotency key. A duplicate submission that arrives before this
          // point instead finds a PROCESSING record (see task.controller.ts)
          // and is told to poll the existing job rather than starting a
          // second race, so the cache is always consistent with real work done.
          if (job.data.idempotencyKey) {
            await idempotencyStore.markCompleted(job.data.idempotencyKey, job.id!, result);
          }

          return result;
        } finally {
          bullmqActiveJobsGauge.dec({ queue: this.QUEUE_NAME });
        }
      },
      {
        connection: redisConnectionOptions,
        concurrency: 5, // Strict capping upper resource bound to mitigate thread starvation spikes
      }
    );
    this.initializeLifecycleListeners();
  }

  private async simulateHeavyComputation(job: Job<ITaskIngressPayload>): Promise<Record<string, any>> {
    console.log(`[Worker Process ${process.pid}] Starting computational job execution layout ${job.id}`);

    // Simulate complex transactional logic processing loop
    await new Promise((resolve) => setTimeout(resolve, 3000));

    console.log(`[Worker Process ${process.pid}] Finished job execution loop ${job.id}`);

    return {
      jobId: job.id,
      taskType: job.data.taskType,
      completedAt: new Date().toISOString(),
    };
  }

  private initializeLifecycleListeners(): void {
    this.worker.on('completed', (job: Job) => {
      console.log(`[Telemetry Alert] Job ID ${job.id} has successfully marked status: COMPLETED.`);
    });

    this.worker.on('failed', async (job: Job<ITaskIngressPayload> | undefined, error: Error) => {
      if (!job) {
        console.error(`[Telemetry Alert] A job failed with no job reference attached. Reason: ${error.message}`);
        return;
      }

      const attemptsMade = job.attemptsMade;
      const maxAttempts = job.opts.attempts ?? 1;
      const isFinalAttempt = attemptsMade >= maxAttempts;

      console.error(
        `[Telemetry Alert] Job ID ${job.id} attempt ${attemptsMade}/${maxAttempts} FAILED. Reason: ${error.message}`
      );

      if (!isFinalAttempt) {
        // BullMQ has already scheduled the next attempt per the exponential
        // backoff policy configured in task.queue.ts (2s / 4s / 8s) — nothing
        // further to do here until the retries are exhausted.
        return;
      }

      // All retries exhausted: quarantine into the DLQ with full error context
      // rather than letting BullMQ silently drop it into its own failed set.
      const dlqEntry: IDeadLetterEntry = {
        originalJobId: job.id!,
        queueName: this.QUEUE_NAME,
        payload: job.data,
        attemptsMade,
        failedReason: error.message,
        stacktrace: (error.stack || '').split('\n'),
        failedAt: Date.now(),
      };
      await this.queueEngine.routeToDeadLetterQueue(dlqEntry);

      if (job.data.idempotencyKey) {
        await idempotencyStore.markFailed(job.data.idempotencyKey, job.id!, error.message);
      }

      console.error(
        `[DLQ] Job ID ${job.id} permanently failed after ${attemptsMade} attempts and has been routed to the Dead Letter Queue.`
      );
    });
  }
}
