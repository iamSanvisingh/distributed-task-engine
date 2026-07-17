import { Request, Response, NextFunction } from 'express';
import { TaskQueueEngine } from '../infra/task.queue';
import { idempotencyStore } from '../infra/idempotency.store';
import { ValidationError, TaskNotFoundError } from '../../shared/errors/app.errors';
import { ITaskStatusResponse } from '../domain/task.interfaces';

/**
 * BullMQ's JobProgress type is intentionally permissive (number | boolean | object | string)
 * since consumers can report progress however they like. The domain contract only exposes
 * a numeric percentage or a structured object, so anything else collapses to 0.
 */
function normalizeProgress(progress: unknown): number | Record<string, any> {
  if (typeof progress === 'number') return progress;
  if (typeof progress === 'object' && progress !== null) return progress as Record<string, any>;
  return 0;
}

/**
 * Resolves the idempotency key from either the dedicated header (preferred —
 * REST convention, keeps it out of the domain payload) or the request body
 * (accepted as a fallback for clients that can't easily set custom headers).
 */
function resolveIdempotencyKey(req: Request): string | undefined {
  const headerKey = req.headers['idempotency-key'];
  if (typeof headerKey === 'string' && headerKey.trim().length > 0) {
    return headerKey.trim();
  }
  if (typeof req.body?.idempotencyKey === 'string' && req.body.idempotencyKey.trim().length > 0) {
    return req.body.idempotencyKey.trim();
  }
  return undefined;
}

export class TaskController {
  private queueEngine: TaskQueueEngine;

  constructor() {
    this.queueEngine = new TaskQueueEngine();
  }

  /**
   * Entry validation and ingestion orchestration point.
   */
  public submitTask = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { taskType, metadata } = req.body;
      if (!taskType) {
        throw new ValidationError('Required field input validation failed: taskType mapping missing.');
      }

      const idempotencyKey = resolveIdempotencyKey(req);

      // No idempotency key supplied: behave exactly as before — every call is
      // a distinct submission. Idempotency is opt-in via the header/body field,
      // not a mandatory contract change on this endpoint.
      if (!idempotencyKey) {
        const job = await this.queueEngine.addJobToQueue({ taskType, metadata: metadata || {} });
        res.status(202).json({
          jobId: job.id,
          message: 'Task successfully parsed and enqueued for out-of-band asynchronous processing.',
        });
        return;
      }

      // Optimistically reserve a placeholder jobId slot in the claim record;
      // it's overwritten with the real BullMQ job id immediately below once
      // the claim succeeds and the job is actually enqueued.
      const claimAttempt = await idempotencyStore.claim(idempotencyKey, 'pending');

      if (!claimAttempt.claimed) {
        const existing = claimAttempt.record;

        if (existing.status === 'COMPLETED') {
          // Network blip / client retry on an already-finished job: return the
          // cached result directly without touching the queue or worker at all.
          res.status(200).json({
            jobId: existing.jobId,
            message: 'Duplicate submission detected for this idempotency key. Returning cached result.',
            idempotent: true,
            result: existing.result,
          });
          return;
        }

        if (existing.status === 'FAILED') {
          res.status(200).json({
            jobId: existing.jobId,
            message: 'Duplicate submission detected for this idempotency key. Prior execution failed permanently.',
            idempotent: true,
            error: existing.error,
          });
          return;
        }

        // PROCESSING: the original request is still in-flight — do not enqueue
        // a second job for the same key, just point the caller at the existing one.
        res.status(202).json({
          jobId: existing.jobId,
          message: 'Duplicate submission detected for this idempotency key. Original task is still processing.',
          idempotent: true,
        });
        return;
      }

      // This request won the claim race — it is the sole owner of this
      // idempotency key and is responsible for doing the real work.
      const job = await this.queueEngine.addJobToQueue({ taskType, metadata: metadata || {}, idempotencyKey });

      // Update the claim record with the real job id now that it exists,
      // so status lookups and future duplicate submissions resolve correctly.
      await idempotencyStore.updateJobId(idempotencyKey, job.id!);

      res.status(202).json({
        jobId: job.id,
        message: 'Task successfully parsed and enqueued for out-of-band asynchronous processing.',
      });
    } catch (error) {
      next(error);
    }
  };

  /**
   * Read polling status evaluation point.
   */
  public getStatus = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const { id } = req.params;
      const job = await this.queueEngine.getJobStatus(id);
      if (!job) {
        throw new TaskNotFoundError(`Task entity identifier key ${id} does not exist inside broker records.`);
      }

      const state = await job.getState();
      const response: ITaskStatusResponse = {
        jobId: job.id!,
        status: state as ITaskStatusResponse['status'],
        progress: normalizeProgress(job.progress),
      };

      res.status(200).json(response);
    } catch (error) {
      next(error);
    }
  };
}
