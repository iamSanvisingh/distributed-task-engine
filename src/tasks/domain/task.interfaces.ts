/**
 * Strict Domain Model interfaces representing the Task state bounds.
 */
export type TaskType = 'HEAVY_DATA_COMPUTATION' | 'BULK_NOTIFICATION' | 'RECONCILIATION';

export interface ITaskIngressPayload {
  taskType: TaskType;
  metadata: Record<string, any>;
  /**
   * Optional client-supplied idempotency key (see `Idempotency-Key` header).
   * Carried through onto the job payload so the worker can write the
   * idempotency cache entry once processing completes.
   */
  idempotencyKey?: string;
}

export interface ITaskStatusResponse {
  jobId: string;
  status: 'waiting' | 'active' | 'completed' | 'failed' | 'delayed' | 'unknown';
  progress: number | Record<string, any>;
}

/**
 * Shape of a job payload once it has exhausted all retry attempts and is
 * routed to the Dead Letter Queue for isolated, manual inspection.
 */
export interface IDeadLetterEntry {
  originalJobId: string;
  queueName: string;
  payload: ITaskIngressPayload;
  attemptsMade: number;
  failedReason: string;
  stacktrace: string[];
  failedAt: number;
}
