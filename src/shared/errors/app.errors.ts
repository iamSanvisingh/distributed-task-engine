/**
 * Base Application Error providing strict type-safety across layers.
 */
export abstract class AppError extends Error {
  abstract readonly statusCode: number;

  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class ValidationError extends AppError {
  readonly statusCode = 400;
}

export class TaskNotFoundError extends AppError {
  readonly statusCode = 404;
}

export class QueueOperationError extends AppError {
  readonly statusCode = 500;
}
