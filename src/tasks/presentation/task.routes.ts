import { Router } from 'express';
import { TaskController } from './task.controller';
import { createTokenBucketLimiter } from '../../shared/middleware/rate-limiter.middleware';

const router = Router();
const controller = new TaskController();

// Token bucket: 10 request/second sustained rate, per client IP, with a burst
// capacity equal to the same 10 so a single instantaneous spike of up to 10
// requests is still admitted before throttling kicks in. Scoped narrowly to
// this write endpoint (not applied service-wide) since it's the one that
// enqueues real work onto the BullMQ broker and is the actual abuse surface.
const submitTaskRateLimiter = createTokenBucketLimiter({
  capacity: 10,
  refillRatePerSecond: 10,
  keyPrefix: 'submit-task',
});

// Map clear execution ingress channels
router.post('/submit-task', submitTaskRateLimiter, controller.submitTask);
router.get('/status/:id', controller.getStatus);

export const taskRoutes = router;
