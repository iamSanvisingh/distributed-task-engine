import express, { Request, Response, NextFunction } from 'express';
import path from 'path';
import { taskRoutes } from './tasks/presentation/task.routes';
import { metricsInterceptor, metricsEndpoint } from './telemetry/metrics.middleware';
import { AppError } from './shared/errors/app.errors';

const app = express();

app.use(express.json());
app.use(metricsInterceptor);

// Prometheus scrape target — deliberately mounted before express.static and
// outside /api so it's a stable, unauthenticated-by-default operational
// endpoint independent of API versioning or asset routing changes.
app.get('/metrics', metricsEndpoint);

// Wire presentation asset delivery tiers
app.use(express.static(path.join(__dirname, '../public')));

app.use('/api/tasks', taskRoutes);

/**
 * Standard Application Central Error Handler Boundary
 */
app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  if (err instanceof AppError) {
    res.status(err.statusCode).json({ error: err.message });
    return;
  }

  console.error('[Unhandled System Core Crash]:', err);
  res.status(500).json({ error: 'Internal system runtime failure encountered.' });
});

export default app;
