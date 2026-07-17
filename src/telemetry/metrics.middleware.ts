import { Request, Response, NextFunction } from 'express';
import client from 'prom-client';

/**
 * Prometheus Observability Layer.
 *
 * A dedicated Registry (rather than the global `client.register` default) is
 * used so this module has one clearly-owned source of truth for every metric
 * this service exposes, and so unit tests can spin up isolated registries
 * without cross-contaminating global state.
 */
export const register = new client.Registry();

// Default runtime metrics: process memory (RSS/heap), CPU usage, event loop
// lag, active handles/requests, GC duration. This is what satisfies the
// "memory usage" requirement — process.memoryUsage() sampled on an interval
// and exposed as process_resident_memory_bytes / nodejs_heap_size_*_bytes.
client.collectDefaultMetrics({
  register,
  prefix: 'taskworker_',
});

/**
 * HTTP request latency, the primary "is the API healthy" signal.
 * Labeled by method/route/status_code so Grafana can slice p50/p95/p99 per
 * endpoint and immediately spot e.g. submit-task degrading under load while
 * status/:id stays fast.
 */
export const httpRequestDuration = new client.Histogram({
  name: 'taskworker_http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds, labeled by method/route/status_code',
  labelNames: ['method', 'route', 'status_code'],
  // Bucket boundaries tuned for a service whose fast path (202 Accepted on
  // submit) should resolve in single-digit milliseconds, while still leaving
  // room to observe slow outliers.
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [register],
});

export const httpRequestsTotal = new client.Counter({
  name: 'taskworker_http_requests_total',
  help: 'Total count of HTTP requests, labeled by method/route/status_code',
  labelNames: ['method', 'route', 'status_code'],
  registers: [register],
});

/**
 * Live gauge of jobs currently being processed by THIS worker process
 * (incremented/decremented directly in the job processor in task.worker.ts).
 * Complements bullmqJobCountGauge below, which reflects the cluster-wide
 * view from Redis rather than a single replica's in-flight count.
 */
export const bullmqActiveJobsGauge = new client.Gauge({
  name: 'taskworker_bullmq_active_jobs',
  help: 'Number of jobs currently being processed by this worker process',
  labelNames: ['queue'],
  registers: [register],
});

/**
 * Cluster-wide job counts per BullMQ lifecycle state (waiting/active/
 * completed/failed/delayed), sampled from Redis every 5s in task.queue.ts.
 * This is the metric the Grafana "queue throughput" panel graphs.
 */
export const bullmqJobCountGauge = new client.Gauge({
  name: 'taskworker_bullmq_job_count',
  help: 'BullMQ job counts by queue and lifecycle state, sampled from Redis',
  labelNames: ['queue', 'state'],
  registers: [register],
});

/**
 * Express middleware: wraps every request in a latency timer and increments
 * the request counter on response finish. Uses req.route.path (the matched
 * route pattern, e.g. "/status/:id") rather than req.originalUrl so that
 * distinct job IDs don't explode the metric into unbounded cardinality —
 * a classic Prometheus footgun.
 */
export function metricsInterceptor(req: Request, res: Response, next: NextFunction): void {
  const endTimer = httpRequestDuration.startTimer();

  res.on('finish', () => {
    const route = resolveRouteLabel(req);
    const labels = { method: req.method, route, status_code: res.statusCode.toString() };
    endTimer(labels);
    httpRequestsTotal.inc(labels);
  });

  next();
}

function resolveRouteLabel(req: Request): string {
  const matchedPath = (req as Request & { route?: { path?: string } }).route?.path;
  if (matchedPath) {
    return `${req.baseUrl}${matchedPath}`;
  }
  // Unmatched routes (404s) fall back to a fixed label instead of the raw
  // URL, for the same cardinality-control reason as above.
  return req.baseUrl || 'unmatched';
}

/**
 * GET /metrics — the Prometheus scrape target. Content-Type MUST be set from
 * register.contentType (includes the exposition format version) or
 * Prometheus's scraper will reject the payload as malformed.
 */
export async function metricsEndpoint(_req: Request, res: Response): Promise<void> {
  res.set('Content-Type', register.contentType);
  res.end(await register.metrics());
}
