/**
 * ============================================================================
 * Distributed Task Worker — Ingress Latency Stress Test
 * ============================================================================
 * Uses the Autocannon engine's programmatic API to orchestrate a synthetic
 * load surge against the POST /api/tasks/submit-task ingress channel and
 * asserts that the gateway upholds its sub-architectural latency budget.
 *
 * Load profile:
 *   - 100 concurrent open connections
 *   - Sustained continuously for a 10 second window
 *   - Every request submits a HEAVY_DATA_COMPUTATION task payload
 *
 * NOTE ON PERCENTILE SELECTION:
 * Autocannon's histogram output exposes a fixed set of percentile buckets
 * (p2_5, p50, p75, p90, p97_5, p99, p99_9, p99_99, p99_999) and does not
 * expose an exact p95 bucket. We use `p97_5` as the gating metric — it is
 * the closest available bucket at or above the 95th percentile, which makes
 * the sub-10.00ms assertion strictly *more* conservative than a true p95
 * check would be.
 *
 * Run with: npm run test:load   (server must already be running on :5000)
 * ============================================================================
 */

'use strict';

const autocannon = require('autocannon');

const TARGET_URL = 'http://localhost:5000/api/tasks/submit-task';
const CONNECTIONS = 100;
const DURATION_SECONDS = 10;
const LATENCY_BUDGET_MS = 10.0;

const REQUEST_PAYLOAD = JSON.stringify({
  taskType: 'HEAVY_DATA_COMPUTATION',
  metadata: { environment: 'load-test' },
});

function printBanner(title) {
  console.log('\n' + '='.repeat(70));
  console.log(title);
  console.log('='.repeat(70));
}

function runLoadTest() {
  printBanner('[Load Test] Initializing synthetic ingress load surge');
  console.log(`[Load Test] Target:            ${TARGET_URL}`);
  console.log(`[Load Test] Connections:       ${CONNECTIONS}`);
  console.log(`[Load Test] Duration:          ${DURATION_SECONDS}s`);
  console.log(`[Load Test] Latency budget:    p97_5 <= ${LATENCY_BUDGET_MS.toFixed(2)}ms\n`);

  const instance = autocannon(
    {
      url: TARGET_URL,
      connections: CONNECTIONS,
      duration: DURATION_SECONDS,
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: REQUEST_PAYLOAD,
    },
    (err, result) => {
      if (err) {
        console.error('[Load Test] Fatal execution error:', err);
        process.exit(1);
        return;
      }
      evaluateResult(result);
    }
  );

  // Live progress ticker to stdout while the surge is running
  autocannon.track(instance, { renderProgressBar: true });
}

function evaluateResult(result) {
  const { latency, requests, throughput, errors, timeouts, non2xx } = result;

  printBanner('[Load Test] Run complete — evaluating telemetry');
  console.log(`[Telemetry] Total requests:        ${requests.total}`);
  console.log(`[Telemetry] Requests/sec (avg):     ${requests.average.toFixed(2)}`);
  console.log(`[Telemetry] Throughput (avg B/s):   ${throughput.average.toFixed(2)}`);
  console.log(`[Telemetry] Errors:                 ${errors}`);
  console.log(`[Telemetry] Timeouts:               ${timeouts}`);
  console.log(`[Telemetry] Non-2xx responses:      ${non2xx}`);
  console.log('');
  console.log(`[Telemetry] Latency p50:            ${latency.p50.toFixed(3)}ms`);
  console.log(`[Telemetry] Latency p90:            ${latency.p90.toFixed(3)}ms`);
  console.log(`[Telemetry] Latency p97_5 (gate):    ${latency.p97_5.toFixed(3)}ms`);
  console.log(`[Telemetry] Latency p99:            ${latency.p99.toFixed(3)}ms`);
  console.log(`[Telemetry] Latency max:            ${latency.max.toFixed(3)}ms`);

  printBanner('[Validation] Architectural latency budget check');

  const gateLatency = latency.p97_5;
  const passed = gateLatency <= LATENCY_BUDGET_MS;

  if (passed) {
    console.log(`[Validation] PASS — p97_5 latency ${gateLatency.toFixed(3)}ms is within the ${LATENCY_BUDGET_MS.toFixed(2)}ms budget.`);
    console.log('[Validation] Ingress gateway sustains sub-latency bounds under 100-connection concurrent load.\n');
    process.exit(0);
  } else {
    console.error(`[Validation] FAIL — p97_5 latency ${gateLatency.toFixed(3)}ms exceeds the ${LATENCY_BUDGET_MS.toFixed(2)}ms budget.`);
    console.error('[Validation] Ingress gateway violated its sub-latency architectural design parameter.\n');
    process.exit(1);
  }
}

runLoadTest();
