import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const acceptedOrders = new Counter('accepted_orders');
const events = JSON.parse(__ENV.EVENT_IDS || '[]');
if (!__ENV.BASE_URL || !__ENV.RUN_ID || events.length === 0) {
  throw new Error('BASE_URL, RUN_ID and a nonempty EVENT_IDS JSON array are required');
}

export const options = {
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate: 8,
      timeUnit: '1s',
      duration: '120s',
      preAllocatedVUs: 16,
      maxVUs: 64,
      gracefulStop: '30s',
    },
  },
  thresholds: {
    checks: ['rate==1'],
    http_req_failed: ['rate==0'],
    dropped_iterations: ['count==0'],
    accepted_orders: ['count>0'],
  },
};

export default function () {
  const iteration = exec.scenario.iterationInTest;
  const key = `${__ENV.RUN_ID}-${iteration}`;
  const body = JSON.stringify({
    eventId: events[iteration % events.length],
    customerId: key,
    quantity: 1,
  });
  const params = {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    timeout: '10s',
  };
  // Lost replies retry the identical order, never a fresh customer or key.
  for (let attempt = 0; attempt < 3; attempt++) {
    const response = http.post(`${__ENV.BASE_URL}/orders`, body, params);
    if (response.status === 202 && response.json('orderId')) {
      acceptedOrders.add(1);
      check(response, { 'order acknowledged': () => true });
      return;
    }
    sleep(0.2);
  }
  check(null, { 'order acknowledged': () => false });
}
