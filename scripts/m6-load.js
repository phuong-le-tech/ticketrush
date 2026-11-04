import purchase, { options as baseline } from './m5-load.js';

// Allocate upfront: creating VUs during a pod restart can itself drop arrivals on a busy laptop.
// The M5 request flow, 8/s arrival rate and strict thresholds stay unchanged.
// Sequential ramp phases need 240s; other M6 experiments retain the M5 120s duration.
export const options = {
  ...baseline,
  scenarios: { orders: { ...baseline.scenarios.orders, preAllocatedVUs: 64,
    duration: __ENV.M6_EXPERIMENT === 'consumer-ramp' ? '240s' : '120s' } },
};
export default purchase;
