import purchase, { options as baseline } from './autoscale-load.js';

// Allocate upfront: creating VUs during a pod restart can itself drop arrivals on a busy laptop.
// The M5 request flow, 8/s arrival rate and strict thresholds stay unchanged.
// Incremental ramp phases need 360s; other M6 experiments retain the M5 120s duration.
export const options = {
  ...baseline,
  scenarios: { orders: { ...baseline.scenarios.orders, preAllocatedVUs: 64,
    duration: __ENV.M6_EXPERIMENT === 'consumer-ramp' ? '360s' : '120s' } },
};
export default purchase;
