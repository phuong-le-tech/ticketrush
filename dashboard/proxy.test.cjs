const assert = require('node:assert/strict');
const { test } = require('node:test');
const {
  loadProxyConfiguration,
} = require('./node_modules/@angular/build/src/utils/load-proxy-config.js');

test('AC7 dev proxy isolates service paths and preserves query parameters', async () => {
  const config = await loadProxyConfiguration(__dirname, 'proxy.conf.json');
  // Vite uses the first matching entry and treats non-regex keys as prefixes.
  function route(url) {
    const match = Object.entries(config).find(([key]) =>
      key.startsWith('^') ? new RegExp(key).test(url) : url.startsWith(key),
    );
    return match ? { target: match[1].target, path: match[1].rewrite(url) } : null;
  }
  assert.deepEqual(route('/api/orders/activity?limit=20'), {
    target: 'http://localhost:8084',
    path: '/orders/activity?limit=20',
  });
  assert.deepEqual(route('/api/orders'), {
    target: 'http://localhost:8080',
    path: '/orders',
  });
  assert.deepEqual(route('/api/events/concert-1/stock'), {
    target: 'http://localhost:8081',
    path: '/events/concert-1/stock',
  });
  assert.deepEqual(route('/api/metrics/query?query=up'), {
    target: 'http://localhost:9090',
    path: '/api/v1/query?query=up',
  });
  assert.deepEqual(route('/api/metrics/query_range?query=up&step=5'), {
    target: 'http://localhost:9090',
    path: '/api/v1/query_range?query=up&step=5',
  });
  for (const path of ['/api/metrics', '/api/metrics/', '/api/metrics/admin/tsdb', '/api/metrics/querymore']) {
    assert.equal(route(path), null, path);
  }
  for (const path of [
    '/actuator/prometheus',
    '/api/actuator',
    '/api/orders/private',
    '/api/orders/activity/private',
  ]) {
    assert.equal(route(path), null, path);
  }
});
