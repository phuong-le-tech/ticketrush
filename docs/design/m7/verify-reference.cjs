const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const html = fs.readFileSync(process.argv[2], 'utf8');
assert(!/[\u2013\u2014]/u.test(html));
assert(!/fetch\(|XMLHttpRequest|customerId|idempotencyKey/.test(html));
assert(html.includes("connect-src 'none'"));
for (const zone of ['stock-zone', 'throughput-zone', 'buy-zone', 'orders-zone']) {
  assert(html.includes(`data-od-id="${zone}"`));
}
const elements = new Map();
const element = id => {
  if (!elements.has(id)) elements.set(id, {
    value: id === 'quantity' ? '1' : id === 'event-id' ? 'concert-1' : '',
    style: {}, classList: {add() {}, remove() {}},
    setAttribute() {}, reportValidity() {return true;},
    addEventListener(type, fn) {this[type] = fn;}
  });
  return elements.get(id);
};
const timers = [];
const context = vm.createContext({
  document: {getElementById: element},
  setTimeout: fn => timers.push(fn),
  setInterval: (_, delay) => assert.equal(delay, 2000)
});
vm.runInContext(html.split('<script>')[1].split('</script>')[0], context);
assert.equal(element('rate-value').textContent, '3.2');
const submit = () => element('buy-form').submit({preventDefault() {}});
element('event-id').value = 'test-event';
element('quantity').value = '4';
submit();
assert.equal(element('buy-button').disabled, true);
const key = vm.runInContext('attempt.key', context);
submit();
assert.equal(timers.length, 1);
timers.shift()();
assert.equal(element('buy-button').textContent, 'Retry');
assert.equal(element('event-id').value, 'test-event');
assert.equal(element('quantity').value, '4');
submit();
assert.equal(vm.runInContext('attempt.key', context), key);
timers.shift()();
assert.equal(element('buy-button').textContent, 'Buy');
assert.equal(element('buy-button').disabled, false);
assert(element('buy-message').textContent.includes('4 tickets for test-event'));
element('quantity').value = '5';
submit();
assert.equal(timers.length, 0);
element('demo-state').change({target: {value: 'stale'}});
assert(element('freshness-text').textContent.includes('STALE'));
for (const state of ['loading', 'empty', 'error']) {
  element('demo-state').change({target: {value: state}});
  assert.equal(element('table-container').hidden, true);
}
element('demo-state').change({target: {value: 'ready'}});
assert.equal(element('stock-value').textContent, 248);
assert.equal(element('table-container').hidden, false);
console.log('PASS: four zones, no network/private fields/forbidden dashes, rate formula, 2s cadence, pending guard, retry preservation, quantity validation, stale/loading/empty/error states.');
console.log('Scope: isolated demo JavaScript checks, not real browser or backend integration.');
