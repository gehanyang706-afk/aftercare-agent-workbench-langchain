import test from 'node:test';
import assert from 'node:assert/strict';
import { createRefreshQueue } from '../../src/aftercare/web/static/refresh_queue.js';

const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};
const tick = async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); };
function fixture() {
  const timers = new Map(), reads = [], rendered = [];
  let timerId = 0;
  const queue = createRefreshQueue({
    read: (key, signal) => { const read = { key, signal, ...deferred() }; reads.push(read); return read.promise; },
    apply: (snapshot, key) => rendered.push({ snapshot, key }),
    setTimer: (fn, ms) => { const id = ++timerId; timers.set(id, { fn, ms }); return id; },
    clearTimer: id => timers.delete(id),
  });
  return { queue, reads, rendered, timers,
    fire(ms) { for (const [id, item] of [...timers]) if (item.ms === ms) { timers.delete(id); item.fn(); } },
  };
}

test('200 simultaneous event reads coalesce into one complete snapshot', async () => {
  const f = fixture(), requests = Array.from({ length: 200 }, () => f.queue.request('A', { immediate: false }));
  assert.equal(f.reads.length, 0); f.fire(60); await tick(); assert.equal(f.reads.length, 1);
  const value = { messages: ['kept'], previews: ['kept'], operations: ['kept'] };
  f.reads[0].resolve(value); assert((await Promise.all(requests)).every(row => row === value));
  assert.deepEqual(f.rendered, [{ snapshot: value, key: 'A' }]);
});
test('a post-write refresh waits for a fresh tail instead of the already running read', async () => {
  const f = fixture(), first = f.queue.request('A'); await tick();
  let finished = false;
  const command = f.queue.request('A').then(value => { finished = true; return value; });
  const events = Array.from({ length: 100 }, () => f.queue.request('A', { immediate: false }));
  f.reads[0].resolve({ revision: 1 }); await first; await tick();
  assert.equal(finished, false); assert.equal(f.reads.length, 2);
  f.reads[1].resolve({ revision: 2 }); assert.deepEqual(await command, { revision: 2 }); await Promise.all(events);
  assert.equal(f.reads.length, 2);
});
test('conversation switch aborts old work and never renders its late response', async () => {
  const f = fixture(), old = f.queue.request('A'); await tick();
  f.queue.reset('B'); assert.equal(f.reads[0].signal.aborted, true); assert.equal(await old, undefined);
  const current = f.queue.request('B'); f.reads[0].resolve({ stale: true }); await tick();
  assert.equal(f.reads.length, 2); assert.deepEqual(f.rendered, []);
  f.reads[1].resolve({ fresh: true }); await current;
  assert.deepEqual(f.rendered, [{ key: 'B', snapshot: { fresh: true } }]);
});
test('leaving and returning to the same conversation still invalidates the old epoch', async () => {
  const f = fixture(), old = f.queue.request('A'); await tick(); f.queue.reset('B');
  const latest = f.queue.request('A'); f.reads[0].resolve({ stale: true }); await old; await tick();
  assert.deepEqual(f.rendered, []); f.reads[1].resolve({ latest: true }); await latest;
  assert.deepEqual(f.rendered, [{ key: 'A', snapshot: { latest: true } }]);
});
test('reset cancels a pending debounce without sending its request', async () => {
  const f = fixture(), old = f.queue.request('A', { immediate: false }); f.queue.reset('B');
  assert.equal(await old, undefined); f.fire(60); await tick(); assert.equal(f.reads.length, 0);
});
test('explicit refresh flushes the pending event debounce without an extra read', async () => {
  const f = fixture(), event = f.queue.request('A', { immediate: false }), command = f.queue.request('A');
  await tick(); f.fire(60); assert.equal(f.reads.length, 1); f.reads[0].resolve({ fresh: true });
  await Promise.all([event, command]); assert.equal(f.reads.length, 1);
});
test('read failure rejects current callers and permits the queued fresh read to finish', async () => {
  const f = fixture(), failed = f.queue.request('A'); const rejected = assert.rejects(failed, /network/);
  await tick(); const later = f.queue.request('A'); f.reads[0].reject(Error('network')); await rejected; await tick();
  f.reads[1].resolve({ recovered: true }); assert.deepEqual(await later, { recovered: true });
});
test('timeout aborts a hanging read and frees the queue for the pending request', async () => {
  const f = fixture(), failed = f.queue.request('A'); const rejected = assert.rejects(failed, /读取状态超时/);
  await tick(); const later = f.queue.request('A'); f.fire(10000); await rejected; await tick();
  assert.equal(f.reads[0].signal.aborted, true); assert.equal(f.reads.length, 2);
  f.reads[1].resolve({ fresh: true }); await later; f.reads[0].resolve({ stale: true }); await tick();
  assert.deepEqual(f.rendered, [{ key: 'A', snapshot: { fresh: true } }]);
});
test('an empty conversation never sends a read', async () => {
  const f = fixture(); assert.equal(await f.queue.request(''), undefined); assert.equal(f.reads.length, 0);
});
