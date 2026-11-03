// Coalesce reads within a conversation; requests arriving in flight need a fresh tail read.
export function createRefreshQueue({ read, apply, delay = 60, timeout = 10000,
  setTimer = setTimeout, clearTimer = clearTimeout }) {
  let key = '', epoch = 0, active = null, timer = null, pending = [];
  const cancelTimer = () => { if (timer !== null) clearTimer(timer); timer = null; };
  const settle = (waiters, value) => waiters.forEach(waiter => waiter.resolve(value));
  function reset(next = '') {
    epoch++; key = next; cancelTimer();
    settle(pending, undefined); pending = [];
    if (active) { active.controller.abort(); settle(active.waiters, undefined); }
  }
  function start() {
    if (active || !pending.length) return;
    cancelTimer();
    const run = { key, epoch, waiters: pending, controller: new AbortController() };
    pending = []; active = run;
    void (async () => {
      let deadline;
      try {
        const expires = new Promise((_, reject) => {
          deadline = setTimer(() => {
            run.controller.abort(); reject(Error('读取状态超时，请重试。'));
          }, timeout);
        });
        const snapshot = await Promise.race([
          Promise.resolve().then(() => read(run.key, run.controller.signal)), expires,
        ]);
        if (run.epoch === epoch && run.key === key) {
          apply(snapshot, run.key); settle(run.waiters, snapshot);
        } else settle(run.waiters, undefined);
      } catch (error) {
        if (run.epoch !== epoch || run.controller.signal.aborted && error.name === 'AbortError') {
          settle(run.waiters, undefined);
        } else run.waiters.forEach(waiter => waiter.reject(error));
      } finally {
        clearTimer(deadline);
        if (active === run) { active = null; if (pending.length) start(); }
      }
    })();
  }
  function request(next, { immediate = true } = {}) {
    if (!next) return Promise.resolve(undefined);
    if (next !== key) reset(next);
    const result = new Promise((resolve, reject) => pending.push({ resolve, reject }));
    if (!active) {
      if (immediate) start();
      else if (timer === null) timer = setTimer(() => { timer = null; start(); }, delay);
    }
    return result;
  }
  return { request, reset };
}
