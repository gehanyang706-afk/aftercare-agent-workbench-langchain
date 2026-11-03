// Replay the actual trace callback and refresh function with a deterministic fake API.
// This measures JS read invocations and concurrency, not browser HTTP or TTI.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import vm from 'node:vm';
import { pathToFileURL } from 'node:url';

const [mode, sourcePath, outputPath] = process.argv.slice(2);
assert(['before', 'after'].includes(mode));
const root = process.cwd();
assert(path.resolve(outputPath).startsWith(path.join(root, 'runtime') + path.sep));
const sourceText = await readFile(sourcePath, 'utf8');
const part = (start, end) => {
  const left = sourceText.indexOf(start), right = sourceText.indexOf(end, left);
  assert(left >= 0 && right > left, `Missing bounded function marker: ${start}`);
  return sourceText.slice(left, right);
};
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const elements = new Map(), streams = [];
let calls = 0, active = 0, peak = 0, delivered = 0;
const context = vm.createContext({
  console, Map, JSON, Promise, AbortController,
  conversation: 'probe-conversation', source: null, events: new Map(), lastEvent: 0,
  lastSnapshot: null, refreshTicket: 0, labels: {},
  $: key => {
    if (!elements.has(key)) elements.set(key, {
      textContent: '', scrollHeight: 1, replaceChildren() {},
    });
    return elements.get(key);
  },
  node: () => ({}), renderEvents() {}, renderPreview() {},
  scheduleTimelineRender() {}, error: value => { throw value; },
  EventSource: class {
    constructor(url) { this.url = url; this.handlers = {}; streams.push(this); }
    addEventListener(name, handler) { this.handlers[name] = handler; }
    close() { this.closed = true; }
  },
});
context.api = async () => {
  calls++; active++; peak = Math.max(peak, active);
  const revision = delivered;
  try {
    await pause(20);
    return { revision, status: 'WAITING_USER', messages: [], previews: [], operations: [], jobs: [] };
  } finally { active--; }
};
let queueHash = null;
if (mode === 'after') {
  const file = path.join(root, 'src/aftercare/web/static/refresh_queue.js');
  const { createRefreshQueue } = await import(pathToFileURL(file));
  queueHash = createHash('sha256').update(await readFile(file)).digest('hex');
  context.refresher = createRefreshQueue({
    read: key => context.api(`/api/conversations/${key}`),
    apply: (snapshot, key) => {
      if (key === context.conversation) context.renderSnapshot(snapshot);
    },
    delay: 60,
  });
}
vm.runInContext(part('function openEvents(', 'function renderEvents('), context);
vm.runInContext(part('async function refresh(', 'function renderPreview('), context);
vm.runInContext('openEvents()', context);
const old = streams.at(-1);
const emit = id => {
  delivered = id;
  old.handlers.trace({ data: JSON.stringify({ id, event_type: 'tool_result', payload: {} }) });
};
for (let id = 1; id <= 100; id++) emit(id);
await pause(70); // The coalesced first request is in flight when wave two arrives.
for (let id = 101; id <= 200; id++) emit(id);
await pause(130);
assert.equal(active, 0);
assert.equal(context.events.size, 200);
assert.equal(context.lastSnapshot.revision, 200);
const burstCalls = calls, burstPeak = peak;
emit(200);
await pause(90);
assert.equal(calls, burstCalls, 'duplicate event must not refresh');
context.conversation = 'another-conversation';
context.events = new Map();
context.lastEvent = 0;
old.close();
if (mode === 'after') context.refresher.reset(context.conversation);
vm.runInContext('openEvents()', context);
emit(201); // An old source callback was queued before close and is delivered late.
await pause(100);
const ignoredLateOldSource = context.events.size === 0 && calls === burstCalls;
if (mode === 'after') {
  assert.equal(burstCalls, 2);
  assert.equal(burstPeak, 1);
  assert(ignoredLateOldSource);
} else {
  assert.equal(burstCalls, 200);
  assert.equal(burstPeak, 100);
}
const report = {
  status: 'PASSED', mode,
  scope: 'actual trace callback and refresh function; fake API; not browser/TCP/TTI',
  source_sha256: createHash('sha256').update(sourceText).digest('hex'),
  refresh_queue_sha256: queueHash,
  event_count: 200, waves: 2, fake_api_delay_ms: 20,
  read_invocations: burstCalls, peak_concurrent_reads: burstPeak,
  final_snapshot_revision: 200, duplicate_ignored: true,
  late_old_source_ignored: ignoredLateOldSource,
};
await writeFile(outputPath, JSON.stringify(report, null, 2) + '\n', { flag: 'wx' });
console.log(JSON.stringify(report));
