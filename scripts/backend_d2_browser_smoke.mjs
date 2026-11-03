// Real local Chromium checks. Requires an existing Playwright runtime and Chrome; no downloads.
import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';
import { spawnSync } from 'node:child_process';

const require = createRequire(import.meta.url);
const { chromium } = require(process.env.D2_PLAYWRIGHT_PATH);
assert(process.env.ALLOW_REAL_MODEL_CALLS === 'false');
const root = process.cwd(), output = path.join(root, 'runtime/backend-D2/browser-first');
const python = path.join(root, '.venv/bin/python');
const report = { status: 'RUNNING', scope: 'real local browser functional checks; no model or Worker', checks: [] };
const requests = [], errors = [];
const dbCode = `
import json,sys
from datetime import timedelta
from sqlalchemy import select
from aftercare.config import Settings
from aftercare.domain.contracts import utc_now
from aftercare.repositories.database import session_factory
from aftercare.repositories.models import DemoSession
from aftercare.services.business import BusinessService
settings=Settings(_env_file=None,database_url='postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test')
sessions=session_factory(settings);service=BusinessService(sessions)
with sessions.begin() as s:
 if sys.argv[1]=='emit':
  conv=service._conversation(s,'alice',sys.argv[2])
  for number in range(int(sys.argv[3])):service.event(s,conv.id,'read_probe',{'synthetic':True,'number':number})
 else:
  rows=list(s.scalars(select(DemoSession).where(DemoSession.customer_id=='alice',DemoSession.expires_at>utc_now())))
  assert len(rows)==1,'ISOLATED_BROWSER_SESSION_REQUIRED'
  rows[0].expires_at=utc_now()-timedelta(seconds=1)
sessions.kw['bind'].dispose();print(json.dumps({'status':'DONE'}))
`;
function db(action, ...args) {
  const child = spawnSync(python, ['-c', dbCode, action, ...args], {
    cwd: root, env: { ...process.env, PYTHONPATH: 'src:.', ALLOW_REAL_MODEL_CALLS: 'false' },
    encoding: 'utf8', timeout: 10000,
  });
  assert.equal(child.status, 0, 'Isolated local probe failed');
}
let browser;
try {
  browser = await chromium.launch({
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    headless: true,
    args: ['--disable-background-networking', '--disable-component-update', '--no-first-run'],
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  await context.route('**/*', route => {
    const url = new URL(route.request().url());
    return url.hostname === '127.0.0.1' ? route.continue() : route.abort();
  });
  const page = await context.newPage();
  page.on('pageerror', error => errors.push({ name: error.name, message: error.message }));
  page.on('request', request => {
    const url = new URL(request.url());
    if (url.pathname.startsWith('/api/')) requests.push({ method: request.method(), path: url.pathname });
  });
  page.setDefaultTimeout(10000);
  await page.goto('http://127.0.0.1:8137', { waitUntil: 'domcontentloaded' });
  await page.locator('#login').click();
  await page.waitForFunction(() => document.querySelector('#connection').textContent === '实时连接');
  const firstId = await page.locator('#conversation').inputValue();
  assert(firstId.startsWith('conv_'));
  const message = 'D2 浏览器验证：先不要提交任何申请';
  await page.locator('#message').fill(message);
  await page.locator('#send').click();
  await page.waitForFunction(text => document.querySelector('#chat').textContent.includes(text), message);
  await page.waitForFunction(() => !document.querySelector('#cancel').disabled);
  await page.locator('#cancel').click();
  await page.waitForFunction(() => document.querySelector('#cancel').disabled);
  await page.waitForFunction(() => parseInt(document.querySelector('#event-count').textContent, 10) >= 4);
  await page.waitForTimeout(300);
  report.checks.push('login, message rendered, pending job cancelled without Worker');

  const snapshots = () => requests.filter(row => row.method === 'GET' && row.path === `/api/conversations/${firstId}`).length;
  const baseline = snapshots();
  db('emit', firstId, '200');
  await page.waitForFunction(() => parseInt(document.querySelector('#event-count').textContent, 10) >= 204);
  await page.waitForTimeout(350);
  report.burst_snapshot_http_requests = snapshots() - baseline;
  assert(report.burst_snapshot_http_requests >= 1 && report.burst_snapshot_http_requests <= 3);
  const snapshot = await (await context.request.get(`http://127.0.0.1:8137/api/conversations/${firstId}`)).json();
  assert(snapshot.messages.some(row => row.text === message));
  assert.equal(snapshot.operations.length, 0);
  assert(snapshot.jobs.every(row => row.status === 'CANCELLED'));
  report.checks.push('200 real SSE events retained with coalesced HTTP snapshot reads');
  await page.screenshot({ path: path.join(output, 'after-burst.png'), fullPage: true });

  let release, entered;
  const intercepted = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  await page.route(`**/api/conversations/${firstId}`, async route => {
    const response = await route.fetch(); entered(); await gate;
    try { await route.fulfill({ response }); } catch { /* Reset deliberately aborts the old read. */ }
  });
  db('emit', firstId, '1');
  await intercepted;
  await page.locator('#new-conversation').click();
  await page.waitForFunction(old => document.querySelector('#conversation').value !== old, firstId);
  release();
  await page.waitForFunction(text => !document.querySelector('#chat').textContent.includes(text), message);
  await page.unroute(`**/api/conversations/${firstId}`);
  report.checks.push('switching conversations aborts or ignores an old in-flight response');

  await page.waitForFunction(() => document.querySelector('#connection').textContent === '实时连接');
  db('expire');
  await page.waitForFunction(() => document.querySelector('#connection').textContent === '连接已结束');
  assert((await page.locator('#error').textContent()).includes('会话已失效'));
  const streamCount = requests.filter(row => row.path.endsWith('/events')).length;
  await page.waitForTimeout(2500);
  assert.equal(requests.filter(row => row.path.endsWith('/events')).length, streamCount);
  report.checks.push('expired session closes EventSource without reconnect loop');
  await page.screenshot({ path: path.join(output, 'session-expired.png'), fullPage: true });
  assert.deepEqual(errors, []);
  report.status = 'PASSED';
  report.conversation_ids = [firstId, await page.locator('#conversation').inputValue()];
} catch (error) {
  report.status = 'FAILED'; report.error_name = error.name; report.error_message = error.message;
} finally {
  if (browser) await browser.close();
  report.browser_closed = true;
  await writeFile(path.join(output, 'browser-first.json'), JSON.stringify({ ...report, requests, errors }, null, 2) + '\n', { flag: 'wx' });
}
console.log(JSON.stringify(report));
process.exitCode = report.status === 'PASSED' ? 0 : 1;
