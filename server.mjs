#!/usr/bin/env node
// Substrate Scope — a live visualizer for Agent Substrate
// (https://github.com/agent-substrate/substrate).
// Zero dependencies; needs node >= 18 and kubectl on the PATH.
//
//   node server.mjs                        # simulated feed (built into the page)
//   node server.mjs --live                 # watch the current kubectl context
//   node server.mjs --live --source crd    # force the kubectl-only adapter
//
// Live-mode source adapters (auto-detected by default):
//   kagent  Full fidelity. Polls the kagent controller's substrate inventory
//           (/api/substrate/status): per-actor runtime state and worker
//           assignments straight from ateapi, plus chat ingestion from
//           kagent's sessions API.
//   crd     Works on ANY substrate cluster, no kagent required: WorkerPools,
//           worker pods, and ActorTemplates (golden-snapshot phase) via
//           kubectl. Per-session actor state lives in ateapi (gRPC + JWT) and
//           is not visible here — a direct ateapi adapter is a welcome
//           contribution (ateapi Control: ListActors/ListWorkers).
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { execFile, spawn } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), 'public');
const LIVE = process.argv.includes('--live');
const PORT = Number(process.env.PORT || 8123);
const srcIdx = process.argv.indexOf('--source');
const SOURCE = srcIdx > -1 ? process.argv[srcIdx + 1] : 'auto';   // kagent | crd | auto
let source = SOURCE === 'auto' ? null : SOURCE;
// kagent's controller API; the kagent adapter port-forwards
// svc/kagent-controller 8083 itself unless this is overridden.
const KAGENT_API = process.env.KAGENT_API || 'http://127.0.0.1:8083';

const clients = new Set();
const send = ev => {
  const line = `data: ${JSON.stringify(ev)}\n\n`;
  for (const res of clients) res.write(line);
};

const server = createServer(async (req, res) => {
  if (req.url === '/scale' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      res.setHeader('Content-Type', 'application/json');
      if (!LIVE) return res.end(JSON.stringify({ ok: false, error: 'not in --live mode' }));
      const pool = Object.values(state.pools)[0];
      if (!pool) return res.end(JSON.stringify({ ok: false, error: 'no workerpool seen yet' }));
      let replicas;
      try { replicas = Math.max(1, Math.min(8, Number(JSON.parse(body).replicas))); }
      catch { return res.end(JSON.stringify({ ok: false, error: 'bad request' })); }
      // the documented ephemeral scaling path: kubectl scale workerpool
      execFile('kubectl', ['scale', 'workerpools.ate.dev', pool.name,
                           '-n', pool.ns, `--replicas=${replicas}`],
        { timeout: 10_000 },
        err => res.end(JSON.stringify(err ? { ok: false, error: String(err.message).slice(0, 200) }
                                          : { ok: true, replicas })));
    });
    return;
  }
  if (req.url === '/reset' && req.method === 'POST') {
    // frees workers pinned by ghost actors (v0.0.6 wedge after aborted
    // sessions): bounce the WorkerPool's deployment; snapshots survive
    res.setHeader('Content-Type', 'application/json');
    if (!LIVE) { res.end(JSON.stringify({ ok: false, error: 'not in --live mode' })); return; }
    const pool = Object.values(state.pools)[0];
    if (!pool) { res.end(JSON.stringify({ ok: false, error: 'no workerpool seen yet' })); return; }
    execFile('kubectl', ['rollout', 'restart', `deploy/${pool.name}-deployment`, '-n', pool.ns],
      { timeout: 15_000 },
      err => res.end(JSON.stringify(err ? { ok: false, error: String(err.message).slice(0, 200) }
                                        : { ok: true })));
    return;
  }
  if (req.url === '/queue' && req.method === 'POST') {
    // stimulate.mjs reports which agents are waiting on a full pool (substrate
    // rejects rather than queues, so the retry-queue lives client-side)
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      res.setHeader('Content-Type', 'application/json');
      try {
        stimWaiting = (JSON.parse(body).waiting ?? []).map(w => w.name ?? w);
        broadcastQueue();
        res.end('{"ok":true}');
      } catch { res.writeHead(400); res.end('{"ok":false}'); }
    });
    return;
  }
  if (req.url === '/demo') {
    // master kill switch: stimulate.mjs polls this and stops dispatching
    // real (billable) chats when run=false; surge respects it too
    res.setHeader('Content-Type', 'application/json');
    if (req.method === 'POST') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try { demoRun = !!JSON.parse(body).run; } catch { demoRun = !demoRun; }
        send({ type: 'demo_state', run: demoRun });
        res.end(JSON.stringify({ ok: true, run: demoRun }));
      });
    } else res.end(JSON.stringify({ run: demoRun }));
    return;
  }
  if (req.url === '/activity' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      res.setHeader('Content-Type', 'application/json');
      try { recordActivity(JSON.parse(body)); res.end('{"ok":true}'); }
      catch { res.writeHead(400); res.end('{"ok":false}'); }
    });
    return;
  }
  if (req.url === '/surge' && req.method === 'POST') {
    res.setHeader('Content-Type', 'application/json');
    if (!LIVE) { res.end(JSON.stringify({ ok: false, error: 'not in --live mode' })); return; }
    if (source !== 'kagent') { res.end(JSON.stringify({ ok: false, error: 'surge requires the kagent source' })); return; }
    surge().then(n => res.end(JSON.stringify({ ok: true, fired: n })));
    return;
  }
  if (req.url === '/autoscale' && req.method === 'POST') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      res.setHeader('Content-Type', 'application/json');
      try { autoscale = !!JSON.parse(body).on; } catch { autoscale = !autoscale; }
      upStreak = 0; downStreak = 0;
      send({ type: 'autoscale_state', on: autoscale });
      res.end(JSON.stringify({ ok: true, on: autoscale }));
    });
    return;
  }
  if (req.url === '/events') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream',
                         'Cache-Control': 'no-cache', Connection: 'keep-alive' });
    clients.add(res);
    res.write(`data: ${JSON.stringify({ type: 'mode', live: LIVE })}\n\n`);
    if (LIVE) replayState(res);
    req.on('close', () => clients.delete(res));
    return;
  }
  const path = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  try {
    const body = await readFile(join(ROOT, path));
    res.writeHead(200, { 'Content-Type':
      path.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/plain; charset=utf-8' });
    res.end(body);
  } catch { res.writeHead(404); res.end('not found'); }
});

// ── live mode: kubectl polling ───────────────────────────────────────────────
const state = { pools: {}, actors: {} };   // last-seen, for diffing + replay

// ── metrics: kubelet stats (no metrics-server needed) ────────────────────────
// Benchmark data is REAL: the default kagent Agents run as always-on pods in
// the same cluster; their measured per-pod cost × (number of substrate agents)
// is the dotted "if these were all pods" line.
const metrics = [];              // ring buffer of sample points
const METRICS_MAX = 1200;        // ~1h at 3s
let queuedNow = 0;               // updated by /queue posts from stimulate.mjs
let nodeName = null;

// Per-unit reservation: what one always-on agent pod would request. The fair
// comparison in the telemetry is RESERVED capacity (requests), not idle
// usage. One substrate worker needs the same per-session unit an agent pod
// does — you just need `slots` of them instead of `agents`. Override with
// UNIT_CPU_M / UNIT_MEM_MI; with kagent present the real values are read from
// a default agent Deployment.
const unit = { cpu: Number(process.env.UNIT_CPU_M) || 50,
               mem: Number(process.env.UNIT_MEM_MI) || 128 };
const parseCpu = s => !s ? 0 : s.endsWith('m') ? parseInt(s) : parseFloat(s) * 1000;
const parseMem = s => !s ? 0 : s.endsWith('Gi') ? parseFloat(s) * 1024 : parseInt(s);
async function fetchUnit() {
  if (process.env.UNIT_CPU_M || source !== 'kagent') return;
  const d = await kubectl(['get', 'deploy', 'k8s-agent', '-n', 'kagent', '-o', 'json']);
  const r = d?.spec?.template?.spec?.containers?.[0]?.resources?.requests;
  if (r?.cpu) unit.cpu = parseCpu(r.cpu) || unit.cpu;
  if (r?.memory) unit.mem = parseMem(r.memory) || unit.mem;
}

async function sampleMetrics() {
  if (!LIVE) return;
  if (!nodeName) {
    const n = await kubectl(['get', 'nodes', '-o', 'json']);
    nodeName = n?.items?.[0]?.metadata?.name;
    if (!nodeName) return;
  }
  const s = await kubectl(['get', '--raw', `/api/v1/nodes/${nodeName}/proxy/stats/summary`]);
  const snap = state.lastSnap;
  if (!s || !snap) return;
  const poolKeys = Object.values(state.pools)
    .map(p => ({ ns: p.ns, prefix: `${p.name}-deployment-` }));
  let wCpu = 0, wMem = 0, aCpu = 0, aMem = 0, aN = 0;
  for (const p of s.pods ?? []) {
    const ns = p.podRef.namespace, name = p.podRef.name;
    const cpu = ((p.cpu ?? {}).usageNanoCores ?? 0) / 1e6;         // mCPU
    const mem = ((p.memory ?? {}).workingSetBytes ?? 0) / 1048576; // MiB
    if (poolKeys.some(k => k.ns === ns && name.startsWith(k.prefix))) { wCpu += cpu; wMem += mem; }
    // measured always-on baseline: kagent's default agent pods, when present
    else if (source === 'kagent' && ns === 'kagent'
             && !name.startsWith('kagent-') && /-agent-/.test(name)) { aCpu += cpu; aMem += mem; aN++; }
  }
  const agents = (snap.actorTemplates ?? []).length;
  const slots = (snap.workers ?? []).length;
  const active = (snap.workers ?? []).filter(w => w.actorId).length;
  const point = {
    t: Date.now(),
    wCpu: Math.round(wCpu), wMem: Math.round(wMem),
    benchCpu: aN ? Math.round(aCpu / aN * agents) : 0,
    benchMem: aN ? Math.round(aMem / aN * agents) : 0,
    unitCpu: unit.cpu, unitMem: unit.mem,
    active, slots, queued: queuedNow, agents,
  };
  metrics.push(point);
  if (metrics.length > METRICS_MAX) metrics.shift();
  send({ type: 'metrics', p: point });
  autoscaleTick(point);
}

// ── autoscaler: demand-driven (queue depth up, idle capacity down) ───────────
// CPU is the wrong signal here: workers are slot-bound, and LLM turns are
// mostly I/O wait. Demand (queued + busy) vs slots is what actually matters.
let autoscale = LIVE && !process.argv.includes('--no-autoscale'),
    lastScaleAt = 0, upStreak = 0, downStreak = 0;
const AS = { MIN: 2, MAX: 8, COOL_UP: 8_000, COOL_DOWN: 20_000,
             UP_TICKS: 2, DOWN_TICKS: 6, WINDOW: 10 };
const demandWin = [];   // rolling demand (busy + queued), ~30s at 3s samples

function scaleTo(pool, n, why) {
  lastScaleAt = Date.now();
  execFile('kubectl', ['scale', 'workerpools.ate.dev', pool.name, '-n', pool.ns,
                       `--replicas=${n}`], { timeout: 10_000 },
    err => send({ type: 'autoscale', replicas: n, why, ok: !err }));
}

// Target-based, both directions: scale straight to what demand needs, not ±1.
// Up = current demand (fast, queued work is user-visible latency). Down = the
// PEAK demand over the rolling window (one decisive jump, but a brief dip
// can't slash the pool).
function autoscaleTick(point) {
  if (!autoscale) return;
  const pool = Object.values(state.pools)[0];
  if (!pool) return;
  const clamp = n => Math.min(AS.MAX, Math.max(AS.MIN, n));
  const demand = point.active + point.queued;
  demandWin.push(demand);
  if (demandWin.length > AS.WINDOW) demandWin.shift();
  const upTarget = clamp(demand);
  const downTarget = clamp(Math.max(...demandWin));
  if (point.queued > 0) { upStreak++; downStreak = 0; }
  else if (downTarget < point.slots) { downStreak++; upStreak = 0; }
  else { upStreak = 0; downStreak = 0; }
  const now = Date.now();
  if (upStreak >= AS.UP_TICKS && upTarget > point.slots && now - lastScaleAt > AS.COOL_UP) {
    upStreak = 0;
    scaleTo(pool, upTarget, `demand ${demand} (${point.queued} queued)`);
  } else if (downStreak >= AS.DOWN_TICKS && downTarget < point.slots
             && now - lastScaleAt > AS.COOL_DOWN) {
    downStreak = 0;
    scaleTo(pool, downTarget, `peak demand ${Math.max(...demandWin)} over 30s`);
  }
}

// ── per-agent activity: prompts, replies, latency — the "click into an agent"
//    stream. Reported by stimulate.mjs and surge (in-sandbox stdout is not
//    reachable: substrate exposes network ingress only, so chat I/O IS the
//    agent's observable output).
const activity = [];
const ACT_MAX = 400;
function recordActivity(ev) {
  const e = { t: Date.now(), ...ev };
  activity.push(e);
  if (activity.length > ACT_MAX) activity.shift();
  send({ type: 'activity', e });
}

// ── ingest kagent-UI chats from the sessions API ──────────────────────────────
// Chats sent from the kagent UI don't self-report like stimulate/surge do, so
// live mode polls kagent's session store and folds them into the same
// activity stream. Our own traffic is skipped by contextId prefix.
const KAGENT_USER = process.env.KAGENT_USER || 'admin@kagent.dev';
const OWN_SESSION = /^(stim|surge|speed|probe|haiku|model)-/;
const seenTasks = new Map();          // taskId -> last observed state
let sessionsSince = Date.now() - 120_000;
const agentFromAdk = app => {
  const i = (app ?? '').indexOf('__NS__');
  return i > 0 ? app.slice(i + 6).replace(/_/g, '-') : (app || 'unknown');
};
async function pollSessions() {
  try {
    const q = `user_id=${encodeURIComponent(KAGENT_USER)}`;
    const r = await fetch(`${KAGENT_API}/api/sessions?${q}`, { signal: AbortSignal.timeout(5000) });
    const j = await r.json();
    const fresh = (j.data ?? []).filter(s => !OWN_SESSION.test(s.id)
      && new Date(s.updated_at).getTime() > sessionsSince - 5000);
    sessionsSince = Date.now();
    for (const s of fresh.slice(0, 10)) {
      const tr = await fetch(`${KAGENT_API}/api/sessions/${s.id}/tasks?${q}`,
                             { signal: AbortSignal.timeout(5000) });
      const tj = await tr.json();
      for (const task of tj.data ?? []) {
        const state = task.status?.state;
        const prev = seenTasks.get(task.id);
        if (prev === state) continue;
        const agent = agentFromAdk(task.metadata?.adk_app_name);
        if (prev === undefined) {
          const um = (task.history ?? []).find(m => m.role === 'user');
          const text = (um?.parts ?? []).map(p => p.text).filter(Boolean).join(' ');
          if (text) recordActivity({ agent, kind: 'prompt', text: text.slice(0, 400), via: 'kagent-ui' });
        }
        if (state === 'completed' || state === 'failed') {
          const parts = [...(task.status?.message?.parts ?? []),
                         ...((task.artifacts ?? []).flatMap(a => a.parts ?? []))];
          const text = parts.map(p => p.text).filter(Boolean).join(' ').slice(0, 400);
          recordActivity({ agent, kind: state === 'failed' ? 'error' : 'reply',
                           text: text || '(no text)', via: 'kagent-ui' });
        }
        seenTasks.set(task.id, state);
        if (seenTasks.size > 500) seenTasks.delete(seenTasks.keys().next().value);
      }
    }
  } catch {}
}

// ── queue bookkeeping: union of stimulator-reported and surge-local waits ─────
let stimWaiting = [];                 // names reported by stimulate.mjs
const surgeWaiting = new Set();       // names of surge chats bouncing off a full pool
function broadcastQueue() {
  const names = [...new Set([...stimWaiting, ...surgeWaiting])];
  queuedNow = names.length;
  send({ type: 'queue', waiting: names.map(name => ({ name })) });
}

let demoRun = true;   // master switch for anything that costs LLM tokens

// ── surge: server-driven burst of real chats across every SandboxAgent ───────
async function surge() {
  if (!demoRun) return 0;
  const list = await new Promise(res => {
    fetch(`${KAGENT_API}/api/agents`, { signal: AbortSignal.timeout(8000) })
      .then(r => r.json()).then(j => res(j.data ?? [])).catch(() => res([]));
  });
  const targets = list.filter(a => a.agent?.kind === 'SandboxAgent')
    .map(a => `${a.agent.metadata.namespace}/${a.agent.metadata.name}`);
  for (const ref of targets) {
    surgeChat(ref);                       // fire-and-forget, retries inside
    await new Promise(r => setTimeout(r, 120));
  }
  return targets.length;
}

async function surgeChat(ref) {
  const [ns, name] = ref.split('/');
  const id = `surge-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  const prompt = 'Explain in about 120 words what you would do first in a production incident.';
  recordActivity({ agent: name, kind: 'prompt', text: prompt, via: 'surge' });
  const t0 = Date.now();
  for (let tries = 0; tries < 60; tries++) {
    try {
      const r = await fetch(`${KAGENT_API}/api/a2a-sandboxes/${ns}/${name}/`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id, method: 'message/send',
          params: { message: { kind: 'message', messageId: id, contextId: id, role: 'user',
            parts: [{ kind: 'text', text: prompt }] } } }),
        signal: AbortSignal.timeout(180_000),
      });
      const j = await r.json();
      if (j.error && /no free workers|worker pool/i.test(j.error.message ?? '')) {
        if (!surgeWaiting.has(name)) { surgeWaiting.add(name); broadcastQueue(); }
        await new Promise(res => setTimeout(res, 1500 + Math.random() * 1500));
        continue;
      }
      const ms = Date.now() - t0;
      if (j.error) recordActivity({ agent: name, kind: 'error', text: j.error.message?.slice(0, 300), ms });
      else {
        const text = (j.result?.artifacts ?? []).flatMap(a => a.parts ?? [])
          .map(p => p.text).filter(Boolean).join(' ');
        recordActivity({ agent: name, kind: 'reply', text: text.slice(0, 400) || '(no text)', ms });
      }
      break;
    } catch (e) { recordActivity({ agent: name, kind: 'error', text: String(e.message).slice(0, 200) }); break; }
  }
  if (surgeWaiting.delete(name)) broadcastQueue();
}

function kubectl(args) {
  return new Promise(resolve => {
    execFile('kubectl', args, { timeout: 10_000 }, (err, stdout) => {
      if (err) return resolve(null);
      try { resolve(JSON.parse(stdout)); } catch { resolve(null); }
    });
  });
}

function replayState(res) {
  res.write(`data: ${JSON.stringify({ type: 'demo_state', run: demoRun })}\n\n`);
  if (activity.length)
    res.write(`data: ${JSON.stringify({ type: 'activity_history', items: activity.slice(-200) })}\n\n`);
  if (metrics.length)
    res.write(`data: ${JSON.stringify({ type: 'metrics_history', points: metrics })}\n\n`);
  res.write(`data: ${JSON.stringify({ type: 'autoscale_state', on: autoscale })}\n\n`);
  if (state.lastSnap) { res.write(`data: ${JSON.stringify(state.lastSnap)}\n\n`); return; }
  for (const p of Object.values(state.pools))
    res.write(`data: ${JSON.stringify(p)}\n\n`);
  for (const a of Object.values(state.actors))
    res.write(`data: ${JSON.stringify({ type: 'actor', name: a.name, ns: a.ns })}\n\n`);
}

// Preferred live source: the kagent controller's substrate inventory — the
// same data the kagent UI's Substrate page shows. Returns runtime actor state
// and worker assignments straight from ate-api — the stuff CRDs can't see.
// (v0.9.9 serves it as REST at /api/substrate/status; newer builds also expose
// it as gRPC-Web SystemService/GetSubstrateStatus.)
async function fetchStatus() {
  try {
    const r = await fetch(`${KAGENT_API}/api/substrate/status`,
                          { signal: AbortSignal.timeout(4000) });
    if (!r.ok) return null;
    const j = await r.json();
    return j?.data ?? null;
  } catch { return null; }
}

// crd adapter: synthesize the same snapshot shape from what any substrate
// cluster exposes to kubectl. Worker occupancy and per-session actor state
// are not knowable here (they live in ateapi); templates render as agents
// with their golden-snapshot phase.
async function crdStatus() {
  const pools = await kubectl(['get', 'workerpools.ate.dev', '-A', '-o', 'json']);
  if (!pools) return null;
  const workerPools = [], workers = [];
  for (const item of pools.items ?? []) {
    const ns = item.metadata.namespace, name = item.metadata.name;
    workerPools.push({ namespace: ns, name, replicas: item.spec?.replicas ?? 0,
                       ateomImage: item.spec?.ateomImage ?? '' });
    const pods = await kubectl(['get', 'pods', '-n', ns, '-o', 'json']);
    for (const p of pods?.items ?? [])
      if (p.metadata.name.startsWith(`${name}-deployment-`) && p.status?.phase === 'Running')
        workers.push({ workerNamespace: ns, workerPool: name, workerPod: p.metadata.name });
  }
  const actorTemplates = [], actors = [];
  const tpls = await kubectl(['get', 'actortemplates.ate.dev', '-A', '-o', 'json']);
  for (const t of tpls?.items ?? []) {
    const ns = t.metadata.namespace, name = t.metadata.name;
    const phase = t.status?.phase ?? 'Unknown';
    actorTemplates.push({ namespace: ns, name, phase, goldenActorId: t.status?.goldenActorID });
    actors.push({ actorId: `tpl-${ns}-${name}`, status: phase === 'Ready' ? 'Suspended' : phase,
                  actorTemplateNamespace: ns, actorTemplateName: name });
  }
  return { enabled: true, workerPools, actorTemplates, actors, workers };
}

let pollTick = 0;
async function poll() {
  pollTick++;
  let snap = null;
  if (source !== 'crd') snap = await fetchStatus();
  if (!snap && source !== 'kagent' && pollTick % 3 === 1) snap = await crdStatus();
  if (!snap) return;
  for (const p of snap.workerPools ?? [])
    state.pools[p.name] = { type: 'pool', name: p.name, ns: p.namespace, replicas: p.replicas };
  state.lastSnap = { type: 'snapshot', ...snap };
  send(state.lastSnap);
}

async function detectSource() {
  if (source) return;
  const svc = await kubectl(['get', 'svc', 'kagent-controller', '-n', 'kagent', '-o', 'json']);
  source = svc ? 'kagent' : 'crd';
}

server.listen(PORT, async () => {
  console.log(`Substrate Scope → http://localhost:${PORT}  (${LIVE ? 'LIVE, watching the current kubectl context' : 'simulated'})`);
  if (!LIVE) return;
  await detectSource();
  console.log(`source: ${source}${source === 'crd'
    ? ' (kubectl-only; per-session actor state needs the kagent source or a future ateapi adapter)' : ''}`);
  if (source === 'kagent' && !process.env.KAGENT_API) {
    const forward = (svc, ports) => {
      const c = spawn('kubectl', ['port-forward', '-n', 'kagent', svc, ports],
                      { stdio: 'ignore' });
      c.on('exit', () => setTimeout(() => forward(svc, ports), 2000)); // survive pod restarts
    };
    forward('svc/kagent-controller', '8083:8083');
    forward('svc/kagent-ui', `${process.env.KAGENT_UI_PORT || 8001}:8080`);
    console.log(`kagent UI → http://localhost:${process.env.KAGENT_UI_PORT || 8001}  (chat with agents to light up the board)`);
  }
  poll(); setInterval(poll, 800);   // fast enough to catch short LLM sessions on a worker
  fetchUnit();
  setTimeout(() => { sampleMetrics(); setInterval(sampleMetrics, 3000); }, 4000);
  if (source === 'kagent')
    setTimeout(() => { pollSessions(); setInterval(pollSessions, 2500); }, 3000);
});
