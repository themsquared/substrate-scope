#!/usr/bin/env node
// Auto-stimulator: sends REAL chats to random SandboxAgents so the live board
// keeps moving during a POC. Nothing is faked — every prompt restores a real
// actor from its snapshot, runs a real LLM turn, and checkpoints back.
//
//   node stimulate.mjs                      # ~1 chat every few seconds, 2 in flight
//   node stimulate.mjs --concurrency 3 --interval 4
//   KAGENT_API=http://127.0.0.1:8083 node stimulate.mjs
//
// Requires the kagent controller API (server.mjs --live already port-forwards
// svc/kagent-controller 8083; otherwise run your own kubectl port-forward).
const API = process.env.KAGENT_API || 'http://127.0.0.1:8083';
const arg = (name, dflt) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > -1 ? Number(process.argv[i + 1]) : dflt;
};
const VIZ = process.env.VIZ || 'http://127.0.0.1:8123';
const INTERVAL = arg('interval', 2);   // mean seconds between dispatch checks
const LOAD = arg('load', 0.75);        // fraction of long-form prompts
// --concurrency N to pin; otherwise auto-sized to (live worker count +
// oversub) so every bay stays lit AND a visible retry-queue forms.
const CONC_ARG = arg('concurrency', 0);
const OVERSUB = arg('oversub', 4);     // extra in-flight beyond the pool size —
                                       // deep enough that queued agents visibly wait their turn
const BUDGET = arg('budget', 0);       // stop after N chats total (0 = unlimited)
                                       // — overnight-safe: no runaway API spend

const QUICK_PROMPTS = [
  'In one short sentence, what is your job?',
  'Give me one tip from your specialty. One sentence.',
  'What would you check first during an incident? One sentence.',
  'Reply with a haiku about Kubernetes.',
  'One sentence: why do snapshots beat idle pods?',
];
// Long generations hold an actor on its worker for 10–20s — that's what makes
// several bays glow at once instead of a single 2s flash.
const LONG_PROMPTS = [
  'Write a detailed 15-step runbook for a failed rollout, two sentences per step.',
  'Draft a ~450-word incident postmortem for a fictional cache outage, with timeline, root cause, and action items.',
  'Explain in ~400 words how you would triage rising p99 latency, covering dashboards, traces, and rollback criteria.',
  'List 20 things to check after a Kubernetes upgrade, with a sentence of rationale for each.',
  'Write a ~400-word briefing on why idle agents waste cluster capacity and how snapshot-based multiplexing fixes it.',
  'Compose a ~450-word status update to leadership about a resolved sev-2, including impact, response, and prevention.',
];
const PROMPTS = null; // superseded by QUICK_PROMPTS / LONG_PROMPTS

const sleep = ms => new Promise(r => setTimeout(r, ms));
const rand = a => a[Math.floor(Math.random() * a.length)];
const jitter = s => (s * (0.5 + Math.random())) * 1000;

async function listSandboxAgents() {
  const r = await fetch(`${API}/api/agents`, { signal: AbortSignal.timeout(8000) });
  const j = await r.json();
  return (j.data ?? [])
    .filter(a => a.agent?.kind === 'SandboxAgent')
    .map(a => `${a.agent.metadata.namespace}/${a.agent.metadata.name}`);
}

let inFlight = 0, sent = 0, ok = 0, failed = 0;

async function workerCount() {
  try {
    const r = await fetch(`${API}/api/substrate/status`, { signal: AbortSignal.timeout(6000) });
    const j = await r.json();
    return (j.data?.workers ?? []).length || 2;
  } catch { return 2; }
}

// agents whose requests are bouncing off a full pool — reported to the viz
// server so their chips show "queued" on the board
const waiting = new Set();
let queueTimer = null;
function reportQueue(){
  clearTimeout(queueTimer);
  queueTimer = setTimeout(() => {
    fetch(`${VIZ}/queue`, { method: 'POST',
      body: JSON.stringify({ waiting: [...waiting].map(name => ({ name })) }),
      signal: AbortSignal.timeout(3000) }).catch(() => {});
  }, 150);
}

// feed the board's per-agent activity drawer (fire-and-forget)
function reportActivity(ev){
  fetch(`${VIZ}/activity`, { method: 'POST', body: JSON.stringify(ev),
    signal: AbortSignal.timeout(2000) }).catch(() => {});
}

async function chat(agentRef) {
  const [ns, name] = agentRef.split('/');
  const prompt = Math.random() < LOAD ? rand(LONG_PROMPTS) : rand(QUICK_PROMPTS);
  const id = `stim-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
  inFlight++; sent++;
  reportActivity({ agent: name, kind: 'prompt', text: prompt, via: 'stimulator' });
  try {
    while (!stop) {
      const t0 = Date.now();
      const r = await fetch(`${API}/api/a2a-sandboxes/${ns}/${name}/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        // fresh contextId per chat = a fresh session actor restore every time
        body: JSON.stringify({ jsonrpc: '2.0', id, method: 'message/send',
          params: { message: { kind: 'message', messageId: id, contextId: id,
            role: 'user', parts: [{ kind: 'text', text: prompt }] } } }),
        signal: AbortSignal.timeout(180_000),
      });
      const j = await r.json();
      if (j.error && /no free workers|worker pool/i.test(j.error.message ?? '')){
        if (!waiting.has(name)){ waiting.add(name); reportQueue();
          console.log(`… ${name}: pool full, queued for retry`); }
        await sleep(1500 + Math.random() * 1500);
        continue;                                  // same agent, same prompt
      }
      if (waiting.delete(name)) reportQueue();
      const text = j.result?.artifacts?.flatMap(a => a.parts ?? [])
                     .map(p => p.text).filter(Boolean).join(' ') ?? '';
      const secs = ((Date.now() - t0) / 1000).toFixed(1);
      const ms = Date.now() - t0;
      if (j.error) { failed++; console.log(`✗ ${name} (${secs}s): ${j.error.message?.slice(0, 90)}`);
        reportActivity({ agent: name, kind: 'error', text: j.error.message?.slice(0, 300), ms }); }
      else { ok++; console.log(`✓ ${name} (${secs}s): ${text.slice(0, 90) || '(no text)'}`);
        reportActivity({ agent: name, kind: 'reply', text: text.slice(0, 400) || '(no text)', ms }); }
      break;
    }
  } catch (e) {
    if (waiting.delete(name)) reportQueue();
    failed++; console.log(`✗ ${name}: ${String(e.message).slice(0, 90)}`);
    reportActivity({ agent: name, kind: 'error', text: String(e.message).slice(0, 200) });
  } finally { inFlight--; }
}

const agents = await listSandboxAgents();
if (!agents.length) { console.error(`no SandboxAgents found at ${API}/api/agents`); process.exit(1); }
let concurrency = CONC_ARG || (await workerCount()) + OVERSUB;
console.log(`stimulating ${agents.length} agents via ${API} — ≤${concurrency} in flight`
  + `${CONC_ARG ? '' : ` (auto: workers + ${OVERSUB} oversub; re-checks as you scale)`}, ${Math.round(LOAD*100)}% long-form`
  + (BUDGET ? `, budget ${BUDGET} chats` : ', no budget (Ctrl-C or STOP DEMO to halt)'));
console.log(agents.map(a => '  ' + a).join('\n'));

let stop = false;
process.on('SIGINT', () => { stop = true;
  console.log(`\nstopping… sent=${sent} ok=${ok} failed=${failed}`); });

// the viz server's STOP DEMO button is the master switch for billable chats
let demoRun = true, lastDemoCheck = 0;
async function checkDemo(){
  if (Date.now() - lastDemoCheck < 2000) return;
  lastDemoCheck = Date.now();
  try {
    const j = await (await fetch(`${VIZ}/demo`, { signal: AbortSignal.timeout(2000) })).json();
    if (demoRun !== j.run) console.log(j.run ? '▶ demo resumed' : '■ demo stopped — no new chats');
    demoRun = j.run;
  } catch {}   // viz server absent → keep running standalone
}

let lastSize = Date.now();
while (!stop) {
  if (BUDGET && sent >= BUDGET){
    console.log(`■ budget reached: ${sent}/${BUDGET} chats dispatched — stopping`);
    // flip the board's master switch so STOP DEMO shows why traffic ended
    fetch(`${VIZ}/demo`, { method: 'POST', body: JSON.stringify({ run: false }),
      signal: AbortSignal.timeout(2000) }).catch(() => {});
    break;
  }
  await checkDemo();
  if (!CONC_ARG && Date.now() - lastSize > 10_000){   // follow live pool scaling
    lastSize = Date.now();
    workerCount().then(n => { const c = n + OVERSUB; if (c !== concurrency){
      console.log(`pool is now ${n} workers — concurrency → ${c}`); concurrency = c; } });
  }
  while (demoRun && inFlight < concurrency && !stop
         && !(BUDGET && sent >= BUDGET)) { chat(rand(agents)); await sleep(400); }
  await sleep(jitter(INTERVAL));
}
while (inFlight > 0) await sleep(500);
process.exit(0);
