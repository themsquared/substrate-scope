# Substrate Scope

A live visualizer for [Agent Substrate](https://github.com/agent-substrate/substrate),
the Kubernetes-native runtime that runs AI agents as snapshot-backed actors in
gVisor sandboxes instead of always-on pods.

`kubectl get pods` makes substrate look boring because the interesting state
is not in pods. Substrate Scope shows the runtime the way it actually works:

- **WorkerPool bays** across the top: each pre-warmed gVisor sandbox, who's
  running in it right now, and how many sessions it has served.
- **Restore queue**: agents waiting for a free worker when demand exceeds the
  pool (substrate rejects rather than queues, so the retry queue is rendered
  from client reports).
- **Object storage shelf**: every suspended agent as a zstd snapshot, with
  per-agent snapshot counts.
- **Chips that move**: agents physically fly storage → queue → worker → storage
  as they restore, run, and checkpoint.
- **Telemetry**: reserved capacity (workerpool slots × unit) vs. the dotted
  "if these were all always-on pods" line, agents-active, and pool capacity
  over time. Reservation is the honest comparison: idle pods use ~no CPU but
  reserve it forever.
- **Click into an agent**: a drawer with that agent's stream — prompts in,
  replies out with latency, errors verbatim, restores and checkpoints.
- **Controls that are real**: workers +/- runs `kubectl scale workerpool`,
  AUTOSCALE is a demand-driven scaler acting on the real CR, RESET POOL
  recovers wedged workers, STOP DEMO halts all traffic generation.

## Quickstart

Requirements: node >= 18, `kubectl` pointed at a cluster running Agent
Substrate. No npm install — the tool has zero dependencies.

```bash
git clone https://github.com/themsquared/substrate-scope.git
cd substrate-scope

node server.mjs            # simulated mode: no cluster needed, full animation
node server.mjs --live     # live mode: watches your current kubectl context
```

Open http://localhost:8123.

## Live-mode sources

The server picks a data source automatically (`--source kagent|crd` to force):

| Source | Requires | What you see |
| --- | --- | --- |
| `kagent` | [kagent](https://kagent.dev/) >= 0.9.7 with substrate enabled | Everything: per-actor runtime state and worker assignments from ateapi (via the kagent controller's `/api/substrate/status`), chats ingested from kagent's sessions API into the per-agent drawer, SURGE, and a port-forwarded kagent UI at :8001. |
| `crd` | any Agent Substrate cluster | WorkerPools, live worker pods, ActorTemplates with golden-snapshot phase, real scaling and autoscaling. Per-session actor state (which actor is on which worker right now) lives in ateapi (gRPC + JWT) and is not visible from CRDs. |

**Contributions welcome**: a direct ateapi adapter would give `crd`-class
clusters the full experience. The ateapi `Control` service already exposes
`ListActors` and `ListWorkers` (see
[`pkg/proto/ateapipb/ateapi.proto`](https://github.com/agent-substrate/substrate/blob/main/pkg/proto/ateapipb/ateapi.proto));
what's needed is the gRPC client + ServiceAccount JWT wiring.

## Load generation (kagent clusters)

`stimulate.mjs` drives real chats at SandboxAgents so the board moves:

```bash
node stimulate.mjs --budget 300     # stop after 300 chats (spend-capped)
node stimulate.mjs --oversub 6 --load 0.9
```

Flags: `--budget N` stop after N chats and flip the board's STOP DEMO switch;
`--oversub N` in-flight beyond the pool size, makes the queue visible
(default 4); `--load 0..1` fraction of long-form prompts (default 0.75);
`--concurrency N` pin in-flight instead of auto-sizing to the pool.

Every chat is real: a genuine actor restore, an LLM turn, a checkpoint. The
per-agent drawer shows each prompt and reply, tagged by source.

## Autoscaling

The built-in autoscaler (on by default in live mode; `--no-autoscale` to
disable, or toggle in the UI) is demand-driven, because CPU is the wrong
signal for this runtime: workers are slot-bound and LLM turns are mostly I/O
wait. Policy:

```
target      = busy + queued, clamped to [2, 8]
scale UP    straight to target when queued > 0 for 2 samples, cooldown 8s
scale DOWN  straight to max(demand over a 30s window) after ~18s of surplus,
            cooldown 20s
```

Note: `kubectl scale` takes field ownership of `WorkerPool.spec.replicas`;
subsequent helm upgrades that manage the pool need `--force-conflicts`.

## Endpoints

The server exposes a small HTTP API the UI (and your own tooling) can use:
`GET /events` (SSE stream), `POST /scale {replicas}`, `POST /reset` (bounce
wedged workers; snapshots survive), `POST /surge`, `POST /autoscale {on}`,
`POST /demo {run}` (master kill switch), `POST /activity` and `POST /queue`
(load-generator reporting).

## Origin

Built while standing up Agent Substrate with kagent on kind; the full
write-up (install, autoscaling analysis, and every gotcha with verbatim
errors) is at
[webofmike.com/kagent-agent-substrate](https://webofmike.com/kagent-agent-substrate/),
and the original demo environment lives at
[themsquared/kagent-substrate-demo](https://github.com/themsquared/kagent-substrate-demo).

## License

Apache-2.0
