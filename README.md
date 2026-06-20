# ⚡ bolt — Ray in Rust

A minimal, dependency-light **distributed task & actor runtime** in the spirit of
[Ray](https://www.ray.io/), built for the **Lornu AI agent fleet** and showcased
in the [AIVCS](https://aivcs.io) demo.

bolt gives you three Ray-style primitives on safe async Rust:

| Ray | bolt |
|-----|------|
| `ray.init()` | `Runtime::new()` / `Runtime::builder().workers(n).build()` |
| `@ray.remote` task | `rt.submit(\|\| …)` → `ObjectRef<T>` |
| `ray.get(ref)` | `rt.get(obj)` / `rt.get_all(vec)` |
| `@ray.remote` actor | `rt.actor(state)` → `ActorHandle<S>` (serialized, lock-free mailbox) |

```rust
use bolt::Runtime;

let rt = Runtime::builder().workers(4).build();

// Fan out remote tasks, then `get` the results.
let refs: Vec<_> = (0..8).map(|i| rt.submit(move || i * i)).collect();
let squares = rt.get_all(refs);            // [0, 1, 4, 9, 16, 25, 36, 49]

// A stateful actor with a serialized mailbox — no locks.
let counter = rt.actor(0u64);
for _ in 0..10 { let _ = counter.call(|n: &mut u64| *n += 1); }
assert_eq!(rt.get(counter.call(|n: &mut u64| *n)), 10);
```

## AIVCS showcase

The [`aivcs_swarm`](examples/aivcs_swarm.rs) example mirrors the AIVCS
agentic-VCS narrative (**swarm → build → test → fix → evolve**): a pool of
autonomous agents each works a **shadow branch** through a build→test→**APR fix
loop**, fanned out across bolt's worker pool, with a shared **ledger actor**
tallying every outcome lock-free — the role the real release ledger / HITL
review queue plays in the demo.

```console
$ cargo run --example aivcs_swarm
⚡ bolt — Ray in Rust
   runtime: 8 workers

dispatching 16 agents across shadow branches…
  ✓ merged   shadow/agent-00     fixes=0
  ⚠ escalate shadow/agent-03     fixes=3
  …
── AIVCS run ledger ──────────────────────────────
  agents completed : 16
  branches merged  : 12
  escalated to HITL: 4
  total APR fixes  : 24
  wall-clock       : 0.56s
──────────────────────────────────────────────────
```

## Why bolt

The fleet already leans on **Ray** (Python) for distributed inference —
[`sparky`](https://github.com/lornu-ai/sparky) runs vLLM **on Ray** on the DGX
Spark. bolt is the Rust-native path to that same shape: one runtime, the same
`submit` / `get` / actor API a multi-node scheduler exposes, but type-safe,
GC-free, and embeddable directly in the Rust agent services
([`bullpen`](https://github.com/lornu-ai/bullpen), `agent-scheduler`).

## Status & roadmap

**v0 (this crate):** local multi-threaded scheduler — tasks, `get`/`get_all`,
actors. The public API is deliberately the shape a distributed scheduler would
expose, so the following are *additive*, not rewrites:

- [ ] Multi-node head/worker over gRPC (tonic)
- [ ] Shared object store with content-addressed refs (CAS — aligns with AIVCS)
- [ ] Placement/affinity (pin actors to the DGX, GPU-aware tasks)
- [ ] Backpressure + bounded actor mailboxes
- [ ] Metrics + tracing surface (org-standard `/healthz` + `/metrics`)

## Develop

```sh
cargo test                          # unit + doc tests
cargo clippy --all-targets -- -D warnings
cargo fmt --check
cargo run --example aivcs_swarm     # the showcase
```

## License

MIT — see [LICENSE](LICENSE).
