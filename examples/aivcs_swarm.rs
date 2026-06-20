//! # aivcs swarm — bolt showcase for the Lornu AI AIVCS demo
//!
//! Mirrors the AIVCS agentic-VCS narrative (swarm → build → test → fix → evolve):
//! a pool of autonomous agents each works a **shadow branch** through a
//! build→test→**APR fix loop**, fanned out across bolt's worker pool. A shared
//! **ledger actor** tallies the run with no locks — the same role the real
//! `release-ledger` / HITL review queue plays in the demo.
//!
//! Run it:
//! ```text
//! cargo run --example aivcs_swarm
//! ```

use std::thread;
use std::time::{Duration, Instant};

use bolt::Runtime;

/// What one agent produced on its shadow branch.
#[derive(Clone, Debug)]
struct AgentRun {
    branch: String,
    passed: bool,
    fixes: u32,
}

/// Simulate one agentic-VCS worker: cut a shadow branch, build, then run an
/// adversarial-PR-review (APR) fix loop until tests pass or we give up.
fn run_agent(agent: usize) -> AgentRun {
    let branch = format!("shadow/agent-{agent:02}");
    thread::sleep(Duration::from_millis(100)); // build

    // Deterministic-but-varied: a few agents need fixes, one is stubborn.
    let mut remaining_bugs = match agent % 4 {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => 4, // stubborn — will exhaust the APR budget
    };
    let mut fixes = 0;
    let apr_budget = 3;
    while remaining_bugs > 0 && fixes < apr_budget {
        thread::sleep(Duration::from_millis(60)); // test + fix iteration
        remaining_bugs -= 1;
        fixes += 1;
    }

    AgentRun {
        branch,
        passed: remaining_bugs == 0,
        fixes,
    }
}

/// Append-only run ledger — the showcase's stateful actor.
#[derive(Default)]
struct Ledger {
    completed: u32,
    merged: u32,
    escalated: u32,
    total_fixes: u32,
}

fn main() {
    const AGENTS: usize = 16;

    let rt = Runtime::builder().workers(8).build();
    println!("⚡ bolt — Ray in Rust");
    println!("   runtime: {} workers\n", rt.workers());
    println!("dispatching {AGENTS} agents across shadow branches…\n");

    let ledger = rt.actor(Ledger::default());
    let started = Instant::now();

    // Fan out the swarm as remote tasks, then `get` all results.
    let refs: Vec<_> = (0..AGENTS)
        .map(|i| rt.submit(move || run_agent(i)))
        .collect();
    let runs = rt.get_all(refs);

    // Record each outcome in the ledger actor (FIFO, lock-free).
    for run in &runs {
        let passed = run.passed;
        let fixes = run.fixes;
        let _ = ledger.call(move |l: &mut Ledger| {
            l.completed += 1;
            l.total_fixes += fixes;
            if passed {
                l.merged += 1;
            } else {
                l.escalated += 1;
            }
        });
        let status = if run.passed {
            "✓ merged "
        } else {
            "⚠ escalate"
        };
        println!(
            "  {status}  {:<18}  fixes={fixes}",
            run.branch,
            fixes = run.fixes
        );
    }

    // The final read is processed after every record (mailbox is FIFO).
    let (completed, merged, escalated, total_fixes) =
        rt.get(ledger.call(|l: &mut Ledger| (l.completed, l.merged, l.escalated, l.total_fixes)));
    let elapsed = started.elapsed();

    println!("\n── AIVCS run ledger ──────────────────────────────");
    println!("  agents completed : {completed}");
    println!("  branches merged  : {merged}");
    println!("  escalated to HITL: {escalated}");
    println!("  total APR fixes  : {total_fixes}");
    println!("  wall-clock       : {:.2}s", elapsed.as_secs_f64());
    println!("──────────────────────────────────────────────────");
    println!(
        "\n{AGENTS} agents in ~{:.2}s on {} workers — bolt fanned the swarm out;",
        elapsed.as_secs_f64(),
        rt.workers()
    );
    println!("the ledger actor tallied every outcome with zero locks.");
}
