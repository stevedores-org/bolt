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

/// A captured swarm run — what gets printed (pretty or JSON).
struct SwarmReport {
    workers: usize,
    agents: usize,
    elapsed_ms: u128,
    runs: Vec<AgentRun>,
    completed: u32,
    merged: u32,
    escalated: u32,
    total_fixes: u32,
}

/// Run the swarm on bolt and collect the report.
fn run_swarm() -> SwarmReport {
    const AGENTS: usize = 16;

    let rt = Runtime::builder().workers(8).build();
    let ledger = rt.actor(Ledger::default());
    let started = Instant::now();

    // Fan out the swarm as remote tasks, then `get` all results.
    let refs: Vec<_> = (0..AGENTS)
        .map(|i| rt.submit(move || run_agent(i)))
        .collect();
    let runs = rt.get_all(refs);

    // Record each outcome in the ledger actor (FIFO, lock-free).
    for run in &runs {
        let (passed, fixes) = (run.passed, run.fixes);
        let _ = ledger.call(move |l: &mut Ledger| {
            l.completed += 1;
            l.total_fixes += fixes;
            if passed {
                l.merged += 1;
            } else {
                l.escalated += 1;
            }
        });
    }

    // The final read is processed after every record (mailbox is FIFO).
    let (completed, merged, escalated, total_fixes) =
        rt.get(ledger.call(|l: &mut Ledger| (l.completed, l.merged, l.escalated, l.total_fixes)));

    SwarmReport {
        workers: rt.workers(),
        agents: AGENTS,
        elapsed_ms: started.elapsed().as_millis(),
        runs,
        completed,
        merged,
        escalated,
        total_fixes,
    }
}

fn print_pretty(r: &SwarmReport) {
    println!("⚡ bolt — Ray in Rust");
    println!("   runtime: {} workers\n", r.workers);
    println!("dispatching {} agents across shadow branches…\n", r.agents);
    for run in &r.runs {
        let status = if run.passed {
            "✓ merged "
        } else {
            "⚠ escalate"
        };
        println!("  {status}  {:<18}  fixes={}", run.branch, run.fixes);
    }
    println!("\n── AIVCS run ledger ──────────────────────────────");
    println!("  agents completed : {}", r.completed);
    println!("  branches merged  : {}", r.merged);
    println!("  escalated to HITL: {}", r.escalated);
    println!("  total APR fixes  : {}", r.total_fixes);
    println!("  wall-clock       : {:.2}s", r.elapsed_ms as f64 / 1000.0);
    println!("──────────────────────────────────────────────────");
    println!(
        "\n{} agents in ~{:.2}s on {} workers — bolt fanned the swarm out;",
        r.agents,
        r.elapsed_ms as f64 / 1000.0,
        r.workers
    );
    println!("the ledger actor tallied every outcome with zero locks.");
}

/// Machine-readable output for the AIVCS demo UI. Branch names are
/// `shadow/agent-NN` (ASCII), so no JSON string escaping is needed.
fn print_json(r: &SwarmReport) {
    let runs: Vec<String> = r
        .runs
        .iter()
        .map(|run| {
            format!(
                "{{ \"branch\": \"{}\", \"passed\": {}, \"fixes\": {} }}",
                run.branch, run.passed, run.fixes
            )
        })
        .collect();
    println!(
        "{{\n  \"runtime\": \"bolt\",\n  \"workers\": {},\n  \"agents\": {},\n  \"elapsedMs\": {},\n  \"ledger\": {{ \"completed\": {}, \"merged\": {}, \"escalated\": {}, \"totalFixes\": {} }},\n  \"runs\": [\n    {}\n  ]\n}}",
        r.workers,
        r.agents,
        r.elapsed_ms,
        r.completed,
        r.merged,
        r.escalated,
        r.total_fixes,
        runs.join(",\n    ")
    );
}

fn main() {
    let as_json = std::env::args().any(|a| a == "--json");
    let report = run_swarm();
    if as_json {
        print_json(&report);
    } else {
        print_pretty(&report);
    }
}
