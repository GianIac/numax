---
title: What's coming in Numax v0.1.2
description: Performance & Profiling, and why "make it fast" is the wrong first question.
---

`v0.1.1` was about **shape**. The sync manager stopped being one giant file,
the wire protocol grew an explicit version, the persisted sync schemas got
proper metadata, and `nx migrate` turned "upgrade the datastore" into an
actual workflow instead of a leap of faith.

`v0.1.2` is about a different question, and I want to be honest about it up
front: it is not *"make Numax faster"*.

It is *"make Numax honest about how fast it is"*.

Those are very different projects.

---

## The problem with "make it faster"

Every runtime, sooner or later, ends up in the same trap: someone opens a PR,
CI is green, everything looks fine, and three weeks later a graph in production
starts drifting. p99 latency creeps up. RSS grows a little more than it should.
Throughput drops five percent per release and nobody notices, because five
percent per release is invisible until you compare month to month.

You don't fix that by writing faster code. You fix that by making the current
speed **visible**, **comparable**, and **hard to regress silently**.

That's the whole theme of `v0.1.2`.

---

## Where we start from

Numax isn't starting from zero here, which is nice.

The runtime already exposes an **opt-in observability endpoint**, either
through `RuntimeConfig.observability`, the `--observability-listen` CLI flag,
or the `[observability]` config section. That endpoint already serves
`/health`, `/ready`, and a Prometheus-compatible `/metrics`.

The current `/metrics` covers the obvious things: ops processed, connected
peers, sync errors, peer connects/disconnects, broadcast activity, store keys
and bytes, and the observability endpoint's own request/error counters. Solid
baseline. Not enough.

Load benchmarks are also already there:

- `crates/nx-core/benches/three_node_sync_load.rs`
- `crates/nx-core/benches/chaos_sync_load.rs`
- `crates/nx-store/benches/single_node_load.rs`

They already emit JSON reports with throughput and latency percentiles, and
historical reports live under `crates/*/reports/load/`.

So the raw material is there. What's missing is the layer on top: **comparison,
baselines, and hot-path visibility**.

---

## Three moves, in order

`v0.1.2` extends the performance story in three directions, and the order
matters.

### 1. Profiling on demand, invisible by default

You should be able to answer "which tasks are busy?", "where is CPU going?",
"is this memory pattern expected?" without permanently paying for the
instrumentation.

That means `tokio-console`, CPU flamegraphs, and heap profiling arrive as
**opt-in integrations**. They must not change the default runtime profile.
The base binary stays the same shape. You turn profiling on when you need to
look, and only then.

### 2. Benchmarks that compare themselves

A benchmark report on its own is a number in a vacuum. A benchmark report
compared to a baseline is a **signal**.

The plan: commit baseline history under `crates/*/reports/baselines/`, teach
CI to compare a run against the baseline, and print a clear delta with a clear
verdict. Not "here's a JSON blob, good luck", but "sync p99 moved from X to Y,
that's Z% worse, here are the top three suspects".

### 3. Prometheus metrics that describe the hot paths

The roadmap calls out three new metrics:

- `numax_module_cpu_ms` - per-WASM-module CPU time
- `numax_module_memory_bytes` - per-WASM-module memory
- `numax_op_apply_duration_ms` - CRDT op apply latency distribution

These aren't picked for aesthetics. They map exactly to the places where
Numax's performance actually gets decided: **inside guest execution** and
**inside the sync apply path**. Everything else in the runtime is glue.

The design constraint is the same one I'll be repeating a lot in this release:
these metrics have to be **cheap, bounded, and living in the right module**.
Module-level profiling belongs around WASM execution. Op-apply timing belongs
near the sync apply path. Not scattered through the codebase because it was
convenient.

---

## The rule I care about most: shadow before blocking

The closing criterion for `v0.1.2` is deliberately sharp:

> A pull request that worsens sync p99 by more than 5% is automatically blocked
> by CI, with the regression details.

That is the end state. It is **not** the first step.

Performance gates are notorious for being introduced too early and then
disabled two sprints later because "CI keeps flaking on the benchmark". I've
seen it happen. I don't want it to happen here.

So the gate ships in two phases:

- **Shadow phase**: CI runs the comparison, prints the result, uploads the
  reports, and *never fails a PR*. The point is to understand the noise floor
  of the benchmark environment.
- **Blocking phase**: once the signal is stable and we trust it, the gate
  becomes blocking, starting with sync p99, extending later to the others.

If a metric is too noisy to trust, it stays in shadow. No exceptions.

---

## The planned path

Here's how I want to walk through `v0.1.2`, in small steps, each one
reviewable on its own:

1. Define the benchmark report shape and baseline policy.
2. Add a shadow comparator for p99, throughput, RSS, and error rate.
3. Run short benchmark scenarios in CI and upload reports as artifacts.
4. Add the new Prometheus metrics, opt-in, no change to default behavior.
5. Add task-level visibility via `tokio-console`, behind an explicit flag.
6. Produce CPU and heap profiling artifacts from benchmark runs.
7. Promote the regression gate from shadow to blocking, one metric at a time.

Profiler dependencies (`pprof-rs`, `samply`, `dhat`, and friends) will be
chosen **deliberately**. Not slipped in as incidental deps because they made
one benchmark easier to write. Every one of them changes the compile profile
and the story we tell about the binary; that deserves a decision, not a shrug.

---

## One last thing

I care about this release more than it probably sounds from the outside.

"Performance & Profiling" doesn't have the drama of a new CRDT or the shine of
a dashboard. Nobody stars a repo because it added a regression gate. But every
project I've watched slowly get worse over time got worse for the same reason:
nobody was measuring, so nobody argued, so the small regressions won.

`v0.1.2` is the release that makes those arguments possible.

After this, when someone claims a change is "basically free", the benchmark
report will have an opinion. That's the whole point.