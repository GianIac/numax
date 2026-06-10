---
title: "Quickstart: 5 Minutes"
description: Run a distributed Numax example from zero.
---

## What you're about to do

Now we do together: Two modules. Two nodes. One counter.

You run them on the same machine. They find each other, exchange an increment,
and both agree the counter is **2**.

You write **zero networking code**. Numax handles it.

---

## What you need

- [Rust](https://rustup.rs/) with the `wasm32-unknown-unknown` target
- Git

```bash
rustup target add wasm32-unknown-unknown
```

That's it.

---

## Step 1 - Get Numax

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release
```

If you want, you can set `nx` as an environment variable so every command stays short:

```bash
export NX=./target/release/nx
```

> Now on every command uses `$NX`.

---

## Step 2 - Build the example

```bash
cd examples/distributed_counter
cargo build --release --target wasm32-unknown-unknown
cd ../..
```

You now have a `.wasm` module at:
```
examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
```

A counter that increments by 1 every time it runs.

---

## Step 3 - Run two nodes

Open **two terminals** in the `numax/` root.
In **both** of them, set the variable again:

```bash
export NX=./target/release/nx
export WASM=examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
```

**Terminal 1 - Node A:**

```bash
$NX run $WASM \
    --listen 0.0.0.0:9000 \
    --peer 127.0.0.1:9001 \
    --datastore-path ./data-a \
    --wait-before-run 1500ms \
    --settle-for 5s \
    --print-gcounter counter:visits \
    -v
```

**Terminal 2 - Node B:**

```bash
$NX run $WASM \
    --listen 0.0.0.0:9001 \
    --peer 127.0.0.1:9000 \
    --datastore-path ./data-b \
    --wait-before-run 1500ms \
    --settle-for 5s \
    --print-gcounter counter:visits \
    -v
```

Start them within a few seconds of each other.

---

## Step 4 - Watch it converge

After ~6 seconds, **both terminals** print:

```text
counter:visits = 2
```

Node A incremented once. Node B incremented once. They found each other,
exchanged their state, and converged to the truth - **without you doing anything**.

---

## What just happened?

```
Node A                            Node B
  |                                 |
  +-- increment → local slot = 1    +-- increment → local slot = 1
  |                                 |
  +-- broadcast to B ────────────>  +-- receive A's slot
  |                                 |
  +<──────────── broadcast to A ────+
  |                                 |
  +-- merge: sum(1, 1) = 2          +-- merge: sum(1, 1) = 2
  |                                 |
"counter:visits = 2"            "counter:visits = 2"
```

This is a **GCounter** - a grow-only CRDT. Each node owns its own slot.
The total is the sum of all slots. Merging is just taking the max per slot.
No coordinator. No conflict. Always converges.

Your `.wasm` module called exactly one function:

```rust
gcounter::inc("counter:visits", 1);
```

Everything else - networking, sync, persistence, merge - was Numax.

---

## Clean up & run again

```bash
rm -rf ./data-a ./data-b
```

GCounter state is durable. Without removing the data directories the next run
continues from where it left off.

Now try adding more nodes, go from 2 to 3 or 6 ! Just add more `--peer` flags and watch them all converge.

---

## Want to go further?

If you want to go wild, the [examples directory](https://github.com/GianIac/numax/tree/main/examples)
has everything - pick one, read 100 lines of Rust, and you'll understand exactly what Numax does:

| Example | What it shows |
|---|---|
| [`distributed_counter`](https://github.com/GianIac/numax/tree/main/examples/distributed_counter) | GCounter - grow-only replicated counter |
| [`distributed_status`](https://github.com/GianIac/numax/tree/main/examples/distributed_status) | LWW-Register - last writer wins |
| [`distributed_settings`](https://github.com/GianIac/numax/tree/main/examples/distributed_settings) | LWW-Map - replicated config map |
| [`distributed_tags`](https://github.com/GianIac/numax/tree/main/examples/distributed_tags) | ORSet - add/remove tags, no conflicts |
| [`distributed_comments`](https://github.com/GianIac/numax/tree/main/examples/distributed_comments) | RGA - ordered replicated comment stream |
| [`distributed_inventory`](https://github.com/GianIac/numax/tree/main/examples/distributed_inventory) | restock / sale / return on a shared SKU |
| [`distributed_chat`](https://github.com/GianIac/numax/tree/main/examples/distributed_chat) | local chat log with the key-value API |