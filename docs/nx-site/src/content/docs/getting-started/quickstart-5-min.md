---
title: "Quickstart: 5 Minutes"
description: Run a distributed Numax example from zero.
---

## What you're about to do

Now we do together: Two modules. Two nodes. One counter.

The first path runs them on the same machine with **manually configured peers**.
They exchange increments and both agree the counter is **2**. The second path
uses **automatic peer discovery with mDNS**: three local nodes discover each
other without peer addresses and should converge to **3**.

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

## Path 1 - Configure peers manually

Use fresh `data-a` and `data-b` directories for the expected total of 2.

### Step 3 - Run two nodes

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

### Step 4 - Watch it converge

After ~6 seconds, **both terminals** print:

```text
counter:visits = 2
```

Node A incremented once. Node B incremented once. They connected using your
`--peer` addresses, exchanged CRDT increment operations, and converged to the
same value. Both commands then exit.

---

### What just happened?

![Two nodes exchange GCounter increment operations and converge on a value of two](/numax/diagrams/concepts/quickstart-gcounter-sync.svg)

[Mermaid source for this diagram](/numax/diagrams/concepts/quickstart-gcounter-sync.mmd)

This is a **GCounter** - a grow-only CRDT. Each node owns its own slot.
The total is the sum of all slots. When merging GCounter states, each slot takes
the maximum value; in this example Numax sends increment operations and applies
each unseen operation once.
No coordinator. No conflict. Always converges.

Your `.wasm` module called exactly one function:

```rust
gcounter::inc("counter:visits", 1);
```

Everything else - networking, sync, persistence, merge - was Numax.

---

### Clean up & run again

```bash
rm -rf ./data-a ./data-b
```

GCounter state is durable. Without removing the data directories the next run
continues from where it left off.

Now try adding more nodes, go from 2 to 3 or 6 ! Just add more `--peer` flags and watch them all converge.

---

## Path 2 - Automatic peer discovery

Same guest. This time, **three nodes find each other with mDNS**, without `--peer`.
They run on the **same machine, connected to a LAN**. Stop other Numax demos
first and use fresh `auto-a`, `auto-b`, `auto-c` directories.

### Step 3 - Run three nodes

Open **three terminals** in `numax/`. In **each**, set these variables, replacing
`192.168.1.20` with your machine's actual LAN IPv4:

```bash
export NX=./target/release/nx
export WASM=examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
export LAN_IP=192.168.1.20
```

**Terminal 1 - Node A:**

```bash
$NX run $WASM \
    --listen "$LAN_IP:9001" \
    --discovery-mode mdns \
    --mdns-instance node-a \
    --datastore-path ./auto-a \
    --wait-before-run 10s \
    --settle-for 45s \
    --print-gcounter counter:visits
```

**Terminal 2 - Node B:**

```bash
$NX run $WASM \
    --listen "$LAN_IP:9002" \
    --discovery-mode mdns \
    --mdns-instance node-b \
    --datastore-path ./auto-b \
    --wait-before-run 10s \
    --settle-for 45s \
    --print-gcounter counter:visits
```

**Terminal 3 - Node C:**

```bash
$NX run $WASM \
    --listen "$LAN_IP:9003" \
    --discovery-mode mdns \
    --mdns-instance node-c \
    --datastore-path ./auto-c \
    --wait-before-run 10s \
    --settle-for 45s \
    --print-gcounter counter:visits
```

Start them within a few seconds of each other.

### Step 4 - Watch it converge

After about a minute, **all three terminals** should print:

```text
counter:visits = 3
```

Each node incremented once. mDNS found the other nodes; Numax synchronized the
counter. The commands then exit. To repeat from zero, use fresh data directories.

> Use a trusted LAN with multicast UDP **5353** and TCP **9001–9003** allowed.
> Firewalls, Wi-Fi client isolation, VLANs or VPNs can block discovery; this demo
> uses unencrypted TCP. If the total is below 3, check these settings and start
> all three nodes together again with fresh stores. The wait is not a guarantee
> of convergence. For separate devices, see the
> [LAN demo](https://github.com/GianIac/numax/tree/main/examples/discovery_lan).

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
