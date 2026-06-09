---
title: Foundations
description: A beginner-friendly glossary for the ideas behind Numax.
---

You do not need to be a distributed systems expert to start with Numax.
This page explains the core words you will see in the docs, examples and
roadmap.

## WebAssembly

WebAssembly, often shortened to **WASM**, is a portable binary format for
running code in a sandbox.

For Numax, the important idea is simple:

- you write a module in a language like Rust;
- the module is compiled to `.wasm`;
- Numax loads it, runs it, and decides which host functions it can call.

Think of WASM as a small, portable program format. The guest module does not get
free access to the filesystem, network or process. It can only do what the host
runtime exposes.

Useful links:

- [MDN: WebAssembly](https://developer.mozilla.org/en-US/docs/WebAssembly)
- [MDN: WebAssembly concepts](https://developer.mozilla.org/en-US/docs/WebAssembly/Concepts)
- [Bytecode Alliance](https://bytecodealliance.org/)

## Runtime

A **runtime** is the program that executes your module and provides the services
around it.

Numax uses **Wasmtime** as its WebAssembly execution engine.

In Numax, the runtime:

- loads and validates a WASM module;
- exposes Host API functions such as database, time, crypto and CRDT helpers;
- owns the local datastore;
- starts networking and synchronization when configured;
- shuts everything down cleanly.

Your module is the application logic. Numax is the environment it runs inside.

Useful links:

- [Wasmtime documentation](https://docs.wasmtime.dev/)

## Host API

The **Host API** is the contract between guest code and Numax.

A WASM module cannot directly call Rust functions inside Numax. Instead, Numax
exports explicit functions such as:

- `db_set`
- `db_get`
- `gcounter_inc`
- `time_now`
- `random_bytes`

The SDK wraps those low-level imports in nicer Rust functions, so guest modules
can call `db::set(...)` or `gcounter::inc(...)`.

Read more:

- [Numax Host API](/reference/host-api/)
- [WebAssembly imports and exports on MDN](https://developer.mozilla.org/en-US/docs/WebAssembly/Guides/Understanding_the_text_format#imports_and_exports)

## Local-first

**Local-first** means the local node remains useful even when the network is
slow, broken or missing.

Instead of asking a remote server for every operation, the app writes locally
first and synchronizes later. This is why Numax keeps state in an embedded local
store and uses CRDTs for convergence.

Useful link:

- [Ink & Switch: Local-first Software](https://www.inkandswitch.com/local-first/)

## CRDT

A **CRDT** is a data structure designed for replicated systems.

The practical promise is:

- several nodes can update their local copy independently;
- updates can arrive in different orders;
- once nodes have seen the same updates, they converge to the same value.

This avoids a lot of manual conflict-resolution code. Numax currently includes
CRDTs such as GCounter, PNCounter, LWW-Register, ORSet, LWW-Map and RGA.

Useful links:

- [CRDT resources](https://crdt.tech/resources)
- [Conflict-free Replicated Data Types paper](https://arxiv.org/abs/1805.06358)

## Gossip

**Gossip** is a style of communication where nodes spread information by talking
to peers over time.

Instead of one central coordinator deciding everything, nodes exchange operations
with connected peers. Numax uses this idea together with reconnect and
anti-entropy loops, so missed operations can be recovered later.

Useful links:

- [Gossip protocol overview](https://en.wikipedia.org/wiki/Gossip_protocol)
- [Epidemic Algorithms for Replicated Database Maintenance](https://paperswelove.org/papers/epidemic-algorithms-for-replicated-database-mainte-9283e904/)

## Anti-entropy

**Anti-entropy** is the repair loop.

Normal sync sends operations as they happen. Anti-entropy periodically asks a
peer for operations again, so a node can recover from dropped messages, missed
broadcasts or reconnects.

In simple terms: push is the fast path, anti-entropy is the safety net.

Useful links:

- [Dynamo: Amazon's highly available key-value store](https://www.amazon.science/publications/dynamo-amazons-highly-available-key-value-store)
- [CRDT resources](https://crdt.tech/resources)

## mTLS

**mTLS** means mutual TLS. Both sides of a connection present certificates, so
each peer can verify the other.

Numax uses this for permissioned clusters. A node identity can be derived from
its certificate, and peers can be restricted with an allowlist.

Useful links:

- [Apache APISIX: What is mutual TLS?](https://apisix.apache.org/learning-center/what-is-mutual-tls/)
- [Mozilla: Transport Layer Security](https://developer.mozilla.org/en-US/docs/Web/Security/Transport_Layer_Security)

## Component Model

The **WebAssembly Component Model** is the future direction for richer,
multi-language WASM interfaces.

Today Numax exposes a legacy-style Host API. The roadmap moves toward WIT and
the Component Model so Rust, Go, JavaScript, Python and other guest languages
can share a clearer ABI.

Useful links:

- [WebAssembly Component Model concepts](https://component-model.bytecodealliance.org/design/component-model-concepts.html)
- [Why the Component Model?](https://component-model.bytecodealliance.org/design/why-component-model.html)

## How to read the Numax docs

If you are new, use this order:

1. Read this page.
2. Run the [Quickstart](/getting-started/quickstart-5-min/).
3. Open one distributed example and read the guest module.
4. Read [CRDT and state](/concepts/crdt-and-state/) when you want to understand convergence.
5. Read [Host API](/reference/host-api/) when you start writing your own module.
