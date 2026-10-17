---
title: Schema Versioning
description: Persistence schema boundaries and validation rules.
---

## Scope

Schema versioning applies to the Numax-managed sync namespaces stored in sled.
Application keys and runtime identity metadata have separate ownership.

The managed tables are:

- materialized and durable state for each CRDT family;
- seen-operation metadata;
- the durable operation log;
- sequence allocation, receipt tracking, and retained replay positions.

These are logical tables implemented with key prefixes in one sled database.

## Table metadata

Each logical table has a metadata key under:

```text
__nx/schema/<table-name>
```

Its value is an eight-byte header:

```text
[magic: 4 bytes][schema version: u16 BE][table id: u16 BE]
```

The magic value is `NXDB`. Versions and table identifiers use big-endian
encoding.

The table identifier prevents valid metadata for one namespace from being
accepted for another. Each table owns its schema version independently.

## Sequenced replication in v0.1.6

The op-log table (id 14) now uses schema 2. Its `1 -> 2` migration validates
existing operation payloads without assigning historical sequences or rewriting
those payloads. The version change also makes v0.1.5 reject an upgraded store
before it can mutate CRDT state without updating sequence metadata.

The additive `sequences` table (id 15, schema 2) uses `__nx/crdt/sequence/`:

- `local`: the bound origin, current generation, and allocated sequence;
- `origin/<origin>`: contiguous prefix, known high sequence, sparse identities,
  optional unavailable-history evidence (source, interval and request ID), and
  legacy baseline requirement;
- `usage`: origin count, serialized receipt bytes, and sparse-position count;
- `replay/<OpId>`: original origin/generation/sequence, retained alongside the
  corresponding replay record and deleted when that record expires.

The sequence-table `1 -> 2` migration preserves contiguous coverage, sparse
identities, known maxima and baseline requirements. It discards the old
uncorrelated unavailable boolean instead of inventing a source or interval.
Migration is resumable; receipt-byte accounting is recomputed and committed
atomically with the schema-2 header and checkpoint removal at completion.

Local state and sequence allocation commit in one batch before the covering
flush. Remote state, positions, and receipt metadata likewise commit atomically
before publication. Compacted receipt prefixes survive replay and seen-ID
retention. Missing or corrupt authoritative sequence records fail closed.

Existing stores require explicit offline migration with
`nx migrate --datastore-path /path/to/data`. UUID-only records remain legacy;
their automatic remote replay is rejected, and the need for a legacy state
baseline is advertised separately. Historical fixture payloads remain intact.

## Startup validation

The sync manager validates every managed table before hydrating persisted data:

| State | Result |
|---|---|
| Empty table without metadata | Initialize current metadata |
| Current metadata | Continue |
| Data without metadata | Reject as legacy |
| Older version | Reject; migration required |
| Future version | Reject as unsupported |
| Invalid magic, length, or table id | Reject as corrupted metadata |

Missing metadata is initialized in one atomic sled batch, after all tables have
passed validation. Validation never partially initializes a store.

The runtime uses the fallible sync-manager constructor and propagates schema
errors to the caller. It does not continue with empty in-memory CRDT state after
a persistence error.

## Migration boundary

Schema validation does not modify existing table data. Legacy and older
schemas require an explicit migration.

A migration must:

1. validate the source version;
2. transform records deterministically;
3. update data and schema metadata atomically where possible;
4. be safe to retry;
5. reject unknown future versions;
6. have fixture-based tests for both successful and interrupted migration.

## Migration registry

Each table owns an independent ordered registry of `N -> N+1` steps. A step
may replace, create, move, or delete records. Its mutations and checkpoint
advancement use the same atomic sled batch. Every completed step writes its
schema version before the next step starts.

Inside the table being scanned, a record step may only mutate its own source
key. New keys and moves must target a different namespace, preventing checkpoint
gaps and double-processing.

## Legacy migration

The migration engine supports `v0 -> v1` for the original tables and
`v1 -> v2` for the op log. Version `0` means either
legacy data without metadata or an explicit version-zero header.

This migration certifies existing payloads without rewriting them:

1. process at most a configured number of records per batch;
2. validate each key and payload with its table-specific decoder;
3. persist the last validated key under `__nx/migration/<table-name>`;
4. resume after that key when restarted;
5. write the version-one schema header and remove the checkpoint atomically
   after the whole table has been validated.

The default limits are 512 records and 4 MiB per batch. Both limits apply:

- the record limit prevents unbounded table scans;
- one shared byte budget covers scanned input and generated mutations;
- a single record larger than the byte limit is rejected before its payload is
  cloned into the migration batch.

Memory use is bounded by the configured batch limits rather than the total
table size.

Migration acquires an exclusive in-process write lease for its entire
execution. Store clones may continue reading, but their mutations wait until
the migration session ends. The sled database lock separately prevents another
process from opening the same datastore.

The engine does not perform implicit migration during `Runtime::new`.

If validation fails, the checkpoint is not advanced and the schema header is
not written. Existing payload bytes remain unchanged.

Compatibility with the legacy layout is covered by a committed logical-store
fixture generated from tag `v0.1.0`. The test reconstructs the sled records,
migrates them with the current engine, verifies every payload byte is
unchanged, and validates the resulting CRDT state and operation log.
