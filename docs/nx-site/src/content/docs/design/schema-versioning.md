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

The op-log table (id 14) now uses schema 3. Its `1 -> 2` migration validates
existing operation payloads without assigning historical sequences or rewriting
those payloads. The version change also makes v0.1.5 reject an upgraded store
before it can mutate CRDT state without updating sequence metadata.

The additive `sequences` table (id 15, schema 3) uses `__nx/crdt/sequence/`:

- `local`: the bound origin, current generation, and allocated sequence;
- `origin/<origin>`: contiguous prefix, known high sequence, sparse identities,
  optional unavailable-history evidence (source, interval and request ID), and
  legacy baseline requirement;
- `usage`: origin count, serialized receipt bytes, and sparse-position count;
- `replay/<OpId>`: original origin/generation/sequence, retained alongside the
  corresponding replay record and deleted when that record expires;
- `baseline/<origin>`: installed coverage of that origin's pre-sequence state;
- `failure/<origin>`: the current source/reason rejection tuple, or JSON `null`
  after invalidation (at most one record per tracked origin);
- `baseline-unconfirmed`: a marker installed with a baseline batch and removed
  only after a covering flush succeeds.

The sequence-table `1 -> 2` migration preserves contiguous coverage, sparse
identities, known maxima and baseline requirements. It discards the old
uncorrelated unavailable boolean instead of inventing a source or interval.
Migration is resumable; receipt-byte accounting is recomputed and committed
atomically with the schema-2 header and checkpoint removal at completion.

The op-log and sequence-table `2 -> 3` steps validate and preserve payloads,
then advance their headers to fence runtimes that do not preserve transferred
baselines. Other CRDT tables remain at schema 1. Initialization establishes only
the store's own legacy baseline provenance; receiving an advertisement from
another origin never establishes receipt of that baseline.

Baseline state, materialized values, per-origin receipt coverage and installed
legacy provenance commit together. A rejected batch publishes nothing. After
a failed flush, the complete batch may survive and its in-memory state is kept
coherent, but `baseline-unconfirmed` remains set and the store rejects writes
until reopen. A successful restart flush covers surviving state before clearing
that marker. An accepted transfer does not synthesize or replay old UUID-only
operations, and known maxima beyond its cut remain pending.

Failure records describe one source under the current recovery target; they do
not prove permanent cluster-wide loss. Local pending requests include a receipt
fingerprint, so a still-live refusal after another source repaired the gap is
ignored. Recorded failures are invalidated when receipt coverage or known history
changes. The fingerprint is local request metadata; the schema-3 record layout is unchanged.
Storage faults retain their original cause rather than becoming data rejections.

Local state and sequence allocation commit in one batch before the covering
flush. Remote state, positions, and receipt metadata likewise commit atomically
before publication. Compacted receipt prefixes survive replay and seen-ID
retention. Missing or corrupt authoritative sequence records fail closed.

Existing stores require explicit offline migration with
`nx migrate --datastore-path /path/to/data`. UUID-only records remain legacy;
their automatic remote replay is rejected, and the need for a legacy state
baseline is advertised separately. Historical fixture payloads remain intact.

## Upgrade, historical state, and downgrade contract

Package, wire and table-schema versions are independent. The current storage
transition is deliberately explicit:

| Input | Current runtime / migration result |
| --- | --- |
| Empty store | Initialize current table headers |
| v0.1.0 unversioned sync records | Runtime rejects; offline migration validates `0 -> 1`, then upgrades op log and sequences to 3 |
| v0.1.5 schema-1 store | Runtime rejects the old op-log schema; offline migration validates its payloads and upgrades op-log and sequence headers to 3 |
| Development sequence-table schema 1 | Offline `1 -> 2` migration converts receipts, drops uncorrelated unavailable flags and recomputes byte usage, then validates `2 -> 3` |
| Development schema-2 op-log/sequence tables | Runtime rejects; offline `2 -> 3` validates payloads and advances the headers |
| Current schema-3 store | Open normally; migration is idempotent |
| Unknown future schema or malformed source | Reject; do not interpret it as empty data |
| v0.1.5 opening an upgraded store | Old runtime rejects op-log schema 3 before CRDT mutations |

Stop the runtime and preserve an offline backup before running `nx migrate
--datastore-path /path/to/data`. Reopen with the new runtime only after migration
succeeds. A failed migration can have completed earlier table steps; it is
resumable, not a whole-database rollback transaction. Each record transformation
and checkpoint advance is atomic. Each final table header and checkpoint
removal is atomic, together with sequence usage accounting where required.
The path-based CLI migration confirms a flush before reporting success.
A failed flush has an uncertain outcome: reopen and inspect/retry migration;
never assume that an error means no batch reached storage.

Historical CRDT state and its materialized values survive migration. Old UUID-only
operations retain their original payloads and local log indices; those indices
are **not** origin sequences. Migration does not synthesize sequence positions
or certify that a bounded historical log is complete. Unrelated application
records remain unchanged.

On first sequence initialization, an existing pre-sequence store is marked as
requiring a legacy baseline. The flag survives restart and new contiguous
sequenced writes. It is advertised to other nodes independently of their new
receipt frontiers. Old operations are excluded from sequenced replay; incoming
legacy replay is rejected even after its UUIDs have left the seen cache.
A connected new peer therefore cannot infer historical convergence from a
successful handshake or complete coverage of new operations. Recovery of that
baseline uses the implemented, bounded D3 state-transfer contract. A source
with incomplete coverage, an oversized payload or an incompatible receiver cut
may refuse recovery explicitly; no unrestricted lossless recovery is promised.

A downgrade cannot safely write this store: the old writer would bypass sequence
tracking. There is no reverse migration. Restoring an offline pre-upgrade backup
is a separate operator action and does not preserve writes accepted since that
backup. Mixed protocol-5/protocol-8 replication is also rejected; plan deployment
for that communication interruption rather than assuming a rolling upgrade
continues replication across the version boundary.

<span id="regression-evidence"></span>

### Compatibility verification

Historical fixtures are immutable logical-store records produced by their
release encoders. They include source tags, pinned commits and reproduction
instructions; current encoders must not regenerate them to fit a migration.

| Fixture | Contents |
| --- | --- |
| `sync_manager/fixtures/v0_1_0/` | Unversioned sync records and operation payloads |
| `sync_manager/fixtures/v0_1_5/` | Six CRDT families, tombstones, seen IDs, UUID-only replay, schema-1 headers and an application key |

Verification reconstructs disposable stores and checks the following boundaries:

- Payload bytes and unrelated application records survive migration unchanged.
- Record checkpoints resume after reopen, including partially converted receipts.
- Storage rejection or corrupted input does not advance the failed record's checkpoint.
- Final schema headers and checkpoint removal remain atomic across process termination.
- Legacy replay remains rejected after real seen-ID and op-log eviction.
- A new sequence frontier does not establish receipt of a historical baseline.
- Baseline installation preserves full internal state, receipt coverage and legacy
  provenance across restart, delayed duplicates and flush failure.
- Recovery refusal remains specific to the requested source and current coverage;
  an alternate source can still complete recovery.

Focused verification entry points:

```sh
cargo test -p nx-core baseline_
cargo test -p nx-core v015
cargo test -p nx-core sequence_v1_migration
cargo test -p nx-core migrates_fixture_generated_by_v0_1_0
cargo test -p nx-net
```

For real-process compatibility, build `examples/distributed_counter` for
`wasm32-unknown-unknown`, build `nx-cli` from the pinned v0.1.5 sources, set
`NUMAX_PREVIOUS_NX_BIN` to that release binary, then run:

```sh
cargo test -p nx-cli --test multiprocess_smoke \
  different_protocol_versions_reject_connection_without_exchanging_ops \
  -- --exact --ignored
```

These tests require local TCP sockets; the process-crash test also requires
permission to spawn and terminate its child process. They never operate on an
operator's datastore.

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
`v1 -> v2 -> v3` for op log and sequences. Version `0` means either
legacy data without metadata or an explicit version-zero header.

The original-table `v0 -> v1` and op-log steps validate existing payloads without
rewriting them. The sequence-table `1 -> 2` receipt conversion is described
above. Each original-table validation step proceeds as follows:

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

The `v0.1.0` logical-store fixture supplies the original layout for the
[compatibility verification](#compatibility-verification), including byte
preservation and validation of the resulting CRDT state and operation log.
