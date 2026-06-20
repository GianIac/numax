# Sync manager tests

The sync manager test suite follows the same ownership boundaries as the
production module:

- Unit and component tests stay beside the production code they exercise.
- Manager integration tests live in `mod.rs`.
- End-to-end convergence tests live in `e2e/`, with one file per CRDT family.
- Shared setup, operation, persistence, and convergence helpers live in
  `support.rs`.

The E2E files must exercise behavior through the sync manager and its handle.
They must not contain family-independent setup or duplicate helpers from
`support.rs`.

When adding a CRDT family:

1. Add its shared test operations and convergence helpers to `support.rs`.
2. Add an `e2e/<family>.rs` file.
3. Register the file in `e2e/mod.rs`.
4. Cover replication, convergence, materialized state, and durable state where
   applicable.

Run the complete suite with:

```bash
cargo test -p nx-core
```
