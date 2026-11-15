# Numax v0.1.5 persistence fixture

`records.hex` is a logical-store fixture produced using the release sources:

- tag: `v0.1.5`
- commit: `471a2281d0ffc8b7a3c20550320854812b8af726`
- SHA-256: `abf4c4bb492e8bda675ea73258522de7ec4b524925de9f3dd51a2011ed7fe6f2`

The standalone generator must be copied into an extraction of that exact tag.
Its relative dependencies then resolve to the historical crates, including
`nx-core`. Seed its lockfile from the release lockfile to retain historical
dependency versions; never run it against the current crates to update this fixture.
The release migration engine writes schema-1 headers and its sync manager
validates and hydrates the result. JSON state objects are canonicalized after
release serialization to make object ordering reproducible.

The fixture contains materialized and durable state for all six CRDT families,
including map, set and RGA tombstones; six UUID-only replay records with seen-ID
metadata; fourteen schema-1 headers; and an unrelated application key.
It represents retained history, not a complete operation log or a guest API
acceptance trace. The counters' replay indices are local log ordering, never
per-origin positions. There are no sequence records.

Each data line contains a hex key and value separated by a tab. Logical records
avoid depending on sled's internal file layout or operating system.
Tests reconstruct disposable databases; this file and the v0.1.0 fixture remain
immutable references, independent of the current migration implementation.

To reproduce into a separate output for comparison, from the repository root:

```sh
git rev-parse 'refs/tags/v0.1.5^{commit}'
# Verify the commit printed above equals the pinned commit before continuing.
fixture_source=$(mktemp -d)
git archive refs/tags/v0.1.5 | tar -x -C "$fixture_source"
mkdir -p "$fixture_source/tools"
cp -R crates/nx-core/src/sync_manager/fixtures/v0_1_5/generator \
  "$fixture_source/tools/v015-fixture-generator"
cp "$fixture_source/Cargo.lock" "$fixture_source/tools/v015-fixture-generator/Cargo.lock"
cargo run --release \
  --manifest-path "$fixture_source/tools/v015-fixture-generator/Cargo.toml" \
  -- "$fixture_source/reproduced.hex"
cmp crates/nx-core/src/sync_manager/fixtures/v0_1_5/records.hex \
  "$fixture_source/reproduced.hex"
```
