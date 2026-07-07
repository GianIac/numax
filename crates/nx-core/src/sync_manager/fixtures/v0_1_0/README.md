# Numax v0.1.0 persistence fixture

`records.hex` contains the logical sled records produced by code compiled from:

- tag: `v0.1.0`
- commit: `9f4753b8d0706b069988487ac7f6e3939f6e9dbc`

Each non-comment line contains a hexadecimal key and value separated by a tab.
The logical records are stored instead of sled's internal database files so the
fixture is independent of the operating system and sled file-layout details.

The generator is intentionally kept beside the fixture. To reproduce it:

```bash
tmp="$(mktemp -d)"
git archive v0.1.0 | tar -x -C "$tmp"
mkdir -p "$tmp/tools"
cp -R crates/nx-core/src/sync_manager/fixtures/v0_1_0/generator \
  "$tmp/tools/legacy-fixture-generator"
cargo run --manifest-path "$tmp/tools/legacy-fixture-generator/Cargo.toml" -- \
  "$PWD/crates/nx-core/src/sync_manager/fixtures/v0_1_0/records.hex"
```

The generator depends on `nx-store` and `nx-sync` from the extracted tag, not
from the current worktree.
