# Wire protocol fuzzing

The fuzz package exercises the production Numax wire decoder through four
focused targets:

- `wire_hello`
- `wire_push_ops`
- `wire_pull_since`
- `wire_framing`

Each message target starts from both JSON and Bincode seeds. The framing target
calls the same asynchronous length-prefix reader used by network connections,
including its size limit and short-read handling. The `PushOps` corpus contains
every current `OpKind` variant.

Pull requests and pushes to `main` run 1,000 iterations per target as a regular
CI smoke test. There is no scheduled fuzzing job.

Install a pinned cargo-fuzz release and run one target with its seed corpus:

```bash
cargo install cargo-fuzz --version 0.13.2 --locked
cargo +nightly fuzz run wire_hello fuzz/corpus/wire_hello -- \
  -max_len=1048576 -timeout=10
```

Crashes are written under `fuzz/artifacts/<target>/`. A minimized input that
reproduces a fixed bug should be added to the corresponding corpus directory.

After changing a JSON seed, regenerate its deterministic Bincode counterpart:

```bash
cargo run --manifest-path fuzz/Cargo.toml --example generate_binary_corpus
```

## Handling crashes

When a fuzz target finds a crash, the following steps explain how to reproduce,
minimize, and report it.

### Artifacts directory

Crashed inputs are stored in:

```
fuzz/artifacts/<target>/
```

Each file in this directory is a raw input that triggered a panic or other
failure in the target. The filenames are assigned by cargo-fuzz and do not
carry semantic meaning — keep the original name when reproducing.

### Reproducing a crash

Run the target directly against the crashing input:

```bash
cargo +nightly fuzz run <target> fuzz/artifacts/<target>/<crash-file> -- \
  -max_len=1048576 -timeout=10
```

This replays the exact input and should produce the same backtrace you saw
during the original fuzz run. If the crash no longer reproduces, the bug
has likely been fixed by an intervening commit.

### Minimizing a crash

`cargo fuzz tmin` strips bytes from the crashing input while preserving the
bug-triggering behavior. A smaller input is easier to read, debug, and add
to a regression test.

```bash
cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/<crash-file> \
  --output minimized.bin
```

The minimized file lands in the current directory. Inspect it to confirm the
reduced input still triggers the same code path:

```bash
cargo +nightly fuzz run <target> minimized.bin -- \
  -max_len=1048576 -timeout=10
```

### When to add to the seed corpus

A minimized input belongs in `fuzz/corpus/<target>/` when it exercises a
code path not covered by the existing seeds — for example, a new branch
condition, a different error path, or an edge case in length-prefix
framing. If the minimized input is a duplicate of an existing seed, there
is no benefit to adding it.

Promote a minimized crash to the corpus with:

```bash
cp minimized.bin fuzz/corpus/<target>/
```

For JSON-based targets, consider whether the same shape can be expressed
as a Bincode seed as well. After changing a JSON seed, regenerate the
deterministic Bincode counterpart:

```bash
cargo run --manifest-path fuzz/Cargo.toml --example generate_binary_corpus
```
