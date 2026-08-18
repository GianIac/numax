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
