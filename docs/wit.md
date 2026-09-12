# WIT host API exploration

Numax currently exposes guest capabilities as raw WebAssembly imports in the
`nx` namespace. The contract is implemented manually in `nx-core`, wrapped by
`nx-sdk`, and documented in the
[host API reference](nx-site/src/content/docs/reference/host-api.md).

[Issue #57](https://github.com/GianIac/numax/issues/57) explores whether
[WebAssembly Interface Types (WIT)](https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md)
and the Component Model could eventually provide one machine-readable source of
truth for that contract. Generated bindings could make additional guest
languages easier to support and reduce drift between the host, SDKs, and docs.

## Current status

The files under `wit/` are discussion skeletons only. They are not connected to
the Numax runtime, do not generate `nx-sdk`, and do not change the raw guest ABI.
Existing core Wasm modules continue to import functions such as `nx.db_get` and
`nx.host_log_v2` directly.

The skeleton deliberately covers only three small capability groups:

- `numax:db` sketches basic key/value operations;
- `numax:log` explores structured log levels that the current ABI does not have;
- `numax:crdt` sketches GCounter operations, not the full CRDT surface.

## Reading the files

Each file contains an independent WIT `package` with a semantic version. An
`interface` groups related functions and types. A `world` describes the imports
and exports available to a component; these guest worlds import a single Numax
interface and export nothing.

For example, `numax-db-guest` imports the `db` interface. The `string` and
`list<u8>` parameters are Component Model values rather than the raw pointer and
length pairs used by today's core Wasm ABI. A future component adapter or native
Component Model runtime path would be responsible for lowering those values.

Use `wasm-tools` to parse and normalize a draft independently:

```sh
wasm-tools component wit wit/numax-db.wit
wasm-tools component wit wit/numax-log.wit
wasm-tools component wit wit/numax-crdt.wit
```

The files are separate packages, so validating them one at a time is
intentional. See the Component Model's
[WIT documentation](https://component-model.bytecodealliance.org/design/wit.html)
for a broader introduction to packages, interfaces, worlds, and resources.

## Open questions

- **Migration and dual ABI:** Should WIT begin as a living specification over
  the existing raw ABI, or should Numax add a Component Model execution path?
  How long would both ABIs need to coexist?
- **Adapters:** Can generated adapters preserve compatibility for existing
  `nx-sdk` guests, and where should canonical ABI lifting and lowering happen?
- **World structure:** Are per-interface worlds the right capability boundary,
  or should Numax also publish an aggregated `numax-guest` world? An aggregated
  world is convenient but couples every guest to the full API surface.
- **Errors:** Should interfaces use strings, a shared error variant matching the
  current numeric codes, or interface-specific error types?
- **Values and resources:** Is `list<u8>` sufficient for database values, or
  would resources provide safer ownership and streaming semantics?
- **Generated SDKs:** Which languages and toolchains should be treated as
  compatibility gates before WIT becomes authoritative?
- **Versioning:** Should database, logging, and CRDT packages evolve
  independently, and what compatibility policy should apply before 1.0?

These questions are intentionally unresolved. The skeleton exists to make them
concrete enough for experiments and community review without committing Numax
to a runtime migration.
