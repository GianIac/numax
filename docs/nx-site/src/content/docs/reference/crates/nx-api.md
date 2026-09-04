---
title: nx-api
description: Authenticated Management API transport and lifecycle.
---

`nx-api` owns the HTTP transport for the Management API. It is deliberately
separate from `nx-core`: HTTP remains an adapter over the shared runtime control interfaces rather than becoming runtime logic.

## Current responsibilities

| Component | Responsibility |
|---|---|
| `ManagementConfig` | Validates the listener, authentication and transport limits without exposing the bearer token through `Debug` |
| `ManagementServer::start()` | Binds the listener and applies authentication, body/header/request limits and concurrency admission |
| `ManagementServer::shutdown()` | Stops accepting connections, drains within the daemon shutdown bound, then aborts and joins residual connection tasks |

The listener is disabled when no bearer token is configured. Its default
address is `127.0.0.1:9102`; non-loopback exposure requires explicit opt-in.
Request bodies are capped at 16 MiB and at most 64 authenticated requests run
concurrently. Excess load is rejected immediately with `429` and `Retry-After`.
These are hard upper bounds; embedded users may select lower values through
`ManagementConfig`.

The OpenAPI 3.1 contract lives in `docs/api/openapi.yaml`. It is validated in
the documentation CI before endpoint implementations are accepted.

The crate currently exposes no management operations. Authenticated requests
therefore return `404` until the shared `RuntimeIntrospection` /
`RuntimeManagement` interfaces and the specified handlers are implemented.
