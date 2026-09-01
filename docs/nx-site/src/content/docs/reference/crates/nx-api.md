---
title: nx-api
description: Authenticated Management API transport and lifecycle.
---

`nx-api` owns the HTTP transport for the Management API. It is deliberately
separate from `nx-core`: HTTP remains an adapter over the shared runtime control interfaces rather than becoming runtime logic.

## Current responsibilities

| Component | Responsibility |
|---|---|
| `ManagementConfig` | Validates the listener, external-bind opt-in, bearer token and request timeout without exposing the secret through `Debug` |
| `ManagementServer::start()` | Binds the dedicated listener and applies authentication, header-read timeout and routed-request timeout |
| `ManagementServer::shutdown()` | Stops accepting connections, drains within the daemon shutdown bound, then aborts and joins residual connection tasks |

The listener is disabled when no bearer token is configured. Its default
address is `127.0.0.1:9102`; non-loopback exposure requires explicit opt-in.

The crate currently exposes no management operations. Authenticated requests
therefore return `404` until the OpenAPI contract and shared
`RuntimeIntrospection` / `RuntimeManagement` interfaces are implemented.
