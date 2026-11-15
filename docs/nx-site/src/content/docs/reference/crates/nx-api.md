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
| `ManagementServer::start()` | Binds the listener, attaches a shared runtime control handle and applies authentication and transport limits |
| `ManagementServer::shutdown()` | Stops accepting connections, drains within the daemon shutdown bound, then aborts and joins residual connection tasks |

The listener is disabled when no bearer token is configured. Its default
address is `127.0.0.1:9102`; non-loopback exposure requires explicit opt-in.
Request bodies are capped at 16 MiB and at most 64 authenticated requests run
concurrently. Excess load is rejected immediately with `429` and `Retry-After`.
These are hard upper bounds; embedded users may select lower values through
`ManagementConfig`.

The OpenAPI 3.1 contract lives in `docs/api/openapi.yaml`. It is validated in
the documentation CI before endpoint implementations are accepted.

Endpoint handlers are adapters over `nx-core`'s shared `RuntimeIntrospection`
and `RuntimeManagement` interfaces. They expose module registration, inspection,
one-shot execution and deletion, persistent manual peer registration, peer
observation views, and read-only datastore/probe operations. Unknown routes return a stable JSON `404` response.


## Peer management

`POST /api/v1/peers` delegates to `RuntimeManagement::add_peer`. It accepts a
persistent endpoint candidate and does not confirm connection or authentication.
Its JSON body is limited to 4 KiB, or a smaller global request-body bound.

`GET /api/v1/peers` retains its default active-peer page. The optional
`view=candidates|connections|membership|recovery` selects independent paginated
observations through `RuntimeIntrospection::inspect_peers`. Cursors remain scoped
to their view. Recovery introspection allows one worker-held scan per runtime;
concurrent scans return `503` rather than queueing more blocking work.

See the [Management API guide](/numax/guides/management-api/#add-a-persistent-peer-candidate)
for examples, error responses, restart behavior and unknown outcomes.
