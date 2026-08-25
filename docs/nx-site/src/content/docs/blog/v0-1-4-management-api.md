---
title: What's coming in Numax v0.1.4
description: The Management API that will become the foundation for the next Numax releases.
---

Hi ! The last two releases focused on the quieter work ...n ecessary work, absolutely. A little less exciting to build? Also yes...

Finally with `v0.1.4`, Numax returns to shipping major runtime capabilities !! 

The release introduces `nx serve`, a daemon that can start without a WASM
module and then be managed through an authenticated REST API.

Underneath, `RuntimeIntrospection` and `RuntimeManagement` will provide the
shared control layer for the CLI, REST API and, later, the dashboard and TUI.
The OpenAPI contract, authentication, pagination and resource limits are part
of the feature.

This makes `v0.1.4` more than an HTTP layer, it's the management foundation
that the next releases will build on: peer discovery, reactive modules, hot
reload and operational tooling.

The foundations are ready. Now we get back to the big features !! :)

**We’re starting this one like a rocket!!!** 
