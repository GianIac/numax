---
title: What is coming in Numax v0.1.5 and v0.1.6
description: Two releases to help nx nodes find each other
---

Hello! Now comes a part of the project that I have personally been eagerly awaiting.

Today, you provide Numax with the addresses of its peers. The concept works, but every new machine means another address to manage. For a runtime built around local-first distributed applications, I want bringing a few nodes together to feel much more natural.

The next two releases will take us there, one step at a time.

**`v0.1.5` introduces Peer Discovery.** The plan covers static peer lists,
bootstrapping through a known node, mDNS for the LAN, DNS-SRV, and a peer file that can be updated externally. Existing `--peer` configurations will still have their place, while new deployments will have more ways to get started.

The demo I want is simple: 10 machines on the same LAN, running Numax, that "discover" each other without a single `--peer` flag. Then a CRDT update, and we watch how one node reaches the other nodes.

**`v0.1.6` brings those foundations into SWIM and K-fanout gossip.** Finding a node is the beginning. A growing cluster must also handle nodes joining, connections disappearing, and peers returning after an interruption.

SWIM-style membership and failure detection will help the runtime maintain its view of the group. K-fanout will spread updates through a selected set of peers, with those peers forwarding them further. Periodic reconciliation will help recover operations lost within the supported recovery window.

There is serious work behind that promise: keeping traffic bounded, avoiding duplicate effects, and making sure a busy node does not cause a wave of false failure reports. The roadmap includes a partition-recovery scenario with 50 nodes, packet-loss tests, and rolling restarts. These are goals we have to earn with reproducible results.

These are two important releases for the kind of runtime I want Numax to
become. The [roadmap](/numax/roadmap/) contains the development details.

**It is time to get these nodes talking. Stay tuned!!**