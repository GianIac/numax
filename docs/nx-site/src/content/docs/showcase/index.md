---
title: Showcase
description: Projects built with Numax.
---

This page collects projects, experiments, and tools built with Numax.

---

## Submit your project

Built something with Numax? We would love to see it.

Open a pull request that adds an entry to this page. A one-line side project, a weekend experiment,
a demo module, a tool, a course exercise — everything is welcome.
The only requirement is that it uses Numax in some way.

### What to include

Add a new section with this format:

```markdown
### Your Project Name

**Repo:** [github.com/you/your-project](https://github.com/you/your-project)
**What it does:** one or two sentences describing what the project does and which Numax features it uses.
```

That is all. No screenshots required, no minimum lines of code, no production deployment needed.

### How to open the pull request

1. Fork the [numax repository](https://github.com/GianIac/numax).
2. Edit `docs/nx-site/src/content/docs/showcase.md`.
3. Add your entry under the appropriate section below, or add a new section if nothing fits.
4. Open a pull request with the title `showcase: add <your project name>`.

We will merge it with great honor !

---

## Projects

### Distributed Ant Colony

**Created by:** [Alessandro Basile (@abasile-tf)](https://github.com/abasile-tf)

**Project:** [`examples/distributed_ants`](https://github.com/GianIac/numax/tree/main/examples/distributed_ants) · [PR #113](https://github.com/GianIac/numax/pull/113) · [sample report](https://github.com/GianIac/numax/blob/main/examples/distributed_ants/screenshots/swarm-full.png)

**What it does:** Runs a distributed Ant Colony Optimization swarm in which every
Numax node controls an independent ant. The ants coordinate without a leader or
shared server: pheromone deposits and evaporation are modeled as a convergent
`PNCounter` grid, while completed trips are tracked with a swarm-wide `GCounter`.

The example includes a configurable multi-node demo and a standard-library-only
renderer that turns a converged replica into a self-contained HTML pheromone
heatmap with derived paths between the nest and food hotspots.
