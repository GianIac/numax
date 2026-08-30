#!/usr/bin/env python3
"""Render the converged occupancy heatmap + particle/anchor topology from a
distributed_magnets NUMAX_RENDER=1 log into a single self-contained HTML
file.

The guest never computes a "final layout" itself -- it only exposes the raw
converged PNCounter occupancy grid (`FIELD,x,y,value`) plus `ANCHOR,x,y,weight`
and `PARTICLE,id,x,y,mass` snapshot lines (see src/lib.rs). This script turns
that into a topology report: an occupancy heatmap, anchor/particle markers
sized and colored by weight/mass, and a table of every particle's final
position, mass, and distance to its nearest anchor.

Usage:
    python3 render_topology.py logs/particle-0.log --out topology.html
"""

from __future__ import annotations

import argparse
import html
import re
from dataclasses import dataclass, field

LINE_RE = re.compile(r"(ANCHOR|PARTICLE|FIELD),(-?\d+),(-?\d+),(-?\d+)(?:,(-?\d+))?")
SETTLED_RE = re.compile(r"mag:settled_events\s*=\s*(\d+)")
START_RE = re.compile(r"distributed_magnets: start")
NODE_RE = re.compile(r"node=([0-9a-fA-F-]+)")

# Cool steel/blue sequential ramp for the field plate -- deliberately
# distinct from distributed_ants' warm amber pheromone plate so the two
# reports read as different examples at a glance, while keeping the same
# "printed plate" convention (fixed palette, independent of the page's
# light/dark theme).
HEAT_STOPS = [
    (0.0, (235, 241, 247)),
    (0.5, (74, 130, 180)),
    (1.0, (16, 42, 74)),
]

PARTICLE_COLOR = "#c0392b"
ANCHOR_COLOR = "#1f6f4a"


@dataclass
class World:
    anchors: list[tuple[int, int, int]] = field(default_factory=list)
    particles: dict[int, tuple[int, int, int]] = field(default_factory=dict)
    field_grid: dict[tuple[int, int], int] = field(default_factory=dict)


@dataclass
class RunStats:
    node_id: str | None = None
    ticks: int = 0
    settled_events: int | None = None


def parse_log(path: str) -> tuple[World, RunStats]:
    world = World()
    stats = RunStats()
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if START_RE.search(line):
                stats.ticks += 1
            if m := NODE_RE.search(line):
                stats.node_id = m.group(1)
            if m := SETTLED_RE.search(line):
                stats.settled_events = int(m.group(1))

            m = LINE_RE.search(line)
            if not m:
                continue
            kind, a, b, c, d = m.groups()
            a, b, c = int(a), int(b), int(c)
            if kind == "ANCHOR":
                if not any((ax, ay) == (a, b) for ax, ay, _ in world.anchors):
                    world.anchors.append((a, b, c))
            elif kind == "PARTICLE":
                # PARTICLE,id,x,y,mass
                particle_id, x, y, mass = a, b, c, int(d) if d is not None else 0
                world.particles[particle_id] = (x, y, mass)
            elif kind == "FIELD":
                world.field_grid[(a, b)] = c
    return world, stats


def infer_grid_size(world: World, grid_width: int | None, grid_height: int | None) -> tuple[int, int]:
    if grid_width and grid_height:
        return grid_width, grid_height
    xs = [x for (x, _y) in world.field_grid]
    ys = [y for (_x, y) in world.field_grid]
    return (max(xs) + 1 if xs else 14), (max(ys) + 1 if ys else 14)


def manhattan(a: tuple[int, int], b: tuple[int, int]) -> int:
    return abs(a[0] - b[0]) + abs(a[1] - b[1])


def color_for(value: int, max_value: int) -> str:
    t = 0.0 if max_value <= 0 else min(1.0, value / max_value)
    lo, mid, hi = HEAT_STOPS
    (t0, c0), (t1, c1) = (lo, mid) if t <= mid[0] else (mid, hi)
    span = t1 - t0 or 1.0
    local_t = (t - t0) / span
    r = round(c0[0] + (c1[0] - c0[0]) * local_t)
    g = round(c0[1] + (c1[1] - c0[1]) * local_t)
    b = round(c0[2] + (c1[2] - c0[2]) * local_t)
    return f"rgb({r},{g},{b})"


def build_svg(world: World, grid_w: int, grid_h: int) -> str:
    cell = 34
    pad = 2
    width = grid_w * cell + pad * 2
    height = grid_h * cell + pad * 2
    max_value = max(world.field_grid.values(), default=0)
    max_mass = max((m for _, _, m in world.particles.values()), default=1) or 1

    cells_svg = []
    for y in range(grid_h):
        for x in range(grid_w):
            v = world.field_grid.get((x, y), 0)
            cx = pad + x * cell
            cy = pad + y * cell
            cells_svg.append(
                f'<rect x="{cx}" y="{cy}" width="{cell}" height="{cell}" '
                f'fill="{color_for(v, max_value)}" stroke="#00000014" stroke-width="1" />'
            )

    anchors_svg = []
    for ax, ay, weight in world.anchors:
        cx, cy = pad + ax * cell + cell / 2, pad + ay * cell + cell / 2
        r = 6 + 2.2 * weight
        anchors_svg.append(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" '
            f'stroke="{ANCHOR_COLOR}" stroke-width="2.5" stroke-dasharray="3,2" />'
        )
        anchors_svg.append(
            f'<circle cx="{cx}" cy="{cy}" r="4" fill="{ANCHOR_COLOR}" />'
        )

    particles_svg = []
    for pid, (x, y, mass) in sorted(world.particles.items()):
        cx, cy = pad + x * cell + cell / 2, pad + y * cell + cell / 2
        r = 5 + 6 * min(1.0, mass / max_mass)
        particles_svg.append(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{PARTICLE_COLOR}" '
            f'stroke="#fbf3e4" stroke-width="2" />'
        )
        particles_svg.append(
            f'<text x="{cx}" y="{cy - r - 4}" font-size="9.5" text-anchor="middle" '
            f'fill="{PARTICLE_COLOR}" font-family="\'IBM Plex Mono\', monospace" '
            f'font-weight="600">P{pid}</text>'
        )

    svg = (
        f'<svg width="100%" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Magnetic field occupancy grid with anchors and converged particle positions">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ebf1f7" />'
        f"{''.join(cells_svg)}{''.join(anchors_svg)}{''.join(particles_svg)}"
        f"</svg>"
    )
    return svg


def render_html(world: World, grid_w: int, grid_h: int, stats: RunStats, settle_radius: int) -> str:
    svg = build_svg(world, grid_w, grid_h)

    stat_chips = [
        ("grid", f"{grid_w}×{grid_h}"),
        ("anchors", str(len(world.anchors))),
        ("particles", str(len(world.particles))),
        ("ticks observed", str(stats.ticks)),
    ]
    if stats.settled_events is not None:
        stat_chips.append(("settled events", str(stats.settled_events)))

    chips_html = "".join(
        f'<div class="chip"><span class="chip-label">{html.escape(label)}</span>'
        f'<span class="chip-value">{html.escape(value)}</span></div>'
        for label, value in stat_chips
    )

    rows_html = []
    for pid, (x, y, mass) in sorted(world.particles.items()):
        nearest = min((manhattan((x, y), (ax, ay)) for ax, ay, _ in world.anchors), default=None)
        converged = nearest is not None and nearest <= settle_radius
        status = (
            '<span class="status status-ok">converged</span>'
            if converged
            else '<span class="status status-warn">drifting</span>'
        )
        nearest_display = str(nearest) if nearest is not None else "n/a"
        rows_html.append(
            "<tr>"
            f'<td><span class="swatch" style="background:{PARTICLE_COLOR}"></span>particle {pid}</td>'
            f'<td class="num">({x}, {y})</td>'
            f'<td class="num">{mass}</td>'
            f'<td class="num">{nearest_display}</td>'
            f"<td>{status}</td>"
            "</tr>"
        )
    table_rows = "".join(rows_html) if rows_html else (
        '<tr><td colspan="5" class="muted">no particles observed</td></tr>'
    )

    node_line = (
        f'<span class="mono muted">node {html.escape(stats.node_id[:8])}…</span>'
        if stats.node_id
        else ""
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Magnetic Topology</title>
<style>
  :root {{
    --bg: #eef2f6;
    --surface: #fdfeff;
    --ink: #161c24;
    --muted: #5b6a7a;
    --rule: #d8e1ea;
    --accent: #2f6a92;
    --accent-soft: #dbe9f2;
    --shadow: 0 1px 2px rgba(10, 20, 40, 0.06), 0 8px 24px rgba(10, 20, 40, 0.05);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --bg: #0c1118;
      --surface: #131a24;
      --ink: #e7edf4;
      --muted: #8fa0b3;
      --rule: #26313f;
      --accent: #5fa3d6;
      --accent-soft: #17222f;
      --shadow: 0 1px 2px rgba(0, 0, 0, 0.3), 0 8px 28px rgba(0, 0, 0, 0.35);
    }}
  }}
  :root[data-theme="dark"] {{
    --bg: #0c1118;
    --surface: #131a24;
    --ink: #e7edf4;
    --muted: #8fa0b3;
    --rule: #26313f;
    --accent: #5fa3d6;
    --accent-soft: #17222f;
    --shadow: 0 1px 2px rgba(0, 0, 0, 0.3), 0 8px 28px rgba(0, 0, 0, 0.35);
  }}

  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--bg);
    color: var(--ink);
    font-family: 'IBM Plex Mono', ui-monospace, monospace;
    -webkit-font-smoothing: antialiased;
  }}
  .page {{
    max-width: 880px;
    margin: 0 auto;
    padding: 56px 24px 80px;
    display: flex;
    flex-direction: column;
    gap: 36px;
  }}
  .eyebrow {{
    font-size: 12px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--accent);
    font-weight: 600;
  }}
  h1 {{
    font-family: 'Fraunces', Georgia, serif;
    font-optical-sizing: auto;
    font-weight: 600;
    font-size: clamp(32px, 5vw, 44px);
    line-height: 1.05;
    margin: 10px 0 0;
    text-wrap: balance;
  }}
  .dek {{
    max-width: 62ch;
    color: var(--muted);
    font-size: 15px;
    line-height: 1.6;
    margin: 14px 0 0;
  }}
  .mono {{ font-family: 'IBM Plex Mono', ui-monospace, monospace; }}
  .muted {{ color: var(--muted); }}

  .stats {{
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
  }}
  .chip {{
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 8px;
    padding: 10px 14px;
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 96px;
  }}
  .chip-label {{
    font-size: 10.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
  }}
  .chip-value {{
    font-size: 18px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
  }}

  .plate {{
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 18px;
  }}
  .plate-frame {{
    border-radius: 6px;
    overflow: hidden;
    border: 1px solid #c7d6e3;
  }}
  .plate-caption {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-top: 12px;
    font-size: 12px;
  }}
  .legend {{
    display: flex;
    flex-wrap: wrap;
    gap: 16px;
    font-size: 12px;
    color: var(--muted);
  }}
  .legend-item {{ display: inline-flex; align-items: center; gap: 6px; }}
  .dot {{ width: 9px; height: 9px; border-radius: 50%; display: inline-block; }}
  .swatch {{ width: 9px; height: 9px; display: inline-block; margin-right: 8px; border-radius: 50%; }}

  table {{ width: 100%; border-collapse: collapse; font-size: 13.5px; }}
  th, td {{ text-align: left; padding: 9px 10px; border-bottom: 1px solid var(--rule); }}
  th {{
    font-size: 10.5px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
    font-weight: 600;
  }}
  td.num {{ font-variant-numeric: tabular-nums; }}
  tr:last-child td {{ border-bottom: none; }}

  .status {{
    font-size: 11px;
    padding: 2px 8px;
    border-radius: 999px;
    font-weight: 600;
  }}
  .status-ok {{ background: var(--accent-soft); color: var(--accent); }}
  .status-warn {{ background: var(--rule); color: var(--muted); }}

  .table-card {{
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 6px 18px 4px;
    overflow-x: auto;
  }}

  footer {{
    font-size: 12px;
    color: var(--muted);
    border-top: 1px solid var(--rule);
    padding-top: 18px;
  }}
</style>
</head>
<body>
<div class="page">
  <header>
    <div class="eyebrow">Distributed swarm POC &middot; numax / magnetic optimization</div>
    <h1>Magnetic Topology</h1>
    <p class="dek">
      Every particle is an independent numax node; each publishes its
      position as an <span class="mono">LWW-Register</span> that every other
      particle reads before moving, so lighter particles get pulled toward
      whichever peer or anchor currently has the most magnetic mass. The
      occupancy field below is a <span class="mono">PNCounter</span> per
      grid cell, incremented on entry and decremented on exit &mdash; it
      shows where the swarm is clustered right now, not an accumulated
      trail.
    </p>
    <div class="stats">{chips_html}</div>
  </header>

  <section class="plate">
    <div class="plate-frame">{svg}</div>
    <div class="plate-caption">
      <div class="legend">
        <span class="legend-item"><span class="dot" style="background:{ANCHOR_COLOR}"></span>anchor (dashed ring = weight)</span>
        <span class="legend-item"><span class="dot" style="background:{PARTICLE_COLOR}"></span>particle (size = mass)</span>
        <span class="legend-item muted">cell shade = current occupancy</span>
      </div>
      {node_line}
    </div>
  </section>

  <section class="table-card">
    <table>
      <thead>
        <tr><th>particle</th><th>final position</th><th>mass</th><th>dist. to nearest anchor</th><th>outcome</th></tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </section>

  <footer>
    Rendered by <span class="mono">render_topology.py</span> from a single
    node's <span class="mono">NUMAX_RENDER=1</span> log &mdash; a snapshot of
    one converged replica's view of the swarm, not a live view.
  </footer>
</div>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", help="Path to a node's log captured with NUMAX_RENDER=1")
    parser.add_argument("--grid-width", type=int, default=None)
    parser.add_argument("--grid-height", type=int, default=None)
    parser.add_argument("--settle-radius", type=int, default=1)
    parser.add_argument("--out", default="topology.html")
    args = parser.parse_args()

    world, stats = parse_log(args.log)
    if not world.field_grid:
        raise SystemExit(
            f"no FIELD lines found in {args.log!r} -- rerun demo.sh "
            "(the last tick of particle-0 sets NUMAX_RENDER=1)"
        )

    grid_w, grid_h = infer_grid_size(world, args.grid_width, args.grid_height)

    out_html = render_html(world, grid_w, grid_h, stats, args.settle_radius)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(out_html)

    print(f"wrote {args.out}")
    print(f"anchors={[(a, b) for a, b, _ in world.anchors]}")
    for pid, (x, y, mass) in sorted(world.particles.items()):
        nearest = min((manhattan((x, y), (ax, ay)) for ax, ay, _ in world.anchors), default=None)
        print(f"particle {pid}: pos=({x},{y}) mass={mass} dist_to_nearest_anchor={nearest}")


if __name__ == "__main__":
    main()
