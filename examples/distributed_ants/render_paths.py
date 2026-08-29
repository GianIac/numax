#!/usr/bin/env python3
"""Render the pheromone heatmap + derived shortest paths from a
distributed_ants NUMAX_RENDER=1 log into a single self-contained HTML file.

The guest never runs pathfinding itself -- it only exposes the raw
converged PNCounter grid (`PHERO,x,y,value`) plus `NEST,x,y` /
`HOTSPOT,x,y` markers as log lines (see src/lib.rs). This script turns that
emergent pheromone trail into an explicit "shortest path" for every pair
of points of interest (nest-to-hotspot and hotspot-to-hotspot) by greedy
gradient ascent, purely for display -- it does not feed back into the
simulation.

Usage:
    python3 render_paths.py logs/ant-0.log --out swarm.html
"""

from __future__ import annotations

import argparse
import html
import itertools
import re
from dataclasses import dataclass, field

LINE_RE = re.compile(r"(NEST|HOTSPOT|PHERO),(-?\d+),(-?\d+)(?:,(-?\d+))?")
TRIPS_RE = re.compile(r"ant:trips_completed\s*=\s*(\d+)")
START_RE = re.compile(r"distributed_ants: start")
NODE_RE = re.compile(r"node=([0-9a-fA-F-]+)")

PATH_COLORS = [
    "#c05a12", "#2f6a92", "#4f7a3a", "#8a5aa8", "#1a8a8a",
    "#b8862e", "#6b4fa0", "#3a8f5c", "#a0455a", "#4f6a9c",
]

# Warm sequential ramp for the pheromone plate: parchment -> amber -> brick.
# Chosen once and reused for every cell so the "figure plate" reads as a
# single deliberate print regardless of the surrounding page's light/dark
# theme (the plate intentionally does not follow the page's theme tokens,
# the way a printed field map keeps its own paper tone).
HEAT_STOPS = [
    (0.0, (251, 243, 228)),
    (0.5, (232, 130, 47)),
    (1.0, (122, 43, 18)),
]


@dataclass
class World:
    nest: tuple[int, int] | None = None
    hotspots: list[tuple[int, int]] = field(default_factory=list)
    pheromone: dict[tuple[int, int], int] = field(default_factory=dict)


@dataclass
class RunStats:
    node_id: str | None = None
    ticks: int = 0
    trips_completed: int | None = None


def parse_log(path: str) -> tuple[World, RunStats]:
    world = World()
    stats = RunStats()
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if START_RE.search(line):
                stats.ticks += 1
            if m := NODE_RE.search(line):
                stats.node_id = m.group(1)
            if m := TRIPS_RE.search(line):
                stats.trips_completed = int(m.group(1))

            m = LINE_RE.search(line)
            if not m:
                continue
            kind, x, y, v = m.groups()
            x, y = int(x), int(y)
            if kind == "NEST":
                world.nest = (x, y)
            elif kind == "HOTSPOT":
                if (x, y) not in world.hotspots:
                    world.hotspots.append((x, y))
            elif kind == "PHERO":
                world.pheromone[(x, y)] = int(v) if v is not None else 0
    return world, stats


def infer_grid_size(world: World, grid_width: int | None, grid_height: int | None) -> tuple[int, int]:
    if grid_width and grid_height:
        return grid_width, grid_height
    xs = [x for (x, _y) in world.pheromone] + [world.nest[0] if world.nest else 0]
    ys = [y for (_x, y) in world.pheromone] + [world.nest[1] if world.nest else 0]
    return (max(xs) + 1 if xs else 12), (max(ys) + 1 if ys else 12)


def neighbors(pos: tuple[int, int], w: int, h: int) -> list[tuple[int, int]]:
    x, y = pos
    out = []
    if x > 0:
        out.append((x - 1, y))
    if x + 1 < w:
        out.append((x + 1, y))
    if y > 0:
        out.append((x, y - 1))
    if y + 1 < h:
        out.append((x, y + 1))
    return out


def manhattan(a: tuple[int, int], b: tuple[int, int]) -> int:
    return abs(a[0] - b[0]) + abs(a[1] - b[1])


def gradient_path(
    start: tuple[int, int],
    goal: tuple[int, int],
    pheromone: dict[tuple[int, int], int],
    grid_w: int,
    grid_h: int,
) -> list[tuple[int, int]]:
    """Greedy walk from start to goal: prefer the neighbor with the most
    pheromone among those that make progress toward goal; break ties (or a
    flat/zero trail) by picking the neighbor closest to goal."""
    path = [start]
    visited = {start}
    current = start
    step_cap = 3 * (grid_w + grid_h)

    for _ in range(step_cap):
        if current == goal:
            break
        cands = [n for n in neighbors(current, grid_w, grid_h) if n not in visited]
        if not cands:
            cands = neighbors(current, grid_w, grid_h)
        cur_dist = manhattan(current, goal)
        progressing = [n for n in cands if manhattan(n, goal) < cur_dist] or cands

        def score(n: tuple[int, int]) -> tuple[int, int]:
            return (pheromone.get(n, 0), -manhattan(n, goal))

        nxt = max(progressing, key=score)
        path.append(nxt)
        visited.add(nxt)
        current = nxt

    return path


@dataclass
class Connection:
    label: str
    a: tuple[int, int]
    b: tuple[int, int]
    path: list[tuple[int, int]]


def build_connections(world: World, grid_w: int, grid_h: int) -> list[Connection]:
    """Every pairwise connection between the nest and every hotspot, *and*
    between hotspots themselves -- not just nest-radiating spokes. Ants
    never walk hotspot-to-hotspot directly, so those legs are read off the
    same converged pheromone field the nest-hotspot trails reinforced,
    rather than a route the swarm specifically laid down; still a
    meaningful shortest-path reading of the field, just a weaker signal."""
    points: list[tuple[str, tuple[int, int]]] = []
    if world.nest:
        points.append(("nest", world.nest))
    for i, h in enumerate(world.hotspots):
        points.append((f"hotspot {i + 1}", h))

    connections = []
    for (label_a, a), (label_b, b) in itertools.combinations(points, 2):
        path = gradient_path(a, b, world.pheromone, grid_w, grid_h)
        connections.append(Connection(f"{label_a} ↔ {label_b}", a, b, path))
    return connections


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


def build_svg(world: World, grid_w: int, grid_h: int, connections: list[Connection]) -> tuple[str, int, int]:
    cell = 34
    pad = 2
    width = grid_w * cell + pad * 2
    height = grid_h * cell + pad * 2
    max_value = max(world.pheromone.values(), default=0)

    cells_svg = []
    for (x, y), v in world.pheromone.items():
        cx = pad + x * cell
        cy = pad + y * cell
        cells_svg.append(
            f'<rect x="{cx}" y="{cy}" width="{cell}" height="{cell}" '
            f'fill="{color_for(v, max_value)}" stroke="#00000014" stroke-width="1" />'
        )
        if v > max_value * 0.08:
            text_fill = "#2a1608" if v < max_value * 0.55 else "#fbf1e0"
            cells_svg.append(
                f'<text x="{cx + cell / 2}" y="{cy + cell / 2 + 3.5}" '
                f'font-size="9.5" text-anchor="middle" fill="{text_fill}" '
                f'font-family="\'IBM Plex Mono\', monospace">{v}</text>'
            )

    paths_svg = []
    for i, conn in enumerate(connections):
        color = PATH_COLORS[i % len(PATH_COLORS)]
        points = " ".join(
            f"{pad + x * cell + cell / 2},{pad + y * cell + cell / 2}" for (x, y) in conn.path
        )
        paths_svg.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" '
            f'stroke-width="3.5" stroke-linejoin="round" stroke-linecap="round" opacity="0.92" />'
        )
        paths_svg.append(
            f'<polyline points="{points}" fill="none" stroke="#1a120a" '
            f'stroke-width="5.5" stroke-linejoin="round" stroke-linecap="round" opacity="0.12" />'
        )

    markers_svg = []
    if world.nest:
        nxx, nyy = world.nest
        cx, cy = pad + nxx * cell + cell / 2, pad + nyy * cell + cell / 2
        markers_svg.append(
            f'<circle cx="{cx}" cy="{cy}" r="{cell / 3.1}" fill="#1f6f4a" '
            f'stroke="#fbf3e4" stroke-width="2.5" />'
        )
        markers_svg.append(
            f'<text x="{cx}" y="{cy + 3}" font-size="10" text-anchor="middle" '
            f'fill="#fbf3e4" font-family="\'IBM Plex Mono\', monospace" font-weight="600">N</text>'
        )
    for hx, hy in world.hotspots:
        cx, cy = pad + hx * cell + cell / 2, pad + hy * cell + cell / 2
        markers_svg.append(
            f'<circle cx="{cx}" cy="{cy}" r="{cell / 3.1}" fill="#a8283a" '
            f'stroke="#fbf3e4" stroke-width="2.5" />'
        )
        markers_svg.append(
            f'<text x="{cx}" y="{cy + 3}" font-size="10" text-anchor="middle" '
            f'fill="#fbf3e4" font-family="\'IBM Plex Mono\', monospace" font-weight="600">H</text>'
        )

    svg = (
        f'<svg width="100%" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Pheromone concentration grid with derived shortest paths">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#fbf3e4" />'
        f"{''.join(cells_svg)}{''.join(paths_svg)}{''.join(markers_svg)}"
        f"</svg>"
    )
    return svg, width, height


def render_html(
    world: World,
    grid_w: int,
    grid_h: int,
    connections: list[Connection],
    stats: RunStats,
) -> str:
    svg, _w, _h = build_svg(world, grid_w, grid_h, connections)

    stat_chips = [
        ("grid", f"{grid_w}×{grid_h}"),
        ("hotspots", str(len(world.hotspots))),
        ("ticks observed", str(stats.ticks)),
    ]
    if stats.trips_completed is not None:
        stat_chips.append(("round trips", str(stats.trips_completed)))

    chips_html = "".join(
        f'<div class="chip"><span class="chip-label">{html.escape(label)}</span>'
        f'<span class="chip-value">{html.escape(value)}</span></div>'
        for label, value in stat_chips
    )

    rows_html = []
    for i, conn in enumerate(connections):
        reached = conn.path[-1] == conn.b
        color = PATH_COLORS[i % len(PATH_COLORS)]
        status = (
            '<span class="status status-ok">reached</span>'
            if reached
            else '<span class="status status-warn">step cap</span>'
        )
        rows_html.append(
            "<tr>"
            f'<td><span class="swatch" style="background:{color}"></span>{html.escape(conn.label)}</td>'
            f"<td class=\"num\">({conn.a[0]}, {conn.a[1]}) &rarr; ({conn.b[0]}, {conn.b[1]})</td>"
            f'<td class="num">{len(conn.path) - 1}</td>'
            f"<td>{status}</td>"
            "</tr>"
        )
    table_rows = "".join(rows_html) if rows_html else (
        '<tr><td colspan="4" class="muted">no hotspots configured</td></tr>'
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
<title>Pheromone Field</title>
<style>
  :root {{
    --bg: #f6f1e6;
    --surface: #fffdf8;
    --ink: #241f14;
    --muted: #786c56;
    --rule: #e2d6bd;
    --accent: #c05a12;
    --accent-soft: #f0dbbb;
    --shadow: 0 1px 2px rgba(40, 28, 10, 0.06), 0 8px 24px rgba(40, 28, 10, 0.05);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --bg: #14110b;
      --surface: #1d180f;
      --ink: #f0e6d4;
      --muted: #a5977c;
      --rule: #392e1d;
      --accent: #e8822f;
      --accent-soft: #3b2313;
      --shadow: 0 1px 2px rgba(0, 0, 0, 0.3), 0 8px 28px rgba(0, 0, 0, 0.35);
    }}
  }}
  :root[data-theme="dark"] {{
    --bg: #14110b;
    --surface: #1d180f;
    --ink: #f0e6d4;
    --muted: #a5977c;
    --rule: #392e1d;
    --accent: #e8822f;
    --accent-soft: #3b2313;
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
    border: 1px solid #d8c9a8;
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
  .swatch {{ width: 16px; height: 3px; display: inline-block; margin-right: 8px; border-radius: 2px; }}

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
    <div class="eyebrow">Distributed swarm POC &middot; numax / ant colony</div>
    <h1>Pheromone Field</h1>
    <p class="dek">
      Every ant is an independent numax node; the trail below is a
      <span class="mono">PNCounter</span> per grid cell, merged by CRDT
      convergence with no coordinator. Paths are a gradient-ascent reading
      of that converged trail, drawn here for display only &mdash; the
      swarm itself never sees them.
    </p>
    <div class="stats">{chips_html}</div>
  </header>

  <section class="plate">
    <div class="plate-frame">{svg}</div>
    <div class="plate-caption">
      <div class="legend">
        <span class="legend-item"><span class="dot" style="background:#1f6f4a"></span>nest</span>
        <span class="legend-item"><span class="dot" style="background:#a8283a"></span>hotspot</span>
        <span class="legend-item muted">cell shade &amp; label = converged pheromone count</span>
      </div>
      {node_line}
    </div>
  </section>

  <section class="table-card">
    <table>
      <thead>
        <tr><th>trail</th><th>connects</th><th>steps</th><th>outcome</th></tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </section>

  <footer>
    Rendered by <span class="mono">render_paths.py</span> from a single node's
    <span class="mono">NUMAX_RENDER=1</span> log &mdash; a snapshot of one
    converged replica, not a live view of the swarm.
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
    parser.add_argument("--out", default="swarm.html")
    args = parser.parse_args()

    world, stats = parse_log(args.log)
    if not world.pheromone:
        raise SystemExit(
            f"no PHERO lines found in {args.log!r} -- rerun demo.sh "
            "(the last tick of ant-0 sets NUMAX_RENDER=1)"
        )

    grid_w, grid_h = infer_grid_size(world, args.grid_width, args.grid_height)
    nest = world.nest or (0, 0)
    connections = build_connections(world, grid_w, grid_h)

    out_html = render_html(world, grid_w, grid_h, connections, stats)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(out_html)

    print(f"wrote {args.out}")
    print(f"nest={nest} hotspots={world.hotspots}")
    for conn in connections:
        reached = "reached" if conn.path[-1] == conn.b else "did not reach (step cap hit)"
        print(f"{conn.label}: {len(conn.path)} steps, {reached}")


if __name__ == "__main__":
    main()
