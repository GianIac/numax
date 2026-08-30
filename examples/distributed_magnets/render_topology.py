#!/usr/bin/env python3
"""Render the adaptive topology story from a distributed_magnets
NUMAX_RENDER=1 log into a single self-contained HTML file.

The guest never computes a "final layout" itself -- it only exposes the raw
converged PNCounter occupancy grid (`FIELD,x,y,value`), the current
effective `ANCHOR,x,y,weight` list, and `PARTICLE,id,x,y,mass` snapshots
(see src/lib.rs). demo.sh's scripted scenario captures several such
snapshots over the run -- one per "phase" of the adaptive weight scenario,
each opened with a `SNAPSHOT,<label>` marker line -- so this script can
render every phase as its own tab, letting you step through how the swarm
re-optimizes each time an anchor's live weight changes.

Usage:
    python3 render_topology.py logs/particle-0.log --out topology.html
"""

from __future__ import annotations

import argparse
import html
import math
import re
import sys
from dataclasses import dataclass, field

# Labels can contain spaces/commas ("phase 2: after 0:18,1:-5"), so this
# captures the rest of the line rather than stopping at the first
# whitespace -- but stops before an embedded ANSI escape or carriage
# return, since concurrent processes writing to the same log file can
# interleave a fragment of another log line onto the same physical line
# with no newline in between.
SNAPSHOT_RE = re.compile(r"SNAPSHOT,([^\x1b\r\n]+)")
LINE_RE = re.compile(r"(ANCHOR|PARTICLE|FIELD),(-?\d+),(-?\d+),(-?\d+)(?:,(-?\d+))?")
BOOST_RE = re.compile(r"BOOST,(-?\d+),(-?\d+)")
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

# Every particle's magnetic polarity is derived from its id parity alone
# (see `is_positive` in src/lib.rs) -- never transmitted in the log -- so
# this mirrors that rule exactly for coloring.
POSITIVE_COLOR = "#c0392b"
NEGATIVE_COLOR = "#1f5fa8"


def particle_color(pid: int) -> str:
    return POSITIVE_COLOR if pid % 2 == 0 else NEGATIVE_COLOR
ANCHOR_COLOR = "#1f6f4a"
BOOST_UP_COLOR = "#1f6f4a"
BOOST_DOWN_COLOR = "#a8283a"


@dataclass
class World:
    anchors: list[tuple[int, int, int]] = field(default_factory=list)
    particles: dict[int, tuple[int, int, int]] = field(default_factory=dict)
    field_grid: dict[tuple[int, int], int] = field(default_factory=dict)


@dataclass
class Phase:
    label: str
    world: World
    tick: int
    settled_events: int | None
    boosts_since_previous: list[tuple[int, int]]


@dataclass
class RunStats:
    node_id: str | None = None
    ticks: int = 0


def parse_log(path: str) -> tuple[list[Phase], RunStats]:
    """Split a log into one `Phase` per `SNAPSHOT,<label>` marker, tracking
    which anchor boosts landed since the previous snapshot so each phase can
    say what just changed."""
    stats = RunStats()
    phases: list[Phase] = []
    current: Phase | None = None
    pending_boosts: list[tuple[int, int]] = []

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except FileNotFoundError:
        sys.exit(f"error: log file not found: {path!r}")
    except OSError as e:
        sys.exit(f"error: could not read {path!r}: {e}")

    for line in lines:
        if START_RE.search(line):
            stats.ticks += 1
        if m := NODE_RE.search(line):
            stats.node_id = m.group(1)
        if m := BOOST_RE.search(line):
            pending_boosts.append((int(m.group(1)), int(m.group(2))))

        if m := SNAPSHOT_RE.search(line):
            current = Phase(
                label=m.group(1).rstrip(),
                world=World(),
                tick=stats.ticks,
                settled_events=None,
                boosts_since_previous=pending_boosts,
            )
            pending_boosts = []
            phases.append(current)
            continue

        if m := SETTLED_RE.search(line):
            if current is not None:
                current.settled_events = int(m.group(1))
            continue

        m = LINE_RE.search(line)
        if not m or current is None:
            continue
        kind, a, b, c, d = m.groups()
        try:
            a, b, c = int(a), int(b), int(c)
        except ValueError:
            continue
        if kind == "ANCHOR":
            if not any((ax, ay) == (a, b) for ax, ay, _ in current.world.anchors):
                current.world.anchors.append((a, b, c))
        elif kind == "PARTICLE":
            particle_id, x, y, mass = a, b, c, int(d) if d is not None else 0
            current.world.particles[particle_id] = (x, y, mass)
        elif kind == "FIELD":
            current.world.field_grid[(a, b)] = c

    return phases, stats


def infer_grid_size(phases: list[Phase], grid_width: int | None, grid_height: int | None) -> tuple[int, int]:
    if grid_width and grid_height:
        return grid_width, grid_height
    xs = [x for phase in phases for (x, _y) in phase.world.field_grid]
    ys = [y for phase in phases for (_x, y) in phase.world.field_grid]
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
    cell = 30
    pad = 2
    width = grid_w * cell + pad * 2
    height = grid_h * cell + pad * 2
    max_value = max(world.field_grid.values(), default=0)

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
        r = 5 + 2.0 * weight
        anchors_svg.append(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="none" '
            f'stroke="{ANCHOR_COLOR}" stroke-width="2.5" stroke-dasharray="3,2" />'
        )
        anchors_svg.append(f'<circle cx="{cx}" cy="{cy}" r="3.5" fill="{ANCHOR_COLOR}" />')
        anchors_svg.append(
            f'<text x="{cx}" y="{cy - r - 4}" font-size="9" text-anchor="middle" '
            f'fill="{ANCHOR_COLOR}" font-family="ui-monospace, monospace" '
            f'font-weight="600">w={weight}</text>'
        )

    # Particles sharing a cell are drawn as separate small dots arranged in
    # a tight ring around the cell center, not one bigger dot -- so a
    # cluster of 6 particles on one anchor visibly reads as 6 particles,
    # not as a single blob whose size you'd have to decode.
    by_cell: dict[tuple[int, int], list[int]] = {}
    for pid, (x, y, _mass) in world.particles.items():
        by_cell.setdefault((x, y), []).append(pid)

    particle_dot_r = 3.2
    cluster_ring_r = cell * 0.24

    particles_svg = []
    for (x, y), pids in by_cell.items():
        cx0, cy0 = pad + x * cell + cell / 2, pad + y * cell + cell / 2
        pids = sorted(pids)
        n = len(pids)
        for k, pid in enumerate(pids):
            if n == 1:
                cx, cy = cx0, cy0
            else:
                angle = 2 * math.pi * k / n
                cx = cx0 + cluster_ring_r * math.cos(angle)
                cy = cy0 + cluster_ring_r * math.sin(angle)
            particles_svg.append(
                f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{particle_dot_r}" '
                f'fill="{particle_color(pid)}" stroke="#fbf3e4" stroke-width="1" />'
            )

    svg = (
        f'<svg width="100%" viewBox="0 0 {width} {height}" '
        f'role="img" aria-label="Magnetic field occupancy grid with anchors and particle positions">'
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ebf1f7" />'
        f"{''.join(cells_svg)}{''.join(anchors_svg)}{''.join(particles_svg)}"
        f"</svg>"
    )
    return svg


def render_phase_panel(
    phase: Phase,
    index: int,
    grid_w: int,
    grid_h: int,
    settle_radius: int,
    anchor_count: int,
    settled_delta: int | None,
) -> str:
    svg = build_svg(phase.world, grid_w, grid_h)

    stat_chips = [
        ("tick", str(phase.tick)),
        ("particles", str(len(phase.world.particles))),
    ]
    if phase.settled_events is not None:
        stat_chips.append(("settled events", str(phase.settled_events)))
    if settled_delta is not None:
        stat_chips.append(("new settles this phase", f"+{settled_delta}"))

    chips_html = "".join(
        f'<div class="chip"><span class="chip-label">{html.escape(label)}</span>'
        f'<span class="chip-value">{html.escape(value)}</span></div>'
        for label, value in stat_chips
    )

    boost_notes = []
    for anchor_idx, delta in phase.boosts_since_previous:
        if anchor_idx >= anchor_count:
            continue
        color = BOOST_UP_COLOR if delta >= 0 else BOOST_DOWN_COLOR
        sign = "+" if delta >= 0 else ""
        boost_notes.append(
            f'<div class="boost-note" style="color:{color}">'
            f"anchor {anchor_idx} weight {sign}{delta} applied before this snapshot</div>"
        )
    boosts_html = "".join(boost_notes)

    rows_html = []
    for pid, (x, y, mass) in sorted(phase.world.particles.items()):
        nearest = min(
            (manhattan((x, y), (ax, ay)) for ax, ay, _ in phase.world.anchors), default=None
        )
        converged = nearest is not None and nearest <= settle_radius
        status = (
            '<span class="status status-ok">converged</span>'
            if converged
            else '<span class="status status-warn">drifting</span>'
        )
        nearest_display = str(nearest) if nearest is not None else "n/a"
        polarity = "+" if pid % 2 == 0 else "−"
        rows_html.append(
            "<tr>"
            f'<td><span class="swatch" style="background:{particle_color(pid)}"></span>'
            f"particle {pid} ({polarity})</td>"
            f'<td class="num">({x}, {y})</td>'
            f'<td class="num">{mass}</td>'
            f'<td class="num">{nearest_display}</td>'
            f"<td>{status}</td>"
            "</tr>"
        )
    table_rows = "".join(rows_html) if rows_html else (
        '<tr><td colspan="5" class="muted">no particles observed in this snapshot</td></tr>'
    )

    return f"""
  <section class="tab-panel" id="panel-{index}">
    <div class="phase-heading">
      <h2>{html.escape(phase.label)}</h2>
      <div class="stats">{chips_html}</div>
    </div>
    {boosts_html}
    <div class="plate">
      <div class="plate-frame">{svg}</div>
      <div class="plate-caption">
        <div class="legend">
          <span class="legend-item"><span class="dot" style="background:{ANCHOR_COLOR}"></span>anchor (dashed ring + label = live effective weight)</span>
          <span class="legend-item"><span class="dot" style="background:{POSITIVE_COLOR}"></span>positive-polarity particle</span>
          <span class="legend-item"><span class="dot" style="background:{NEGATIVE_COLOR}"></span>negative-polarity particle</span>
          <span class="legend-item muted">a cell's dots ring outward as more particles land there; cell shade = current occupancy</span>
        </div>
      </div>
    </div>
    <div class="table-card">
      <table>
        <thead>
          <tr><th>particle</th><th>position</th><th>mass</th><th>dist. to nearest anchor</th><th>outcome</th></tr>
        </thead>
        <tbody>
          {table_rows}
        </tbody>
      </table>
    </div>
  </section>
"""


def build_carousel_css(n: int, seconds_per_phase: float) -> tuple[str, str, float]:
    """Pure-CSS autoplay carousel: for each phase i of n, builds a
    `@keyframes` block that is "on" only during that phase's slice of one
    shared cycle, and a matching one for its progress dot. No JS -- the
    slide switch is a hard cut (two keyframe stops `eps` apart) rather than
    a crossfade, so it reads as a deliberate step, not an animation."""
    total = n * seconds_per_phase
    eps = 0.05
    panel_blocks = []
    dot_blocks = []
    for i in range(n):
        start = i / n * 100
        end = (i + 1) / n * 100
        stops: list[tuple[float, bool]] = []
        if start <= 1e-9:
            stops.append((0.0, True))
        else:
            stops.append((0.0, False))
            stops.append((start, False))
            stops.append((min(start + eps, end), True))
        if end >= 100 - 1e-9:
            stops.append((100.0, True))
        else:
            stops.append((end, True))
            stops.append((min(end + eps, 100.0), False))
            stops.append((100.0, False))

        deduped: list[tuple[float, bool]] = []
        for pct, on in stops:
            if deduped and abs(deduped[-1][0] - pct) < 1e-9:
                deduped[-1] = (pct, on)
            else:
                deduped.append((pct, on))

        panel_kf = "\n".join(
            f"    {pct:.4f}% {{ opacity: {1 if on else 0}; visibility: {'visible' if on else 'hidden'}; }}"
            for pct, on in deduped
        )
        panel_blocks.append(f"  @keyframes phase-cycle-{i} {{\n{panel_kf}\n  }}")

        dot_kf = "\n".join(
            f"    {pct:.4f}% {{ background: {'var(--accent)' if on else 'var(--rule)'}; "
            f"transform: scale({1.4 if on else 1}); }}"
            for pct, on in deduped
        )
        dot_blocks.append(f"  @keyframes dot-cycle-{i} {{\n{dot_kf}\n  }}")

    return "\n".join(panel_blocks), "\n".join(dot_blocks), total


SECONDS_PER_PHASE = 4.5


def render_html(phases: list[Phase], grid_w: int, grid_h: int, stats: RunStats, settle_radius: int) -> str:
    anchor_count = max((len(p.world.anchors) for p in phases), default=0)
    n = len(phases)

    settled_deltas: list[int | None] = []
    prev_settled: int | None = None
    for phase in phases:
        if phase.settled_events is None or prev_settled is None:
            settled_deltas.append(None)
        else:
            settled_deltas.append(max(0, phase.settled_events - prev_settled))
        if phase.settled_events is not None:
            prev_settled = phase.settled_events

    panels = "".join(
        render_phase_panel(
            phase, i, grid_w, grid_h, settle_radius, anchor_count, settled_deltas[i]
        )
        for i, phase in enumerate(phases)
    )

    carousel_css = ""
    dots_html = ""
    if n > 1:
        panel_keyframes_css, dot_keyframes_css, total_duration = build_carousel_css(
            n, SECONDS_PER_PHASE
        )
        panel_animation_css = "\n".join(
            f"  #panel-{i} {{ animation: phase-cycle-{i} {total_duration:.2f}s infinite; }}"
            for i in range(n)
        )
        dot_animation_css = "\n".join(
            f"  #dot-{i} {{ animation: dot-cycle-{i} {total_duration:.2f}s infinite; }}"
            for i in range(n)
        )
        carousel_css = "\n".join(
            [panel_keyframes_css, dot_keyframes_css, panel_animation_css, dot_animation_css]
        )
        dots_html = "".join(f'<span class="carousel-dot" id="dot-{i}"></span>' for i in range(n))

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
    font-family: ui-monospace, "SFMono-Regular", Consolas, monospace;
    -webkit-font-smoothing: antialiased;
  }}
  .page {{
    max-width: 880px;
    margin: 0 auto;
    padding: 56px 24px 80px;
    display: flex;
    flex-direction: column;
    gap: 28px;
  }}
  .eyebrow {{
    font-size: 12px;
    letter-spacing: 0.14em;
    text-transform: uppercase;
    color: var(--accent);
    font-weight: 600;
  }}
  h1 {{
    font-family: Georgia, "Times New Roman", serif;
    font-weight: 600;
    font-size: clamp(32px, 5vw, 44px);
    line-height: 1.05;
    margin: 10px 0 0;
    text-wrap: balance;
  }}
  h2 {{
    font-family: Georgia, "Times New Roman", serif;
    font-weight: 600;
    font-size: 20px;
    margin: 0;
  }}
  .dek {{
    max-width: 62ch;
    color: var(--muted);
    font-size: 15px;
    line-height: 1.6;
    margin: 14px 0 0;
  }}
  .mono {{ font-family: ui-monospace, "SFMono-Regular", Consolas, monospace; }}
  .muted {{ color: var(--muted); }}

  .stats {{ display: flex; flex-wrap: wrap; gap: 10px; }}
  .chip {{
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 8px;
    padding: 8px 12px;
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 88px;
  }}
  .chip-label {{
    font-size: 10px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
  }}
  .chip-value {{ font-size: 16px; font-weight: 600; font-variant-numeric: tabular-nums; }}

  .carousel {{ display: grid; }}
  .tab-panel {{
    grid-area: 1 / 1;
    display: flex;
    flex-direction: column;
    gap: 16px;
    opacity: 1;
    visibility: visible;
  }}

  .carousel-dots {{ display: flex; gap: 8px; align-items: center; }}
  .carousel-dot {{
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--rule);
    display: inline-block;
  }}

{carousel_css}

  .phase-heading {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    flex-wrap: wrap;
    gap: 10px;
  }}

  .boost-note {{
    font-size: 12.5px;
    font-weight: 600;
    background: var(--accent-soft);
    border-radius: 8px;
    padding: 8px 12px;
    width: fit-content;
  }}

  .plate {{
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 16px;
  }}
  .plate-frame {{ border-radius: 6px; overflow: hidden; border: 1px solid #c7d6e3; }}
  .plate-caption {{ margin-top: 10px; font-size: 12px; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 16px; font-size: 12px; color: var(--muted); }}
  .legend-item {{ display: inline-flex; align-items: center; gap: 6px; }}
  .dot {{ width: 9px; height: 9px; border-radius: 50%; display: inline-block; }}
  .swatch {{ width: 9px; height: 9px; display: inline-block; margin-right: 8px; border-radius: 50%; }}

  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--rule); }}
  th {{
    font-size: 10px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
    font-weight: 600;
  }}
  td.num {{ font-variant-numeric: tabular-nums; }}
  tr:last-child td {{ border-bottom: none; }}

  .status {{ font-size: 11px; padding: 2px 8px; border-radius: 999px; font-weight: 600; }}
  .status-ok {{ background: var(--accent-soft); color: var(--accent); }}
  .status-warn {{ background: var(--rule); color: var(--muted); }}

  .table-card {{
    background: var(--surface);
    border: 1px solid var(--rule);
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 4px 16px 2px;
    overflow-x: auto;
  }}

  footer {{ font-size: 12px; color: var(--muted); border-top: 1px solid var(--rule); padding-top: 18px; }}
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
      particle reads before moving, pulled toward whichever peer or anchor
      currently has the most magnetic mass. Anchor weights are adaptive: a
      live <span class="mono">PNCounter</span> boost on top of each anchor's
      base weight can retarget the whole swarm mid-run. The plate below
      rotates through every captured phase on its own, {SECONDS_PER_PHASE:g}s
      each, on a loop -- open this file in a browser and watch it replay the
      adaptation, no interaction needed.
    </p>
    {node_line}
  </header>

  {f'<div class="carousel-dots">{dots_html}</div>' if n > 1 else ""}
  <div class="carousel">
  {panels}
  </div>

  <footer>
    Rendered by <span class="mono">render_topology.py</span> from a single
    node's <span class="mono">NUMAX_RENDER=1</span> log &mdash; {len(phases)}
    snapshot(s) of one replica's view of the swarm over {stats.ticks} observed
    ticks, not a live view.
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

    if args.settle_radius < 0:
        sys.exit("error: --settle-radius must be >= 0")

    phases, stats = parse_log(args.log)
    if not phases:
        sys.exit(
            f"error: no SNAPSHOT sections found in {args.log!r} -- rerun demo.sh "
            "(it sets NUMAX_RENDER=1 + NUMAX_SNAPSHOT_LABEL at each scripted checkpoint)"
        )
    phases_with_data = [p for p in phases if p.world.field_grid]
    if not phases_with_data:
        sys.exit(f"error: no FIELD data found in any snapshot in {args.log!r}")

    grid_w, grid_h = infer_grid_size(phases, args.grid_width, args.grid_height)

    out_html = render_html(phases, grid_w, grid_h, stats, args.settle_radius)
    try:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_html)
    except OSError as e:
        sys.exit(f"error: could not write {args.out!r}: {e}")

    print(f"wrote {args.out} ({len(phases)} phase(s))")
    prev_settled: int | None = None
    for phase in phases:
        print(f"-- {phase.label} (tick {phase.tick}) --")
        if phase.boosts_since_previous:
            for anchor_idx, delta in phase.boosts_since_previous:
                print(f"   boost applied: anchor {anchor_idx} weight {delta:+d}")
        if phase.settled_events is not None:
            if prev_settled is not None:
                print(
                    f"   settled_events={phase.settled_events} "
                    f"(+{max(0, phase.settled_events - prev_settled)} this phase)"
                )
            else:
                print(f"   settled_events={phase.settled_events}")
            prev_settled = phase.settled_events
        print(f"   anchors={[(a, b, w) for a, b, w in phase.world.anchors]}")
        for pid, (x, y, mass) in sorted(phase.world.particles.items()):
            nearest = min(
                (manhattan((x, y), (ax, ay)) for ax, ay, _ in phase.world.anchors), default=None
            )
            print(f"   particle {pid}: pos=({x},{y}) mass={mass} dist_to_nearest_anchor={nearest}")


if __name__ == "__main__":
    main()
