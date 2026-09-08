#!/usr/bin/env python3
"""Generate SVG bar charts for the connectrpc-rs vs tonic vs tonic-protobuf benchmarks.

Reads benchmark data from this file's BENCHMARKS dict (update after
running `task bench:echo -- --multi-conn=8`, `task bench:log` and
`task bench:cross`) and emits SVG charts to benches/charts/ plus a
README-ready markdown table block.

Usage:
    python3 benches/charts/generate.py
"""

from __future__ import annotations
import math
from dataclasses import dataclass
from pathlib import Path

# ── Benchmark data ──────────────────────────────────────────────────────
# Update these after running the benchmarks. Values are requests/sec, except
# the latency block (microseconds). The README tables add descriptive row
# labels and a unary_large row that is kept out of the chart for scale.
# Source: task bench:echo -- --multi-conn=8, task bench:log, task bench:cross
# Machine: c7i.metal-24xl (Intel Xeon Platinum 8488C, bare metal, turbo off),
# 2026-09-08; tonic 0.14.6, tonic-protobuf from grpc-rust @ 7053afcd.

BENCHMARKS = {
    # Echo: 64-byte string, pure framework overhead (8 h2 connections)
    "echo": {
        "title": "Echo (64-byte payload, framework overhead)",
        "unit": "requests/sec",
        "groups": ["c=16", "c=64", "c=256"],
        "series": {
            "connectrpc-rs":  [189_624, 299_826, 270_927],
            "tonic":          [193_164, 301_387, 266_473],
            "tonic-protobuf": [191_938, 299_864, 267_400],
        },
    },
    # Log-ingest: 50 records × ~22KB, decode-heavy (8 h2 connections)
    "log-ingest": {
        "title": "Log ingest (50 records, ~22 KB, decode-heavy)",
        "unit": "requests/sec",
        "groups": ["c=16", "c=64", "c=256"],
        "series": {
            "connectrpc-rs":  [30_660, 75_166, 134_741],
            "tonic":          [27_488, 71_887, 119_628],
            "tonic-protobuf": [30_246, 77_588, 131_365],
        },
    },
    # Single-request latency (criterion, no contention)
    "latency": {
        "title": "Single-request latency (criterion, concurrency=1)",
        "unit": "microseconds (lower is better)",
        "groups": ["unary_small", "unary_logs_50", "client_stream", "server_stream"],
        "series": {
            "connectrpc-rs":  [ 79.6, 211.8, 186.4, 107.5],
            "tonic":          [ 79.7, 297.5, 170.4, 106.1],
            "tonic-protobuf": [ 78.4, 247.3, 160.6, 110.5],
        },
    },
}

# ── Colors (matching buffa's palette) ──────────────────────────────────

COLORS = {
    "connectrpc-rs":        "#4C78A8",  # blue (our primary)
    "tonic":                "#F58518",  # orange
    "tonic-protobuf":       "#54A24B",  # green
}

# ── SVG generation (adapted from buffa/benchmarks/charts/generate.py) ──

@dataclass
class Series:
    name: str
    color: str
    data: list[float]


def _format_value(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f}M"
    if v >= 1000:
        return f"{v / 1000:.0f}k"
    if v < 10:
        return f"{v:.1f}"
    return str(int(v))


def _nice_max(values: list[float]) -> float:
    raw_max = max(values)
    magnitude = 10 ** math.floor(math.log10(raw_max))
    return math.ceil(raw_max / magnitude) * magnitude


def generate_chart(
    title: str, unit: str, groups: list[str], series_list: list[Series]
) -> str:
    bar_h = 22
    bar_gap = 4
    group_gap = 20
    label_w = 130
    chart_left = label_w + 10
    chart_w = 520
    legend_h = 40
    title_h = 30
    top_margin = title_h + legend_h + 10
    bottom_margin = 35

    n_bars = len(series_list)
    group_h = n_bars * (bar_h + bar_gap) - bar_gap + group_gap
    total_chart_h = len(groups) * group_h - group_gap
    svg_h = top_margin + total_chart_h + bottom_margin
    svg_w = chart_left + chart_w + 90

    all_vals = [v for s in series_list for v in s.data]
    max_val = _nice_max(all_vals)
    scale = chart_w / max_val

    n_grid = 5
    grid_step = max_val / n_grid

    lines: list[str] = []
    a = lines.append

    a(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{svg_w}" height="{svg_h}"'
        f' viewBox="0 0 {svg_w} {svg_h}">'
    )
    a("  <style>")
    a(
        "    text { font-family: -apple-system, BlinkMacSystemFont, "
        '"Segoe UI", Helvetica, Arial, sans-serif; }'
    )
    a("    .title { font-size: 16px; font-weight: 600; fill: #24292f; }")
    a("    .label { font-size: 12px; fill: #24292f; }")
    a("    .value { font-size: 11px; fill: #57606a; }")
    a("    .axis-label { font-size: 11px; fill: #57606a; }")
    a("    .legend-text { font-size: 12px; fill: #24292f; }")
    a("    .grid { stroke: #d0d7de; stroke-width: 0.5; }")
    a("  </style>")
    a('  <rect width="100%" height="100%" fill="white"/>')

    a(
        f'  <text x="{svg_w / 2}" y="{title_h - 5}" text-anchor="middle"'
        f' class="title">{title}</text>'
    )

    # Legend
    lx = chart_left
    for s in series_list:
        a(
            f'  <rect x="{lx}" y="{title_h + 5}" width="14" height="14"'
            f' rx="2" fill="{s.color}"/>'
        )
        a(
            f'  <text x="{lx + 18}" y="{title_h + 16}"'
            f' class="legend-text">{s.name}</text>'
        )
        lx += len(s.name) * 7.5 + 32

    # Grid lines
    for i in range(n_grid + 1):
        val = grid_step * i
        x = chart_left + val * scale
        a(
            f'  <line x1="{x:.1f}" y1="{top_margin}"'
            f' x2="{x:.1f}" y2="{top_margin + total_chart_h}" class="grid"/>'
        )
        a(
            f'  <text x="{x:.1f}" y="{top_margin + total_chart_h + 15}"'
            f' text-anchor="middle" class="axis-label">'
            f"{_format_value(round(val))}</text>"
        )

    a(
        f'  <text x="{chart_left + chart_w / 2}"'
        f' y="{svg_h - 5}" text-anchor="middle" class="axis-label">{unit}</text>'
    )

    # Bars
    for gi, grp in enumerate(groups):
        gy = top_margin + gi * group_h
        label_y = gy + (n_bars * (bar_h + bar_gap) - bar_gap) / 2 + 4
        a(
            f'  <text x="{label_w}" y="{label_y:.1f}" text-anchor="end"'
            f' class="label">{grp}</text>'
        )

        for si, s in enumerate(series_list):
            val = s.data[gi]
            by = gy + si * (bar_h + bar_gap)
            bw = max(val * scale, 1)
            a(
                f'  <rect x="{chart_left}" y="{by:.1f}" width="{bw:.1f}"'
                f' height="{bar_h}" rx="2" fill="{s.color}"/>'
            )
            # Value label (format depends on magnitude)
            if val >= 1000:
                label = f"{int(val):,}"
            else:
                label = f"{val:.1f}"
            a(
                f'  <text x="{chart_left + bw + 4:.1f}" y="{by + bar_h / 2 + 4:.1f}"'
                f' class="value">{label}</text>'
            )

    a("</svg>")
    return "\n".join(lines)


# ── README table generation ────────────────────────────────────────────


def _pct(val: float, baseline: float) -> str:
    """Format value with percentage diff vs baseline."""
    if val >= 1000:
        v = f"{int(round(val)):,}"
    else:
        v = f"{val:.1f}"
    diff = (val - baseline) / baseline * 100
    if abs(diff) < 0.5:
        return v
    sign = "+" if diff > 0 else "\u2212"
    return f"{v} ({sign}{abs(diff):.0f}%)"


def generate_readme_tables() -> str:
    sections: list[str] = []

    for key, bench in BENCHMARKS.items():
        series_names = list(bench["series"].keys())
        # Baseline for % comparison is the first series (connectrpc-rs)
        baseline_name = series_names[0]
        baseline_data = bench["series"][baseline_name]

        header = "| Concurrency | " + " | ".join(series_names) + " |"
        sep = "|---|" + "|".join("---:" for _ in series_names) + "|"

        rows: list[str] = []
        for gi, grp in enumerate(bench["groups"]):
            baseline = baseline_data[gi]
            cells = [_pct(bench["series"][s][gi], baseline) for s in series_names]
            rows.append(f"| {grp} | " + " | ".join(cells) + " |")

        # For latency chart, use the group name as the benchmark name column
        if key == "latency":
            header = header.replace("Concurrency", "Benchmark")

        sections.append(header + "\n" + sep + "\n" + "\n".join(rows))

    return "\n\n".join(sections)


# ── Main ────────────────────────────────────────────────────────────────


def main() -> None:
    charts_dir = Path(__file__).parent

    for key, bench in BENCHMARKS.items():
        series_list = [
            Series(name=name, color=COLORS[name], data=vals)
            for name, vals in bench["series"].items()
        ]
        svg = generate_chart(bench["title"], bench["unit"], bench["groups"], series_list)
        path = charts_dir / f"{key}.svg"
        path.write_text(svg + "\n")
        print(f"  wrote {path}")

    readme = generate_readme_tables()
    readme_path = charts_dir / "tables.md"
    readme_path.write_text(readme + "\n")
    print(f"  wrote {readme_path} (copy into README.md Performance section)")


if __name__ == "__main__":
    main()
