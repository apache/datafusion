#!/usr/bin/env python3
"""
Compare Criterion benchmarks using minimum times from raw samples.

This script extracts the minimum time per iteration from Criterion's raw
sample data, which is more stable than mean/median for comparing performance
(minimums are less affected by CPU throttling, background processes, etc.)

Usage:
    python3 scripts/compare_benchmarks.py [baseline_name] [filter]

Examples:
    python3 scripts/compare_benchmarks.py d68b629dc
    python3 scripts/compare_benchmarks.py d68b629dc Float32
    python3 scripts/compare_benchmarks.py d68b629dc "list=28"
"""

import json
import sys
from pathlib import Path


def load_sample_data(sample_path: Path) -> dict | None:
    """Load sample.json and return iters/times arrays."""
    if not sample_path.exists():
        return None
    with open(sample_path) as f:
        return json.load(f)


def compute_min_time_per_iter(data: dict) -> float:
    """Compute minimum time per iteration in nanoseconds."""
    iters = data["iters"]
    times = data["times"]
    return min(t / i for t, i in zip(times, iters))


def format_time(ns: float) -> str:
    """Format nanoseconds to human-readable string."""
    if ns >= 1e9:
        return f"{ns / 1e9:.3f} s"
    elif ns >= 1e6:
        return f"{ns / 1e6:.3f} ms"
    elif ns >= 1e3:
        return f"{ns / 1e3:.3f} µs"
    else:
        return f"{ns:.3f} ns"


def format_change(old: float, new: float) -> str:
    """Format percentage change with color indicators."""
    if old == 0:
        return "N/A"
    pct = ((new - old) / old) * 100
    if pct < -5:
        return f"\033[32m{pct:+.1f}%\033[0m"  # Green for improvement
    elif pct > 5:
        return f"\033[31m{pct:+.1f}%\033[0m"  # Red for regression
    else:
        return f"{pct:+.1f}%"  # No color for small changes


def main():
    # Parse arguments
    baseline = sys.argv[1] if len(sys.argv) > 1 else None
    filter_str = sys.argv[2] if len(sys.argv) > 2 else None

    if not baseline:
        print("Usage: python3 compare_benchmarks.py <baseline_name> [filter]")
        print("Example: python3 compare_benchmarks.py d68b629dc Float32")
        sys.exit(1)

    # Find criterion directory
    criterion_dir = Path("target/criterion")
    if not criterion_dir.exists():
        print(f"Error: {criterion_dir} not found")
        sys.exit(1)

    # Collect all benchmark directories
    bench_dirs = sorted([d for d in criterion_dir.iterdir() if d.is_dir()])

    # Filter if requested
    if filter_str:
        bench_dirs = [d for d in bench_dirs if filter_str in d.name]

    if not bench_dirs:
        print("No benchmarks found matching filter")
        sys.exit(1)

    # Print header
    print(f"\n{'Benchmark':<55} {'Baseline':>12} {'Current':>12} {'Change':>12}")
    print("-" * 95)

    results = []
    for bench_dir in bench_dirs:
        # Load baseline and new samples
        baseline_sample = load_sample_data(bench_dir / baseline / "sample.json")
        new_sample = load_sample_data(bench_dir / "new" / "sample.json")

        if not baseline_sample or not new_sample:
            continue

        # Get benchmark name from benchmark.json if available
        bench_json = bench_dir / "new" / "benchmark.json"
        if bench_json.exists():
            with open(bench_json) as f:
                bench_name = json.load(f).get("full_id", bench_dir.name)
        else:
            bench_name = bench_dir.name

        # Compute minimum times
        baseline_min = compute_min_time_per_iter(baseline_sample)
        new_min = compute_min_time_per_iter(new_sample)

        results.append({
            "name": bench_name,
            "baseline": baseline_min,
            "new": new_min,
            "change_pct": ((new_min - baseline_min) / baseline_min) * 100
        })

        # Print row
        print(
            f"{bench_name:<55} "
            f"{format_time(baseline_min):>12} "
            f"{format_time(new_min):>12} "
            f"{format_change(baseline_min, new_min):>12}"
        )

    # Print summary
    if results:
        print("-" * 95)
        improved = sum(1 for r in results if r["change_pct"] < -5)
        regressed = sum(1 for r in results if r["change_pct"] > 5)
        unchanged = len(results) - improved - regressed
        print(f"\nSummary: {improved} improved, {regressed} regressed, {unchanged} unchanged (±5% threshold)")

        # Show biggest changes
        sorted_results = sorted(results, key=lambda r: r["change_pct"])
        if sorted_results:
            print(f"\nBiggest improvements:")
            for r in sorted_results[:3]:
                if r["change_pct"] < 0:
                    print(f"  {r['name']}: {r['change_pct']:+.1f}%")

            print(f"\nBiggest regressions:")
            for r in sorted_results[-3:][::-1]:
                if r["change_pct"] > 0:
                    print(f"  {r['name']}: {r['change_pct']:+.1f}%")


if __name__ == "__main__":
    main()
