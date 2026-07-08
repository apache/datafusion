#!/usr/bin/env python3
"""Compare interleaved A/B dfbench results: per-query min(off rounds) vs min(on rounds)."""
import json
import sys
from pathlib import Path

OUT = Path("/tmp/datafusion-batch-normalizer/ab_results")


def load_mins(suite: str, cfg: str) -> dict[str, float]:
    mins: dict[str, float] = {}
    for f in sorted(OUT.glob(f"{suite}_{cfg}_r*.json")):
        d = json.load(open(f))
        for q in d["queries"]:
            if not q.get("success", True):
                mins[q["query"]] = float("nan")
                continue
            m = min(i["elapsed"] for i in q["iterations"])
            k = q["query"]
            mins[k] = min(mins.get(k, float("inf")), m)
    return mins


def main() -> None:
    for suite in sys.argv[1:] or ["tpch", "clickbench", "tpcds"]:
        off, on = load_mins(suite, "off"), load_mins(suite, "on")
        if not off or not on:
            print(f"== {suite}: missing results ==")
            continue
        print(f"\n== {suite} (per-query min across rounds, off vs on) ==")
        tot_off = tot_on = 0.0
        worst: list[tuple[float, str, float, float]] = []
        for q in off:
            if q not in on:
                continue
            a, b = off[q], on[q]
            tot_off += a
            tot_on += b
            worst.append((b / a if a > 0 else 1.0, q, a, b))
        worst.sort(reverse=True)
        print(f"total: off={tot_off:.0f}ms on={tot_on:.0f}ms ratio={tot_on / tot_off:.3f}")
        print("largest regressions (on/off):")
        for r, q, a, b in worst[:5]:
            print(f"  {q}: {a:.1f} -> {b:.1f} ms  ({r:.2f}x)")
        print("largest improvements (on/off):")
        for r, q, a, b in worst[-5:]:
            print(f"  {q}: {a:.1f} -> {b:.1f} ms  ({r:.2f}x)")


if __name__ == "__main__":
    main()
