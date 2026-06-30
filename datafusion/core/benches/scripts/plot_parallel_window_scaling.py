#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Plot the weak-scaling sweep emitted by `parallel_window_scaling.rs`.

The bench emits one CSV row per iteration with header
`cores,with_poc,iter,seconds,rows`. Rows scale linearly with cores, so
under perfect scaling the PoC's wall-clock stays constant and its
throughput grows linearly with cores; the baseline's wall-clock grows
linearly with cores (no parallelism ⇒ pure serial work) and its
throughput stays flat at single-core capacity.

Usage:
    cargo bench --bench parallel_window_scaling > scaling.csv
    python3 plot_parallel_window_scaling.py scaling.csv [--out file.png]
"""

import argparse
import csv
import statistics
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")  # headless PNG only
import matplotlib.pyplot as plt


def read_rows(source):
    reader = csv.DictReader(source)
    seconds = defaultdict(list)
    rows_for = {}
    for row in reader:
        cores = int(row["cores"])
        with_poc = row["with_poc"].lower() == "true"
        secs = float(row["seconds"])
        n_rows = int(row["rows"])
        seconds[(cores, with_poc)].append(secs)
        rows_for[cores] = n_rows
    return seconds, rows_for


def medians(seconds):
    out = defaultdict(dict)
    for (cores, with_poc), samples in seconds.items():
        out[with_poc][cores] = statistics.median(samples)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="CSV path; use '-' for stdin")
    ap.add_argument(
        "--out",
        default="parallel_window_scaling.png",
        help="output PNG path (default: parallel_window_scaling.png)",
    )
    args = ap.parse_args()

    source = sys.stdin if args.csv == "-" else open(args.csv)
    seconds, rows_for = read_rows(source)
    if args.csv != "-":
        source.close()

    by_poc = medians(seconds)
    baseline = by_poc[False]
    parallel = by_poc[True]
    cores = sorted(set(baseline) | set(parallel))

    fig, (ax_time, ax_throughput) = plt.subplots(
        1, 2, figsize=(13, 5), constrained_layout=True
    )

    # Wall-clock vs cores (weak scaling: rows grow with cores).
    bx = sorted(baseline)
    by = [baseline[c] for c in bx]
    px = sorted(parallel)
    py = [parallel[c] for c in px]
    ideal_baseline = [by[0] * (c / bx[0]) for c in bx]
    ax_time.plot(
        bx,
        ideal_baseline,
        linestyle="--",
        color="grey",
        label="ideal serial (y = x · t₁)",
    )
    ax_time.plot(bx, by, marker="o", color="C0", label="ParallelWindow off (baseline)")
    ax_time.plot(px, py, marker="s", color="C1", label="ParallelWindow on (this PR)")
    ax_time.set_xscale("log", base=2)
    ax_time.set_xticks(cores)
    ax_time.set_xticklabels([str(c) for c in cores])
    ax_time.set_xlabel("cores  (= target_partitions = input partitions)")
    ax_time.set_ylabel("wall-clock (seconds)")
    ax_time.set_title("Wall-clock vs cores (weak scaling)")
    ax_time.grid(True, which="both", alpha=0.3)
    ax_time.legend(loc="upper left")

    # Throughput vs cores: under linear scaling PoC follows y = x · t1
    # (matching the dashed reference).
    bt = [rows_for[c] / baseline[c] / 1e6 for c in bx]
    pt = [rows_for[c] / parallel[c] / 1e6 for c in px]
    ideal_parallel = [pt[0] * (c / px[0]) for c in px]
    ax_throughput.plot(
        px,
        ideal_parallel,
        linestyle="--",
        color="grey",
        label="ideal linear scaling (y = x · t₁)",
    )
    ax_throughput.plot(
        bx, bt, marker="o", color="C0", label="ParallelWindow off (baseline)"
    )
    ax_throughput.plot(
        px, pt, marker="s", color="C1", label="ParallelWindow on (this PR)"
    )
    ax_throughput.set_xscale("log", base=2)
    ax_throughput.set_yscale("log", base=2)
    ax_throughput.set_xticks(cores)
    ax_throughput.set_xticklabels([str(c) for c in cores])
    ax_throughput.set_xlabel("cores  (= target_partitions = input partitions)")
    ax_throughput.set_ylabel("throughput (million rows / second, log₂)")
    ax_throughput.set_title("Throughput vs cores")
    ax_throughput.grid(True, which="both", alpha=0.3)
    ax_throughput.legend(loc="upper left")

    rows_per_core = next(iter(rows_for.values())) // cores[0] if cores else 0
    fig.suptitle(
        f"ParallelWindow weak-scaling — {rows_per_core:,} rows per core, "
        f"5 window aggregates over RANGE BETWEEN 100 PRECEDING AND CURRENT ROW"
    )
    fig.savefig(args.out, dpi=120)
    print(f"wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
