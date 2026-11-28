#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Run a baseline and a branch benchmark with interleaved iterations.

Both commands are `dfbench` invocations that accept `--lockstep-in` and
`--lockstep-out`. This script starts both processes, then for each query it
alternates single iterations between them, in ABBA order:

    pair 0: A B    pair 1: B A    pair 2: A B ...

It stops a query only at a pair boundary, so both sides always have the same
number of iterations. A query stops once it has at least `--iterations` pairs
and one of these holds:

* both sides together used `--max-time` seconds on it,
* the 95% CI of the median per-pair ratio (B / A) is within
  `--target-precision` of the median,
* it reached `--max-iterations` pairs.

Without `--max-time`, each query runs exactly `--iterations` pairs. A single
iteration longer than `--max-time` thus gives one iteration per side when
`--iterations` is 1.

Each process writes its own results file (pass `-o` in each command), so
`compare.py` reads the output as usual.

Example:

    ./interleave.py --iterations 3 --max-time 30 \\
        --baseline "../base/target/release/dfbench sort-pushdown ... -o base.json" \\
        --branch "../target/release/dfbench sort-pushdown ... -o branch.json"
"""

import argparse
import math
import os
import shlex
import subprocess
import sys
import tempfile

MIN_SAMPLES_FOR_CI = 6


def median(values):
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def median_ci_rel_half_width(samples):
    """Distribution-free 95% CI of the median, as a fraction of the median.

    Matches `median_ci_rel_half_width` in `src/util/sampler.rs`.
    """
    n = len(samples)
    if n < MIN_SAMPLES_FOR_CI:
        return None
    s = sorted(samples)
    spread = 0.98 * math.sqrt(n)
    lo = max(0, math.floor(n / 2 - spread))
    hi = min(n - 1, math.ceil(n / 2 + spread))
    m = median(s)
    if m <= 0:
        return 0.0
    return (s[hi] - s[lo]) / 2 / m


class Side:
    def __init__(self, name, command, workdir):
        self.name = name
        to_proc = os.path.join(workdir, f"{name}.in")
        from_proc = os.path.join(workdir, f"{name}.out")
        os.mkfifo(to_proc)
        os.mkfifo(from_proc)
        argv = shlex.split(command) + [
            "--lockstep-in",
            to_proc,
            "--lockstep-out",
            from_proc,
        ]
        self.proc = subprocess.Popen(argv)
        self.to_proc_path = to_proc
        self.from_proc_path = from_proc
        self.writer = None
        self.reader = None

    def open(self):
        # The process opens its input first, then its output. Opening in
        # the same order avoids a deadlock.
        self.writer = open(self.to_proc_path, "w", buffering=1)
        self.reader = open(self.from_proc_path, "r")

    def read_line(self):
        line = self.reader.readline()
        return line.strip() if line else None

    def wait_ready(self):
        """Returns True at `ready`, False when the process has finished."""
        line = self.read_line()
        if line is None:
            return False
        if line != "ready":
            raise RuntimeError(f"{self.name}: expected 'ready', got {line!r}")
        return True

    def run_one(self):
        self.writer.write("run\n")
        line = self.read_line()
        if line is None or not line.startswith("elapsed "):
            raise RuntimeError(f"{self.name}: expected 'elapsed', got {line!r}")
        elapsed = float(line.split()[1])
        if not self.wait_ready():
            raise RuntimeError(f"{self.name}: exited inside a query")
        return elapsed

    def stop(self):
        self.writer.write("stop\n")


def should_stop(args, pairs, spent, ratios):
    if pairs < args.iterations:
        return False
    if args.max_time is None:
        return True
    if pairs >= args.max_iterations or spent >= args.max_time:
        return True
    width = median_ci_rel_half_width(ratios)
    return width is not None and width <= args.target_precision


def run_query(args, a, b, query):
    times = {a.name: [], b.name: []}
    ratios = []
    spent = 0.0
    pairs = 0
    while not should_stop(args, pairs, spent, ratios):
        order = (a, b) if pairs % 2 == 0 else (b, a)
        for side in order:
            elapsed = side.run_one()
            times[side.name].append(elapsed)
            spent += elapsed
        ratios.append(times[b.name][-1] / times[a.name][-1])
        pairs += 1
    a.stop()
    b.stop()
    width = median_ci_rel_half_width(ratios)
    width_text = "n/a" if width is None else f"±{width * 100:.1f}%"
    print(
        f"[interleave] query {query}: {pairs} pairs in {spent:.1f}s, "
        f"median ratio {b.name}/{a.name} {median(ratios):.3f} ({width_text})",
        file=sys.stderr,
    )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--baseline", required=True, help="baseline command")
    parser.add_argument("--branch", required=True, help="branch command")
    parser.add_argument("--iterations", type=int, default=3,
                        help="minimum iterations per side (default: 3)")
    parser.add_argument("--max-time", type=float, default=None,
                        help="seconds per query, summed over both sides")
    parser.add_argument("--target-precision", type=float, default=0.02,
                        help="stop when the ratio CI is within this fraction")
    parser.add_argument("--max-iterations", type=int, default=1000,
                        help="maximum iterations per side (default: 1000)")
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be at least 1")

    with tempfile.TemporaryDirectory() as workdir:
        a = Side("baseline", args.baseline, workdir)
        b = Side("branch", args.branch, workdir)
        try:
            a.open()
            b.open()
            query = 0
            while True:
                a_ready, b_ready = a.wait_ready(), b.wait_ready()
                if not a_ready and not b_ready:
                    break
                if a_ready != b_ready:
                    raise RuntimeError(
                        "the two commands ran a different number of queries"
                    )
                query += 1
                run_query(args, a, b, query)
        finally:
            for side in (a, b):
                if side.writer:
                    side.writer.close()
            codes = [a.proc.wait(), b.proc.wait()]
    if any(codes):
        sys.exit(f"benchmark exit codes: baseline={codes[0]} branch={codes[1]}")


if __name__ == "__main__":
    main()
