# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
# SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

"""Checks the verdicts of `compare.py`. Run with `uv run python -m unittest test_compare`."""

import contextlib
import io
import json
import random
import tempfile
import unittest
from pathlib import Path

import compare

QUERIES = 10
ITERATIONS = 5


def noisy_run(rng: random.Random, scale: float, iterations: int = ITERATIONS) -> dict:
    """Return a run where each iteration has about 30% noise around `scale * 100` ms."""
    return {
        "context": {
            "benchmark_version": "0",
            "datafusion_version": "0",
            "num_cpus": 1,
            "start_time": 0,
            "arguments": [],
        },
        "queries": [
            {
                "query": q,
                "start_time": 0,
                "iterations": [
                    {"elapsed": scale * 100 * rng.uniform(0.7, 1.3), "row_count": 1}
                    for _ in range(iterations)
                ],
            }
            for q in range(1, QUERIES + 1)
        ],
    }


def run_compare(baseline: dict, comparison: dict, **kwargs) -> str:
    with tempfile.TemporaryDirectory() as tmp:
        paths = []
        for name, data in [("base", baseline), ("comp", comparison)]:
            path = Path(tmp) / name / "result.json"
            path.parent.mkdir()
            path.write_text(json.dumps(data))
            paths.append(path)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            compare.compare(paths[0], paths[1], 0.05, False, **kwargs)
        return out.getvalue()


class CompareTest(unittest.TestCase):
    def test_noisy_a_a_reports_no_change(self):
        rng = random.Random(1)
        out = run_compare(noisy_run(rng, 1.0), noisy_run(rng, 1.0))
        self.assertEqual(out.count("no change"), QUERIES, out)
        self.assertNotIn("faster", out.split("Queries Faster")[0])
        self.assertNotIn("slower", out.split("Queries Faster")[0])

    def test_two_x_slowdown_is_reported(self):
        rng = random.Random(2)
        out = run_compare(noisy_run(rng, 1.0), noisy_run(rng, 2.0))
        self.assertEqual(out.count("x slower"), QUERIES, out)
        self.assertNotIn("no change", out.split("Queries Faster")[0])

    def test_two_x_speedup_is_reported(self):
        rng = random.Random(3)
        out = run_compare(noisy_run(rng, 2.0), noisy_run(rng, 1.0))
        self.assertEqual(out.count("x faster"), QUERIES, out)

    def test_single_iteration_uses_threshold(self):
        base = noisy_run(random.Random(4), 1.0, iterations=1)
        comp = json.loads(json.dumps(base))
        comp["queries"][0]["iterations"][0]["elapsed"] *= 1.5
        out = run_compare(base, comp, show_ci=True)
        self.assertIn("1.50x slower", out)
        self.assertEqual(out.count("no change"), QUERIES - 1, out)
        self.assertIn("n/a", out)

    def test_ci_is_reproducible(self):
        b = [100.0, 120.0, 90.0, 130.0, 80.0]
        c = [200.0, 230.0, 180.0, 260.0, 170.0]
        first = compare.bootstrap_median_ratio_ci(b, c)
        self.assertEqual(first, compare.bootstrap_median_ratio_ci(b, c))
        self.assertEqual(first[0], 2.0)
        self.assertGreater(first[1], 1.0)


if __name__ == "__main__":
    unittest.main()
