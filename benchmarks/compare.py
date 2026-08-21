#!/usr/bin/env python
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

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from typing import Dict, List, Any
from pathlib import Path
from argparse import ArgumentParser

try:
    from rich.console import Console
    from rich.table import Table
except ImportError:
    print("Couldn't import modules -- run `./bench.sh venv` first")
    raise


@dataclass
class QueryResult:
    elapsed: float
    row_count: int

    @classmethod
    def load_from(cls, data: Dict[str, Any]) -> QueryResult:
        return cls(elapsed=data["elapsed"], row_count=data["row_count"])


@dataclass
class QueryRun:
    # A number for benchmarks that identify queries by index, a name such as
    # "tpch/Q01/sf1" for the ones run by `benchmark_runner`
    query: int | str
    iterations: List[QueryResult]
    start_time: int
    success: bool = True

    @classmethod
    def load_from(cls, data: Dict[str, Any]) -> QueryRun:
        return cls(
            query=data["query"],
            iterations=[QueryResult(**iteration) for iteration in data["iterations"]],
            start_time=data["start_time"],
            success=data.get("success", True),
        )

    @property
    def label(self) -> str:
        """Row label: "Q3" for numeric query ids, the id itself for named ones."""
        query = str(self.query)

        return f"Q{query}" if query.isdigit() else query

    @property
    def min_execution_time(self) -> float:
        assert len(self.iterations) >= 1

        return min(iteration.elapsed for iteration in self.iterations)


    @property
    def max_execution_time(self) -> float:
        assert len(self.iterations) >= 1

        return max(iteration.elapsed for iteration in self.iterations)


    @property
    def mean_execution_time(self) -> float:
        assert len(self.iterations) >= 1

        total = sum(iteration.elapsed for iteration in self.iterations)
        return total / len(self.iterations)


    @property
    def stddev_execution_time(self) -> float:
        assert len(self.iterations) >= 1

        mean = self.mean_execution_time
        squared_diffs = [(iteration.elapsed - mean) ** 2 for iteration in self.iterations]
        variance = sum(squared_diffs) / len(self.iterations)
        return math.sqrt(variance)

    def execution_time_report(self, detailed = False) -> tuple[float, str]:
        if detailed:
            mean_execution_time = self.mean_execution_time
            return (
                mean_execution_time,
                f"{self.min_execution_time:.2f} / {mean_execution_time :.2f} ±{self.stddev_execution_time:.2f} / {self.max_execution_time:.2f} ms"
            )
        else:
            # Use minimum execution time to account for variations / other
            # things the system was doing
            min_execution_time = self.min_execution_time
            return (
                min_execution_time,
                f"{min_execution_time :.2f} ms"
            )


@dataclass
class Context:
    benchmark_version: str
    datafusion_version: str
    num_cpus: int
    start_time: int
    arguments: List[str]

    @classmethod
    def load_from(cls, data: Dict[str, Any]) -> Context:
        return cls(
            benchmark_version=data["benchmark_version"],
            datafusion_version=data["datafusion_version"],
            num_cpus=data["num_cpus"],
            start_time=data["start_time"],
            arguments=data["arguments"],
        )


@dataclass
class BenchmarkRun:
    context: Context
    queries: List[QueryRun]

    @classmethod
    def load_from(cls, data: Dict[str, Any]) -> BenchmarkRun:
        return cls(
            context=Context.load_from(data["context"]),
            queries=[QueryRun.load_from(result) for result in data["queries"]],
        )

    @classmethod
    def load_from_file(cls, path: Path) -> BenchmarkRun:
        with open(path, "r") as f:
            return cls.load_from(json.load(f))


def compare(
    baseline_path: Path,
    comparison_path: Path,
    noise_threshold: float,
    detailed: bool,
    fail_threshold: float | None = None,
    fail_total_threshold: float | None = None,
) -> int:
    """Print the comparison and return the process exit code.

    The exit code is non-zero only when a `fail_*_threshold` is given and the
    comparison run is slower than it allows.
    """
    baseline = BenchmarkRun.load_from_file(baseline_path)
    comparison = BenchmarkRun.load_from_file(comparison_path)

    console = Console(width=200)

    # use basename as the column names
    baseline_header = baseline_path.parent.name
    comparison_header = comparison_path.parent.name

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Query", style="dim", no_wrap=True)
    table.add_column(baseline_header, justify="right", style="dim", no_wrap=True)
    table.add_column(comparison_header, justify="right", style="dim", no_wrap=True)
    table.add_column("Change", justify="right", style="dim", no_wrap=True)

    faster_count = 0
    slower_count = 0
    no_change_count = 0
    failure_count = 0
    total_baseline_time = 0
    total_comparison_time = 0
    # (label, comparison / baseline) for every query that ran on both sides,
    # and the labels of queries that only the comparison run failed
    changes: List[tuple[str, float]] = []
    new_failures: List[str] = []

    for baseline_result, comparison_result in zip(baseline.queries, comparison.queries):
        assert baseline_result.query == comparison_result.query

        base_failed = not baseline_result.success
        comp_failed = not comparison_result.success
        # If a query fails, its execution time is excluded from the performance comparison
        if base_failed or comp_failed:
            change_text = "incomparable"
            failure_count += 1
            if comp_failed and not base_failed:
                new_failures.append(baseline_result.label)
            table.add_row(
                baseline_result.label,
                "FAIL" if base_failed else baseline_result.execution_time_report(detailed)[1],
                "FAIL" if comp_failed else comparison_result.execution_time_report(detailed)[1],
                change_text,
            )
            continue

        baseline_value, baseline_text = baseline_result.execution_time_report(detailed)
        comparison_value, comparison_text = comparison_result.execution_time_report(detailed)

        total_baseline_time += baseline_value
        total_comparison_time += comparison_value

        change = comparison_value / baseline_value
        changes.append((baseline_result.label, change))

        if (1.0 - noise_threshold) <= change <= (1.0 + noise_threshold):
            change_text = "no change"
            no_change_count += 1
        elif change < 1.0:
            change_text = f"+{(1 / change):.2f}x faster"
            faster_count += 1
        else:
            change_text = f"{change:.2f}x slower"
            slower_count += 1

        table.add_row(
            baseline_result.label,
            baseline_text,
            comparison_text,
            change_text,
        )

    console.print(table)

    # Calculate averages
    avg_baseline_time = 0.0
    avg_comparison_time = 0.0
    if len(baseline.queries) - failure_count > 0:
        avg_baseline_time = total_baseline_time / (len(baseline.queries) - failure_count)
    if len(comparison.queries) - failure_count > 0:
        avg_comparison_time = total_comparison_time / (len(comparison.queries) - failure_count)

    total_change = (
        total_comparison_time / total_baseline_time if total_baseline_time else 1.0
    )

    # Summary table
    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("Benchmark Summary", justify="left", style="dim")
    summary_table.add_column("", justify="right", style="dim")

    summary_table.add_row(f"Total Time ({baseline_header})", f"{total_baseline_time:.2f}ms")
    summary_table.add_row(f"Total Time ({comparison_header})", f"{total_comparison_time:.2f}ms")
    summary_table.add_row(f"Average Time ({baseline_header})", f"{avg_baseline_time:.2f}ms")
    summary_table.add_row(f"Average Time ({comparison_header})", f"{avg_comparison_time:.2f}ms")
    summary_table.add_row("Total Change", f"{total_change:.2f}x")
    summary_table.add_row("Queries Faster", str(faster_count))
    summary_table.add_row("Queries Slower", str(slower_count))
    summary_table.add_row("Queries with No Change", str(no_change_count))
    summary_table.add_row("Queries with Failure", str(failure_count))

    console.print(summary_table)

    return report_regressions(
        console,
        baseline_header,
        comparison_header,
        changes,
        new_failures,
        total_change,
        fail_threshold,
        fail_total_threshold,
    )


def report_regressions(
    console: Console,
    baseline_header: str,
    comparison_header: str,
    changes: List[tuple[str, float]],
    new_failures: List[str],
    total_change: float,
    fail_threshold: float | None,
    fail_total_threshold: float | None,
) -> int:
    """Report against the configured limits, returning the process exit code."""
    if fail_threshold is None and fail_total_threshold is None:
        return 0

    problems: List[str] = []

    if fail_threshold is not None:
        problems.extend(
            f"{label} is {change:.2f}x slower (limit {fail_threshold:.2f}x)"
            for label, change in changes
            if change > fail_threshold
        )

    if fail_total_threshold is not None and total_change > fail_total_threshold:
        problems.append(
            f"total time is {total_change:.2f}x slower "
            f"(limit {fail_total_threshold:.2f}x)"
        )

    # A query that only fails on one side is excluded from the timings above,
    # so it would otherwise pass the gate unnoticed.
    problems.extend(
        f"{label} failed in {comparison_header} but not in {baseline_header}"
        for label in new_failures
    )

    if not problems:
        console.print(
            f"No regression: {comparison_header} is within the configured limits "
            f"of {baseline_header}."
        )
        return 0

    console.print(f"Regression: {comparison_header} is slower than {baseline_header}")
    for problem in problems:
        console.print(f"  - {problem}")

    return 1


def main() -> None:
    parser = ArgumentParser()
    compare_parser = parser
    compare_parser.add_argument(
        "baseline_path",
        type=Path,
        help="Path to the baseline summary file.",
    )
    compare_parser.add_argument(
        "comparison_path",
        type=Path,
        help="Path to the comparison summary file.",
    )
    compare_parser.add_argument(
        "--noise-threshold",
        type=float,
        default=0.05,
        help="The threshold for statistically insignificant results (+/- %5).",
    )
    compare_parser.add_argument(
        "--fail-threshold",
        type=float,
        default=None,
        help="Exit non-zero if any single query is slower than this ratio "
        "(e.g. 1.2 for 20%% slower). Off by default.",
    )
    compare_parser.add_argument(
        "--fail-total-threshold",
        type=float,
        default=None,
        help="Exit non-zero if the total time is slower than this ratio "
        "(e.g. 1.05 for 5%% slower). Off by default.",
    )
    compare_parser.add_argument(
        "--detailed",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Show detailed result comparison instead of minimum runtime.",
    )

    options = parser.parse_args()

    raise SystemExit(
        compare(
            options.baseline_path,
            options.comparison_path,
            options.noise_threshold,
            options.detailed,
            options.fail_threshold,
            options.fail_total_threshold,
        )
    )



if __name__ == "__main__":
    main()
