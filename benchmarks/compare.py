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
from typing import Dict, List, Any, Sequence
from pathlib import Path
from argparse import ArgumentParser

try:
    from rich.console import Console
    from rich.table import Table
except ImportError:
    print("Couldn't import modules -- run with uv (`uv run compare.py`)")
    raise


def median(values: Sequence[float]) -> float:
    """The median of a non-empty sequence."""
    ordered = sorted(values)
    middle = len(ordered) // 2

    if len(ordered) % 2 == 1:
        return ordered[middle]

    return (ordered[middle - 1] + ordered[middle]) / 2


def geometric_mean(values: Sequence[float]) -> float:
    """The geometric mean of a non-empty sequence of positive numbers."""
    return math.exp(sum(math.log(value) for value in values) / len(values))


def upward_spread(values: Sequence[float]) -> float:
    """`(median - min) / min`: the slowdown a side already shows against itself.

    The median rather than the max, so one slow round does not set the floor.
    """
    fastest = min(values)
    if fastest <= 0:
        return 0.0

    return (median(values) - fastest) / fastest


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


@dataclass
class Side:
    """One side of the comparison: every round measured for it.

    A single summary file is one round; a directory is one round per file, in
    sorted order.
    """

    header: str
    rounds: List[BenchmarkRun]

    @classmethod
    def load(cls, path: Path) -> Side:
        if path.is_dir():
            paths = sorted(path.glob("*.json"))
            if not paths:
                raise SystemExit(f"no *.json summary files in {path}")
            # A directory is named after the side it holds ("base", "pr"),
            # a file is named after the run and lives in such a directory.
            header = path.name
        else:
            paths = [path]
            header = path.parent.name

        rounds = [BenchmarkRun.load_from_file(round_path) for round_path in paths]

        # Rounds are indexed by position below, so a round that measured a
        # different set of queries has to be caught rather than mispaired.
        queries = [query.query for query in rounds[0].queries]
        for round_path, round in zip(paths[1:], rounds[1:]):
            if [query.query for query in round.queries] != queries:
                raise SystemExit(
                    f"{round_path} measured different queries than {paths[0]}"
                )

        return cls(header, rounds)

    @property
    def queries(self) -> List[QueryRun]:
        """The first round's queries, for labels and ordering."""
        return self.rounds[0].queries

    def merged(self, index: int) -> QueryRun:
        """One query's rounds collapsed into a single run, for display."""
        runs = [round.queries[index] for round in self.rounds]

        return QueryRun(
            query=runs[0].query,
            iterations=[
                iteration for run in runs for iteration in run.iterations
            ],
            start_time=runs[0].start_time,
            success=all(run.success for run in runs),
        )

    def per_round(self, index: int) -> List[float]:
        """One query's fastest time in each round."""
        return [round.queries[index].min_execution_time for round in self.rounds]


@dataclass
class QueryComparison:
    """One query's verdict, and the numbers behind it."""

    label: str
    # comparison / baseline in each round, paired by round
    ratios: List[float]
    # median of `ratios`: the estimate the gate rules on
    ratio: float
    # how much the baseline varies against itself, as a relative slowdown
    noise: float
    # absolute regression in ms, on the per-round medians
    delta_ms: float
    # the limit asked for on the command line
    configured_limit: float
    # the limit the ratio was actually held to: the configured one, raised to
    # the noise floor when the baseline is too unstable to support it
    limit: float

    @property
    def regressed(self) -> bool:
        return self.ratio > self.limit

    @property
    def inconclusive(self) -> bool:
        """Above the configured limit, but not above this query's noise."""
        return not self.regressed and self.ratio > self.configured_limit

    def summary(self) -> str:
        spread = ""
        if len(self.ratios) > 1:
            spread = f" (rounds {min(self.ratios):.2f}-{max(self.ratios):.2f})"

        limit = f"limit {self.limit:.2f}x"
        if self.limit > self.configured_limit:
            limit += (
                f", raised from {self.configured_limit:.2f}x "
                f"by a {self.noise:.0%} noise floor"
            )

        return (
            f"{self.label} is {self.ratio:.2f}x slower{spread}, "
            f"+{self.delta_ms:.0f}ms ({limit})"
        )


def compare(
    baseline_path: Path,
    comparison_path: Path,
    noise_threshold: float,
    detailed: bool,
    fail_threshold: float | None = None,
    fail_total_threshold: float | None = None,
    min_delta_ms: float = 0.0,
) -> int:
    """Print the comparison and return the process exit code.

    The exit code is non-zero only when a `fail_*_threshold` is given and the
    comparison run is slower than it allows.
    """
    baseline = Side.load(baseline_path)
    comparison = Side.load(comparison_path)

    console = Console(width=200)

    rounds = min(len(baseline.rounds), len(comparison.rounds))
    if len(baseline.rounds) != len(comparison.rounds):
        console.print(
            f"[yellow]{baseline.header} has {len(baseline.rounds)} round(s) and "
            f"{comparison.header} has {len(comparison.rounds)}; comparing the "
            f"first {rounds} of each[/yellow]"
        )

    multi_round = rounds > 1

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Query", style="dim", no_wrap=True)
    table.add_column(baseline.header, justify="right", style="dim", no_wrap=True)
    table.add_column(comparison.header, justify="right", style="dim", no_wrap=True)
    table.add_column("Change", justify="right", style="dim", no_wrap=True)
    if multi_round:
        # What the gate rules on, next to the fastest-run ratio in `Change`.
        table.add_column("Per-round", justify="right", style="dim", no_wrap=True)
        table.add_column("Noise", justify="right", style="dim", no_wrap=True)

    faster_count = 0
    slower_count = 0
    no_change_count = 0
    failure_count = 0
    total_baseline_time = 0
    total_comparison_time = 0
    # Per-round totals, so the total is gated the same paired way a query is
    baseline_totals = [0.0] * rounds
    comparison_totals = [0.0] * rounds
    comparisons: List[QueryComparison] = []
    new_failures: List[str] = []

    for index, (baseline_result, comparison_result) in enumerate(
        zip(baseline.queries, comparison.queries)
    ):
        assert baseline_result.query == comparison_result.query

        baseline_merged = baseline.merged(index)
        comparison_merged = comparison.merged(index)

        base_failed = not baseline_merged.success
        comp_failed = not comparison_merged.success
        # If a query fails, its execution time is excluded from the performance comparison
        if base_failed or comp_failed:
            change_text = "incomparable"
            failure_count += 1
            if comp_failed and not base_failed:
                new_failures.append(baseline_merged.label)
            row = [
                baseline_merged.label,
                "FAIL" if base_failed else baseline_merged.execution_time_report(detailed)[1],
                "FAIL" if comp_failed else comparison_merged.execution_time_report(detailed)[1],
                change_text,
            ]
            table.add_row(*(row + ["", ""] if multi_round else row))
            continue

        baseline_value, baseline_text = baseline_merged.execution_time_report(detailed)
        comparison_value, comparison_text = comparison_merged.execution_time_report(detailed)

        total_baseline_time += baseline_value
        total_comparison_time += comparison_value

        baseline_rounds = baseline.per_round(index)[:rounds]
        comparison_rounds = comparison.per_round(index)[:rounds]
        for round_index in range(rounds):
            baseline_totals[round_index] += baseline_rounds[round_index]
            comparison_totals[round_index] += comparison_rounds[round_index]

        ratios = [
            comparison_round / baseline_round
            for baseline_round, comparison_round in zip(baseline_rounds, comparison_rounds)
        ]
        noise = upward_spread(baseline_rounds)
        configured_limit = fail_threshold if fail_threshold is not None else math.inf
        comparisons.append(
            QueryComparison(
                label=baseline_merged.label,
                ratios=ratios,
                ratio=median(ratios),
                noise=noise,
                delta_ms=median(comparison_rounds) - median(baseline_rounds),
                limit=max(configured_limit, 1.0 + noise),
                configured_limit=configured_limit,
            )
        )

        change = comparison_value / baseline_value

        if (1.0 - noise_threshold) <= change <= (1.0 + noise_threshold):
            change_text = "no change"
            no_change_count += 1
        elif change < 1.0:
            change_text = f"+{(1 / change):.2f}x faster"
            faster_count += 1
        else:
            change_text = f"{change:.2f}x slower"
            slower_count += 1

        row = [
            baseline_merged.label,
            baseline_text,
            comparison_text,
            change_text,
        ]
        if multi_round:
            row.append(f"{median(ratios):.2f}x ({min(ratios):.2f}-{max(ratios):.2f})")
            row.append(f"±{noise:.0%}")
        table.add_row(*row)

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

    total_ratios = [
        comparison_total / baseline_total
        for baseline_total, comparison_total in zip(baseline_totals, comparison_totals)
        if baseline_total
    ] or [1.0]
    total_noise = upward_spread(baseline_totals) if baseline_totals[0] else 0.0

    # Summary table
    summary_table = Table(show_header=True, header_style="bold magenta")
    summary_table.add_column("Benchmark Summary", justify="left", style="dim")
    summary_table.add_column("", justify="right", style="dim")

    summary_table.add_row("Rounds", str(rounds))
    summary_table.add_row(
        "Iterations per round",
        str(len(baseline.queries[0].iterations)) if baseline.queries else "0",
    )
    summary_table.add_row("CPUs", str(baseline.rounds[0].context.num_cpus))
    summary_table.add_row(f"Total Time ({baseline.header})", f"{total_baseline_time:.2f}ms")
    summary_table.add_row(f"Total Time ({comparison.header})", f"{total_comparison_time:.2f}ms")
    summary_table.add_row(f"Average Time ({baseline.header})", f"{avg_baseline_time:.2f}ms")
    summary_table.add_row(f"Average Time ({comparison.header})", f"{avg_comparison_time:.2f}ms")
    summary_table.add_row("Total Change", f"{total_change:.2f}x")
    if multi_round:
        summary_table.add_row("Total Change (per-round median)", f"{median(total_ratios):.2f}x")
        summary_table.add_row("Geometric Mean of Query Ratios", f"{geometric_mean([c.ratio for c in comparisons]):.2f}x" if comparisons else "n/a")
        summary_table.add_row(
            f"Noise Floor ({baseline.header}, median / worst query)",
            f"{median([c.noise for c in comparisons]):.1%} / {max([c.noise for c in comparisons]):.1%}"
            if comparisons
            else "n/a",
        )
    summary_table.add_row("Queries Faster", str(faster_count))
    summary_table.add_row("Queries Slower", str(slower_count))
    summary_table.add_row("Queries with No Change", str(no_change_count))
    summary_table.add_row("Queries with Failure", str(failure_count))

    console.print(summary_table)

    return report_regressions(
        console,
        baseline.header,
        comparison.header,
        comparisons,
        new_failures,
        median(total_ratios),
        total_noise,
        fail_threshold,
        fail_total_threshold,
        min_delta_ms,
    )


def report_regressions(
    console: Console,
    baseline_header: str,
    comparison_header: str,
    comparisons: List[QueryComparison],
    new_failures: List[str],
    total_ratio: float,
    total_noise: float,
    fail_threshold: float | None,
    fail_total_threshold: float | None,
    min_delta_ms: float,
) -> int:
    """Report against the configured limits, returning the process exit code.

    A query fails only when its slowdown clears three bars: the configured
    limit, a minimum absolute cost in milliseconds, and the noise floor the
    baseline showed against itself. Clearing the first but not the others is
    reported as inconclusive -- a fact about the run, not about the change.
    """
    if fail_threshold is None and fail_total_threshold is None:
        return 0

    problems: List[str] = []
    notes: List[str] = []

    if fail_threshold is not None:
        for query in comparisons:
            if not query.regressed:
                if query.inconclusive:
                    notes.append(
                        f"{query.label} is {query.ratio:.2f}x slower, within the "
                        f"{query.noise:.0%} spread {baseline_header} showed against "
                        f"itself -- too noisy to call"
                    )
                continue

            if query.delta_ms < min_delta_ms:
                notes.append(
                    f"{query.label} is {query.ratio:.2f}x slower, but only "
                    f"+{query.delta_ms:.0f}ms (below the {min_delta_ms:.0f}ms floor)"
                )
                continue

            problems.append(query.summary())

    if fail_total_threshold is not None:
        total_limit = max(fail_total_threshold, 1.0 + total_noise)
        if total_ratio > total_limit:
            problems.append(
                f"total time is {total_ratio:.2f}x slower (limit {total_limit:.2f}x)"
            )
        elif total_ratio > fail_total_threshold:
            notes.append(
                f"total time is {total_ratio:.2f}x slower, within the "
                f"{total_noise:.0%} spread {baseline_header} showed against itself"
            )

    # A query that only fails on one side is excluded from the timings above,
    # so it would otherwise pass the gate unnoticed.
    problems.extend(
        f"{label} failed in {comparison_header} but not in {baseline_header}"
        for label in new_failures
    )

    if notes:
        console.print("Not counted against the gate:")
        for note in notes:
            console.print(f"  - {note}", markup=False)

    if not problems:
        console.print(
            f"No regression: {comparison_header} is within the configured limits "
            f"of {baseline_header}."
        )
        return 0

    console.print(f"Regression: {comparison_header} is slower than {baseline_header}")
    for problem in problems:
        console.print(f"  - {problem}", markup=False)

    return 1


def main() -> None:
    parser = ArgumentParser()
    compare_parser = parser
    compare_parser.add_argument(
        "baseline_path",
        type=Path,
        help="Path to the baseline summary file, or to a directory holding one "
        "summary file per round.",
    )
    compare_parser.add_argument(
        "comparison_path",
        type=Path,
        help="Path to the comparison summary file, or to a directory holding "
        "one summary file per round. Rounds are paired with the baseline's in "
        "sorted filename order.",
    )
    compare_parser.add_argument(
        "--noise-threshold",
        type=float,
        default=0.05,
        help="The threshold for statistically insignificant results (+/- 5%%).",
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
        "--fail-min-delta-ms",
        type=float,
        default=0.0,
        help="Never fail on a query that got less than this many milliseconds "
        "slower, however large the ratio. Keeps short queries, where a large "
        "relative change is a small absolute one, out of the gate.",
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
            options.fail_min_delta_ms,
        )
    )



if __name__ == "__main__":
    main()
