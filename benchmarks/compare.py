#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Any
from pathlib import Path
from argparse import ArgumentParser

try:
    from rich.console import Console
    from rich.table import Table
except ImportError:
    print("Try `pip install rich` for using this script.")
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
    query: int
    iterations: List[QueryResult]
    start_time: int

    @classmethod
    def load_from(cls, data: Dict[str, Any]) -> QueryRun:
        return cls(
            query=data["query"],
            iterations=[QueryResult(**iteration) for iteration in data["iterations"]],
            start_time=data["start_time"],
        )

    @property
    def execution_time(self) -> float:
        assert len(self.iterations) >= 1

        # Use minimum execution time to account for variations / other
        # things the system was doing
        return min(iteration.elapsed for iteration in self.iterations)


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
) -> None:
    baseline = BenchmarkRun.load_from_file(baseline_path)

    comparison = BenchmarkRun.load_from_file(comparison_path)

    console = Console()

    # use basename as the column names
    baseline_header = baseline_path.parent.stem
    comparison_header = comparison_path.parent.stem

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Query", style="dim", width=12)
    table.add_column(baseline_header, justify="right", style="dim")
    table.add_column(comparison_header, justify="right", style="dim")
    table.add_column("Change", justify="right", style="dim")

    for baseline_result, comparison_result in zip(baseline.queries, comparison.queries):
        assert baseline_result.query == comparison_result.query

        change = comparison_result.execution_time / baseline_result.execution_time

        if (1.0 - noise_threshold) <= change <= (1.0 + noise_threshold):
            change = "no change"
        elif change < 1.0:
            change = f"+{(1 / change):.2f}x faster"
        else:
            change = f"{change:.2f}x slower"

        table.add_row(
            f"Q{baseline_result.query}",
            f"{baseline_result.execution_time:.2f}ms",
            f"{comparison_result.execution_time:.2f}ms",
            change,
        )

    console.print(table)


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

    options = parser.parse_args()

    compare(options.baseline_path, options.comparison_path, options.noise_threshold)



if __name__ == "__main__":
    main()
