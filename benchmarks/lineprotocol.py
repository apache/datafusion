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


""" 
Converts a given json to LineProtocol format that can be 
visualised by grafana/other systems that support LineProtocol. 

Usage example: 
$ python3 lineprotocol.py sort.json 
benchmark,name=sort,version=28.0.0,datafusion_version=28.0.0,num_cpus=8 query="sort utf8",iteration=0,row_count=10838832,elapsed_ms=85626006 1691105678000000000
benchmark,name=sort,version=28.0.0,datafusion_version=28.0.0,num_cpus=8 query="sort utf8",iteration=1,row_count=10838832,elapsed_ms=68694468 1691105678000000000
benchmark,name=sort,version=28.0.0,datafusion_version=28.0.0,num_cpus=8 query="sort utf8",iteration=2,row_count=10838832,elapsed_ms=63392883 1691105678000000000
benchmark,name=sort,version=28.0.0,datafusion_version=28.0.0,num_cpus=8 query="sort utf8",iteration=3,row_count=10838832,elapsed_ms=66388367 1691105678000000000
"""

# sort.json
"""
{
  "queries": [
    {
      "iterations": [
        {
          "elapsed": 85626.006132,
          "row_count": 10838832
        },
        {
          "elapsed": 68694.467851,
          "row_count": 10838832
        },
        {
          "elapsed": 63392.883406,
          "row_count": 10838832
        },
        {
          "elapsed": 66388.367387,
          "row_count": 10838832
        },
      ],
      "query": "sort utf8",
      "start_time": 1691105678
    },
  ],
  "context": {
    "arguments": [
      "sort",
      "--path",
      "benchmarks/data",
      "--scale-factor",
      "1.0",
      "--iterations",
      "4",
      "-o",
      "sort.json"
    ],
    "benchmark_version": "28.0.0",
    "datafusion_version": "28.0.0",
    "num_cpus": 8,
    "start_time": 1691105678
  }
}
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Any
from pathlib import Path
from argparse import ArgumentParser
import sys
print = sys.stdout.write


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
    name: str

    @classmethod
    def load_from(cls, data: Dict[str, Any]) -> Context:
        return cls(
            benchmark_version=data["benchmark_version"],
            datafusion_version=data["datafusion_version"],
            num_cpus=data["num_cpus"],
            start_time=data["start_time"],
            arguments=data["arguments"],
            name=data["arguments"][0]
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


def lineformat(
    baseline: Path,
) -> None:
    baseline = BenchmarkRun.load_from_file(baseline)
    context = baseline.context
    benchamrk_str = f"benchmark,name={context.name},version={context.benchmark_version},datafusion_version={context.datafusion_version},num_cpus={context.num_cpus}"
    for query in baseline.queries:
        query_str = f"query=\"{query.query}\""
        timestamp = f"{query.start_time*10**9}"
        for iter_num, result in enumerate(query.iterations):
            print(f"{benchamrk_str} {query_str},iteration={iter_num},row_count={result.row_count},elapsed_ms={result.elapsed*1000:.0f} {timestamp}\n")
    
def main() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "path",
        type=Path,
        help="Path to the benchmark file.",
    )
    options = parser.parse_args()

    lineformat(options.baseline_path)



if __name__ == "__main__":
    main()
