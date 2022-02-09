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

import collections
import csv
import os
import pathlib
import subprocess

import conbench.runner
from conbench.machine_info import github_info


def _result_in_seconds(row):
    # sample_measured_value - The value of the measurement for this sample.
    # Note that this is the measured value for the whole sample, not the
    # time-per-iteration To calculate the time-per-iteration, use
    # sample_measured_value/iteration_count
    # -- https://bheisler.github.io/criterion.rs/book/user_guide/csv_output.html
    count = int(row["iteration_count"])
    sample = float(row["sample_measured_value"])
    return sample / count / 10**9


def _parse_benchmark_group(row):
    parts = row["group"].split(",")
    if len(parts) > 1:
        suite, name = parts[0], ",".join(parts[1:])
    else:
        suite, name = row["group"], row["group"]
    return suite.strip(), name.strip()


def _read_results(src_dir):
    results = collections.defaultdict(lambda: collections.defaultdict(list))
    path = pathlib.Path(os.path.join(src_dir, "target", "criterion"))
    for path in list(path.glob("**/new/raw.csv")):
        with open(path) as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                suite, name = _parse_benchmark_group(row)
                results[suite][name].append(_result_in_seconds(row))
    return results


def _execute_command(command):
    try:
        print(command)
        result = subprocess.run(command, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        print(e.stderr.decode("utf-8"))
        raise e
    return result.stdout.decode("utf-8"), result.stderr.decode("utf-8")


class CriterionBenchmark(conbench.runner.Benchmark):
    external = True

    def run(self, **kwargs):
        src_dir = os.path.join(os.getcwd(), "..")
        self._cargo_bench(src_dir)
        results = _read_results(src_dir)
        for suite in results:
            self.conbench.mark_new_batch()
            for name, data in results[suite].items():
                yield self._record_result(suite, name, data, kwargs)

    def _cargo_bench(self, src_dir):
        os.chdir(src_dir)
        _execute_command(["cargo", "bench"])

    def _record_result(self, suite, name, data, options):
        tags = {"suite": suite}
        result = {"data": data, "unit": "s"}
        context = {"benchmark_language": "Rust"}
        github = github_info()
        return self.conbench.record(
            result,
            name,
            tags=tags,
            context=context,
            github=github,
            options=options,
        )
