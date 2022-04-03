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

import conbench.runner

import _criterion


@conbench.runner.register_benchmark
class TestBenchmark(conbench.runner.Benchmark):
    name = "test"

    def run(self, **kwargs):
        yield self.conbench.benchmark(
            self._f(),
            self.name,
            options=kwargs,
        )

    def _f(self):
        return lambda: 1 + 1


@conbench.runner.register_benchmark
class CargoBenchmarks(_criterion.CriterionBenchmark):
    name = "datafusion"
    description = "Run Arrow DataFusion micro benchmarks."
